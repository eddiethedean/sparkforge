"""
Supply Chain & Logistics Pipeline Tests

This module tests a realistic supply chain and logistics pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with orders, shipments, inventory,
and logistics performance metrics.
"""

import os
import tempfile
from uuid import uuid4

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark-specific tests require SPARK_MODE=real"
    )

from pyspark.sql import functions as F

from pipeline_builder.models import ParallelConfig
from pipeline_builder.pipeline import PipelineBuilder
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


class TestSupplyChainPipeline:
    """Test supply chain and logistics pipeline with bronze-silver-gold architecture."""

    @pytest.mark.sequential
    def test_complete_supply_chain_pipeline_execution(
        self, spark_session, data_generator, test_assertions
    ):
        """Test complete supply chain pipeline: orders → shipments → logistics insights."""

        # Create realistic supply chain data
        orders_df = data_generator.create_supply_chain_orders(
            spark_session, num_orders=80
        )
        shipments_df = data_generator.create_supply_chain_shipments(
            spark_session, num_shipments=100
        )
        inventory_df = data_generator.create_supply_chain_inventory(
            spark_session, num_items=150
        )
        # Use get_unique_schema for proper concurrent testing isolation (includes worker ID)
        unique_schema = get_unique_schema("bronze")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {unique_schema}")

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )
        builder.config.parallel = ParallelConfig.create_sequential()

        # Bronze Layer: Raw supply chain data validation
        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "order_id": ["not_null"],
                "customer_id": ["not_null"],
                "order_date": ["not_null"],
                "quantity": [["gte", 0]],
            },
            incremental_col="order_date",
        )

        builder.with_bronze_rules(
            name="raw_shipments",
            rules={
                "shipment_id": ["not_null"],
                "order_id": ["not_null"],
                "shipping_date": ["not_null"],
            },
            incremental_col="shipping_date",
        )

        builder.with_bronze_rules(
            name="raw_inventory",
            rules={
                "inventory_id": ["not_null"],
                "product_id": ["not_null"],
                "warehouse_id": ["not_null"],
                "quantity_on_hand": [["gte", 0]],
            },
            incremental_col="snapshot_date",
        )

        # Silver Layer: Processed logistics data
        def processed_orders_transform(spark, df, silvers):
            """Process and enrich order data."""
            return (
                df.withColumn(
                    "order_date_parsed",
                    F.to_timestamp(
                        F.col("order_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "total_value",
                    F.col("quantity") * F.col("unit_price"),
                )
                .withColumn(
                    "is_high_priority",
                    F.when(F.col("priority") == "high", True).otherwise(False),
                )
                .select(
                    "order_id",
                    "customer_id",
                    "product_id",
                    "order_date_parsed",
                    "quantity",
                    "unit_price",
                    "total_value",
                    "warehouse_id",
                    "destination_city",
                    "priority",
                    "is_high_priority",
                )
            )

        builder.add_silver_transform(
            name="processed_orders",
            source_bronze="raw_orders",
            transform=processed_orders_transform,
            rules={
                "order_id": ["not_null"],
                "order_date_parsed": ["not_null"],
                "total_value": [["gte", 0]],
                "warehouse_id": ["not_null"],
                "customer_id": ["not_null"],
                "product_id": ["not_null"],
                "quantity": [["gte", 0]],
            },
            table_name="processed_orders",
        )

        def processed_shipments_transform(spark, df, silvers):
            """Process and enrich shipment data."""
            return (
                df.withColumn(
                    "shipping_date_parsed",
                    F.to_timestamp(
                        F.col("shipping_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "delivery_date_parsed",
                    F.to_timestamp(
                        F.col("delivery_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "days_to_deliver",
                    F.when(
                        F.col("delivery_date_parsed").isNotNull(),
                        F.datediff(
                            F.col("delivery_date_parsed"), F.col("shipping_date_parsed")
                        ),
                    ).otherwise(None),
                )
                .withColumn(
                    "is_delivered",
                    F.when(F.col("status") == "delivered", True).otherwise(False),
                )
                .withColumn(
                    "is_delayed",
                    F.when(F.col("status") == "delayed", True).otherwise(False),
                )
                .select(
                    "shipment_id",
                    "order_id",
                    "shipping_date_parsed",
                    "delivery_date_parsed",
                    "days_to_deliver",
                    "carrier",
                    "tracking_number",
                    "status",
                    "is_delivered",
                    "is_delayed",
                    "cost",
                )
            )

        builder.add_silver_transform(
            name="processed_shipments",
            source_bronze="raw_shipments",
            transform=processed_shipments_transform,
            rules={
                "shipment_id": ["not_null"],
                "order_id": ["not_null"],
                "shipping_date_parsed": ["not_null"],
                "carrier": ["not_null"],
                "is_delivered": ["not_null"],
                "is_delayed": ["not_null"],
                "cost": [["gte", 0]],
                "days_to_deliver": ["not_null"],
            },
            table_name="processed_shipments",
        )

        def processed_inventory_transform(spark, df, silvers):
            """Process and enrich inventory data."""
            return (
                df.withColumn(
                    "snapshot_date_parsed",
                    F.to_timestamp(
                        F.col("snapshot_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "available_quantity",
                    F.col("quantity_on_hand") - F.col("quantity_reserved"),
                )
                .withColumn(
                    "is_low_stock",
                    F.when(
                        F.col("available_quantity") < F.col("reorder_point"), True
                    ).otherwise(False),
                )
                .withColumn(
                    "is_overstocked",
                    F.when(
                        F.col("quantity_on_hand") > F.col("max_stock"), True
                    ).otherwise(False),
                )
                .withColumn(
                    "stock_level",
                    F.when(F.col("is_low_stock"), "low")
                    .when(F.col("is_overstocked"), "high")
                    .otherwise("normal"),
                )
                .select(
                    "inventory_id",
                    "product_id",
                    "warehouse_id",
                    "snapshot_date_parsed",
                    "quantity_on_hand",
                    "quantity_reserved",
                    "available_quantity",
                    "reorder_point",
                    "max_stock",
                    "is_low_stock",
                    "is_overstocked",
                    "stock_level",
                )
            )

        builder.add_silver_transform(
            name="processed_inventory",
            source_bronze="raw_inventory",
            transform=processed_inventory_transform,
            rules={
                "product_id": ["not_null"],
                "warehouse_id": ["not_null"],
                "snapshot_date_parsed": ["not_null"],
                "available_quantity": [["gte", 0]],
                "stock_level": ["not_null"],
                "quantity_on_hand": [["gte", 0]],
                "is_low_stock": ["not_null"],
            },
            table_name="processed_inventory",
        )

        # Gold Layer: Logistics performance metrics
        def delivery_performance_transform(spark, silvers):
            """Calculate delivery performance metrics."""
            processed_orders = silvers.get("processed_orders")
            processed_shipments = silvers.get("processed_shipments")

            if processed_orders is None or processed_shipments is None:
                return spark.createDataFrame(
                    [],
                    [
                        "warehouse_id",
                        "total_orders",
                        "total_shipments",
                        "delivered_count",
                        "delayed_count",
                        "on_time_rate",
                        "avg_days_to_deliver",
                        "total_shipping_cost",
                    ],
                )

            # Join orders with shipments
            order_shipment = processed_orders.join(
                processed_shipments, "order_id", "left"
            )

            # Calculate warehouse-level metrics
            warehouse_metrics = (
                order_shipment.groupBy("warehouse_id")
                .agg(
                    F.countDistinct("order_id").alias("total_orders"),
                    F.countDistinct("shipment_id").alias("total_shipments"),
                    F.sum(F.when(F.col("is_delivered"), 1).otherwise(0)).alias(
                        "delivered_count"
                    ),
                    F.sum(F.when(F.col("is_delayed"), 1).otherwise(0)).alias(
                        "delayed_count"
                    ),
                    F.avg("days_to_deliver").alias("avg_days_to_deliver"),
                    F.sum("cost").alias("total_shipping_cost"),
                )
                .withColumn(
                    "on_time_rate",
                    F.when(
                        F.col("total_shipments") > 0,
                        (F.col("total_shipments") - F.col("delayed_count"))
                        / F.col("total_shipments")
                        * 100,
                    ).otherwise(0),
                )
                .select(
                    "warehouse_id",
                    "total_orders",
                    "total_shipments",
                    "delivered_count",
                    "delayed_count",
                    "on_time_rate",
                    "avg_days_to_deliver",
                    "total_shipping_cost",
                )
            )

            return warehouse_metrics

        builder.add_gold_transform(
            name="delivery_performance",
            transform=delivery_performance_transform,
            rules={
                "warehouse_id": ["not_null"],
                "on_time_rate": [["gte", 0]],
            },
            table_name="delivery_performance",
            source_silvers=["processed_orders", "processed_shipments"],
        )

        def inventory_turnover_transform(spark, silvers):
            """Calculate inventory turnover and stock levels."""
            processed_orders = silvers.get("processed_orders")
            processed_inventory = silvers.get("processed_inventory")

            if processed_orders is None or processed_inventory is None:
                return spark.createDataFrame(
                    [],
                    [
                        "warehouse_id",
                        "product_id",
                        "total_orders",
                        "total_quantity_ordered",
                        "avg_inventory_level",
                        "inventory_turnover",
                        "low_stock_indicators",
                    ],
                )

            # Aggregate orders by warehouse and product
            order_metrics = processed_orders.groupBy("warehouse_id", "product_id").agg(
                F.count("order_id").alias("total_orders"),
                F.sum("quantity").alias("total_quantity_ordered"),
            )

            # Aggregate inventory by warehouse and product
            inventory_metrics = processed_inventory.groupBy(
                "warehouse_id", "product_id"
            ).agg(
                F.avg("quantity_on_hand").alias("avg_inventory_level"),
                F.sum(F.when(F.col("is_low_stock"), 1).otherwise(0)).alias(
                    "low_stock_indicators"
                ),
            )

            # Calculate inventory turnover
            turnover = (
                order_metrics.join(
                    inventory_metrics, ["warehouse_id", "product_id"], "outer"
                )
                .withColumn(
                    "inventory_turnover",
                    F.when(
                        F.col("avg_inventory_level") > 0,
                        F.col("total_quantity_ordered") / F.col("avg_inventory_level"),
                    ).otherwise(0),
                )
                .select(
                    "warehouse_id",
                    "product_id",
                    "total_orders",
                    "total_quantity_ordered",
                    "avg_inventory_level",
                    "inventory_turnover",
                    "low_stock_indicators",
                )
            )

            return turnover

        builder.add_gold_transform(
            name="inventory_turnover",
            transform=inventory_turnover_transform,
            rules={
                "warehouse_id": ["not_null"],
                "product_id": ["not_null"],
            },
            table_name="inventory_turnover",
            source_silvers=["processed_orders", "processed_inventory"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_orders": orders_df,
                "raw_shipments": shipments_df,
                "raw_inventory": inventory_df,
            }
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify data quality
        assert result.status.value == "completed" or result.success
        assert "delivery_performance" in result.gold_results
        assert "inventory_turnover" in result.gold_results

        # Verify gold layer outputs
        delivery_result = result.gold_results["delivery_performance"]
        assert delivery_result.get("rows_processed", 0) > 0

        turnover_result = result.gold_results["inventory_turnover"]
        assert turnover_result.get("rows_processed", 0) >= 0  # Can be 0 if no matches

        # Cleanup: drop schema created for this test
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, unique_schema)
        except Exception:
            pass  # Ignore cleanup errors

    def test_incremental_supply_chain_processing(
        self, spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new supply chain data."""
        # Create initial data
        orders_initial = data_generator.create_supply_chain_orders(
            spark_session, num_orders=30
        )
        # Use get_unique_schema for proper concurrent testing isolation (includes worker ID)
        unique_schema = get_unique_schema("bronze")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {unique_schema}")

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
        )
        builder.config.parallel = ParallelConfig.create_sequential()

        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "order_id": ["not_null"],
                "customer_id": ["not_null"],
            },
            incremental_col="order_date",
        )

        def processed_orders_transform(spark, df, silvers):
            return df.withColumn(
                "order_date_parsed",
                F.to_timestamp(
                    F.col("order_date").cast("string"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="processed_orders",
            source_bronze="raw_orders",
            transform=processed_orders_transform,
            rules={"order_id": ["not_null"]},
            table_name="processed_orders",
        )

        pipeline = builder.to_pipeline()

        # Initial load
        result1 = pipeline.run_initial_load(
            bronze_sources={"raw_orders": orders_initial}
        )

        test_assertions.assert_pipeline_success(result1)

        # Incremental load with new orders
        orders_incremental = data_generator.create_supply_chain_orders(
            spark_session, num_orders=20
        )

        result2 = pipeline.run_incremental(
            bronze_sources={"raw_orders": orders_incremental}
        )

        test_assertions.assert_pipeline_success(result2)
        assert result2.mode.value == "incremental"

    @pytest.mark.pyspark
    @pytest.mark.sequential
    def test_supply_chain_logging(self, spark_session, data_generator, test_assertions):
        """Test comprehensive logging for supply chain pipeline."""
        # Skip if in mock mode (requires real PySpark with Delta Lake)
        if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
            pytest.skip("Requires real PySpark with Delta Lake")
        from pipeline_builder.writer import LogWriter

        # Create test data
        orders_df = data_generator.create_supply_chain_orders(
            spark_session, num_orders=25
        )
        # Use get_unique_schema for proper concurrent testing isolation (includes worker ID)
        unique_schema = get_unique_schema("bronze")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {unique_schema}")

        # Create unique schema for this test
        analytics_schema = get_unique_schema("analytics")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {analytics_schema}")

        # Create LogWriter
        log_writer = LogWriter(
            spark=spark_session,
            schema=analytics_schema,
            table_name="supply_chain_logs",
        )

        # Create pipeline
        builder = PipelineBuilder(
            spark=spark_session, schema=unique_schema, verbose=False
        )
        builder.config.parallel = ParallelConfig.create_sequential()

        builder.with_bronze_rules(
            name="raw_orders",
            rules={"order_id": ["not_null"]},
            incremental_col="order_date",
        )

        def processed_orders_transform(spark, df, silvers):
            return df.withColumn(
                "order_date_parsed",
                F.to_timestamp(
                    F.col("order_date").cast("string"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="processed_orders",
            source_bronze="raw_orders",
            transform=processed_orders_transform,
            rules={"order_id": ["not_null"]},
            table_name="processed_orders",
        )

        pipeline = builder.to_pipeline()

        # Execute pipeline
        result = pipeline.run_initial_load(bronze_sources={"raw_orders": orders_df})

        # Log execution results
        log_result = log_writer.append(result)

        # Verify logging was successful
        test_assertions.assert_pipeline_success(result)
        assert log_result is not None
        assert log_result.get("success") is True

        # Cleanup: drop schema created for this test
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, analytics_schema)
        except Exception:
            pass  # Ignore cleanup errors
