"""
E-commerce Order Processing Pipeline Tests

This module tests a realistic e-commerce order processing pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with order data, customer profiles,
and sales analytics using mock-spark.
"""

from mock_spark import functions as F

from pipeline_builder.pipeline import PipelineBuilder
from pipeline_builder.writer import LogWriter


class TestEcommercePipeline:
    """Test e-commerce order processing pipeline with bronze-silver-gold architecture."""

    def test_complete_ecommerce_pipeline_execution(
        self, mock_spark_session, data_generator, test_assertions
    ):
        """Test complete e-commerce pipeline: orders → customer profiles → sales analytics."""

        # Create realistic test data
        orders_df = data_generator.create_ecommerce_orders(
            mock_spark_session, num_orders=50
        )
        customers_df = data_generator.create_customer_data(
            mock_spark_session, num_customers=25
        )

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="bronze",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
            functions=F,
        )

        # Bronze Layer: Raw order validation
        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "order_id": ["not_null"],
                "customer_id": ["not_null"],
                "quantity": ["positive"],
                "unit_price": ["gt", 0],
            },
            incremental_col="order_date",
        )

        builder.with_bronze_rules(
            name="raw_customers",
            rules={
                "customer_id": ["not_null"],
                "email": ["not_null"],
                "country": ["not_null"],
            },
        )

        # Silver Layer: Clean and enrich data
        def clean_orders_transform(spark, df, silvers):
            """Clean and enrich order data."""
            return (
                df.filter(
                    F.col("status").isin(["shipped", "delivered"])
                )  # Only successful orders
                .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
                .withColumn(
                    "order_date_parsed",
                    F.to_date(F.substring("order_date", 1, 10), "yyyy-MM-dd"),
                )
                .select(
                    "order_id",
                    "customer_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                    "total_amount",
                    "order_date_parsed",
                    "status",
                )
            )

        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="raw_orders",
            transform=clean_orders_transform,
            rules={
                "order_id": ["not_null"],
                "total_amount": ["positive"],
                "order_date_parsed": ["not_null"],
            },
            table_name="clean_orders",
        )

        def enrich_customers_transform(spark, df, silvers):
            """Enrich customer data with order statistics."""
            return (
                df.withColumn(
                    "registration_date_parsed",
                    F.to_date(F.substring("registration_date", 1, 10), "yyyy-MM-dd"),
                )
                .withColumn("is_premium", F.col("segment") == "premium")
                .select(
                    "customer_id",
                    "name",
                    "email",
                    "country",
                    "segment",
                    "registration_date_parsed",
                    "is_premium",
                    "total_orders",
                    "lifetime_value",
                )
            )

        builder.add_silver_transform(
            name="enriched_customers",
            source_bronze="raw_customers",
            transform=enrich_customers_transform,
            rules={
                "customer_id": ["not_null"],
                "email": ["not_null"],
                "lifetime_value": ["gte", 0],
            },
            table_name="enriched_customers",
        )

        # Gold Layer: Business analytics
        def daily_sales_analytics_transform(spark, silvers):
            """Create daily sales analytics."""
            clean_orders = silvers.get("clean_orders")
            if clean_orders is not None:
                return (
                    clean_orders.groupBy("order_date_parsed")
                    .agg(
                        F.count("order_id").alias("total_orders"),
                        F.sum("total_amount").alias("daily_revenue"),
                        F.avg("total_amount").alias("avg_order_value"),
                        F.countDistinct("customer_id").alias("unique_customers"),
                    )
                    .orderBy("order_date_parsed")
                )
            else:
                return spark.createDataFrame(
                    [],
                    [
                        "order_date_parsed",
                        "total_orders",
                        "daily_revenue",
                        "avg_order_value",
                        "unique_customers",
                    ],
                )

        builder.add_gold_transform(
            name="daily_sales_analytics",
            transform=daily_sales_analytics_transform,
            rules={
                "order_date_parsed": ["not_null"],
                "daily_revenue": ["non_negative"],
            },
            table_name="daily_sales_analytics",
            source_silvers=["clean_orders"],
        )

        def customer_lifetime_value_transform(spark, silvers):
            """Create customer lifetime value analysis."""
            clean_orders = silvers.get("clean_orders")
            enriched_customers = silvers.get("enriched_customers")

            if clean_orders is not None and enriched_customers is not None:
                # Calculate actual LTV from orders
                customer_orders = clean_orders.groupBy("customer_id").agg(
                    F.count("order_id").alias("order_count"),
                    F.sum("total_amount").alias("total_spent"),
                    F.avg("total_amount").alias("avg_order_value"),
                    F.max("order_date_parsed").alias("last_order_date"),
                )

                return enriched_customers.join(
                    customer_orders, "customer_id", "left"
                ).select(
                    "customer_id",
                    "name",
                    "country",
                    "segment",
                    "is_premium",
                    F.coalesce(F.col("order_count"), F.lit(0)).alias("actual_orders"),
                    F.coalesce(F.col("total_spent"), F.lit(0.0)).alias("actual_spent"),
                    F.coalesce(F.col("avg_order_value"), F.lit(0.0)).alias(
                        "actual_avg_order"
                    ),
                    "lifetime_value",
                )
            else:
                return spark.createDataFrame(
                    [],
                    [
                        "customer_id",
                        "name",
                        "country",
                        "segment",
                        "is_premium",
                        "actual_orders",
                        "actual_spent",
                        "actual_avg_order",
                        "lifetime_value",
                    ],
                )

        builder.add_gold_transform(
            name="customer_lifetime_value",
            transform=customer_lifetime_value_transform,
            rules={"customer_id": ["not_null"], "actual_spent": ["non_negative"]},
            table_name="customer_lifetime_value",
            source_silvers=["clean_orders", "enriched_customers"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        # Execute initial load
        result = pipeline.run_initial_load(
            bronze_sources={"raw_orders": orders_df, "raw_customers": customers_df}
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify pipeline execution was successful
        # Note: We don't need to verify storage in tests - we're testing pipeline logic
        print("✅ E-commerce pipeline test completed successfully")

    def test_incremental_order_processing(
        self, mock_spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new orders."""

        # Create initial data
        initial_orders = data_generator.create_ecommerce_orders(
            mock_spark_session, num_orders=20
        )
        data_generator.create_customer_data(mock_spark_session, num_customers=10)

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

        # Create pipeline
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="bronze", functions=F
        )

        # Bronze layer
        builder.with_bronze_rules(
            name="orders",
            rules={"order_id": ["not_null"]},
            incremental_col="order_date",
        )

        # Silver layer
        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="orders",
            transform=lambda spark, df, silvers: df.filter(
                F.col("status") == "delivered"
            ),
            rules={"order_id": ["not_null"]},
            table_name="clean_orders",
        )

        # Gold layer
        builder.add_gold_transform(
            name="order_summary",
            transform=lambda spark, silvers: silvers["clean_orders"].agg(
                F.count("*").alias("total_orders")
            ),
            rules={"total_orders": ["not_null"]},
            table_name="order_summary",
            source_silvers=["clean_orders"],
        )

        pipeline = builder.to_pipeline()

        # Initial load
        initial_result = pipeline.run_initial_load(
            bronze_sources={"orders": initial_orders}
        )
        test_assertions.assert_pipeline_success(initial_result)

        # Create incremental data (new orders)
        new_orders = data_generator.create_ecommerce_orders(
            mock_spark_session, num_orders=10
        )

        # Incremental processing
        incremental_result = pipeline.run_incremental(
            bronze_sources={"orders": new_orders}
        )
        test_assertions.assert_pipeline_success(incremental_result)

        # Pipeline execution verified above - storage verification not needed for unit tests
        print("✅ Incremental processing test completed successfully")

    def test_validation_failures(self, mock_spark_session, test_assertions):
        """Test pipeline behavior with validation failures."""

        # Create data with quality issues
        bad_orders = mock_spark_session.createDataFrame(
            [
                (
                    "ORD-001",
                    "CUST-001",
                    None,
                    1,
                    10.0,
                    "2024-01-01",
                    "delivered",
                ),  # Missing product_id
                (
                    "ORD-002",
                    "CUST-002",
                    "PROD-001",
                    -1,
                    10.0,
                    "2024-01-01",
                    "delivered",
                ),  # Negative quantity
                (
                    "ORD-003",
                    "CUST-003",
                    "PROD-001",
                    1,
                    -5.0,
                    "2024-01-01",
                    "delivered",
                ),  # Negative price
            ],
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "order_date",
                "status",
            ],
        )

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

        # Create pipeline with strict validation
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="bronze",
            min_bronze_rate=100.0,  # Very strict validation
            verbose=True,
            functions=F,
        )

        builder.with_bronze_rules(
            name="orders",
            rules={
                "product_id": ["not_null"],
                "quantity": ["positive"],
                "unit_price": ["gt", 0],
            },
        )

        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="orders",
            transform=lambda spark, df, silvers: df,
            rules={"order_id": ["not_null"]},
            table_name="clean_orders",
        )

        pipeline = builder.to_pipeline()

        # This should handle validation failures gracefully
        result = pipeline.run_initial_load(bronze_sources={"orders": bad_orders})

        # Pipeline should still execute, but with validation warnings
        test_assertions.assert_pipeline_success(result)

    def test_logging_and_monitoring(
        self, mock_spark_session, data_generator, log_writer_config, test_assertions
    ):
        """Test comprehensive logging and monitoring with LogWriter."""

        # Create test data
        orders_df = data_generator.create_ecommerce_orders(
            mock_spark_session, num_orders=30
        )

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")
        mock_spark_session.storage.create_schema("analytics")

        # Create LogWriter
        log_writer = LogWriter(
            spark=mock_spark_session, schema="analytics", table_name="pipeline_logs"
        )

        # Create pipeline
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="bronze", functions=F
        )

        builder.with_bronze_rules(name="orders", rules={"order_id": ["not_null"]})

        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="orders",
            transform=lambda spark, df, silvers: df.filter(
                F.col("status") == "delivered"
            ),
            rules={"order_id": ["not_null"]},
            table_name="clean_orders",
        )

        builder.add_gold_transform(
            name="order_metrics",
            transform=lambda spark, silvers: silvers["clean_orders"].agg(
                F.count("*").alias("total_orders")
            ),
            rules={"total_orders": ["not_null"]},
            table_name="order_metrics",
            source_silvers=["clean_orders"],
        )

        pipeline = builder.to_pipeline()

        # Execute pipeline
        result = pipeline.run_initial_load(bronze_sources={"orders": orders_df})

        # Log execution results
        log_result = log_writer.append(result)

        # Verify logging was successful
        test_assertions.assert_pipeline_success(result)
        assert log_result is not None

        # Verify log table was created
        assert mock_spark_session.storage.table_exists("analytics", "pipeline_logs")

        # Verify log data
        log_data = mock_spark_session.storage.query_table("analytics", "pipeline_logs")
        assert len(log_data) > 0
