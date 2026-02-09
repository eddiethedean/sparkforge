"""
Tests that run the exact example code from docs/guides (GETTING_STARTED, USE_CASE_*).

Ensures guide examples are valid and executable with both mock Spark and real PySpark.
"""

import os

import pytest

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]


class TestGettingStartedGuideFirstPipeline:
    """Run the 'Your First Pipeline' example from GETTING_STARTED_GUIDE.md."""

    def test_first_pipeline_builds_and_runs(self, mock_spark_session):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        spark = mock_spark_session
        configure_engine(spark=spark)
        F_local = get_default_functions()

        data = [
            ("user1", "click", "2024-01-01 10:00:00", 1.0),
            ("user2", "view", "2024-01-01 10:01:00", 2.0),
            ("user1", "purchase", "2024-01-01 10:02:00", 29.99),
        ]
        source_df = spark.createDataFrame(data, ["user_id", "action", "timestamp", "value"])

        builder = PipelineBuilder(spark=spark, schema="my_schema")

        builder.with_bronze_rules(
            name="raw_events",
            rules={
                "user_id": [F_local.col("user_id").isNotNull()],
                "action": [F_local.col("action").isNotNull()],
                "value": [F_local.col("value") > 0],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F_local.col("action") == "purchase")

        builder.add_silver_transform(
            name="purchases",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F_local.col("user_id").isNotNull()],
                "value": [F_local.col("value") > 0],
            },
            table_name="purchases",
        )

        def gold_transform(spark, silvers):
            return silvers["purchases"].groupBy("user_id").agg(
                F_local.count("*").alias("count"),
                F_local.sum("value").alias("total"),
            )

        builder.add_gold_transform(
            name="user_totals",
            transform=gold_transform,
            rules={
                "user_id": [F_local.col("user_id").isNotNull()],
                "count": [F_local.col("count") > 0],
            },
            table_name="user_totals",
            source_silvers=["purchases"],
        )

        errors = builder.validate_pipeline()
        assert not errors, errors

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

        assert result.status.value == "completed"
        assert result.bronze_results["raw_events"]["rows_processed"] == 3
        assert result.silver_results["purchases"]["rows_written"] == 1
        assert result.gold_results["user_totals"]["rows_written"] == 1


class TestUseCaseBIGuide:
    """Run the BI use-case pipeline from USE_CASE_BI_GUIDE.md."""

    def test_bi_pipeline_builds_and_runs(self, mock_spark_session):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        spark = mock_spark_session
        configure_engine(spark=spark)
        F_local = get_default_functions()

        import random
        from datetime import datetime, timedelta

        base_date = datetime(2024, 1, 1)
        sales_data = [
            (
                f"TXN_{i:06d}",
                f"CUST_{random.randint(1, 500):04d}",
                f"PROD_{random.randint(1, 100):03d}",
                random.choice(["Electronics", "Clothing", "Home"]),
                round(random.uniform(10, 500), 2),
                random.randint(1, 5),
                (base_date + timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
                random.choice(["North", "South", "East", "West"]),
            )
            for i in range(50)
        ]
        sales_df = spark.createDataFrame(
            sales_data,
            [
                "transaction_id",
                "customer_id",
                "product_id",
                "category",
                "sales_amount",
                "quantity",
                "transaction_date",
                "region",
            ],
        )
        sales_df = sales_df.withColumn("transaction_date", F_local.to_date("transaction_date"))

        builder = PipelineBuilder(
            spark=spark,
            schema="bi_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
        )

        builder.with_bronze_rules(
            name="sales",
            rules={
                "transaction_id": [F_local.col("transaction_id").isNotNull()],
                "customer_id": [F_local.col("customer_id").isNotNull()],
                "sales_amount": [F_local.col("sales_amount") > 0],
                "transaction_date": [F_local.col("transaction_date").isNotNull()],
            },
            incremental_col="transaction_date",
        )

        def enhance_sales(spark, bronze_df, prior_silvers):
            # date_trunc supported in PySpark; use col in mock (sparkless has no date_trunc at runtime)
            month_col = (
                F_local.date_trunc("month", F_local.col("transaction_date"))
                if spark_mode == "real"
                else F_local.col("transaction_date")
            )
            return (
                bronze_df.withColumn(
                    "net_sales", F_local.col("sales_amount") * F_local.col("quantity")
                )
                .withColumn("transaction_month", month_col)
                .withColumn("processed_at", F_local.current_timestamp())
            )

        builder.add_silver_transform(
            name="enhanced_sales",
            source_bronze="sales",
            transform=enhance_sales,
            rules={
                "net_sales": [F_local.col("net_sales") > 0],
                "transaction_month": [F_local.col("transaction_month").isNotNull()],
                "processed_at": [F_local.col("processed_at").isNotNull()],
                "region": [F_local.col("region").isNotNull()],
                "customer_id": [F_local.col("customer_id").isNotNull()],
            },
            table_name="enhanced_sales",
        )

        def sales_performance(spark, silvers):
            return (
                silvers["enhanced_sales"]
                .groupBy("region", "transaction_month")
                .agg(
                    F_local.sum("net_sales").alias("revenue"),
                    F_local.count("*").alias("transactions"),
                    F_local.countDistinct("customer_id").alias("customers"),
                )
            )

        builder.add_gold_transform(
            name="sales_performance",
            transform=sales_performance,
            rules={
                "revenue": [F_local.col("revenue") > 0],
                "transactions": [F_local.col("transactions") > 0],
                "customers": [F_local.col("customers") > 0],
            },
            table_name="sales_performance",
            source_silvers=["enhanced_sales"],
        )

        errors = builder.validate_pipeline()
        assert not errors, errors

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"sales": sales_df})

        assert result.status.value == "completed"
        assert "enhanced_sales" in result.silver_results
        assert "sales_performance" in result.gold_results


class TestUseCaseEcommerceGuide:
    """Run the E-commerce use-case pipeline from USE_CASE_ECOMMERCE_GUIDE.md."""

    def test_ecommerce_pipeline_builds_and_runs(self, mock_spark_session):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        spark = mock_spark_session
        configure_engine(spark=spark)
        F_local = get_default_functions()

        import random
        from datetime import datetime, timedelta

        base_date = datetime(2024, 1, 1)
        orders = [
            (
                f"ORD_{i:05d}",
                f"CUST_{random.randint(1, 200):04d}",
                f"PROD_{random.randint(1, 50):03d}",
                random.choice(["Electronics", "Accessories", "Computers"]),
                random.randint(1, 5),
                round(random.uniform(20, 500), 2),
                (base_date + timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d"),
                random.choice(["North", "South", "East", "West"]),
            )
            for i in range(30)
        ]
        orders_df = spark.createDataFrame(
            orders,
            [
                "order_id",
                "customer_id",
                "product_id",
                "category",
                "quantity",
                "unit_price",
                "order_date",
                "region",
            ],
        )
        orders_df = (
            orders_df.withColumn("order_date", F_local.to_date("order_date"))
            .withColumn("total_amount", F_local.col("quantity") * F_local.col("unit_price"))
        )

        builder = PipelineBuilder(
            spark=spark,
            schema="ecommerce_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
        )

        builder.with_bronze_rules(
            name="orders",
            rules={
                "order_id": [F_local.col("order_id").isNotNull()],
                "customer_id": [F_local.col("customer_id").isNotNull()],
                "total_amount": [F_local.col("total_amount") > 0],
                "order_date": [F_local.col("order_date").isNotNull()],
                "region": [F_local.col("region").isNotNull()],
            },
            incremental_col="order_date",
        )

        def enrich_orders(spark, bronze_df, prior_silvers):
            # date_trunc supported in PySpark; use col in mock (sparkless has no date_trunc at runtime)
            order_month_col = (
                F_local.date_trunc("month", F_local.col("order_date"))
                if spark_mode == "real"
                else F_local.col("order_date")
            )
            return (
                bronze_df.withColumn("order_month", order_month_col)
                .withColumn(
                    "order_size",
                    F_local.when(F_local.col("total_amount") > 500, "large")
                    .when(F_local.col("total_amount") > 100, "medium")
                    .otherwise("small"),
                )
                .withColumn("processed_at", F_local.current_timestamp())
            )

        builder.add_silver_transform(
            name="enriched_orders",
            source_bronze="orders",
            transform=enrich_orders,
            rules={
                "order_month": [F_local.col("order_month").isNotNull()],
                "order_size": [F_local.col("order_size").isNotNull()],
                "processed_at": [F_local.col("processed_at").isNotNull()],
                "total_amount": [F_local.col("total_amount") > 0],
                "order_date": [F_local.col("order_date").isNotNull()],
                "region": [F_local.col("region").isNotNull()],
                "customer_id": [F_local.col("customer_id").isNotNull()],
            },
            table_name="enriched_orders",
        )

        def daily_sales(spark, silvers):
            return (
                silvers["enriched_orders"]
                .groupBy("order_date", "region")
                .agg(
                    F_local.count("*").alias("orders"),
                    F_local.sum("total_amount").alias("revenue"),
                    F_local.countDistinct("customer_id").alias("customers"),
                )
            )

        builder.add_gold_transform(
            name="daily_sales",
            transform=daily_sales,
            rules={
                "order_date": [F_local.col("order_date").isNotNull()],
                "revenue": [F_local.col("revenue") > 0],
                "orders": [F_local.col("orders") > 0],
            },
            table_name="daily_sales",
            source_silvers=["enriched_orders"],
        )

        errors = builder.validate_pipeline()
        assert not errors, errors

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"orders": orders_df})

        assert result.status.value == "completed"
        assert "enriched_orders" in result.silver_results
        assert "daily_sales" in result.gold_results


class TestUseCaseIoTGuide:
    """Run the IoT use-case pipeline from USE_CASE_IOT_GUIDE.md."""

    def test_iot_pipeline_builds_and_runs(self, mock_spark_session):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        spark = mock_spark_session
        configure_engine(spark=spark)
        F_local = get_default_functions()

        import random
        from datetime import datetime, timedelta

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        zones = ["Building_A", "Building_B", "Building_C"]
        sensor_types = ["temperature", "humidity", "pressure"]

        readings = []
        for i in range(50):
            zone = random.choice(zones)
            st = random.choice(sensor_types)
            ts = base_time + timedelta(minutes=i * 5)
            val = (
                round(20 + random.uniform(-5, 5), 2)
                if st == "temperature"
                else round(50 + random.uniform(-10, 10), 2)
            )
            readings.append((f"{zone}_{st}_{i % 10}", st, zone, val, ts, "online"))

        sensor_df = spark.createDataFrame(
            readings,
            ["sensor_id", "sensor_type", "zone", "value", "timestamp", "device_status"],
        )

        builder = PipelineBuilder(
            spark=spark,
            schema="iot_schema",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
        )

        builder.with_bronze_rules(
            name="sensor_readings",
            rules={
                "sensor_id": [F_local.col("sensor_id").isNotNull()],
                "sensor_type": [F_local.col("sensor_type").isNotNull()],
                "value": [F_local.col("value").isNotNull()],
                "timestamp": [F_local.col("timestamp").isNotNull()],
                "zone": [F_local.col("zone").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def process_sensors(spark, bronze_df, prior_silvers):
            # Keep zone so gold can groupBy it (sparkless may not preserve all columns through withColumn)
            return (
                bronze_df.withColumn("hour", F_local.hour("timestamp"))
                .withColumn(
                    "is_anomaly",
                    F_local.when(
                        (F_local.col("sensor_type") == "temperature")
                        & (
                            (F_local.col("value") > 60) | (F_local.col("value") < -10)
                        ),
                        True,
                    ).when(
                        (F_local.col("sensor_type") == "humidity")
                        & (
                            (F_local.col("value") > 90) | (F_local.col("value") < 10)
                        ),
                        True,
                    ).otherwise(False),
                )
                .withColumn(
                    "severity",
                    F_local.when(
                        F_local.col("is_anomaly")
                        & (F_local.col("sensor_type") == "temperature")
                        & (F_local.col("value") > 80),
                        "critical",
                    )
                    .when(F_local.col("is_anomaly"), "medium")
                    .otherwise("normal"),
                )
                .withColumn("processed_at", F_local.current_timestamp())
            )

        builder.add_silver_transform(
            name="processed_sensors",
            source_bronze="sensor_readings",
            transform=process_sensors,
            rules={
                "is_anomaly": [F_local.col("is_anomaly").isNotNull()],
                "severity": [F_local.col("severity").isNotNull()],
                "processed_at": [F_local.col("processed_at").isNotNull()],
                "value": [F_local.col("value").isNotNull()],
                "zone": [F_local.col("zone").isNotNull()],
                "sensor_type": [F_local.col("sensor_type").isNotNull()],
                "hour": [F_local.col("hour").isNotNull()],
            },
            table_name="processed_sensors",
        )

        def zone_analytics(spark, silvers):
            return (
                silvers["processed_sensors"]
                .groupBy("zone", "sensor_type", "hour")
                .agg(
                    F_local.count("*").alias("readings"),
                    F_local.avg("value").alias("avg_value"),
                    F_local.sum(
                        F_local.when(F_local.col("is_anomaly"), 1).otherwise(0)
                    ).alias("anomalies"),
                )
                .withColumn(
                    "anomaly_rate", F_local.col("anomalies") / F_local.col("readings") * 100
                )
            )

        builder.add_gold_transform(
            name="zone_analytics",
            transform=zone_analytics,
            rules={
                "zone": [F_local.col("zone").isNotNull()],
                "avg_value": [F_local.col("avg_value").isNotNull()],
                "readings": [F_local.col("readings") > 0],
            },
            table_name="zone_analytics",
            source_silvers=["processed_sensors"],
        )

        errors = builder.validate_pipeline()
        assert not errors, errors

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"sensor_readings": sensor_df})

        assert result.status.value == "completed"
        assert "processed_sensors" in result.silver_results
        assert "zone_analytics" in result.gold_results
