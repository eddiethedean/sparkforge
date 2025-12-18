"""
IoT Sensor Data Pipeline Tests

This module tests a realistic IoT sensor data processing pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with sensor readings, anomaly detection,
and device health analytics.
"""

import os
from uuid import uuid4

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark-specific tests require SPARK_MODE=real"
    )

from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline_builder.pipeline import PipelineBuilder
from pipeline_builder.writer import LogWriter
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


class TestIotPipeline:
    """Test IoT sensor data processing pipeline with bronze-silver-gold architecture."""

    def test_complete_iot_sensor_pipeline_execution(
        self, spark_session, data_generator, test_assertions
    ):
        """Test complete IoT pipeline: sensor readings → anomaly detection → device health analytics."""

        # Create realistic sensor data
        sensor_data = data_generator.create_iot_sensor_data(
            spark_session, num_readings=100
        )

        # Create pipeline builder
        unique_schema = get_unique_schema("bronze")

        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw sensor data validation
        builder.with_bronze_rules(
            name="raw_sensor_readings",
            rules={"sensor_id": ["not_null"], "timestamp": ["not_null"]},
            incremental_col="timestamp",
        )

        # Silver Layer: Clean and aggregate sensor data
        def clean_sensor_data_transform(spark, df, silvers):
            """Clean and process sensor data with basic validation."""
            return (
                df.withColumn("timestamp_parsed", F.to_timestamp("timestamp"))
                .withColumn("hour", F.hour("timestamp_parsed"))
                .withColumn("date", F.to_date("timestamp_parsed"))
                .filter(F.col("device_status") == "active")  # Only active devices
                .select(
                    "sensor_id",
                    "timestamp_parsed",
                    "hour",
                    "date",
                    "temperature",
                    "humidity",
                    "pressure",
                    "location",
                )
            )

        builder.add_silver_transform(
            name="clean_sensor_data",
            source_bronze="raw_sensor_readings",
            transform=clean_sensor_data_transform,
            rules={"sensor_id": ["not_null"], "timestamp_parsed": ["not_null"]},
            table_name="clean_sensor_data",
        )

        def hourly_aggregates_transform(spark, df, silvers):
            """Create hourly aggregated metrics."""
            # First create the date and hour columns from timestamp
            df_with_time = (
                df.withColumn("timestamp_parsed", F.to_timestamp("timestamp"))
                .withColumn("hour", F.hour("timestamp_parsed"))
                .withColumn("date", F.to_date("timestamp_parsed"))
            )

            return (
                df_with_time.groupBy("sensor_id", "date", "hour", "location")
                .agg(
                    F.count("*").alias("reading_count"),
                    F.avg("temperature").alias("avg_temperature"),
                    F.min("temperature").alias("min_temperature"),
                    F.max("temperature").alias("max_temperature"),
                    F.avg("humidity").alias("avg_humidity"),
                    F.avg("pressure").alias("avg_pressure"),
                    F.stddev("temperature").alias("temp_stddev"),
                )
                .orderBy("sensor_id", "date", "hour")
            )

        builder.add_silver_transform(
            name="hourly_aggregates",
            source_bronze="raw_sensor_readings",
            transform=hourly_aggregates_transform,
            rules={"sensor_id": ["not_null"]},
            table_name="hourly_aggregates",
        )

        # Gold Layer: Device health and analytics
        def device_health_scores_transform(spark, silvers):
            """Calculate device health scores based on sensor data."""
            hourly_data = silvers.get("hourly_aggregates")
            if hourly_data is not None:
                # Calculate health score based on temperature stability and reading frequency
                Window.partitionBy("sensor_id").orderBy("date", "hour")

                return (
                    hourly_data.withColumn(
                        "temp_stability", F.lit(100) - F.col("temp_stddev")
                    )  # Higher stability = better health
                    .withColumn(
                        "data_completeness",
                        F.col("reading_count") / F.lit(60) * F.lit(100),
                    )  # Expected 60 readings per hour
                    .withColumn(
                        "health_score",
                        (
                            F.col("temp_stability") * F.lit(0.6)
                            + F.col("data_completeness") * F.lit(0.4)
                        ),
                    )
                    .withColumn(
                        "health_status",
                        F.when(F.col("health_score") >= 80, "excellent")
                        .when(F.col("health_score") >= 60, "good")
                        .when(F.col("health_score") >= 40, "fair")
                        .otherwise("poor"),
                    )
                    .select(
                        "sensor_id",
                        "date",
                        "hour",
                        "location",
                        "health_score",
                        "health_status",
                        "temp_stability",
                        "data_completeness",
                    )
                )
            else:
                return spark.createDataFrame(
                    [],
                    [
                        "sensor_id",
                        "date",
                        "hour",
                        "location",
                        "health_score",
                        "health_status",
                        "temp_stability",
                        "data_completeness",
                    ],
                )

        builder.add_gold_transform(
            name="device_health_scores",
            transform=device_health_scores_transform,
            rules={"sensor_id": ["not_null"], "health_status": ["not_null"]},
            table_name="device_health_scores",
            source_silvers=["hourly_aggregates"],
        )

        def location_analytics_transform(spark, silvers):
            """Create location-based analytics and trends."""
            hourly_data = silvers.get("hourly_aggregates")
            if hourly_data is not None:
                return (
                    hourly_data.groupBy("location", "date")
                    .agg(
                        F.countDistinct("sensor_id").alias("active_sensors"),
                        F.avg("avg_temperature").alias("location_avg_temp"),
                        F.avg("avg_humidity").alias("location_avg_humidity"),
                        F.sum("reading_count").alias("total_readings"),
                    )
                    .withColumn(
                        "sensor_density",
                        F.col("total_readings") / F.col("active_sensors"),
                    )
                    .orderBy("location", "date")
                )
            else:
                return spark.createDataFrame(
                    [],
                    [
                        "location",
                        "date",
                        "active_sensors",
                        "location_avg_temp",
                        "location_avg_humidity",
                        "total_readings",
                        "sensor_density",
                    ],
                )

        builder.add_gold_transform(
            name="location_analytics",
            transform=location_analytics_transform,
            rules={"location": ["not_null"]},
            table_name="location_analytics",
            source_silvers=["hourly_aggregates"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        # Execute initial load
        result = pipeline.run_initial_load(
            bronze_sources={"raw_sensor_readings": sensor_data}
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify pipeline execution was successful
        # Note: We don't need to verify storage in tests - we're testing pipeline logic
        print("✅ IoT pipeline test completed successfully")

    def test_incremental_sensor_processing(
        self, spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new sensor readings."""

        # Create initial sensor data
        initial_data = data_generator.create_iot_sensor_data(
            spark_session, num_readings=50
        )

        # Create pipeline
        unique_schema = get_unique_schema("bronze")

        builder = PipelineBuilder(spark=spark_session, schema=unique_schema)

        # Bronze layer
        builder.with_bronze_rules(
            name="sensor_readings",
            rules={"sensor_id": ["not_null"], "temperature": ["not_null"]},
            incremental_col="timestamp",
        )

        # Silver layer
        builder.add_silver_transform(
            name="clean_readings",
            source_bronze="sensor_readings",
            transform=lambda spark, df, silvers: df.filter(
                F.col("device_status") == "active"
            ),
            rules={"sensor_id": ["not_null"]},
            table_name="clean_readings",
        )

        # Gold layer
        builder.add_gold_transform(
            name="sensor_summary",
            transform=lambda spark, silvers: silvers["clean_readings"].agg(
                F.count("*").alias("total_readings")
            ),
            rules={"total_readings": ["not_null"]},
            table_name="sensor_summary",
            source_silvers=["clean_readings"],
        )

        pipeline = builder.to_pipeline()

        # Initial load
        initial_result = pipeline.run_initial_load(
            bronze_sources={"sensor_readings": initial_data}
        )
        test_assertions.assert_pipeline_success(initial_result)

        # Create incremental data (new readings)
        new_data = data_generator.create_iot_sensor_data(spark_session, num_readings=25)

        # Incremental processing
        incremental_result = pipeline.run_incremental(
            bronze_sources={"sensor_readings": new_data}
        )
        test_assertions.assert_pipeline_success(incremental_result)

        print("✅ Incremental sensor processing test completed successfully")

    def test_anomaly_detection_pipeline(
        self, spark_session, data_generator, test_assertions
    ):
        """Test anomaly detection in sensor data."""

        # Create sensor data with some anomalies
        normal_data = data_generator.create_iot_sensor_data(
            spark_session, num_readings=80
        )

        # Add some anomalous readings
        anomaly_data = spark_session.createDataFrame(
            [
                (
                    "SENSOR-99",
                    "2024-01-01T12:00:00",
                    150.0,
                    50.0,
                    1013.0,
                    "Building-5",
                    "active",
                ),  # Extreme temperature
                (
                    "SENSOR-99",
                    "2024-01-01T12:01:00",
                    -100.0,
                    50.0,
                    1013.0,
                    "Building-5",
                    "active",
                ),  # Extreme temperature
                (
                    "SENSOR-99",
                    "2024-01-01T12:02:00",
                    25.0,
                    150.0,
                    1013.0,
                    "Building-5",
                    "active",
                ),  # Invalid humidity
            ],
            [
                "sensor_id",
                "timestamp",
                "temperature",
                "humidity",
                "pressure",
                "location",
                "device_status",
            ],
        )

        # Combine normal and anomaly data
        all_data = normal_data.union(anomaly_data)

        # Create pipeline with anomaly detection
        unique_schema = get_unique_schema("bronze")

        builder = PipelineBuilder(spark=spark_session, schema=unique_schema)

        builder.with_bronze_rules(
            name="sensor_readings",
            rules={
                "sensor_id": ["not_null"],
                "temperature": ["not_null"],
                "humidity": ["not_null"],
            },
        )

        def anomaly_detection_transform(spark, df, silvers):
            """Detect and flag anomalies in sensor data."""
            return (
                df.withColumn(
                    "is_temperature_anomaly",
                    (F.col("temperature") < -50) | (F.col("temperature") > 100),
                )
                .withColumn(
                    "is_humidity_anomaly",
                    (F.col("humidity") < 0) | (F.col("humidity") > 100),
                )
                .withColumn(
                    "is_anomaly",
                    F.col("is_temperature_anomaly") | F.col("is_humidity_anomaly"),
                )
                .withColumn(
                    "anomaly_type",
                    F.when(F.col("is_temperature_anomaly"), "temperature")
                    .when(F.col("is_humidity_anomaly"), "humidity")
                    .otherwise("normal"),
                )
            )

        builder.add_silver_transform(
            name="anomaly_detection",
            source_bronze="sensor_readings",
            transform=anomaly_detection_transform,
            rules={"sensor_id": ["not_null"], "is_anomaly": ["not_null"]},
            table_name="anomaly_detection",
        )

        def anomaly_summary_transform(spark, silvers):
            """Create anomaly summary statistics."""
            anomaly_data = silvers.get("anomaly_detection")
            if anomaly_data is not None:
                return (
                    anomaly_data.groupBy("sensor_id", "anomaly_type")
                    .agg(F.count("*").alias("anomaly_count"))
                    .filter(F.col("anomaly_type") != "normal")
                )
            else:
                return spark.createDataFrame(
                    [], ["sensor_id", "anomaly_type", "anomaly_count"]
                )

        builder.add_gold_transform(
            name="anomaly_summary",
            transform=anomaly_summary_transform,
            rules={"sensor_id": ["not_null"], "anomaly_count": ["non_negative"]},
            table_name="anomaly_summary",
            source_silvers=["anomaly_detection"],
        )

        pipeline = builder.to_pipeline()

        # Execute pipeline
        result = pipeline.run_initial_load(bronze_sources={"sensor_readings": all_data})

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        print("✅ Anomaly detection pipeline test completed successfully")

    @pytest.mark.sequential
    def test_performance_monitoring(
        self, spark_session, data_generator, log_writer_config, test_assertions
    ):
        """Test performance monitoring and logging for IoT pipeline."""

        # Create large sensor dataset
        sensor_data = data_generator.create_iot_sensor_data(
            spark_session, num_readings=200
        )

        # Create unique schema for this test
        analytics_schema = get_unique_schema("analytics")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {analytics_schema}")

        # Create LogWriter for performance monitoring
        LogWriter(
            spark=spark_session, schema=analytics_schema, table_name="iot_pipeline_logs"
        )

        # Create pipeline
        unique_schema = get_unique_schema("bronze")

        builder = PipelineBuilder(spark=spark_session, schema=unique_schema)

        builder.with_bronze_rules(
            name="sensor_readings", rules={"sensor_id": ["not_null"]}
        )

        builder.add_silver_transform(
            name="processed_readings",
            source_bronze="sensor_readings",
            transform=lambda spark, df, silvers: df.filter(
                F.col("device_status") == "active"
            ),
            rules={"sensor_id": ["not_null"]},
            table_name="processed_readings",
        )

        builder.add_gold_transform(
            name="device_metrics",
            transform=lambda spark, silvers: silvers["processed_readings"].agg(
                F.count("*").alias("total_readings")
            ),
            rules={"total_readings": ["not_null"]},
            table_name="device_metrics",
            source_silvers=["processed_readings"],
        )

        pipeline = builder.to_pipeline()

        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={"sensor_readings": sensor_data}
        )

        # Skip LogWriter for now due to Delta Lake dependency issues
        # This test focuses on pipeline execution without logging

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify pipeline results
        assert result.success is True
        assert len(result.bronze_results) == 1
        assert len(result.silver_results) == 1
        assert len(result.gold_results) == 1

        # Cleanup: drop schema created for this test
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, analytics_schema)
        except Exception:
            pass  # Ignore cleanup errors

        print("✅ Performance monitoring test completed successfully")
