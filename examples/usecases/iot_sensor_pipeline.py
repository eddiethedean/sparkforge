#!/usr/bin/env python3
"""
IoT Sensor Data Pipeline Example

This example demonstrates processing IoT sensor data through Bronze → Silver → Gold layers
to create real-time analytics and anomaly detection.
"""

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkforge import PipelineBuilder


def create_sensor_data(spark):
    """Create sample IoT sensor data for demonstration."""

    # Sample sensor data
    sensor_data = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    sensor_zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    sensor_types = ["temperature", "humidity", "pressure", "vibration"]

    for i in range(2000):
        zone = random.choice(sensor_zones)
        sensor_type = random.choice(sensor_types)

        # Generate realistic sensor values with some anomalies
        if sensor_type == "temperature":
            base_value = random.uniform(20.0, 25.0)
            if random.random() < 0.05:  # 5% anomalies
                base_value = random.uniform(80.0, 120.0)
        elif sensor_type == "humidity":
            base_value = random.uniform(40.0, 60.0)
            if random.random() < 0.03:  # 3% anomalies
                base_value = random.uniform(90.0, 100.0)
        elif sensor_type == "pressure":
            base_value = random.uniform(1000.0, 1020.0)
            if random.random() < 0.02:  # 2% anomalies
                base_value = random.uniform(500.0, 800.0)
        else:  # vibration
            base_value = random.uniform(0.1, 2.0)
            if random.random() < 0.08:  # 8% anomalies
                base_value = random.uniform(5.0, 15.0)

        sensor_data.append(
            {
                "sensor_id": f"{zone}_{sensor_type}_{i % 10:02d}",
                "sensor_type": sensor_type,
                "zone": zone,
                "value": round(base_value, 2),
                "timestamp": base_time + timedelta(minutes=i),
                "battery_level": random.uniform(20.0, 100.0),
                "signal_strength": random.uniform(60.0, 100.0),
            }
        )

    return spark.createDataFrame(sensor_data)


def main():
    """Main function to run the IoT sensor pipeline."""

    print("🌡️ IoT Sensor Data Pipeline Example")
    print("=" * 50)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("IoT Sensor Pipeline")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create sample data
        print("📊 Creating sample IoT sensor data...")
        sensor_df = create_sensor_data(spark)
        print(f"Created {sensor_df.count()} sensor readings")

        # Build pipeline
        print("\n🏗️ Building pipeline...")
        builder = PipelineBuilder(
            spark=spark,
            schema="iot_analytics",
            min_bronze_rate=90.0,  # Lower threshold for sensor data
            min_silver_rate=95.0,
            min_gold_rate=98.0,
            enable_parallel_silver=True,
            max_parallel_workers=4,
            verbose=True,
        )

        # Bronze Layer: Raw sensor data
        print("🥉 Setting up Bronze layer...")
        builder.with_bronze_rules(
            name="sensor_readings",
            rules={
                "sensor_id": [F.col("sensor_id").isNotNull()],
                "sensor_type": [F.col("sensor_type").isNotNull()],
                "zone": [F.col("zone").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "battery_level": [F.col("battery_level").between(0, 100)],
                "signal_strength": [F.col("signal_strength").between(0, 100)],
            },
            incremental_col="timestamp",
        )

        # Silver Layer: Processed sensor data
        print("🥈 Setting up Silver layer...")

        def process_sensor_data(spark, bronze_df, prior_silvers):
            return (
                bronze_df.withColumn("reading_date", F.date_trunc("day", "timestamp"))
                .withColumn("reading_hour", F.hour("timestamp"))
                .withColumn(
                    "is_anomaly",
                    F.when(F.col("sensor_type") == "temperature", F.col("value") > 50)
                    .when(F.col("sensor_type") == "humidity", F.col("value") > 80)
                    .when(F.col("sensor_type") == "pressure", F.col("value") < 900)
                    .when(F.col("sensor_type") == "vibration", F.col("value") > 5)
                    .otherwise(False),
                )
                .withColumn("is_low_battery", F.col("battery_level") < 30)
                .withColumn("is_weak_signal", F.col("signal_strength") < 70)
                .withColumn(
                    "data_quality",
                    F.when(F.col("is_low_battery") | F.col("is_weak_signal"), "poor")
                    .when(F.col("signal_strength") < 85, "fair")
                    .otherwise("good"),
                )
                .filter(F.col("value").isNotNull())  # Remove null values
            )

        builder.add_silver_transform(
            name="processed_readings",
            source_bronze="sensor_readings",
            transform=process_sensor_data,
            rules={
                "reading_date": [F.col("reading_date").isNotNull()],
                "is_anomaly": [F.col("is_anomaly").isNotNull()],
                "data_quality": [F.col("data_quality").isNotNull()],
            },
            table_name="processed_readings",
            watermark_col="timestamp",
        )

        # Silver Layer: Sensor health monitoring
        def sensor_health_monitoring(spark, bronze_df, prior_silvers):
            processed_readings = prior_silvers["processed_readings"]
            return (
                processed_readings.groupBy("sensor_id", "sensor_type", "zone")
                .agg(
                    F.count("*").alias("total_readings"),
                    F.avg("value").alias("avg_value"),
                    F.stddev("value").alias("value_stddev"),
                    F.sum("is_anomaly").alias("anomaly_count"),
                    F.avg("battery_level").alias("avg_battery"),
                    F.avg("signal_strength").alias("avg_signal"),
                    F.max("timestamp").alias("last_reading"),
                    F.first("data_quality").alias("data_quality"),
                )
                .withColumn(
                    "anomaly_rate", F.col("anomaly_count") / F.col("total_readings")
                )
                .withColumn(
                    "sensor_status",
                    F.when(F.col("anomaly_rate") > 0.1, "critical")
                    .when(F.col("anomaly_rate") > 0.05, "warning")
                    .when(F.col("avg_battery") < 30, "low_battery")
                    .when(F.col("avg_signal") < 70, "weak_signal")
                    .otherwise("healthy"),
                )
                .withColumn(
                    "is_offline",
                    F.current_timestamp() - F.col("last_reading")
                    > F.expr("INTERVAL 1 HOUR"),
                )
            )

        builder.add_silver_transform(
            name="sensor_health",
            source_bronze="sensor_readings",
            transform=sensor_health_monitoring,
            rules={
                "sensor_id": [F.col("sensor_id").isNotNull()],
                "sensor_status": [F.col("sensor_status").isNotNull()],
                "anomaly_rate": [F.col("anomaly_rate").isNotNull()],
            },
            table_name="sensor_health",
            source_silvers=["processed_readings"],
        )

        # Gold Layer: Zone analytics
        print("🥇 Setting up Gold layer...")

        def zone_analytics(spark, silvers):
            processed_readings = silvers["processed_readings"]
            return (
                processed_readings.groupBy("zone", "sensor_type", "reading_date")
                .agg(
                    F.count("*").alias("reading_count"),
                    F.avg("value").alias("avg_value"),
                    F.min("value").alias("min_value"),
                    F.max("value").alias("max_value"),
                    F.sum("is_anomaly").alias("anomaly_count"),
                    F.avg("battery_level").alias("avg_battery"),
                    F.avg("signal_strength").alias("avg_signal"),
                )
                .withColumn(
                    "anomaly_rate", F.col("anomaly_count") / F.col("reading_count")
                )
                .withColumn("value_range", F.col("max_value") - F.col("min_value"))
            )

        builder.add_gold_transform(
            name="zone_analytics",
            transform=zone_analytics,
            rules={
                "zone": [F.col("zone").isNotNull()],
                "sensor_type": [F.col("sensor_type").isNotNull()],
                "reading_date": [F.col("reading_date").isNotNull()],
                "avg_value": [F.col("avg_value").isNotNull()],
            },
            table_name="zone_analytics",
            source_silvers=["processed_readings"],
        )

        # Gold Layer: Anomaly summary
        def anomaly_summary(spark, silvers):
            processed_readings = silvers["processed_readings"]
            return (
                processed_readings.filter(F.col("is_anomaly") is True)
                .groupBy("zone", "sensor_type", "reading_date")
                .agg(
                    F.count("*").alias("anomaly_count"),
                    F.avg("value").alias("avg_anomaly_value"),
                    F.min("value").alias("min_anomaly_value"),
                    F.max("value").alias("max_anomaly_value"),
                    F.countDistinct("sensor_id").alias("affected_sensors"),
                )
                .withColumn(
                    "severity",
                    F.when(F.col("anomaly_count") > 10, "high")
                    .when(F.col("anomaly_count") > 5, "medium")
                    .otherwise("low"),
                )
            )

        builder.add_gold_transform(
            name="anomaly_summary",
            transform=anomaly_summary,
            rules={
                "zone": [F.col("zone").isNotNull()],
                "sensor_type": [F.col("sensor_type").isNotNull()],
                "anomaly_count": [F.col("anomaly_count") > 0],
            },
            table_name="anomaly_summary",
            source_silvers=["processed_readings"],
        )

        # Build and execute pipeline
        print("\n🚀 Building pipeline...")
        pipeline = builder.to_pipeline()

        print("\n📋 Pipeline steps:")
        steps = pipeline.list_steps()
        print(f"Bronze steps: {steps['bronze']}")
        print(f"Silver steps: {steps['silver']}")
        print(f"Gold steps: {steps['gold']}")

        # Execute pipeline
        print("\n⚡ Executing pipeline...")
        result = pipeline.initial_load(bronze_sources={"sensor_readings": sensor_df})

        # Display results
        print("\n📊 Pipeline Results:")
        print(f"Success: {result.success}")
        print(f"Total rows written: {result.totals['total_rows_written']}")
        print(f"Execution time: {result.totals['total_duration_secs']:.2f}s")

        if result.success:
            print("\n✅ Pipeline completed successfully!")

            # Show sample results
            print("\n📈 Sample Results:")

            # Zone analytics
            print("\nZone Analytics (top 10):")
            spark.table("iot_analytics.zone_analytics").show(10)

            # Sensor health
            print("\nSensor Health Status:")
            spark.table("iot_analytics.sensor_health").show(10)

            # Anomaly summary
            print("\nAnomaly Summary:")
            spark.table("iot_analytics.anomaly_summary").show(10)

            # Data quality summary
            print("\nData Quality Summary:")
            spark.table("iot_analytics.processed_readings").groupBy(
                "data_quality"
            ).count().orderBy("count", ascending=False).show()

        else:
            print(f"\n❌ Pipeline failed: {result.error_message}")
            if result.failed_steps:
                print(f"Failed steps: {result.failed_steps}")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        try:
            spark.sql("DROP DATABASE IF EXISTS iot_analytics CASCADE")
            spark.stop()
            print("\n🧹 Cleanup completed")
        except:
            pass


if __name__ == "__main__":
    main()
