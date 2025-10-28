#!/usr/bin/env python3
"""
Basic SparkForge Pipeline Example

This example demonstrates how to create a simple Bronze → Silver → Gold pipeline
using SparkForge.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_builder import PipelineBuilder

# No additional imports needed - PipelineBuilder takes individual parameters


def main():
    """Create and run a basic data pipeline."""

    # Create Spark session
    spark = (
        SparkSession.builder.appName("SparkForgeExample")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    try:
        # Create sample data
        sample_data = [
            ("user1", "click", "2024-01-01 10:00:00", 100),
            ("user2", "view", "2024-01-01 11:00:00", 200),
            ("user3", "purchase", "2024-01-01 12:00:00", 300),
            ("user1", "click", "2024-01-01 13:00:00", 150),
            ("user2", "purchase", "2024-01-01 14:00:00", 250),
        ]

        source_df = spark.createDataFrame(
            sample_data, ["user_id", "action", "timestamp", "value"]
        )

        print("🔧 Building SparkForge Pipeline...")

        # Clean up and create database
        spark.sql("DROP DATABASE IF EXISTS example_schema CASCADE")
        spark.sql("CREATE DATABASE example_schema")

        # Build the pipeline with configuration
        builder = PipelineBuilder(
            spark=spark,
            schema="example_schema",
            min_bronze_rate=90.0,  # 90% of Bronze data must pass validation
            min_silver_rate=95.0,  # 95% of Silver data must pass validation
            min_gold_rate=98.0,  # 98% of Gold data must pass validation
            verbose=True,
        )

        # Bronze Layer: Raw data ingestion with validation
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0],
            },
            incremental_col="timestamp",
            description="Raw event data ingestion",
        )

        # Silver Layer: Cleaned and enriched data
        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events",
            transform=lambda spark, df, prior_silvers: (
                df.withColumn("processed_at", F.current_timestamp())
                .withColumn("event_date", F.to_date("timestamp"))
                .withColumn("hour", F.hour("timestamp"))
                .filter(F.col("user_id").isNotNull())
                .select(
                    "user_id", "action", "value", "event_date", "processed_at", "hour"
                )
            ),
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "processed_at": [F.col("processed_at").isNotNull()],
            },
            table_name="enriched_events",
            watermark_col="timestamp",
            description="Enriched event data with processing metadata",
        )

        # Gold Layer: Business analytics
        builder.add_gold_transform(
            name="daily_analytics",
            source_silvers=["enriched_events"],
            transform=lambda spark, silvers: (
                silvers["enriched_events"]
                .groupBy("event_date")
                .agg(
                    F.count("*").alias("total_events"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.sum(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias(
                        "purchases"
                    ),
                    F.sum(F.when(F.col("action") == "click", 1).otherwise(0)).alias(
                        "clicks"
                    ),
                    F.sum(F.when(F.col("action") == "view", 1).otherwise(0)).alias(
                        "views"
                    ),
                    F.avg("value").alias("avg_value"),
                )
                .orderBy("event_date")
            ),
            rules={
                "event_date": [F.col("event_date").isNotNull()],
                "total_events": [F.col("total_events") > 0],
                "unique_users": [F.col("unique_users") > 0],
            },
            table_name="daily_analytics",
            description="Daily analytics and business metrics",
        )

        # Create the pipeline
        pipeline = builder.to_pipeline()
        total_steps = (
            len(pipeline.bronze_steps)
            + len(pipeline.silver_steps)
            + len(pipeline.gold_steps)
        )
        print(f"✅ Pipeline created with {total_steps} steps")

        # Pipeline is already a runner, execute directly
        runner = pipeline

        print("🚀 Running pipeline...")
        result = runner.run_initial_load(bronze_sources={"events": source_df})

        # Display results
        print("\n📊 Pipeline Results:")
        print(f"   Status: {result.status}")
        print(f"   Total steps: {result.total_steps}")
        print(f"   Successful steps: {result.successful_steps}")
        print(f"   Failed steps: {result.failed_steps}")
        print(f"   Total rows written: {result.metrics.total_rows_written}")
        print(f"   Execution time: {result.metrics.total_duration_secs:.2f}s")
        print(
            f"   Overall validation rate: {result.metrics.overall_validation_rate:.1f}%"
        )

        # Show the final Gold table
        print("\n📈 Daily Analytics (Gold Layer):")
        gold_table = spark.table("example_schema.daily_analytics")
        gold_table.show(truncate=False)

        # Show Silver table
        print("\n🔍 Enriched Events (Silver Layer):")
        silver_table = spark.table("example_schema.enriched_events")
        silver_table.show(5, truncate=False)

        print("\n🎉 Pipeline completed successfully!")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
