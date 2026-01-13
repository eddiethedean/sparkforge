#!/usr/bin/env python3
"""
Simple example demonstrating auto-inference of source_bronze in add_silver_transform.

This example shows how the new feature allows you to omit the source_bronze
parameter when adding silver transforms, making the API more convenient.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession

from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions


def main():
    """Demonstrate auto-inference of source_bronze."""
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("AutoInferSourceBronzeExample")
        .master("local[*]")
        .getOrCreate()
    )

    # Configure engine (required!)
    configure_engine(spark=spark)
    F = get_default_functions()

    try:
        # Create sample data
        events_data = [
            ("user1", "click", "product1", "2024-01-01 10:00:00"),
            ("user2", "purchase", "product2", "2024-01-01 11:00:00"),
            ("user3", "view", "product1", "2024-01-01 12:00:00"),
            ("user4", "click", "product3", "2024-01-01 13:00:00"),
        ]
        events_df = spark.createDataFrame(
            events_data, ["user_id", "action", "product_id", "timestamp"]
        )

        # Create pipeline builder
        builder = PipelineBuilder(spark=spark, schema="analytics")

        # Add bronze step
        print("🟤 Adding Bronze step...")
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Add silver step WITHOUT source_bronze - it will be auto-inferred!
        print("🟡 Adding Silver step with auto-inferred source_bronze...")

        def clean_events(spark, bronze_df, prior_silvers):
            """Clean and filter events data."""
            F = get_default_functions()
            return (
                bronze_df.filter(F.col("action").isin(["click", "view", "purchase"]))
                .withColumn("event_date", F.date_trunc("day", F.col("timestamp")))
                .withColumn("is_purchase", F.col("action") == "purchase")
            )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=clean_events,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "is_purchase": [F.col("is_purchase").isNotNull()],
            },
            table_name="clean_events",
            watermark_col="timestamp",
        )

        # Add another silver step - also auto-inferred!
        print("🟡 Adding another Silver step with auto-inferred source_bronze...")

        def enriched_events(spark, bronze_df, prior_silvers):
            """Enrich events with additional features."""
            F = get_default_functions()
            clean_df = prior_silvers["clean_events"]
            return (
                clean_df.withColumn("hour_of_day", F.hour("timestamp"))
                .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
                .withColumn(
                    "user_activity_level",
                    F.when(F.col("action") == "purchase", "high")
                    .when(F.col("action") == "click", "medium")
                    .otherwise("low"),
                )
            )

        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events",
            transform=enriched_events,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "hour_of_day": [F.col("hour_of_day").isNotNull()],
                "is_purchase": [F.col("is_purchase").isNotNull()],
                "is_weekend": [F.col("is_weekend").isNotNull()],
                "user_activity_level": [F.col("user_activity_level").isNotNull()],
            },
            table_name="enriched_events",
            watermark_col="timestamp",
            source_silvers=["clean_events"],
        )

        # Add gold step
        print("🟨 Adding Gold step...")

        def daily_analytics(spark, silvers):
            """Create daily analytics from silver data."""
            F = get_default_functions()
            enriched_df = silvers["enriched_events"]
            return (
                enriched_df.groupBy("event_date", "user_activity_level")
                .agg(
                    F.count("*").alias("event_count"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias(
                        "purchases"
                    ),
                )
                .withColumn(
                    "conversion_rate", F.col("purchases") / F.col("event_count")
                )
            )

        builder.add_gold_transform(
            name="daily_analytics",
            transform=daily_analytics,
            rules={
                "event_date": [F.col("event_date").isNotNull()],
                "user_activity_level": [F.col("user_activity_level").isNotNull()],
                "event_count": [F.col("event_count") > 0],
                "unique_users": [F.col("unique_users") > 0],
                "purchases": [F.col("purchases") >= 0],
                "conversion_rate": [F.col("conversion_rate") >= 0],
            },
            table_name="daily_analytics",
            source_silvers=["enriched_events"],
        )

        # Build and run pipeline
        print("🚀 Building pipeline...")
        pipeline = builder.to_pipeline()

        print("📊 Running pipeline...")
        result = pipeline.run_initial_load(bronze_sources={"events": events_df})

        # Display results
        print("\n✅ Pipeline execution completed!")
        print(f"📈 Status: {result.status.value}")
        print(f"📈 Total steps: {result.metrics.total_steps}")
        print(f"📈 Successful steps: {result.metrics.successful_steps}")
        print(f"📈 Failed steps: {result.metrics.failed_steps}")
        print(f"⏱️  Total duration: {result.duration_seconds:.2f}s")

        # Show step information
        print("\n🔍 Pipeline steps:")
        print(f"  Bronze steps: {list(builder.bronze_steps.keys())}")
        print(f"  Silver steps: {list(builder.silver_steps.keys())}")
        print(f"  Gold steps: {list(builder.gold_steps.keys())}")

        # Display sample results
        print("\n📋 Sample results from enriched_events:")
        try:
            spark.table("analytics.enriched_events").show(5, truncate=False)
        except Exception as e:
            print(f"⚠️  Could not display: {e}")

        print("\n📋 Sample results from daily_analytics:")
        try:
            spark.table("analytics.daily_analytics").show(5, truncate=False)
        except Exception as e:
            print(f"⚠️  Could not display: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
