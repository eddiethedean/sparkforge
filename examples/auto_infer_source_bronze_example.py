#!/usr/bin/env python3
"""
Example demonstrating auto-inference of source_bronze in add_silver_transform.

This example shows how the new feature allows you to omit the source_bronze
parameter when adding silver transforms, making the API more convenient.
"""

from pyspark.sql import SparkSession, functions as F
from sparkforge import PipelineBuilder


def main():
    """Demonstrate auto-inference of source_bronze."""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("AutoInferSourceBronzeExample") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Create sample data
        events_data = [
            ("user1", "click", "product1", "2024-01-01 10:00:00"),
            ("user2", "purchase", "product2", "2024-01-01 11:00:00"),
            ("user3", "view", "product1", "2024-01-01 12:00:00"),
            ("user4", "click", "product3", "2024-01-01 13:00:00"),
        ]
        events_df = spark.createDataFrame(
            events_data, 
            ["user_id", "action", "product_id", "timestamp"]
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
                "timestamp": [F.col("timestamp").isNotNull()]
            },
            incremental_col="timestamp"
        )

        # Add silver step WITHOUT source_bronze - it will be auto-inferred!
        print("🟡 Adding Silver step with auto-inferred source_bronze...")
        
        def clean_events(spark, bronze_df, prior_silvers):
            """Clean and filter events data."""
            return (bronze_df
                .filter(F.col("action").isin(["click", "view", "purchase"]))
                .withColumn("event_date", F.date_trunc("day", F.col("timestamp")))
                .withColumn("is_purchase", F.col("action") == "purchase")
            )

        builder.add_silver_transform(
            name="clean_events",
            # source_bronze is omitted - will auto-infer from "events"
            transform=clean_events,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "is_purchase": [F.col("is_purchase").isNotNull()]
            },
            table_name="clean_events",
            watermark_col="timestamp"
        )

        # Add another silver step - also auto-inferred!
        print("🟡 Adding another Silver step with auto-inferred source_bronze...")
        
        def enriched_events(spark, bronze_df, prior_silvers):
            """Enrich events with additional features."""
            clean_df = prior_silvers["clean_events"]
            return (clean_df
                .withColumn("hour_of_day", F.hour("timestamp"))
                .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
                .withColumn("user_activity_level", 
                    F.when(F.col("action") == "purchase", "high")
                    .when(F.col("action") == "click", "medium")
                    .otherwise("low")
                )
            )

        builder.add_silver_transform(
            name="enriched_events",
            # source_bronze is omitted - will auto-infer from "events"
            transform=enriched_events,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "hour_of_day": [F.col("hour_of_day").isNotNull()],
                "user_activity_level": [F.col("user_activity_level").isNotNull()]
            },
            table_name="enriched_events",
            watermark_col="timestamp"
        )

        # Add gold step
        print("🟨 Adding Gold step...")
        
        def daily_analytics(spark, silvers):
            """Create daily analytics from silver data."""
            enriched_df = silvers["enriched_events"]
            return (enriched_df
                .groupBy("event_date", "user_activity_level")
                .agg(
                    F.count("*").alias("event_count"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("purchases")
                )
                .withColumn("conversion_rate", 
                    F.col("purchases") / F.col("event_count")
                )
            )

        builder.add_gold_transform(
            name="daily_analytics",
            transform=daily_analytics,
            rules={
                "event_date": [F.col("event_date").isNotNull()],
                "event_count": [F.col("event_count") > 0]
            },
            table_name="daily_analytics"
        )

        # Build and run pipeline
        print("🚀 Building pipeline...")
        pipeline = builder.to_pipeline()

        print("📊 Running pipeline...")
        result = pipeline.initial_load(bronze_sources={"events": events_df})

        # Display results
        print("\n✅ Pipeline execution completed!")
        print(f"📈 Bronze steps executed: {len(result.bronze_results)}")
        print(f"📈 Silver steps executed: {len(result.silver_results)}")
        print(f"📈 Gold steps executed: {len(result.gold_results)}")
        print(f"⏱️  Total duration: {result.totals['total_duration_secs']:.2f}s")

        # Show the auto-inferred source_bronze values
        print("\n🔍 Auto-inferred source_bronze values:")
        for name, step in pipeline.silver_steps.items():
            print(f"  - {name}: source_bronze = {step.source_bronze}")

        # Display sample results
        print("\n📋 Sample results from enriched_events:")
        enriched_df = result.silver_results["enriched_events"]["dataframe"]
        enriched_df.show(5, truncate=False)

        print("\n📋 Sample results from daily_analytics:")
        analytics_df = result.gold_results["daily_analytics"]["dataframe"]
        analytics_df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
