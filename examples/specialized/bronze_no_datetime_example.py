#!/usr/bin/env python3
"""
Bronze Table Without Datetime Column Example

This example demonstrates how to use SparkForge with Bronze tables that don't have
datetime columns. In this case, Silver tables will be forced to use overwrite mode
for full refresh on each run, which is less efficient but necessary for data sources
without temporal information.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge import PipelineBuilder


def main():
    """Create and run a pipeline with Bronze table without datetime column."""

    # Create Spark session
    spark = (
        SparkSession.builder.appName("SparkForgeNoDatetimeExample")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    try:
        # Create sample data WITHOUT datetime columns
        sample_data = [
            ("user1", "click", 100, "mobile"),
            ("user2", "view", 200, "desktop"),
            ("user3", "purchase", 300, "mobile"),
            ("user1", "click", 150, "tablet"),
            ("user2", "purchase", 250, "desktop"),
            ("user4", "view", 180, "mobile"),
            ("user5", "click", 220, "desktop"),
        ]

        # Define schema without datetime
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("device", StringType(), True),
            ]
        )

        source_df = spark.createDataFrame(sample_data, schema)

        print("🔧 Building SparkForge Pipeline (No Datetime Bronze)...")
        print("   Note: Silver tables will use overwrite mode for full refresh")

        # PipelineBuilder will create its own configuration

        # Build the pipeline
        builder = PipelineBuilder(
            spark=spark, schema="no_datetime_schema", verbose=True
        )

        # Bronze Layer: Raw data ingestion WITHOUT incremental column
        # This forces Silver tables to use overwrite mode
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0],
                "device": [F.col("device").isNotNull()],
            }
            # Note: No incremental_col parameter - this forces full refresh
        )

        # Silver Layer: Cleaned and enriched data
        # This will use overwrite mode because Bronze has no incremental column
        def silver_transform(spark, bronze_df, prior_silvers):
            return (
                bronze_df.withColumn("processed_at", F.current_timestamp())
                .withColumn("event_date", F.current_date())
                .withColumn(
                    "device_category",
                    F.when(
                        F.col("device").isin("mobile", "tablet"), "mobile"
                    ).otherwise("desktop"),
                )
                .withColumn(
                    "value_category",
                    F.when(F.col("value") < 100, "low")
                    .when(F.col("value") < 200, "medium")
                    .otherwise("high"),
                )
            )

        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "processed_at": [F.col("processed_at").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "device_category": [F.col("device_category").isNotNull()],
            },
            table_name="enriched_events",
            watermark_col="processed_at",
            description="Enriched event data with processing metadata (full refresh mode)",
        )

        # Gold Layer: Business analytics
        def gold_transform(spark, silvers):
            enriched_df = silvers["enriched_events"]
            return (
                enriched_df.groupBy("device_category", "action")
                .agg(
                    F.count("*").alias("total_events"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.max("processed_at").alias("last_processed"),
                )
                .orderBy("device_category", "action")
            )

        builder.add_gold_transform(
            name="device_analytics",
            source_silvers=["enriched_events"],
            transform=gold_transform,
            rules={
                "device_category": [F.col("device_category").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "total_events": [F.col("total_events") > 0],
                "unique_users": [F.col("unique_users") > 0],
            },
            table_name="device_analytics",
            description="Device-based analytics and business metrics",
        )

        # Create the pipeline
        pipeline = builder.to_pipeline()
        print(f"✅ Pipeline created with {len(pipeline.bronze_steps)} Bronze steps")

        # Create runner and execute
        runner = pipeline

        # Clean up and create the database
        spark.sql("DROP DATABASE IF EXISTS no_datetime_schema CASCADE")
        spark.sql("CREATE DATABASE no_datetime_schema")

        print("🚀 Running pipeline (initial load)...")
        result = runner.initial_load(bronze_sources={"events_no_datetime": source_df})

        # Display results
        print("\n📊 Pipeline Results:")
        print(f"   Status: {result.status}")
        print(f"   Total rows processed: {result.metrics.total_rows_processed}")
        print(f"   Total rows written: {result.metrics.total_rows_written}")
        print(f"   Execution time: {result.metrics.duration_secs:.2f}s")

        # Show the final Gold table
        print("\n📈 Device Analytics (Gold Layer):")
        gold_table = spark.table("no_datetime_schema.device_analytics")
        gold_table.show(truncate=False)

        # Show Silver table
        print("\n🔍 Enriched Events (Silver Layer):")
        silver_table = spark.table("no_datetime_schema.enriched_events")
        silver_table.show(5, truncate=False)

        # Demonstrate that incremental mode still uses overwrite for Silver
        print("\n🔄 Running incremental mode (Silver will still use overwrite)...")
        result2 = runner.run_incremental(
            bronze_sources={"events_no_datetime": source_df}
        )

        print(f"   Incremental run status: {result2.status}")
        print(
            "   Silver write mode: overwrite (forced by Bronze without incremental column)"
        )

        print("\n🎉 Pipeline completed successfully!")
        print("   Key insight: Bronze tables without datetime columns force")
        print("   Silver tables to use overwrite mode for full refresh.")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
