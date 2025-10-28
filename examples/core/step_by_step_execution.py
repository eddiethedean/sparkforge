#!/usr/bin/env python3
"""
Step-by-Step Execution Example

This example demonstrates how to use the simplified execution system
for troubleshooting and debugging pipeline steps independently.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pipeline_builder import PipelineBuilder


def create_sample_data(spark):
    """Create sample data for the example."""
    data = [
        ("user1", "click", 100, "2024-01-01 10:00:00"),
        ("user2", "view", 200, "2024-01-01 11:00:00"),
        ("user3", "purchase", 300, "2024-01-01 12:00:00"),
        ("user4", "click", 150, "2024-01-01 13:00:00"),
        ("user5", "view", 250, "2024-01-01 14:00:00"),
    ]

    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def main():
    """Demonstrate step-by-step execution with the simplified system."""
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("StepByStepExecutionExample")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    try:
        print("🚀 Step-by-Step Execution Example")
        print("=" * 50)

        # Create sample data
        source_df = create_sample_data(spark)
        print("\n📊 Sample Data:")
        source_df.show()

        # Create database
        spark.sql("DROP DATABASE IF EXISTS step_by_step_schema CASCADE")
        spark.sql("CREATE DATABASE step_by_step_schema")

        print("\n🔧 Building pipeline...")

        # Build the pipeline
        builder = PipelineBuilder(spark=spark, schema="step_by_step_schema")

        # Bronze step: Validate raw data
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 0],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Silver step: Clean and enrich data
        def silver_transform(spark, df, silvers):
            return (
                df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("processed_at", F.current_timestamp())
                .filter(F.col("value") > 50)
                .select("user_id", "action", "value", "event_date", "processed_at")
            )

        builder.add_silver_transform(
            name="silver_events",
            source_bronze="events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 50],
                "event_date": [F.col("event_date").isNotNull()],
            },
            table_name="silver_events",
        )

        # Gold step: Aggregate data
        def gold_transform(spark, silvers):
            events_df = silvers.get("silver_events")
            if events_df is not None:
                return (
                    events_df.groupBy("event_date", "action")
                    .agg(
                        F.count("*").alias("event_count"),
                        F.sum("value").alias("total_value"),
                        F.avg("value").alias("avg_value"),
                    )
                    .orderBy("event_date", "action")
                )
            else:
                return spark.createDataFrame(
                    [],
                    "event_date date, action string, event_count long, total_value long, avg_value double",
                )

        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_transform,
            rules={
                "event_date": [F.col("event_date").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "event_count": [F.col("event_count") > 0],
            },
            table_name="gold_summary",
            source_silvers=["silver_events"],
        )

        # Create pipeline
        pipeline = builder.to_pipeline()

        print("\n🔍 Pipeline Structure:")
        print(f"   Bronze steps: {list(builder.bronze_steps.keys())}")
        print(f"   Silver steps: {list(builder.silver_steps.keys())}")
        print(f"   Gold steps: {list(builder.gold_steps.keys())}")

        # Validate pipeline
        errors = builder.validate_pipeline()
        if errors:
            print(f"\n❌ Pipeline validation errors: {errors}")
            return
        else:
            print("\n✅ Pipeline validation passed")

        # Run the complete pipeline
        print("\n🚀 Running complete pipeline...")
        result = pipeline.run_initial_load(bronze_sources={"events": source_df})

        print("\n📊 Pipeline Results:")
        print(f"   Status: {result.status}")
        print(f"   Total steps: {result.total_steps}")
        print(f"   Successful steps: {result.successful_steps}")
        print(f"   Failed steps: {result.failed_steps}")

        # Show results
        try:
            print("\n🎯 Final Results (Gold Layer):")
            spark.table("step_by_step_schema.gold_summary").show()

            print("\n🔍 Intermediate Results (Silver Layer):")
            spark.table("step_by_step_schema.silver_events").show()

        except Exception as e:
            print(f"\n⚠️  Could not display results: {e}")

        # Demonstrate debugging with modified transform
        print("\n🔧 Debugging Example: Modified Silver Transform")
        print("=" * 50)

        # Create a modified silver transform for debugging
        def debug_silver_transform(spark, df, silvers):
            print("   🔍 Debug: Input data shape:", df.count(), "rows")
            print("   🔍 Debug: Input columns:", df.columns)

            # Apply the same transformation but with debug info
            result_df = (
                df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("processed_at", F.current_timestamp())
                .filter(F.col("value") > 100)  # Changed threshold for debugging
                .select("user_id", "action", "value", "event_date", "processed_at")
            )

            print("   🔍 Debug: Output data shape:", result_df.count(), "rows")
            print("   🔍 Debug: Output columns:", result_df.columns)

            return result_df

        # Create a new pipeline with the debug transform
        debug_builder = PipelineBuilder(spark=spark, schema="debug_schema")

        debug_builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        debug_builder.add_silver_transform(
            name="debug_silver_events",
            source_bronze="events",
            transform=debug_silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="debug_silver_events",
        )

        debug_pipeline = debug_builder.to_pipeline()

        print("\n🚀 Running debug pipeline...")
        debug_result = debug_pipeline.run_initial_load(
            bronze_sources={"events": source_df}
        )

        print("\n📊 Debug Pipeline Results:")
        print(f"   Status: {debug_result.status}")
        print(f"   Total steps: {debug_result.total_steps}")

        # Show debug results
        try:
            print("\n🔍 Debug Results (Silver Layer):")
            spark.table("debug_schema.debug_silver_events").show()
        except Exception as e:
            print(f"\n⚠️  Could not display debug results: {e}")

        print("\n🎉 Step-by-Step Execution Example Complete!")
        print("💡 Key takeaways:")
        print("   - Use pipeline.run_initial_load() for complete execution")
        print("   - Create separate pipelines for debugging different transforms")
        print("   - Add debug prints in transform functions for troubleshooting")
        print("   - Validate pipelines before execution")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
