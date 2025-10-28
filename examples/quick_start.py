#!/usr/bin/env python3
"""
SparkForge Quick Start Example

This example demonstrates the basic usage of SparkForge for data pipeline processing.
It shows how to create a simple bronze-to-silver-to-gold pipeline with data validation.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pipeline_builder.models import PipelineConfig
from pipeline_builder.pipeline.builder import PipelineBuilder


def main():
    """Run the quick start example."""
    print("🚀 SparkForge Quick Start Example")
    print("=" * 40)

    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("SparkForgeQuickStart")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create sample data
        print("\n📊 Creating sample data...")
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

        data = [
            ("user1", "prod1", 2, 29.99, "2024-01-01 10:00:00"),
            ("user2", "prod2", 1, 49.99, "2024-01-01 10:15:00"),
            ("user3", "prod1", 3, 29.99, "2024-01-01 10:30:00"),
            ("user4", "prod3", 1, 19.99, "2024-01-01 10:45:00"),
            ("user5", "prod2", 2, 49.99, "2024-01-01 11:00:00"),
        ]

        raw_df = spark.createDataFrame(data, schema)
        print(f"Created {raw_df.count()} records")

        # Configure pipeline
        print("\n⚙️ Configuring pipeline...")
        config = PipelineConfig.create_default("quickstart")
        print(f"Using schema: {config.schema}")
        print(
            f"Validation thresholds: Bronze={config.min_bronze_rate}%, Silver={config.min_silver_rate}%, Gold={config.min_gold_rate}%"
        )

        # Build pipeline
        print("\n🔨 Building pipeline...")
        pipeline = (
            PipelineBuilder(config)
            .add_bronze_step("raw_orders", raw_df)
            .add_silver_step(
                "clean_orders",
                "raw_orders",
                transform_func=lambda df: df.filter(F.col("quantity") > 0),
                validation_rules={
                    "user_id": ["not_null"],
                    "product_id": ["not_null"],
                    "quantity": ["positive"],
                    "price": ["positive"],
                },
            )
            .add_gold_step(
                "order_summary",
                "clean_orders",
                transform_func=lambda df: df.groupBy("product_id").agg(
                    F.count("*").alias("total_orders"),
                    F.sum("quantity").alias("total_quantity"),
                    F.avg("price").alias("avg_price"),
                    F.max("price").alias("max_price"),
                    F.min("price").alias("min_price"),
                ),
            )
            .build()
        )

        print("Pipeline built successfully!")

        # Execute pipeline
        print("\n▶️ Executing pipeline...")
        results = pipeline.execute()

        # Display results
        print("\n📈 Pipeline Results:")
        print("-" * 30)

        for step_name, result in results.items():
            print(f"\n{step_name.upper()}:")
            if hasattr(result, "count"):
                count = result.count()
                print(f"  Records: {count}")
                if count > 0:
                    print("  Sample data:")
                    result.show(5, truncate=False)
            else:
                print(f"  Result: {result}")

        # Show final gold data
        print("\n🏆 Final Gold Data (Order Summary):")
        gold_data = results.get("order_summary")
        if gold_data:
            gold_data.show(truncate=False)

        print("\n✅ Quick start example completed successfully!")

    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        raise

    finally:
        # Clean up
        spark.stop()
        print("\n🧹 Spark session stopped.")


if __name__ == "__main__":
    main()
