#!/usr/bin/env python3
"""
SparkForge Quick Start Example

This example demonstrates the basic usage of SparkForge for data pipeline processing.
It shows how to create a simple bronze-to-silver-to-gold pipeline with data validation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions


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

    # Configure engine (required!)
    configure_engine(spark=spark)
    F = get_default_functions()

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

        # Build pipeline
        print("\n🔨 Building pipeline...")
        builder = PipelineBuilder(
            spark=spark,
            schema="quickstart",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
        )

        # Bronze: Validate raw orders
        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "quantity": [F.col("quantity") > 0],
                "price": [F.col("price") > 0],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Silver: Clean orders
        def clean_orders(spark, bronze_df, prior_silvers):
            F = get_default_functions()
            return bronze_df.filter(F.col("quantity") > 0)

        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="raw_orders",
            transform=clean_orders,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "quantity": [F.col("quantity") > 0],
                "price": [F.col("price") > 0],
            },
            table_name="clean_orders",
        )

        # Gold: Order summary
        def order_summary(spark, silvers):
            F = get_default_functions()
            return (
                silvers["clean_orders"]
                .groupBy("product_id")
                .agg(
                    F.count("*").alias("total_orders"),
                    F.sum("quantity").alias("total_quantity"),
                    F.avg("price").alias("avg_price"),
                    F.max("price").alias("max_price"),
                    F.min("price").alias("min_price"),
                )
            )

        builder.add_gold_transform(
            name="order_summary",
            transform=order_summary,
            rules={
                "product_id": [F.col("product_id").isNotNull()],
                "total_orders": [F.col("total_orders") > 0],
                "total_quantity": [F.col("total_quantity") > 0],
            },
            table_name="order_summary",
            source_silvers=["clean_orders"],
        )

        # Create and execute pipeline
        pipeline = builder.to_pipeline()
        print("Pipeline built successfully!")

        # Execute pipeline
        print("\n▶️ Executing pipeline...")
        result = pipeline.run_initial_load(bronze_sources={"raw_orders": raw_df})

        # Display results
        print("\n📈 Pipeline Results:")
        print("-" * 30)
        print(f"Status: {result.status.value}")
        print(f"Total rows written: {result.metrics.total_rows_written}")
        print(f"Execution time: {result.duration_seconds:.2f}s")

        # Show final gold data
        print("\n🏆 Final Gold Data (Order Summary):")
        try:
            gold_table = spark.table("quickstart.order_summary")
            gold_table.show(truncate=False)
        except Exception as e:
            print(f"⚠️  Could not display table: {e}")

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
