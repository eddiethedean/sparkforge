#!/usr/bin/env python3
"""
Step-by-Step Debugging Example

This example demonstrates how to debug individual pipeline steps using SparkForge's
step-by-step execution capabilities.
"""

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions


def create_sample_data(spark):
    """Create sample data with some quality issues for debugging."""

    # Create data with intentional quality issues
    data = []
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    for i in range(100):
        # Add some invalid data for testing validation
        if i % 20 == 0:  # 5% invalid user_id
            user_id = None
        elif i % 15 == 0:  # Some negative amounts
            amount = -random.uniform(10, 100)
        else:
            user_id = f"user_{i:03d}"
            amount = random.uniform(10, 500)

        data.append(
            {
                "user_id": user_id,
                "product_id": f"prod_{i % 20:03d}",
                "amount": amount,
                "category": random.choice(["Electronics", "Clothing", "Books", "Home"]),
                "timestamp": base_time + timedelta(hours=i),
                "region": random.choice(["North", "South", "East", "West"]),
            }
        )

    return spark.createDataFrame(data)


def main():
    """Main function demonstrating step-by-step debugging."""

    print("🔍 Step-by-Step Debugging Example")
    print("=" * 50)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("Step-by-Step Debugging")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Configure engine (required!)
    configure_engine(spark=spark)
    F = get_default_functions()

    try:
        # Create sample data
        print("📊 Creating sample data with quality issues...")
        sample_df = create_sample_data(spark)
        print(f"Created {sample_df.count()} records")

        # Show data quality issues
        print("\n📋 Data Quality Issues:")
        F = get_default_functions()
        print(
            f"Records with null user_id: {sample_df.filter(F.col('user_id').isNull()).count()}"
        )
        print(
            f"Records with negative amount: {sample_df.filter(F.col('amount') < 0).count()}"
        )

        # Build pipeline
        print("\n🏗️ Building pipeline...")
        builder = PipelineBuilder(
            spark=spark,
            schema="debug_demo",
            min_bronze_rate=90.0,  # Lower threshold to allow some issues
            min_silver_rate=95.0,
            min_gold_rate=98.0,
            verbose=True,
        )

        # Bronze Layer
        builder.with_bronze_rules(
            name="transactions",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "amount": [F.col("amount") > 0],
                "category": [F.col("category").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Silver Layer
        def process_transactions(spark, bronze_df, prior_silvers):
            F = get_default_functions()
            return (
                bronze_df.withColumn("processed_at", F.current_timestamp())
                .withColumn(
                    "amount_category",
                    F.when(F.col("amount") > 200, "high_value")
                    .when(F.col("amount") > 100, "medium_value")
                    .otherwise("low_value"),
                )
                .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
                .filter(F.col("amount") > 0)  # Remove negative amounts
            )

        builder.add_silver_transform(
            name="processed_transactions",
            source_bronze="transactions",
            transform=process_transactions,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "amount": [F.col("amount") > 0],
                "category": [F.col("category").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "processed_at": [F.col("processed_at").isNotNull()],
                "amount_category": [F.col("amount_category").isNotNull()],
                "is_weekend": [F.col("is_weekend").isNotNull()],
            },
            table_name="processed_transactions",
            watermark_col="timestamp",
        )

        # Gold Layer
        def daily_summary(spark, silvers):
            F = get_default_functions()
            transactions_df = silvers["processed_transactions"]
            return transactions_df.groupBy(
                "category", F.date_trunc("day", "timestamp").alias("date")
            ).agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.countDistinct("user_id").alias("unique_users"),
            )

        builder.add_gold_transform(
            name="daily_summary",
            transform=daily_summary,
            rules={
                "category": [F.col("category").isNotNull()],
                "date": [F.col("date").isNotNull()],
                "transaction_count": [F.col("transaction_count") > 0],
                "total_amount": [F.col("total_amount") > 0],
                "avg_amount": [F.col("avg_amount") > 0],
                "unique_users": [F.col("unique_users") > 0],
            },
            table_name="daily_summary",
            source_silvers=["processed_transactions"],
        )

        # Build pipeline
        pipeline = builder.to_pipeline()

        print("\n📋 Available steps:")
        print(f"Bronze: {list(builder.bronze_steps.keys())}")
        print(f"Silver: {list(builder.silver_steps.keys())}")
        print(f"Gold: {list(builder.gold_steps.keys())}")

        # Step 1: Validate pipeline
        print("\n" + "=" * 60)
        print("🔍 STEP 1: Validating Pipeline")
        print("=" * 60)

        errors = builder.validate_pipeline()
        if errors:
            print("❌ Pipeline validation errors:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("✅ Pipeline validation passed!")

        # Step 2: Full pipeline execution
        print("\n" + "=" * 60)
        print("🚀 STEP 2: Full Pipeline Execution")
        print("=" * 60)

        # Run full pipeline
        result = pipeline.run_initial_load(bronze_sources={"transactions": sample_df})

        print(f"Pipeline status: {result.status.value}")
        print(f"Total rows written: {result.metrics.total_rows_written}")
        print(f"Total duration: {result.duration_seconds:.2f}s")

        if result.status.value == "completed":
            print("\n✅ Full pipeline completed successfully!")

            # Show final results
            print("\n📊 Final Results:")
            try:
                spark.table("debug_demo.daily_summary").show()
            except Exception as e:
                print(f"⚠️  Could not display table: {e}")
        else:
            print("\n❌ Pipeline failed")
            if hasattr(result, "errors") and result.errors:
                print(f"Errors: {result.errors}")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        try:
            spark.sql("DROP DATABASE IF EXISTS debug_demo CASCADE")
            spark.stop()
            print("\n🧹 Cleanup completed")
        except Exception:
            pass


if __name__ == "__main__":
    main()
