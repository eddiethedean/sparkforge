#!/usr/bin/env python3
"""
Step-by-Step Debugging Example

This example demonstrates how to debug individual pipeline steps using SparkForge's
step-by-step execution capabilities.
"""

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkforge import PipelineBuilder


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

    try:
        # Create sample data
        print("📊 Creating sample data with quality issues...")
        sample_df = create_sample_data(spark)
        print(f"Created {sample_df.count()} records")

        # Show data quality issues
        print("\n📋 Data Quality Issues:")
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
                "processed_at": [F.col("processed_at").isNotNull()],
                "amount_category": [F.col("amount_category").isNotNull()],
            },
            table_name="processed_transactions",
            watermark_col="timestamp",
        )

        # Gold Layer
        def daily_summary(spark, silvers):
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
                "total_amount": [F.col("total_amount") > 0],
            },
            table_name="daily_summary",
            source_silvers=["processed_transactions"],
        )

        # Build pipeline
        pipeline = builder.to_pipeline()

        print("\n📋 Available steps:")
        steps = pipeline.list_steps()
        print(f"Bronze: {steps['bronze']}")
        print(f"Silver: {steps['silver']}")
        print(f"Gold: {steps['gold']}")

        # Step 1: Debug Bronze step
        print("\n" + "=" * 60)
        print("🥉 STEP 1: Debugging Bronze Layer")
        print("=" * 60)

        bronze_result = pipeline.execute_bronze_step(
            "transactions", input_data=sample_df
        )
        print(f"Bronze execution status: {bronze_result.status.value}")
        print(
            f"Bronze validation passed: {bronze_result.validation_result.validation_passed}"
        )
        print(
            f"Bronze validation rate: {bronze_result.validation_result.validation_rate:.2f}%"
        )
        print(f"Bronze output rows: {bronze_result.output_count}")
        print(f"Bronze duration: {bronze_result.duration_seconds:.2f}s")

        if not bronze_result.validation_result.validation_passed:
            print("\n❌ Bronze validation failed!")
            print("Validation errors:")
            for error in bronze_result.validation_result.validation_errors:
                print(f"  - {error}")

        # Step 2: Debug Silver step
        print("\n" + "=" * 60)
        print("🥈 STEP 2: Debugging Silver Layer")
        print("=" * 60)

        silver_result = pipeline.execute_silver_step("processed_transactions")
        print(f"Silver execution status: {silver_result.status.value}")
        print(
            f"Silver validation passed: {silver_result.validation_result.validation_passed}"
        )
        print(
            f"Silver validation rate: {silver_result.validation_result.validation_rate:.2f}%"
        )
        print(f"Silver output rows: {silver_result.output_count}")
        print(f"Silver duration: {silver_result.duration_seconds:.2f}s")

        if silver_result.status.value == "completed":
            print("\n✅ Silver step completed successfully!")

            # Inspect Silver output
            print("\n📊 Silver output sample:")
            executor = pipeline.create_step_executor()
            silver_output = executor.get_step_output("processed_transactions")
            silver_output.show(10)

            # Check data quality
            print("\n📈 Silver data quality:")
            print(
                f"High value transactions: {silver_output.filter(F.col('amount_category') == 'high_value').count()}"
            )
            print(
                f"Weekend transactions: {silver_output.filter(F.col('is_weekend') is True).count()}"
            )

        # Step 3: Debug Gold step
        print("\n" + "=" * 60)
        print("🥇 STEP 3: Debugging Gold Layer")
        print("=" * 60)

        gold_result = pipeline.execute_gold_step("daily_summary")
        print(f"Gold execution status: {gold_result.status.value}")
        print(
            f"Gold validation passed: {gold_result.validation_result.validation_passed}"
        )
        print(
            f"Gold validation rate: {gold_result.validation_result.validation_rate:.2f}%"
        )
        print(f"Gold output rows: {gold_result.output_count}")
        print(f"Gold duration: {gold_result.duration_seconds:.2f}s")

        if gold_result.status.value == "completed":
            print("\n✅ Gold step completed successfully!")

            # Inspect Gold output
            print("\n📊 Gold output sample:")
            gold_output = executor.get_step_output("daily_summary")
            gold_output.show()

            # Show summary statistics
            print("\n📈 Summary statistics:")
            gold_output.select(
                F.sum("transaction_count").alias("total_transactions"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("avg_amount").alias("overall_avg_amount"),
            ).show()

        # Step 4: Get step information
        print("\n" + "=" * 60)
        print("ℹ️ STEP 4: Step Information")
        print("=" * 60)

        for step_name in ["transactions", "processed_transactions", "daily_summary"]:
            step_info = pipeline.get_step_info(step_name)
            print(f"\n{step_name}:")
            print(f"  Type: {step_info['type']}")
            print(f"  Dependencies: {step_info['dependencies']}")
            print(f"  Dependents: {step_info['dependents']}")

        # Step 5: Check execution state
        print("\n" + "=" * 60)
        print("📊 STEP 5: Execution State")
        print("=" * 60)

        completed_steps = executor.list_completed_steps()
        failed_steps = executor.list_failed_steps()

        print(f"Completed steps: {completed_steps}")
        print(f"Failed steps: {failed_steps}")

        # Step 6: Full pipeline execution
        print("\n" + "=" * 60)
        print("🚀 STEP 6: Full Pipeline Execution")
        print("=" * 60)

        # Clear execution state for fresh run
        executor.clear_execution_state()

        # Run full pipeline
        result = pipeline.initial_load(bronze_sources={"transactions": sample_df})

        print(f"Pipeline success: {result.success}")
        print(f"Total rows written: {result.totals['total_rows_written']}")
        print(f"Total duration: {result.totals['total_duration_secs']:.2f}s")

        if result.success:
            print("\n✅ Full pipeline completed successfully!")

            # Show final results
            print("\n📊 Final Results:")
            spark.table("debug_demo.daily_summary").show()
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
            spark.sql("DROP DATABASE IF EXISTS debug_demo CASCADE")
            spark.stop()
            print("\n🧹 Cleanup completed")
        except:
            pass


if __name__ == "__main__":
    main()
