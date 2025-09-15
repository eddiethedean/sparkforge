#!/usr/bin/env python3
"""
Step-by-Step Execution Example

This example demonstrates how to use the new step-by-step execution feature
for troubleshooting and debugging pipeline steps independently.
"""


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge import PipelineBuilder


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
    """Demonstrate step-by-step execution."""
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
        source_data = create_sample_data(spark)
        print(f"📊 Created sample data: {source_data.count()} rows")
        source_data.show()

        # Build pipeline
        print("\n🔧 Building pipeline...")
        builder = PipelineBuilder(
            spark=spark, schema="troubleshooting_schema", verbose=True
        )

        # Define transforms
        def silver_transform(spark, bronze_df):
            return (
                bronze_df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("processed_at", F.current_timestamp())
                .select("user_id", "action", "value", "event_date", "processed_at")
            )

        def gold_transform(spark, silvers):
            events_df = silvers["silver_events"]
            return events_df.groupBy("action", "event_date").agg(
                F.count("*").alias("event_count"), F.sum("value").alias("total_value")
            )

        # Configure pipeline steps
        pipeline = (
            builder.with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                incremental_col="timestamp",
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="events",
                transform=silver_transform,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="silver_events",
                watermark_col="processed_at",
            )
            .add_gold_transform(
                name="gold_summary",
                transform=gold_transform,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="gold_summary",
                source_silvers=["silver_events"],
            )
            .to_pipeline()
        )

        print("✅ Pipeline built successfully!")

        # ========================================================================
        # STEP-BY-STEP EXECUTION DEMONSTRATION
        # ========================================================================

        print("\n🔍 Step-by-Step Execution Demo")
        print("-" * 30)

        # 1. List all available steps
        print("\n1️⃣ Available steps:")
        steps = pipeline.list_steps()
        for step_type, step_names in steps.items():
            print(f"   {step_type.upper()}: {step_names}")

        # 2. Get detailed information about a step
        print("\n2️⃣ Step information:")
        step_info = pipeline.get_step_info("silver_events")
        if step_info:
            print(f"   Step: {step_info['name']}")
            print(f"   Type: {step_info['type']}")
            print(f"   Dependencies: {step_info['dependencies']}")
            print(f"   Dependents: {step_info['dependents']}")

        # 3. Execute Bronze step
        print("\n3️⃣ Executing Bronze step...")
        bronze_result = pipeline.execute_bronze_step(
            step_name="events",
            input_data=source_data,
            output_to_table=False,  # Don't write to table for demo
        )

        print(f"   Status: {bronze_result.status.value}")
        print(f"   Duration: {bronze_result.duration_seconds:.2f}s")
        print(f"   Output rows: {bronze_result.output_count}")
        if bronze_result.validation_result:
            print(
                f"   Validation passed: {bronze_result.validation_result.validation_passed}"
            )
        if bronze_result.error:
            print(f"   Error: {bronze_result.error}")

        # 4. Execute Silver step
        print("\n4️⃣ Executing Silver step...")
        silver_result = pipeline.execute_silver_step(
            step_name="silver_events",
            output_to_table=False,  # Don't write to table for demo
        )

        print(f"   Status: {silver_result.status.value}")
        print(f"   Duration: {silver_result.duration_seconds:.2f}s")
        print(f"   Output rows: {silver_result.output_count}")
        if silver_result.validation_result:
            print(
                f"   Validation passed: {silver_result.validation_result.validation_passed}"
            )
        if silver_result.error:
            print(f"   Error: {silver_result.error}")

        # 5. Inspect Silver output
        print("\n5️⃣ Inspecting Silver output:")
        silver_output = pipeline.create_step_executor().get_step_output("silver_events")
        if silver_output:
            print("   Silver output schema:")
            silver_output.printSchema()
            print("   Silver output data:")
            silver_output.show()

        # 6. Execute Gold step
        print("\n6️⃣ Executing Gold step...")
        gold_result = pipeline.execute_gold_step(
            step_name="gold_summary",
            output_to_table=False,  # Don't write to table for demo
        )

        print(f"   Status: {gold_result.status.value}")
        print(f"   Duration: {gold_result.duration_seconds:.2f}s")
        print(f"   Output rows: {gold_result.output_count}")
        if gold_result.validation_result:
            print(
                f"   Validation passed: {gold_result.validation_result.validation_passed}"
            )
        if gold_result.error:
            print(f"   Error: {gold_result.error}")

        # 7. Inspect Gold output
        print("\n7️⃣ Inspecting Gold output:")
        gold_output = pipeline.create_step_executor().get_step_output("gold_summary")
        if gold_output:
            print("   Gold output schema:")
            gold_output.printSchema()
            print("   Gold output data:")
            gold_output.show()

        # 8. Demonstrate troubleshooting scenario
        print("\n8️⃣ Troubleshooting scenario:")
        print(
            "   Let's say we want to modify the Silver transform and re-run just that step..."
        )

        # Create a modified Silver transform
        def modified_silver_transform(spark, bronze_df):
            return (
                bronze_df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("processed_at", F.current_timestamp())
                .withColumn("is_high_value", F.col("value") > 200)  # New column
                .select(
                    "user_id",
                    "action",
                    "value",
                    "event_date",
                    "processed_at",
                    "is_high_value",
                )
            )

        # Update the Silver step with new transform
        pipeline.silver_steps["silver_events"].transform = modified_silver_transform

        # Re-execute just the Silver step
        print("   Re-executing Silver step with modified transform...")
        silver_result_2 = pipeline.execute_silver_step(
            step_name="silver_events",
            output_to_table=False,  # Don't write to table for demo
        )

        print(f"   Status: {silver_result_2.status.value}")
        print(f"   Duration: {silver_result_2.duration_seconds:.2f}s")
        print(f"   Output rows: {silver_result_2.output_count}")

        # Show the modified output
        print("   Modified Silver output:")
        modified_silver_output = pipeline.create_step_executor().get_step_output(
            "silver_events"
        )
        if modified_silver_output:
            modified_silver_output.show()

        # 9. Show execution state
        print("\n9️⃣ Execution state summary:")
        executor = pipeline.create_step_executor()
        execution_state = executor.get_execution_state()

        for step_name, result in execution_state.items():
            print(
                f"   {step_name}: {result.status.value} ({result.duration_seconds:.2f}s)"
            )

        print("\n✅ Step-by-step execution demo completed!")
        print("\n💡 Key benefits:")
        print("   • Debug individual steps without running the full pipeline")
        print("   • Inspect intermediate outputs at each stage")
        print("   • Modify and re-run specific steps for testing")
        print("   • Troubleshoot issues without losing previous work")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
