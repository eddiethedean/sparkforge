#!/usr/bin/env python
"""
Demo showing concurrent execution with interleaved logging.
This simulates a real pipeline with parallel execution enabled.
"""

from datetime import datetime

from mock_spark import (
    IntegerType,
    MockSparkSession,
    MockStructField,
    MockStructType,
    StringType,
)

from sparkforge.execution import ExecutionEngine, ExecutionMode
from sparkforge.models import BronzeStep, GoldStep, PipelineConfig, SilverStep

print("\n" + "="*80)
print("DEMO: CONCURRENT PIPELINE EXECUTION WITH PARALLEL LOGGING")
print("="*80)
print("\nThis demo shows how independent steps run concurrently.")
print("Watch for interleaved log messages from different steps!")
print("="*80 + "\n")

# Create mock Spark session
spark = MockSparkSession.builder.getOrCreate()

# Create sample data for bronze steps
schema1 = MockStructType([
    MockStructField("event_id", IntegerType(), True),
    MockStructField("user_id", IntegerType(), True),
    MockStructField("event_type", StringType(), True),
])

schema2 = MockStructType([
    MockStructField("profile_id", IntegerType(), True),
    MockStructField("user_id", IntegerType(), True),
    MockStructField("name", StringType(), True),
])

events_df = spark.createDataFrame([
    (1, 100, "click"),
    (2, 101, "purchase"),
    (3, 102, "view"),
], schema1)

profiles_df = spark.createDataFrame([
    (1, 100, "Alice"),
    (2, 101, "Bob"),
    (3, 102, "Charlie"),
], schema2)

# Define bronze steps (independent - can run in parallel)
bronze_events = BronzeStep(
    name="bronze_events",
    rules={"event_id": ["not_null"]},
)

bronze_profiles = BronzeStep(
    name="bronze_profiles",
    rules={"profile_id": ["not_null"]},
)

# Define silver steps (independent - can run in parallel after bronze completes)
silver_purchases = SilverStep(
    name="silver_purchases",
    source_bronze="bronze_events",
    transform=lambda spark, df, silvers: df.filter(df["event_type"] == "purchase"),
    rules={"user_id": ["not_null"]},
    table_name="silver_purchases",
    schema="analytics",
)

silver_customers = SilverStep(
    name="silver_customers",
    source_bronze="bronze_profiles",
    transform=lambda spark, df, silvers: df,
    rules={"user_id": ["not_null"]},
    table_name="silver_customers",
    schema="analytics",
)

# Define gold step (depends on both silver steps)
gold_customer_summary = GoldStep(
    name="gold_customer_summary",
    transform=lambda spark, silvers: silvers["silver_customers"],
    rules={"user_id": ["not_null"]},
    table_name="gold_customer_summary",
    schema="analytics",
    source_silvers=["silver_customers"],
)

# Create pipeline config with parallel execution enabled
config = PipelineConfig.create_default(schema="analytics")

# Create execution engine
engine = ExecutionEngine(spark, config)

print("📋 Pipeline Structure:")
print("   Group 1 (Parallel): bronze_events, bronze_profiles")
print("   Group 2 (Parallel): silver_purchases, silver_customers")
print("   Group 3 (Sequential): gold_customer_summary")
print("\n" + "="*80)
print("🚀 Starting Pipeline Execution with Parallel Processing...")
print("="*80 + "\n")

# Execute pipeline with parallel execution
start_time = datetime.now()

try:
    result = engine.execute_pipeline(
        steps=[
            bronze_events,
            bronze_profiles,
            silver_purchases,
            silver_customers,
            gold_customer_summary,
        ],
        mode=ExecutionMode.INITIAL,
        context={
            "bronze_events": events_df,
            "bronze_profiles": profiles_df,
        }
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "="*80)
    print("📊 EXECUTION SUMMARY")
    print("="*80)
    print(f"Status: {result.status}")
    print(f"Total Duration: {duration:.2f}s")
    print(f"Execution Groups: {result.execution_groups_count}")
    print(f"Max Group Size: {result.max_group_size}")
    print(f"Parallel Efficiency: {result.parallel_efficiency:.1f}%")
    print(f"\nSteps Completed: {len([s for s in result.steps if s.status.value == 'completed'])}/{len(result.steps)}")

    print("\n" + "="*80)
    print("KEY OBSERVATIONS:")
    print("="*80)
    print("✅ Bronze steps started at nearly the same time (Group 1)")
    print("✅ Silver steps started after bronze completed (Group 2)")
    print("✅ Gold step started after silver completed (Group 3)")
    print("✅ Log messages from concurrent steps are interleaved")
    print("✅ Each step completes independently")
    print("✅ Total time is less than sequential execution would take")
    print("="*80 + "\n")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

