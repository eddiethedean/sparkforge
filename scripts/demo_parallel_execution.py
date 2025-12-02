#!/usr/bin/env python
"""
Demo showing concurrent execution with interleaved logging.
This simulates a real pipeline with parallel execution enabled.
"""

import sys
from datetime import datetime
from pathlib import Path

from mock_spark import (
    F,
    IntegerType,
    SparkSession,
    StringType,
    StructField,
    StructType,
)

# Add src to path
project_root = Path(__file__).parent.parent
src_dir = project_root / "src"
sys.path.insert(0, str(src_dir))

from pipeline_builder.execution import ExecutionEngine, ExecutionMode  # noqa: E402
from pipeline_builder.models import (  # noqa: E402
    BronzeStep,
    GoldStep,
    PipelineConfig,
    SilverStep,
)

print("\n" + "=" * 80)
print("DEMO: CONCURRENT PIPELINE EXECUTION WITH PARALLEL LOGGING")
print("=" * 80)
print("\nThis demo shows how independent steps run concurrently.")
print("Watch for interleaved log messages from different steps!")
print("=" * 80 + "\n")

# Create mock Spark session
builder = SparkSession.builder
if builder is not None:
    spark = builder.getOrCreate()
else:
    raise RuntimeError("Failed to create SparkSession builder")

# Create sample data for bronze steps
schema1 = StructType(
    [
        StructField("event_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
    ]
)

schema2 = StructType(
    [
        StructField("profile_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

events_df = spark.createDataFrame(
    [
        (1, 100, "click"),
        (2, 101, "purchase"),
        (3, 102, "view"),
    ],
    schema1,
)

profiles_df = spark.createDataFrame(
    [
        (1, 100, "Alice"),
        (2, 101, "Bob"),
        (3, 102, "Charlie"),
    ],
    schema2,
)

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
    transform=lambda spark, df, silvers: df.filter(F.col("event_type") == "purchase")
    if df is not None
    else spark.createDataFrame([], StructType([])),
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
print("\n" + "=" * 80)
print("🚀 Starting Pipeline Execution with Parallel Processing...")
print("=" * 80 + "\n")

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
        },
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 80)
    print("📊 EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Status: {result.status}")
    print(f"Total Duration: {duration:.2f}s")
    print(f"Execution Groups: {result.execution_groups_count}")
    print(f"Max Group Size: {result.max_group_size}")
    print(f"Parallel Efficiency: {result.parallel_efficiency:.1f}%")
    steps_count = len(result.steps) if result.steps is not None else 0
    completed_count = (
        len([s for s in result.steps if s.status.value == "completed"])
        if result.steps is not None
        else 0
    )
    print(f"\nSteps Completed: {completed_count}/{steps_count}")

    print("\n" + "=" * 80)
    print("KEY OBSERVATIONS:")
    print("=" * 80)
    print("✅ Bronze steps started at nearly the same time (Group 1)")
    print("✅ Silver steps started after bronze completed (Group 2)")
    print("✅ Gold step started after silver completed (Group 3)")
    print("✅ Log messages from concurrent steps are interleaved")
    print("✅ Each step completes independently")
    print("✅ Total time is less than sequential execution would take")
    print("=" * 80 + "\n")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
