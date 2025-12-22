"""
Manual Step-by-Step Execution to Find Exact Failure Point

This manually executes each step of the PipelineBuilder execution flow
to identify exactly where the sparkless code fails.
"""

import os
import sys
import traceback

os.environ["SPARK_MODE"] = "mock"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from pipeline_builder.engine_config import configure_engine
from sparkless import functions as mock_functions
from sparkless import spark_types as mock_types
from sparkless import AnalysisException as MockAnalysisException
from sparkless import Window as MockWindow
from sparkless.functions import desc as mock_desc
from sparkless import DataFrame as MockDataFrame
from sparkless import SparkSession as MockSparkSession
from sparkless import Column as MockColumn

configure_engine(
    functions=mock_functions,
    types=mock_types,
    analysis_exception=MockAnalysisException,
    window=MockWindow,
    desc=mock_desc,
    engine_name="mock",
    dataframe_cls=MockDataFrame,
    spark_session_cls=MockSparkSession,
    column_cls=MockColumn,
)

from sparkless import SparkSession, functions as F
from pipeline_builder.execution import ExecutionEngine, ExecutionMode
from pipeline_builder.models import BronzeStep, SilverStep, PipelineConfig
from pipeline_builder.validation.data_validation import apply_column_rules
from datetime import datetime, timedelta

print("=" * 80)
print("MANUAL STEP-BY-STEP EXECUTION")
print("=" * 80)
print("\nManually executing each step to find exact failure point.\n")

spark = SparkSession.builder.appName("manual_execution").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Create bronze data
impressions_data = []
for i in range(10):
    impressions_data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook"][i % 2],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile"][i % 2],
    })

bronze_df = spark.createDataFrame(impressions_data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",  # incremental_col
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("STEP 1: Create bronze step")
print("=" * 80)

bronze_rules = {
    "impression_id": [F.col("impression_id").isNotNull()],
    "campaign_id": [F.col("campaign_id").isNotNull()],
    "customer_id": [F.col("customer_id").isNotNull()],
    "impression_date": [F.col("impression_date").isNotNull()],
}

bronze_step = BronzeStep(
    name="raw_impressions",
    rules=bronze_rules,
    incremental_col="impression_date",
)

print(f"✅ Bronze step created")
print(f"  incremental_col: {bronze_step.incremental_col}")

print("\nSTEP 2: Create silver step")
print("=" * 80)

def processed_impressions_transform(spark, df, silvers):
    return (
        df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
        .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
        .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
        )
    )

silver_rules = {
    "impression_id": [F.col("impression_id").isNotNull()],
    "impression_date_parsed": [F.col("impression_date_parsed").isNotNull()],
    "campaign_id": [F.col("campaign_id").isNotNull()],
    "customer_id": [F.col("customer_id").isNotNull()],
    "channel": [F.col("channel").isNotNull()],
    "cost_per_impression": [F.col("cost_per_impression") >= 0],
    "hour_of_day": [F.col("hour_of_day").isNotNull()],
    "device_type": [F.col("device_type").isNotNull()],
}

silver_step = SilverStep(
    name="processed_impressions",
    source_bronze="raw_impressions",
    transform=processed_impressions_transform,
    rules=silver_rules,
    table_name="processed_impressions",
    schema="silver",
    source_incremental_col="impression_date",  # Inherited from bronze
    watermark_col=None,  # Not set
)

print(f"✅ Silver step created")
print(f"  source_incremental_col: {silver_step.source_incremental_col}")
print(f"  watermark_col: {silver_step.watermark_col}")

print("\nSTEP 3: Execute silver step (manual execution)")
print("=" * 80)

# Create execution engine
from pipeline_builder_base.models import ValidationThresholds, ParallelConfig
config = PipelineConfig(
    schema="bronze",
    thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),
)
engine = ExecutionEngine(spark=spark, config=config, functions=F)

# Step 3a: Get bronze data from context
context = {"raw_impressions": bronze_df}
bronze_df_from_context = context[silver_step.source_bronze]
print(f"3a. Get bronze data: ✅")

# Step 3b: Incremental filtering (skipped in INITIAL mode)
mode = ExecutionMode.INITIAL
print(f"3b. Incremental filtering: Skipped (INITIAL mode)")

# Step 3c: Apply transform
print(f"\n3c. Apply transform:")
try:
    output_df = silver_step.transform(spark, bronze_df_from_context, {})
    print(f"  ✅ Transform completed")
    print(f"  Columns: {output_df.columns}")
    print(f"  'impression_date' in columns: {'impression_date' in output_df.columns}")
except Exception as e:
    print(f"  ❌ Transform failed: {e}")
    traceback.print_exc()
    spark.stop()
    exit(1)

# Step 3d: Materialization
print(f"\n3d. Materialization (_ensure_materialized_for_validation):")
try:
    if hasattr(output_df, "cache"):
        output_df = output_df.cache()
        print(f"  ✅ DataFrame cached")
    
    count = output_df.count()
    print(f"  ✅ Materialization successful: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Materialization failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print(f"\n  ⚠️  THIS IS WHERE THE ERROR OCCURS!")
        print(f"  Materialization (df.count()) triggers the error!")
    traceback.print_exc()
    spark.stop()
    exit(1)

# Step 3e: Use ExecutionEngine.execute_step() to see exact error
print(f"\n3e. Use ExecutionEngine.execute_step() (EXACT from PipelineBuilder):")
print(f"  This will execute the exact code path that fails in the test")
try:
    result = engine.execute_step(
        step=silver_step,
        context=context,
        mode=mode,
    )
    print(f"  ✅ Step execution successful")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Step execution failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print(f"\n  ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print(f"  ExecutionEngine.execute_step() triggers the error!")
        print(f"\n  Full traceback:")
        traceback.print_exc()
    else:
        print(f"\n  Different error - full traceback:")
        traceback.print_exc()
    spark.stop()
    exit(1)

# Step 3f: Write to table
print(f"\n3f. Write to table:")
try:
    # Simulate write - just check if we can collect
    collected = output_df.collect()
    print(f"  ✅ Write simulation successful: {len(collected)} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Write failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print(f"\n  ⚠️  THIS IS WHERE THE ERROR OCCURS!")
        print(f"  Write operation triggers the error!")
    traceback.print_exc()
    spark.stop()
    exit(1)

print("\n" + "=" * 80)
print("ALL STEPS COMPLETED SUCCESSFULLY")
print("=" * 80)
print("\nIf all steps completed, the error must be in:")
print("1. How ExecutionEngine.execute_step() calls these steps")
print("2. Some additional processing in execute_step()")
print("3. Parallel execution context")

spark.stop()

