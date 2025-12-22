"""
Exact sparkless Code Execution Trail - With Detailed Logging

This script reproduces the exact execution trail and adds detailed logging
to identify exactly where sparkless code fails.
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
from pipeline_builder.pipeline import PipelineBuilder

print("=" * 80)
print("EXACT sparkless CODE EXECUTION TRAIL")
print("=" * 80)
print("\nReproducing the exact execution with detailed logging.\n")

spark = SparkSession.builder.appName("exact_trail").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Create data matching test (using data generator structure)
from datetime import datetime, timedelta

impressions_data = []
for i in range(150):
    impressions_data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile", "tablet"][i % 3],
    })

impressions_df = spark.createDataFrame(impressions_data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("STEP 1: Create PipelineBuilder and steps")
print("=" * 80)

builder = PipelineBuilder(
    spark=spark,
    functions=F,
    schema="bronze",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    verbose=True,
)

builder.with_bronze_rules(
    name="raw_impressions",
    rules={
        "impression_id": ["not_null"],
        "campaign_id": ["not_null"],
        "customer_id": ["not_null"],
        "impression_date": ["not_null"],
    },
    incremental_col="impression_date",  # KEY: This is stored
)

def processed_impressions_transform(spark, df, silvers):
    """EXACT transform from test"""
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

builder.add_silver_transform(
    name="processed_impressions",
    source_bronze="raw_impressions",
    transform=processed_impressions_transform,
    rules={
        "impression_id": ["not_null"],
        "impression_date_parsed": ["not_null"],
        "campaign_id": ["not_null"],
        "customer_id": ["not_null"],
        "channel": ["not_null"],
        "cost_per_impression": [["gte", 0]],
        "hour_of_day": ["not_null"],
        "device_type": ["not_null"],
    },
    table_name="processed_impressions",
)

print("✅ Pipeline configured")
print("  Bronze: incremental_col='impression_date'")
print("  Silver: source_incremental_col='impression_date' (inherited)")

print("\nSTEP 2: Execute pipeline and capture exact error")
print("=" * 80)

pipeline = builder.to_pipeline()

try:
    result = pipeline.run_initial_load(bronze_sources={"raw_impressions": impressions_df})
    if result.status.value == "failed":
        print(f"❌ Pipeline execution failed!")
        print(f"Errors: {result.errors}")
        if result.errors:
            print(f"\nFirst error: {result.errors[0]}")
    else:
        print("✅ Pipeline execution successful!")
except Exception as e:
    print(f"❌ Pipeline execution failed with exception!")
    print(f"\nError: {e}")
    print(f"\nError type: {type(e)}")
    print(f"\nFull traceback:")
    traceback.print_exc()
    
    # Extract the exact sparkless code that failed
    print("\n" + "=" * 80)
    print("EXACT SPARKLESS CODE TRAIL")
    print("=" * 80)
    
    # The error message shows:
    # "cannot resolve 'impression_date' given input columns: [...]"
    # This means sparkless is trying to resolve 'impression_date' on a DataFrame
    # where that column doesn't exist (it was dropped)
    
    error_str = str(e)
    if "cannot resolve 'impression_date'" in error_str:
        print("\n✅ ERROR CONFIRMED:")
        print("   sparkless is trying to resolve 'impression_date'")
        print("   on a DataFrame where that column was dropped")
        print("\n   The exact sparkless code that fails is:")
        print("   - Some operation tries: F.col('impression_date') or df['impression_date']")
        print("   - On a DataFrame with columns: [impression_id, ..., impression_date_parsed, ...]")
        print("   - sparkless raises: 'cannot resolve impression_date'")
        print("\n   This happens AFTER the transform, during:")
        print("   1. Materialization (df.count())")
        print("   2. Validation (apply_column_rules)")
        print("   3. Table write operations")
        print("   4. Or some other post-processing")

spark.stop()

