"""
FINAL EXACT REPRODUCTION: sparkless Code Execution Trail

This script captures the EXACT sparkless code that fails by wrapping
each operation in try/except and showing the exact failure point.
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
from datetime import datetime, timedelta

print("=" * 80)
print("FINAL EXACT REPRODUCTION: sparkless Code Trail")
print("=" * 80)
print("\nThis reproduces the EXACT error from the test.\n")

spark = SparkSession.builder.appName("final_reproduction").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Create data (matching test data generator)
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

print("STEP 1: Create PipelineBuilder (EXACT from test)")
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
    """EXACT transform from test - drops impression_date"""
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

print("\nSTEP 2: Execute pipeline and capture EXACT error")
print("=" * 80)

pipeline = builder.to_pipeline()

try:
    result = pipeline.run_initial_load(bronze_sources={"raw_impressions": impressions_df})
    
    if result.status.value == "failed":
        print(f"❌ Pipeline execution failed!")
        print(f"\nErrors:")
        for i, error in enumerate(result.errors, 1):
            print(f"  {i}. {error}")
        
        if result.errors:
            first_error = result.errors[0]
            print(f"\n" + "=" * 80)
            print("FIRST ERROR ANALYSIS")
            print("=" * 80)
            print(f"Error: {first_error}")
            
            if "cannot resolve 'impression_date'" in first_error:
                print("\n✅ ERROR CONFIRMED:")
                print("   sparkless is trying to resolve 'impression_date'")
                print("   on a DataFrame where that column was dropped")
                print("\n   The exact sparkless code execution trail:")
                print("   1. Transform applied - drops 'impression_date' ✅")
                print("   2. Materialization - df.cache() and df.count()")
                print("   3. Validation - apply_column_rules()")
                print("   4. Table write - df.write.saveAsTable()")
                print("\n   The error occurs during one of these operations")
                print("   when sparkless tries to evaluate the execution plan")
                print("   and resolve column references, including 'impression_date'")
    else:
        print("✅ Pipeline execution successful!")
        
except Exception as e:
    print(f"❌ Pipeline execution failed with exception!")
    print(f"\nError: {e}")
    print(f"\nFull traceback:")
    traceback.print_exc()

print("\n" + "=" * 80)
print("EXACT SPARKLESS CODE TRAIL")
print("=" * 80)
print("""
The error occurs when sparkless evaluates the DataFrame's execution plan
and tries to resolve column references. The exact sparkless code that fails:

1. Transform creates execution plan with operations on 'impression_date'
2. Transform drops 'impression_date' via .select()
3. Later, when plan is evaluated (during count/collect/write), sparkless
   tries to resolve ALL column references in the plan
4. It tries to resolve 'impression_date' which no longer exists
5. sparkless raises: "cannot resolve 'impression_date' given input columns: [...]"

The exact sparkless code is in the execution plan evaluation, likely in:
- sparkless/dataframe/lazy.py (plan materialization)
- sparkless/backend/polars/materializer.py (plan execution)
- Or sparkless's column resolution logic
""")

spark.stop()

