"""
Exact PipelineBuilder Execution with Error Capture

This uses the actual PipelineBuilder to reproduce the exact error,
then extracts the sparkless code execution trail.
"""

import os
import sys
import traceback

# Set up environment
os.environ["SPARK_MODE"] = "mock"

# Add src to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Configure engine (like conftest.py does)
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
print("EXACT PipelineBuilder EXECUTION WITH ERROR")
print("=" * 80)
print("\nUsing actual PipelineBuilder to capture the exact error.\n")

spark = SparkSession.builder.appName("pipelinebuilder_error").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Create test data (matching test structure)
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
]

impressions_df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print("STEP 1: Create PipelineBuilder")
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

print("✅ PipelineBuilder created")

print("\nSTEP 2: Add Bronze step")
print("=" * 80)

builder.with_bronze_rules(
    name="raw_impressions",
    rules={
        "impression_id": ["not_null"],
        "campaign_id": ["not_null"],
        "customer_id": ["not_null"],
        "impression_date": ["not_null"],
    },
    incremental_col="impression_date",  # This is the key!
)

print("✅ Bronze step added with incremental_col='impression_date'")

print("\nSTEP 3: Add Silver transform (EXACT from test)")
print("=" * 80)

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
            "impression_date_parsed",  # New column
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
            # impression_date is DROPPED
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

print("✅ Silver step added")
print("  Transform drops 'impression_date' via .select()")
print("  Bronze step has incremental_col='impression_date'")
print("  Silver step inherits source_incremental_col='impression_date'")

print("\nSTEP 4: Create pipeline and run")
print("=" * 80)

pipeline = builder.to_pipeline()
print("✅ Pipeline created")

print("\nSTEP 5: Execute pipeline (THIS IS WHERE THE ERROR OCCURS)")
print("=" * 80)
print("  Code: result = pipeline.initial_load(bronze_sources={'raw_impressions': impressions_df})")
print("  This will execute the exact sparkless code trail...")

try:
    result = pipeline.run_initial_load(bronze_sources={"raw_impressions": impressions_df})
    print("  ✅ Pipeline execution successful!")
    print(f"  Status: {result.status.value}")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Pipeline execution failed!")
    print(f"  Error: {error_msg}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("  Error: cannot resolve 'impression_date' given input columns: [...]")
        print("\n  Full traceback:")
        traceback.print_exc()
        
        print("\n" + "=" * 80)
        print("ERROR ANALYSIS")
        print("=" * 80)
        print("""
The error occurs during PipelineBuilder execution, specifically in the
processed_impressions silver step.

The execution trail is:
1. Bronze step created with incremental_col='impression_date' ✅
2. Silver step created, inherits source_incremental_col='impression_date' ✅
3. Transform applied, drops 'impression_date' via .select() ✅
4. ERROR: Something tries to reference 'impression_date' on transformed DataFrame ❌

The error message shows columns AFTER transform, so the error occurs
AFTER the transform is applied, during:
- Materialization (df.count())
- Validation (apply_column_rules)
- Table write operations
- Or some other post-processing step

The exact sparkless code that fails is shown in the traceback above.
""")

spark.stop()

