"""
Exact PipelineBuilder Execution Sequence Reproduction

This replicates the EXACT sequence of operations that PipelineBuilder performs,
step by step, to find what triggers the bug.
"""

import os
import sys

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
from datetime import datetime, timedelta

print("=" * 80)
print("EXACT PipelineBuilder SEQUENCE REPRODUCTION")
print("=" * 80)
print("\nReplicating the exact sequence PipelineBuilder uses.\n")

spark = SparkSession.builder.appName("exact_sequence").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Create data (matching test)
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

bronze_df = spark.createDataFrame(impressions_data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("STEP 1: Create bronze DataFrame")
print(f"  Rows: {bronze_df.count()}")
print(f"  Columns: {bronze_df.columns}")

# STEP 2: Store in context (like PipelineBuilder does)
print("\nSTEP 2: Store in context (like PipelineBuilder)")
print("=" * 80)
context = {"raw_impressions": bronze_df}
bronze_df_from_context = context["raw_impressions"]
print("  ✅ Stored and retrieved from context")

# STEP 3: Apply transform (EXACT from test)
print("\nSTEP 3: Apply transform (EXACT from test)")
print("=" * 80)

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

try:
    output_df = processed_impressions_transform(spark, bronze_df_from_context, {})
    print(f"  ✅ Transform completed")
    print(f"  Columns: {output_df.columns}")
    print(f"  'impression_date' in columns: {'impression_date' in output_df.columns}")
except Exception as e:
    print(f"  ❌ Transform failed: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# STEP 4: Materialization (EXACT from execution.py:656-658)
print("\nSTEP 4: Materialization (_ensure_materialized_for_validation)")
print("=" * 80)
print("  Code: df.cache() then df.count()")

rules = {
    "impression_id": ["not_null"],
    "impression_date_parsed": ["not_null"],
    "campaign_id": ["not_null"],
    "customer_id": ["not_null"],
    "channel": ["not_null"],
    "cost_per_impression": [["gte", 0]],
    "hour_of_day": ["not_null"],
    "device_type": ["not_null"],
}

if rules:
    try:
        if hasattr(output_df, "cache"):
            output_df = output_df.cache()
            print("  ✅ DataFrame cached")
        
        count = output_df.count()  # THIS IS WHERE IT MIGHT FAIL
        print(f"  ✅ Materialization successful: {count} rows")
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Materialization failed: {error_msg[:200]}")
        if "cannot resolve 'impression_date'" in error_msg:
            print("\n  ⚠️  THIS IS THE EXACT ERROR!")
            print("  Materialization (df.count()) triggers the error!")
            import traceback
            traceback.print_exc()
            spark.stop()
            exit(1)
else:
    print("  ✅ No rules, skipping materialization")

# STEP 5: Validation (EXACT from execution.py:659-665)
print("\nSTEP 5: Validation (apply_column_rules)")
print("=" * 80)
print("  Code: df.filter(validation_predicate) then df.count()")

# Import validation function
from pipeline_builder.validation.data_validation import apply_column_rules

try:
    valid_df, invalid_df, validation_stats = apply_column_rules(
        output_df,
        rules,
        "pipeline",
        "processed_impressions",
        functions=F,
    )
    print(f"  ✅ Validation completed")
    if validation_stats:
        print(f"  Validation rate: {validation_stats.validation_rate}%")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Validation failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE EXACT ERROR!")
        print("  Validation triggers the error!")
        import traceback
        traceback.print_exc()
        spark.stop()
    exit(1)

# STEP 6: Write to table (EXACT from execution.py:673-689)
print("\nSTEP 6: Write to table")
print("=" * 80)
print("  Code: df.write.format('delta').mode('overwrite').saveAsTable(...)")

try:
    writer = output_df.write.format("delta").mode("overwrite")
    writer.saveAsTable("silver.processed_impressions")
    print("  ✅ Table write successful")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Write failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE EXACT ERROR!")
        print("  Table write triggers the error!")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
print("SEQUENCE COMPLETE")
print("=" * 80)
print("\nIf all steps completed, the bug might be in:")
print("1. How PipelineBuilder stores/retrieves DataFrames")
print("2. Some state that PipelineBuilder maintains")
print("3. Or a specific combination of operations in PipelineBuilder's execution flow")

spark.stop()

