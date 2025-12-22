"""
Exact Execution Trail: Marketing Pipeline Test Failure

This script reproduces the EXACT execution trail from the failing marketing pipeline test,
showing step-by-step what sparkless code is executed and where it fails.

Based on: tests/builder_tests/test_marketing_pipeline.py::test_complete_marketing_pipeline_execution
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("EXACT EXECUTION TRAIL: Marketing Pipeline Test")
print("=" * 80)
print("\nThis reproduces the exact sparkless code execution from the failing test.\n")

# STEP 1: Create Spark Session (exact match to test)
spark = SparkSession.builder.appName("marketing_pipeline_test").getOrCreate()

# STEP 2: Create schemas (exact match to test)
print("STEP 1: Create schemas")
print("=" * 80)
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print("✅ Schemas created")

# STEP 3: Create test data (matching data_generator.create_marketing_impressions)
print("\nSTEP 2: Create test data")
print("=" * 80)

# Sample data matching the structure from data generator
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
    ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3", "web", "ad_1", "desktop", 0.04),
]

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This is the incremental_col
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print(f"Bronze DataFrame created:")
print(f"  Columns: {bronze_df.columns}")
print(f"  Rows: {bronze_df.count()}")
print(f"  incremental_col: 'impression_date'")

# STEP 4: Apply bronze validation rules (simulated)
print("\nSTEP 3: Apply bronze validation (simulated)")
print("=" * 80)
print("  Rules: impression_id, campaign_id, customer_id, impression_date - all not_null")
print("  ✅ Validation passed (simulated)")

# STEP 5: Incremental filtering (simulated - in INITIAL mode, this doesn't run)
print("\nSTEP 4: Incremental filtering (INITIAL mode - skipped)")
print("=" * 80)
print("  Mode: INITIAL (not INCREMENTAL)")
print("  ✅ Incremental filtering skipped")

# STEP 6: Apply silver transform (EXACT transform from test)
print("\nSTEP 5: Apply silver transform (EXACT from test)")
print("=" * 80)
print("  Transform: processed_impressions_transform")
print("  Source: raw_impressions (bronze_df)")

def processed_impressions_transform(spark, df, silvers):
    """EXACT transform from test_marketing_pipeline.py:95-132"""
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
            "impression_date_parsed",  # New column, original impression_date is DROPPED
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
        )
    )

print("\n  Executing transform...")
try:
    silver_df = processed_impressions_transform(spark, bronze_df, {})
    print(f"  ✅ Transform completed")
    print(f"  Columns after transform: {silver_df.columns}")
    print(f"  'impression_date' in columns: {'impression_date' in silver_df.columns}")
    print(f"  'impression_date_parsed' in columns: {'impression_date_parsed' in silver_df.columns}")
except Exception as e:
    print(f"  ❌ Transform failed: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# STEP 7: Materialize for validation (EXACT from execution.py:560-592)
print("\nSTEP 6: Materialize for validation (EXACT from execution.py:560-592)")
print("=" * 80)
print("  Code: _ensure_materialized_for_validation(df, rules)")
print("  This calls: df.cache() and df.count()")

try:
    if hasattr(silver_df, "cache"):
        silver_df = silver_df.cache()
        print("  ✅ DataFrame cached")
    
    count = silver_df.count()
    print(f"  ✅ Materialization successful: {count} rows")
except Exception as e:
    print(f"  ❌ Materialization failed: {e}")
    print(f"  This is where the error occurs!")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# STEP 8: Apply validation rules (EXACT from execution.py:659-665)
print("\nSTEP 7: Apply validation rules (EXACT from execution.py:659-665)")
print("=" * 80)
print("  Rules:")
print("    - impression_id: not_null")
print("    - impression_date_parsed: not_null")
print("    - campaign_id: not_null")
print("    - customer_id: not_null")
print("    - channel: not_null")
print("    - cost_per_impression: gte 0")
print("    - hour_of_day: not_null")
print("    - device_type: not_null")

# Simulate validation (simplified)
validation_rules = {
    "impression_id": [F.col("impression_id").isNotNull()],
    "impression_date_parsed": [F.col("impression_date_parsed").isNotNull()],
    "campaign_id": [F.col("campaign_id").isNotNull()],
    "customer_id": [F.col("customer_id").isNotNull()],
    "channel": [F.col("channel").isNotNull()],
    "cost_per_impression": [F.col("cost_per_impression") >= 0],
    "hour_of_day": [F.col("hour_of_day").isNotNull()],
    "device_type": [F.col("device_type").isNotNull()],
}

try:
    # Create validation predicate
    validation_expr = None
    for col_name, rules in validation_rules.items():
        for rule in rules:
            if validation_expr is None:
                validation_expr = rule
            else:
                validation_expr = validation_expr & rule
    
    if validation_expr is not None:
        valid_df = silver_df.filter(validation_expr)
        invalid_df = silver_df.filter(~validation_expr)
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        print(f"  ✅ Validation completed: {valid_count} valid, {invalid_count} invalid")
    else:
        print("  ✅ Validation skipped (no rules)")
except Exception as e:
    print(f"  ❌ Validation failed: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# STEP 9: Write to table (EXACT from execution.py:673-689)
print("\nSTEP 8: Write to table (EXACT from execution.py:673-689)")
print("=" * 80)
print("  Table: silver.processed_impressions")
print("  Mode: overwrite (INITIAL mode)")

try:
    # Simulate table write
    # In real PipelineBuilder, this would be: df.write.format("delta").mode("overwrite").saveAsTable("silver.processed_impressions")
    # For reproduction, we'll just try to collect to see if there are any issues
    collected = silver_df.collect()
    print(f"  ✅ Table write simulation successful: {len(collected)} rows")
except Exception as e:
    print(f"  ❌ Table write failed: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

print("\n" + "=" * 80)
print("EXECUTION TRAIL COMPLETE")
print("=" * 80)
print("\nIf this script runs successfully, the issue is in PipelineBuilder's")
print("execution flow, not in the transform itself.")
print("\nIf this script fails, we've reproduced the exact sparkless bug!")

spark.stop()

