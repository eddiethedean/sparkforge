"""
Exact PipelineBuilder Execution Trail

This reproduces the EXACT execution flow from PipelineBuilder, step by step,
to identify where the error occurs.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("EXACT PipelineBuilder EXECUTION TRAIL")
print("=" * 80)
print("\nReproducing the exact execution flow from PipelineBuilder.\n")

spark = SparkSession.builder.appName("pipelinebuilder_execution").getOrCreate()

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Create bronze data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
]

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # incremental_col
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print("STEP 1: Bronze DataFrame created")
print(f"  Columns: {bronze_df.columns}")
print(f"  incremental_col: 'impression_date'")

# Simulate _execute_silver_step (execution.py:1978-1998)
print("\nSTEP 2: _execute_silver_step (execution.py:1978-1998)")
print("=" * 80)

# Step 2a: Get bronze data from context (simulated)
context = {"raw_impressions": bronze_df}
source_bronze = "raw_impressions"
bronze_df_from_context = context[source_bronze]
print(f"2a. Get bronze data from context: ✅")

# Step 2b: Incremental filtering (execution.py:1994-1995)
# In INITIAL mode, this is skipped, but let's check the logic
mode = "INITIAL"  # Not INCREMENTAL
incremental_col = "impression_date"  # source_incremental_col
watermark_col = None  # Not set in test

if mode == "INCREMENTAL":
    print("2b. Incremental filtering would run here")
    # This would call _filter_incremental_bronze_input
    # Which uses F.col(incremental_col) on bronze_df (correct!)
else:
    print("2b. Incremental filtering skipped (INITIAL mode)")

# Step 2c: Apply transform (execution.py:1998)
print("\n2c. Apply transform (execution.py:1998)")
print("  Code: return step.transform(self.spark, bronze_df, {})")

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

try:
    output_df = processed_impressions_transform(spark, bronze_df_from_context, {})
    print(f"  ✅ Transform completed")
    print(f"  Columns: {output_df.columns}")
except Exception as e:
    print(f"  ❌ Transform failed: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# Step 3: execute_step continues (execution.py:646-671)
print("\nSTEP 3: execute_step - Materialization and Validation (execution.py:646-671)")
print("=" * 80)

# Step 3a: Materialization (execution.py:656-658)
print("3a. Materialization (_ensure_materialized_for_validation)")
print("  Code: output_df = self._ensure_materialized_for_validation(output_df, step.rules)")

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
        
        count = output_df.count()
        print(f"  ✅ Materialization successful: {count} rows")
    except Exception as e:
        print(f"  ❌ Materialization failed: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        exit(1)
else:
    print("  ✅ No rules, skipping materialization")

# Step 3b: Validation (execution.py:659-665)
print("\n3b. Validation (apply_column_rules)")
print("  Code: output_df, _, validation_stats = apply_column_rules(...)")

# Simulate validation - this is where the error might occur
# The validation uses F.col() expressions, which should work fine
# But let's check if there's any reference to impression_date

try:
    # Check if validation rules reference impression_date (they shouldn't)
    for col_name in rules.keys():
        if col_name == "impression_date":
            print(f"  ⚠️  WARNING: Validation rule references 'impression_date'!")
            # This would cause the error
            validation_expr = F.col("impression_date").isNotNull()
            filtered = output_df.filter(validation_expr)
            count = filtered.count()
        else:
            # Normal validation
            validation_expr = F.col(col_name).isNotNull()
            filtered = output_df.filter(validation_expr)
            count = filtered.count()
    
    print("  ✅ Validation completed (simulated)")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Validation failed: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE EXACT ERROR!")
        print("  Validation is trying to reference 'impression_date'!")
    import traceback
    traceback.print_exc()

# Step 4: Write to table (execution.py:673-689)
print("\nSTEP 4: Write to table (execution.py:673-689)")
print("=" * 80)
print("  Table: silver.processed_impressions")
print("  Mode: overwrite (INITIAL mode)")

# The write operation might trigger plan evaluation
try:
    # Simulate write - just collect to see if there are issues
    collected = output_df.collect()
    print(f"  ✅ Write simulation successful: {len(collected)} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Write failed: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE EXACT ERROR!")
        print("  Write operation triggers plan evaluation that references 'impression_date'!")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("EXECUTION TRAIL COMPLETE")
print("=" * 80)
print("\nIf all steps completed successfully, the issue must be in:")
print("1. How PipelineBuilder stores/uses incremental_col after transform")
print("2. Some post-processing step that references the original column")
print("3. A sparkless bug in plan optimization/evaluation")

spark.stop()

