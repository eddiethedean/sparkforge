"""
Exact Execution Trail WITH Incremental Processing

This reproduces the scenario where incremental processing might try to use
the incremental column AFTER the transform, which would cause the error.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("EXACT EXECUTION TRAIL: With Incremental Processing")
print("=" * 80)
print("\nTesting if incremental processing tries to use dropped column.\n")

spark = SparkSession.builder.appName("incremental_test").getOrCreate()

# Create test data
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

print("STEP 1: Bronze DataFrame")
print("=" * 80)
print(f"Columns: {bronze_df.columns}")
print(f"incremental_col: 'impression_date'")

# Apply transform (drops impression_date)
print("\nSTEP 2: Apply transform (drops 'impression_date')")
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

silver_df = processed_impressions_transform(spark, bronze_df, {})
print(f"Columns after transform: {silver_df.columns}")
print(f"'impression_date' in columns: {'impression_date' in silver_df.columns}")

# STEP 3: Simulate what incremental processing might do
# From execution.py:2079 - it uses F.col(incremental_col) on bronze_df (BEFORE transform) ✅
# But what if something tries to use it on silver_df (AFTER transform)?

print("\nSTEP 3: Test incremental column reference on TRANSFORMED DataFrame")
print("=" * 80)
print("  This simulates what might happen if code tries to use incremental_col")
print("  on the transformed DataFrame instead of the bronze DataFrame")

incremental_col = "impression_date"  # This is source_incremental_col from bronze step

print(f"\n3a. Check if incremental_col exists in transformed DataFrame:")
if incremental_col in silver_df.columns:
    print(f"   ✅ Column exists (but it shouldn't!)")
else:
    print(f"   ✅ Column doesn't exist (correct - it was dropped)")

print(f"\n3b. Try to filter using incremental_col on TRANSFORMED DataFrame (THE BUG):")
print(f"   This is what might happen if code accidentally uses silver_df instead of bronze_df")
try:
    # This is what would happen if _filter_incremental_bronze_input was called
    # with the transformed DataFrame instead of the bronze DataFrame
    filtered = silver_df.filter(F.col(incremental_col) > F.lit("2024-01-01"))
    count = filtered.count()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error (expected): {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   Code is trying to use incremental_col on transformed DataFrame!")

print(f"\n3c. Try to select incremental_col from TRANSFORMED DataFrame:")
try:
    selected = silver_df.select("impression_id", F.col(incremental_col))
    result = selected.collect()
    print(f"   ❌ Should have failed!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error (expected): {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   Code is trying to select incremental_col from transformed DataFrame!")

# STEP 4: Check what happens during materialization
print("\nSTEP 4: Materialization (this is where the error might occur)")
print("=" * 80)
print("  Code: df.cache() and df.count()")
print("  This might trigger plan evaluation that references dropped columns")

try:
    cached_df = silver_df.cache()
    count = cached_df.count()
    print(f"  ✅ Materialization successful: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Materialization failed: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  THIS IS THE BUG!")
        print("  Materialization triggers plan evaluation that references dropped column!")

# STEP 5: Check if there's a watermark column issue
print("\nSTEP 5: Watermark column scenario")
print("=" * 80)
print("  If watermark_col is set to 'impression_date', it might try to use it")
print("  on the transformed DataFrame")

watermark_col = "impression_date"  # What if this is set incorrectly?

print(f"\n5a. Check if watermark_col exists in transformed DataFrame:")
if watermark_col in silver_df.columns:
    print(f"   ✅ Column exists")
else:
    print(f"   ✅ Column doesn't exist (correct - it was dropped)")

print(f"\n5b. Try to select watermark_col from TRANSFORMED DataFrame:")
try:
    selected = silver_df.select(watermark_col)
    result = selected.collect()
    print(f"   ❌ Should have failed!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error (expected): {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   Code is trying to use watermark_col on transformed DataFrame!")

print("\n" + "=" * 80)
print("ROOT CAUSE HYPOTHESIS")
print("=" * 80)
print("""
The error occurs when code tries to use incremental_col or watermark_col
on the TRANSFORMED DataFrame instead of the BRONZE DataFrame.

The correct flow:
1. Use incremental_col on bronze_df (BEFORE transform) ✅
2. Apply transform (drops column) ✅
3. Use transformed DataFrame (AFTER transform) ✅

The buggy flow:
1. Use incremental_col on bronze_df (BEFORE transform) ✅
2. Apply transform (drops column) ✅
3. Try to use incremental_col on silver_df (AFTER transform) ❌

This could happen if:
- Code stores a reference to the incremental_col name
- Later tries to use it on the wrong DataFrame
- Or if there's a bug in how sparkless handles the execution plan
""")

spark.stop()

