"""
Pure sparkless code reproducing the incremental column reference bug

The marketing pipeline test has:
- Bronze step with incremental_col="impression_date"
- Silver transform that drops "impression_date" 
- Error: "cannot resolve 'impression_date' given input columns: [...]"

This suggests that after the transform drops the column, something is still
trying to reference it, possibly related to incremental processing.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("PURE SPARKLESS: Incremental Column Reference Bug")
print("=" * 80)
print("\nThis reproduces the scenario from the marketing pipeline test.\n")

spark = SparkSession.builder.appName("sparkless_incremental_bug").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This is the incremental column in bronze
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print("STEP 1: Original DataFrame (Bronze layer)")
print("=" * 80)
print(f"Columns: {df.columns}")
print(f"'impression_date' exists: {'impression_date' in df.columns}")
print("Note: Bronze step has incremental_col='impression_date'")

# Apply the transform that drops the column
print("\n" + "=" * 80)
print("STEP 2: Apply Silver transform (drops 'impression_date')")
print("=" * 80)

df_transformed = (
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
        # 'impression_date' is dropped!
    )
)

print(f"Columns after transform: {df_transformed.columns}")
print(f"'impression_date' exists: {'impression_date' in df_transformed.columns}")

# Simulate what might happen if incremental processing tries to use the original column
print("\n" + "=" * 80)
print("STEP 3: Simulating incremental processing scenario")
print("=" * 80)

print("\n3a. What if code tries to filter by incremental column (original name)?")
print("   This might happen if incremental processing uses the bronze column name")
print("   instead of checking what columns are actually available")

try:
    # Simulate: incremental processing might try to filter by the original column
    incremental_filter = F.col("impression_date") > "2024-01-15"
    filtered = df_transformed.filter(incremental_filter)
    count = filtered.count()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   Error: cannot resolve 'impression_date' given input columns: [...]")
        print("   This suggests incremental processing is trying to use the original column name!")

print("\n3b. What if code tries to select the incremental column?")
try:
    # Simulate: code might try to select the incremental column for processing
    selected = df_transformed.select("impression_id", "impression_date")
    result = selected.collect()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   Code is trying to reference the incremental column after it was dropped!")

print("\n3c. What if code tries to use the incremental column in an expression?")
try:
    # Simulate: code might try to use the incremental column in a calculation
    expr = F.col("impression_date").cast("timestamp")
    result = df_transformed.withColumn("test", expr)
    count = result.count()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   Code is trying to use the incremental column in an expression!")

print("\n" + "=" * 80)
print("ROOT CAUSE HYPOTHESIS")
print("=" * 80)
print("""
The bug likely occurs because:

1. Bronze step defines incremental_col="impression_date" ✅
2. Silver transform drops "impression_date" via .select() ✅
3. Later processing (possibly incremental filtering or validation) tries to
   reference the incremental column using the original name ❌
4. sparkless correctly identifies the column doesn't exist ✅
5. But the error suggests code is trying to reference it anyway ❌

The issue is that PipelineBuilder or sparkless might be:
- Storing the incremental column name from bronze step
- Trying to use it later in silver/gold steps
- Not checking if the column still exists after transforms

This is a logic issue where:
- The incremental column name is stored/remembered
- But the transform explicitly drops it
- Code later tries to use the stored name
- sparkless correctly says it doesn't exist

The fix would be to either:
1. Not drop the incremental column in transforms
2. Update the incremental column reference to the new column name
3. Check if the column exists before trying to use it
""")

spark.stop()

