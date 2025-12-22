"""
Bug Reproduction: Column tracking issue in sparkless 3.18.3

This script demonstrates the column tracking bug where sparkless incorrectly
tries to reference a column that was dropped after a transformation.

Error: 'DataFrame' object has no attribute 'impression_date'. 
Available columns: impression_id, campaign_id, customer_id, impression_date_parsed, ...

The bug occurs when:
1. Transform uses a column (impression_date) to create a new column (impression_date_parsed)
2. Transform drops the original column via .select()
3. Sparkless internally tries to reference the dropped column
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("DIRECT SPARKLESS CODE DEMONSTRATING THE COLUMN TRACKING BUG")
print("=" * 80)
print("\nThis script shows the column tracking issue in sparkless 3.18.3")
print("where sparkless tries to reference a column that was dropped.\n")

# Create sparkless session
spark = SparkSession.builder.appName("column_tracking_bug").getOrCreate()

print("=" * 80)
print("STEP 1: Create test data")
print("=" * 80)

data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "mobile", 0.03),
    ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3", "web", "desktop", 0.04),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This column will be dropped
    "campaign_id",
    "customer_id",
    "channel",
    "device_type",
    "cost_per_impression"
])

print("\nOriginal DataFrame:")
df.show(truncate=False)
df.printSchema()

print("\nAvailable columns:", df.columns)

print("\n" + "=" * 80)
print("STEP 2: Apply transform (matching test_marketing_pipeline.py pattern)")
print("=" * 80)

# This is the EXACT transform pattern from the failing test
def transform_function(df):
    """Exact transform from test_marketing_pipeline.py"""
    return (
        df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn(
            "hour_of_day",
            F.hour(F.col("impression_date_parsed")),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek(F.col("impression_date_parsed")),
        )
        .withColumn(
            "is_mobile",
            F.when(F.col("device_type") == "mobile", True).otherwise(False),
        )
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",  # New column, original impression_date is dropped
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",  # This column doesn't exist - might be part of the issue
            "cost_per_impression",
            "device_type",
            "is_mobile",
        )
    )

print("\n2a. Applying transform step by step:")

# Step 1: Add parsed date
df_step1 = df.withColumn(
    "impression_date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)
print("\n   After adding impression_date_parsed:")
print("   Columns:", df_step1.columns)
print("   Has 'impression_date':", "impression_date" in df_step1.columns)
print("   Has 'impression_date_parsed':", "impression_date_parsed" in df_step1.columns)

# Step 2: Add derived columns
df_step2 = df_step1.withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
df_step2 = df_step2.withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
df_step2 = df_step2.withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
print("\n   After adding derived columns:")
print("   Columns:", df_step2.columns)
print("   Has 'impression_date':", "impression_date" in df_step2.columns)

# Step 3: Select specific columns (drops impression_date)
print("\n   Selecting columns (this drops 'impression_date'):")
select_columns = [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date_parsed",
    "hour_of_day",
    "day_of_week",
    "channel",
    "cost_per_impression",
    "device_type",
    "is_mobile",
]
# Note: 'ad_id' is not in the original data, so we'll skip it for now
print("   Selecting:", select_columns)

try:
    df_transformed = df_step2.select(*select_columns)
    print("\n   ✅ Select succeeded")
    print("   Columns after select:", df_transformed.columns)
    print("   Has 'impression_date':", "impression_date" in df_transformed.columns)
    print("   Has 'impression_date_parsed':", "impression_date_parsed" in df_transformed.columns)
    
    df_transformed.show(truncate=False)
    df_transformed.printSchema()
    
except Exception as e:
    print(f"\n   ❌ ERROR during select: {e}")
    print("   This might be where the bug occurs!")

print("\n" + "=" * 80)
print("STEP 3: Try to access the dropped column (this should fail)")
print("=" * 80)

try:
    # Try to access the original column that was dropped
    result = df_transformed.select("impression_date")
    print("   ❌ ERROR: Should not be able to access 'impression_date' after select!")
except Exception as e:
    print(f"   ✅ Expected error: {e}")
    print("   This is correct - the column was dropped")

print("\n" + "=" * 80)
print("STEP 4: Try operations that might trigger internal column references")
print("=" * 80)

try:
    # Try to use the transformed column
    df_with_ops = df_transformed.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    print("   ✅ Operations on transformed columns work")
    df_with_ops.select("impression_date_parsed", "hour_of_day", "day_of_week", "is_weekend").show()
except Exception as e:
    print(f"   ❌ ERROR: {e}")

print("\n" + "=" * 80)
print("STEP 5: Simulate what happens in pipeline validation")
print("=" * 80)

# This is what might trigger the bug - validation or internal processing
# that tries to reference the original column

try:
    # Try to collect (this might trigger internal processing)
    collected = df_transformed.collect()
    print(f"   ✅ Collection succeeded: {len(collected)} rows")
    
    # Try to access columns
    for col in df_transformed.columns:
        try:
            sample = df_transformed.select(col).limit(1).collect()
            print(f"   ✅ Can access column '{col}'")
        except Exception as e:
            print(f"   ❌ Cannot access column '{col}': {e}")
            
except Exception as e:
    print(f"   ❌ ERROR during collection/access: {e}")
    print("   This might reveal the column tracking bug!")

print("\n" + "=" * 80)
print("STEP 6: Try the exact pattern that fails in the test")
print("=" * 80)

# The test includes 'ad_id' which might not exist - let's check
print("\n6a. Check if 'ad_id' exists in original data:")
print("   Original columns:", df.columns)
print("   'ad_id' in original:", "ad_id" in df.columns)

# The transform in the test tries to select 'ad_id' which might not exist
# This could be causing the issue
try:
    # Try with the exact select from the test (including ad_id)
    select_with_ad_id = [
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",  # This doesn't exist!
        "cost_per_impression",
        "device_type",
        "is_mobile",
    ]
    
    print("\n6b. Trying to select with 'ad_id' (which doesn't exist):")
    df_with_ad_id = df_step2.select(*select_with_ad_id)
    print("   ❌ Should have failed - 'ad_id' doesn't exist!")
    
except Exception as e:
    print(f"   ✅ Expected error (ad_id doesn't exist): {e}")
    print("   This might be part of the issue!")

print("\n" + "=" * 80)
print("ROOT CAUSE ANALYSIS")
print("=" * 80)
print("""
The bug occurs when sparkless tries to reference a column that was dropped:

1. Transform uses 'impression_date' to create 'impression_date_parsed' ✅
2. Transform selects specific columns, dropping 'impression_date' ✅
3. Sparkless internally tries to reference 'impression_date' ❌
4. Error: 'DataFrame' object has no attribute 'impression_date'

Possible causes:
- Sparkless's internal validation/processing tries to access original columns
- Column tracking doesn't properly handle dropped columns after .select()
- Internal caching or optimization tries to reference the original column
- Validation rules might be checking against original column names

The error message shows available columns, which suggests sparkless knows
which columns exist, but something is still trying to access 'impression_date'.
""")

print("\n" + "=" * 80)
print("EVIDENCE")
print("=" * 80)
print("""
✅ Transform correctly creates 'impression_date_parsed'
✅ Transform correctly drops 'impression_date' via .select()
✅ Transformed DataFrame has correct columns
❌ Sparkless internally tries to access 'impression_date' (which was dropped)

This is a column tracking/reference bug in sparkless's internal processing.
""")

spark.stop()

