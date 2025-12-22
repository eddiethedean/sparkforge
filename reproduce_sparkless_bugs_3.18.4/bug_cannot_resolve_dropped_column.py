"""
Pure sparkless code reproducing the "cannot resolve" error

This reproduces the exact bug from the marketing pipeline test:
- Transform uses a column (impression_date)
- Creates a new column from it (impression_date_parsed)
- Drops the original column via .select()
- Later code tries to reference the dropped column
- sparkless raises: "cannot resolve 'impression_date' given input columns: [...]"

The bug occurs when sparkless tries to resolve a column that was dropped.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("PURE SPARKLESS: Cannot Resolve Dropped Column Bug")
print("=" * 80)
print("\nThis reproduces the exact error from the marketing pipeline test.\n")

spark = SparkSession.builder.appName("sparkless_cannot_resolve_bug").getOrCreate()

# Create test data matching the marketing pipeline
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
    ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3", "web", "ad_1", "desktop", 0.04),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This will be dropped
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print("STEP 1: Original DataFrame")
print("=" * 80)
print(f"Columns: {df.columns}")
print(f"'impression_date' exists: {'impression_date' in df.columns}")

# Apply the EXACT transform from the test
print("\n" + "=" * 80)
print("STEP 2: Apply transform (exact pattern from test)")
print("=" * 80)

def processed_impressions_transform(df):
    """Exact transform from test_marketing_pipeline.py"""
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
            # Note: 'impression_date' is NOT in select() - it's dropped!
        )
    )

df_transformed = processed_impressions_transform(df)

print(f"Columns after transform: {df_transformed.columns}")
print(f"'impression_date' exists: {'impression_date' in df_transformed.columns}")
print(f"'impression_date_parsed' exists: {'impression_date_parsed' in df_transformed.columns}")

# Now try to reference the dropped column - this is where the bug occurs
print("\n" + "=" * 80)
print("STEP 3: Try to reference dropped column (THE BUG)")
print("=" * 80)

print("\n3a. Using F.col() on dropped column in filter:")
try:
    # This is what might happen in validation or downstream processing
    filtered = df_transformed.filter(F.col("impression_date").isNotNull())
    count = filtered.count()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   Error: cannot resolve 'impression_date' given input columns: [...]")

print("\n3b. Using F.col() on dropped column in select:")
try:
    selected = df_transformed.select("impression_id", F.col("impression_date"))
    result = selected.collect()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   select() with F.col() on dropped column triggers the error!")

print("\n3c. Using string column name in select (dropped column):")
try:
    selected = df_transformed.select("impression_id", "impression_date")
    result = selected.collect()
    print(f"   ❌ Should have failed - column was dropped!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   select() with string on dropped column triggers the error!")

# Simulate what might happen in validation
print("\n" + "=" * 80)
print("STEP 4: Simulating validation scenario")
print("=" * 80)

print("\n4a. What if validation rules reference the dropped column?")
print("   Rules: {'impression_date': ['not_null']}  # This column was dropped!")

# This is what might happen - validation rules might reference the original column
try:
    # Simulate validation checking the dropped column
    validation_expr = F.col("impression_date").isNotNull()
    filtered = df_transformed.filter(validation_expr)
    count = filtered.count()
    print(f"   ❌ Should have failed - column doesn't exist!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Expected error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE BUG!")
        print("   Validation rules referencing dropped columns cause this error!")

# Simulate what might happen with incremental processing
print("\n4b. What if incremental processing references the dropped column?")
print("   incremental_col='impression_date'  # This column was dropped!")

# This might happen if incremental processing tries to use the original column name
try:
    # Simulate incremental processing trying to use the column
    incremental_expr = F.col("impression_date") > "2024-01-01"
    filtered = df_transformed.filter(incremental_expr)
    count = filtered.count()
    print(f"   ❌ Should have failed - column doesn't exist!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Expected error: {error_msg[:200]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE BUG!")
        print("   Incremental processing referencing dropped columns causes this error!")

print("\n" + "=" * 80)
print("ROOT CAUSE ANALYSIS")
print("=" * 80)
print("""
The bug occurs when:

1. Transform uses a column (impression_date) ✅
2. Creates a new column from it (impression_date_parsed) ✅
3. Drops the original column via .select() ✅
4. Later code tries to reference the dropped column ❌
   - This could be in validation rules
   - This could be in incremental processing
   - This could be in downstream transforms
5. sparkless raises: "cannot resolve 'impression_date' given input columns: [...]"

The error message is more informative than the previous AttributeError, but the
underlying issue is that code is trying to reference a column that was explicitly
dropped in the transform.

Possible causes:
- Validation rules reference the original column name
- Incremental processing uses the original column name
- Downstream transforms expect the original column to exist
- Column name mapping/tracking issue
""")

print("\n" + "=" * 80)
print("COMPARISON WITH PYSPARK")
print("=" * 80)
print("""
In PySpark:
- Same error would occur: "cannot resolve 'impression_date'"
- But PySpark would fail earlier, during validation rule setup
- Or PySpark would handle it more gracefully

The issue is that the code (PipelineBuilder or transform logic) is trying to
reference a column that was explicitly dropped. This is a logic error in the
pipeline code, not necessarily a sparkless bug.

However, the error message suggests sparkless is correctly identifying that
the column doesn't exist, which is good. The question is: why is code trying
to reference a dropped column?
""")

spark.stop()

