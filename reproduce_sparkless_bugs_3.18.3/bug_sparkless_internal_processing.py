"""
Direct sparkless code showing internal processing that causes the bug

This script demonstrates what happens when sparkless processes a DataFrame
that has dropped columns, showing the exact code pattern that causes the bug.

The bug occurs when sparkless's internal code tries to access columns using
patterns that fail when columns are dropped.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("SPARKLESS INTERNAL PROCESSING BUG")
print("=" * 80)
print("\nThis shows what happens when sparkless processes DataFrames internally.\n")

spark = SparkSession.builder.appName("sparkless_internal_bug").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
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

# Apply the exact transform from the test
print("\n" + "=" * 80)
print("STEP 2: Apply transform (drops 'impression_date')")
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

print(f"Columns after transform: {df_transformed.columns}")
print(f"'impression_date' in columns: {'impression_date' in df_transformed.columns}")

# Now simulate what PipelineBuilder might do internally
print("\n" + "=" * 80)
print("STEP 3: Simulating PipelineBuilder internal processing")
print("=" * 80)

# Pattern 1: Check columns using getattr (this is what execution.py does)
print("\n3a. Pattern 1: Using getattr to check columns (from execution.py line 2021):")
print("   Code: getattr(bronze_df, 'columns', [])")

try:
    columns = getattr(df_transformed, "columns", [])
    print(f"   ✅ getattr works: {len(columns)} columns")
    print(f"   'impression_date' in result: {'impression_date' in columns}")
except Exception as e:
    print(f"   ❌ getattr failed: {e}")

# Pattern 2: Try to access a column that might be referenced
# The error suggests something is trying to access the original column name
print("\n3b. Pattern 2: What if code tries to access original column name?")
print("   This might happen if code stores the original column name and tries to access it later")

# Simulate: code might have stored "impression_date" as a column to check
original_column_name = "impression_date"  # This might be stored from before the transform

print(f"   Original column name stored: {original_column_name}")
print(f"   Trying to check if this column exists in transformed DataFrame...")

# Check if column exists (correct way)
if original_column_name in df_transformed.columns:
    print(f"   ✅ Column exists (but it doesn't!)")
else:
    print(f"   ✅ Column doesn't exist (correct)")

# But what if code tries attribute access?
print(f"\n   Trying attribute access to stored column name...")
try:
    # This is what might be happening - code tries df.impression_date
    result = getattr(df_transformed, original_column_name, None)
    if result is not None:
        print(f"   ⚠️  getattr returned something: {type(result)}")
    else:
        print(f"   ✅ getattr returns None (safe)")
except AttributeError as e:
    print(f"   ❌ getattr raised AttributeError: {e}")
    print("   This might be what's happening!")

# Pattern 3: Direct attribute access (this definitely fails)
print(f"\n   Trying direct attribute access...")
try:
    result = df_transformed.impression_date
    print(f"   ❌ Should have failed!")
except AttributeError as e:
    error_msg = str(e)
    print(f"   ✅ Direct access fails: {error_msg[:100]}")
    if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR!")
        print("   This proves something is using direct attribute access!")

# Pattern 4: What if validation or processing tries to iterate over columns?
print("\n3c. Pattern 3: What if code iterates and tries to access each column?")
print("   This might happen during validation or schema checking")

try:
    # Simulate: code might iterate over original columns and try to access them
    original_columns = df.columns
    print(f"   Original columns: {original_columns}")
    
    for col_name in original_columns:
        if col_name in df_transformed.columns:
            # Column still exists - try to access it
            try:
                col = getattr(df_transformed, col_name, None)
                if col is not None:
                    print(f"   ✅ Column '{col_name}': exists and accessible")
            except AttributeError as e:
                print(f"   ❌ Column '{col_name}': AttributeError - {e}")
        else:
            # Column was dropped - but what if code still tries to access it?
            print(f"   ⚠️  Column '{col_name}': was dropped")
            try:
                col = getattr(df_transformed, col_name, None)
                if col is None:
                    print(f"      getattr returns None (safe)")
                else:
                    print(f"      getattr returned: {type(col)}")
            except AttributeError as e:
                print(f"      ❌ getattr raised AttributeError: {e}")
                print(f"      THIS IS THE BUG - trying to access dropped column!")
                
except Exception as e:
    print(f"   ❌ Error during iteration: {e}")

# Pattern 5: What if there's column name mapping or tracking?
print("\n3d. Pattern 4: Column name mapping/tracking issue?")
print("   What if code tracks original column names and tries to map them?")

# Simulate: code might have a mapping like {"impression_date": "impression_date_parsed"}
# But then tries to access the original name
column_mapping = {
    "impression_date": "impression_date_parsed"  # Maps old to new
}

print(f"   Column mapping: {column_mapping}")

for old_name, new_name in column_mapping.items():
    print(f"\n   Processing mapping: {old_name} -> {new_name}")
    
    # Check if new column exists
    if new_name in df_transformed.columns:
        print(f"   ✅ New column '{new_name}' exists")
    else:
        print(f"   ❌ New column '{new_name}' doesn't exist")
    
    # But what if code accidentally tries to access the old name?
    print(f"   Checking if old column '{old_name}' still exists...")
    if old_name in df_transformed.columns:
        print(f"   ⚠️  Old column still exists (unexpected)")
    else:
        print(f"   ✅ Old column was dropped (expected)")
        
        # But what if code tries to access it anyway?
        try:
            result = getattr(df_transformed, old_name, None)
            if result is None:
                print(f"   ✅ getattr returns None for dropped column (safe)")
            else:
                print(f"   ⚠️  getattr returned something: {type(result)}")
        except AttributeError as e:
            print(f"   ❌ getattr raised AttributeError for dropped column: {e}")
            print(f"   THIS COULD BE THE BUG - code tries to access old column name!")

print("\n" + "=" * 80)
print("ROOT CAUSE HYPOTHESIS")
print("=" * 80)
print("""
The bug likely occurs when:

1. PipelineBuilder or sparkless internal code stores/tracks original column names
2. After a transform drops columns, it tries to access the original column names
3. It uses attribute access (df.column_name) or getattr() that raises AttributeError
4. This happens during validation, schema checking, or column tracking

The error "'DataFrame' object has no attribute 'impression_date'" suggests:
- Code is using attribute access (df.impression_date)
- The column was dropped, so attribute access fails
- This happens in sparkless's internal processing, not in user code

Possible locations:
- Column validation/tracking code
- Schema checking code  
- Column dependency tracking
- Incremental processing (which checks original column names)
""")

spark.stop()

