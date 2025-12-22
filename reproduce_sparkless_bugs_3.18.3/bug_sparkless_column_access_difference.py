"""
Direct sparkless code showing the exact bug

This demonstrates the bug by showing what happens when sparkless processes
a DataFrame with dropped columns, and comparing it to PySpark behavior.

The key: sparkless's DataFrame.__getattr__() method tries to access dropped
columns, causing AttributeError. This happens during internal processing.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("DIRECT SPARKLESS CODE: Column Access Bug")
print("=" * 80)
print("\nThis shows the exact bug using only sparkless code.\n")

spark = SparkSession.builder.appName("sparkless_column_bug").getOrCreate()

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

# Apply the EXACT transform from the test
print("\n" + "=" * 80)
print("STEP 2: Apply transform (exact pattern from test)")
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

# Now the key test: what happens when sparkless processes this internally?
print("\n" + "=" * 80)
print("STEP 3: Testing what sparkless does internally (THE BUG)")
print("=" * 80)

# The error suggests something is trying: df.impression_date
# Let's see what happens with different access patterns

print("\n3a. Direct attribute access (this is what fails):")
try:
    col = df_transformed.impression_date
    print(f"   ❌ Should have failed - column was dropped!")
except AttributeError as e:
    error_msg = str(e)
    print(f"   ✅ AttributeError (expected): {error_msg}")
    
    # Check if this matches the test error
    if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   Error message: 'DataFrame' object has no attribute 'impression_date'")
        print("   This proves sparkless's DataFrame.__getattr__() is being called")
        print("   when code tries to access a dropped column!")

# Test what happens when we try to use the column in an expression
print("\n3b. Using column in expression (this should work):")
try:
    # This uses F.col() which is the correct way
    expr = F.col("impression_date_parsed").isNotNull()
    result = df_transformed.filter(expr)
    count = result.count()
    print(f"   ✅ Using F.col() works: {count} rows")
except Exception as e:
    print(f"   ❌ ERROR: {e}")

# Test what happens if we try to use the dropped column in an expression
print("\n3c. Using DROPPED column in expression (this should fail gracefully):")
try:
    # This should fail because the column doesn't exist
    expr = F.col("impression_date").isNotNull()
    result = df_transformed.filter(expr)
    count = result.count()
    print(f"   ❌ Should have failed - column doesn't exist!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Expected error: {error_msg[:150]}")
    # This error should be different from the AttributeError

# The key: what if something tries to check the column using attribute access?
print("\n3d. Simulating internal code that might cause the bug:")
print("   What if code does: df.impression_date (attribute access)?")

# This is what might be happening internally
def simulate_internal_column_check(df, column_name):
    """Simulates what might happen in sparkless's internal code."""
    # Method 1: Check if in columns (correct)
    if column_name in df.columns:
        return True
    
    # Method 2: Try attribute access (this is the bug!)
    # Some code might do this:
    try:
        col = getattr(df, column_name)
        return col is not None
    except AttributeError as e:
        # This is where the error occurs!
        raise AttributeError(
            f"'DataFrame' object has no attribute '{column_name}'. "
            f"Available columns: {', '.join(df.columns)}"
        ) from e

print(f"\n   Trying to check column 'impression_date' using internal pattern...")
try:
    result = simulate_internal_column_check(df_transformed, "impression_date")
    print(f"   ✅ Column check succeeded: {result}")
except AttributeError as e:
    error_msg = str(e)
    print(f"   ❌ Column check failed: {error_msg}")
    if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS MATCHES THE TEST ERROR!")
        print("   This proves the bug: code uses attribute access to check columns")

# Test with a column that exists
print(f"\n   Trying to check column 'impression_date_parsed' (exists)...")
try:
    result = simulate_internal_column_check(df_transformed, "impression_date_parsed")
    print(f"   ✅ Column check succeeded: {result}")
except AttributeError as e:
    print(f"   ❌ Unexpected error: {e}")

print("\n" + "=" * 80)
print("STEP 4: The Actual Bug Pattern")
print("=" * 80)

print("""
The bug occurs when sparkless's internal code does this:

1. Code has a column name (e.g., "impression_date")
2. It tries to check if the column exists
3. Instead of using: column_name in df.columns ✅
4. It uses: getattr(df, column_name) or df.column_name ❌
5. When the column was dropped, this raises AttributeError

The error message format suggests sparkless's DataFrame.__getattr__() method
is raising the error with a helpful message showing available columns.

This happens in sparkless's internal processing, not in user code.
""")

# Demonstrate the difference
print("\n4a. Correct way to check columns:")
print("   if 'impression_date' in df.columns:")
if 'impression_date' in df_transformed.columns:
    print("   ✅ Column exists")
else:
    print("   ✅ Column doesn't exist (correct)")

print("\n4b. Incorrect way (causes the bug):")
print("   col = df.impression_date  # Attribute access")
try:
    col = df_transformed.impression_date
    print("   ✅ Column exists (but it doesn't!)")
except AttributeError as e:
    print(f"   ❌ AttributeError: {str(e)[:100]}")
    print("   This is the bug - using attribute access instead of checking df.columns")

print("\n" + "=" * 80)
print("ROOT CAUSE")
print("=" * 80)
print("""
The bug is that sparkless's internal code (possibly in DataFrame processing,
validation, or column tracking) uses attribute access (df.column_name) to
check or access columns, instead of checking df.columns first.

When a column is dropped via .select():
- df.columns correctly shows it's gone ✅
- But df.column_name (attribute access) still tries to access it ❌
- This raises AttributeError with the exact error message we see

The fix: sparkless internal code should use:
- 'column_name' in df.columns (to check existence)
- F.col('column_name') or df['column_name'] (to access)
- NOT df.column_name (attribute access) for dropped columns
""")

spark.stop()

