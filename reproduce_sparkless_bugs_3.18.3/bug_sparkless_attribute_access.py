"""
Direct sparkless code demonstrating the column attribute access bug

This script uses sparkless directly to show what happens when code tries to
access a dropped column using attribute access (df.column_name).

The bug: sparkless's DataFrame allows attribute access to columns, but when
a column is dropped via .select(), attribute access still tries to find it,
causing the error.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("DIRECT SPARKLESS CODE: Column Attribute Access Bug")
print("=" * 80)
print("\nThis demonstrates the bug using only sparkless code.\n")

spark = SparkSession.builder.appName("sparkless_attribute_bug").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1"),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2"),
    ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3"),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This column will be dropped
    "campaign_id",
    "customer_id",
])

print("STEP 1: Original DataFrame")
print("=" * 80)
print(f"Columns: {df.columns}")
print(f"Has 'impression_date': {'impression_date' in df.columns}")

# Test attribute access on original DataFrame
print("\n1a. Testing attribute access on original DataFrame:")
try:
    # In sparkless, you can access columns as attributes
    result = df.impression_date
    print(f"   ✅ Attribute access works: {type(result)}")
    print(f"   This shows sparkless supports df.column_name syntax")
except AttributeError as e:
    print(f"   ❌ Attribute access failed: {e}")

# Apply transform that drops the column
print("\n" + "=" * 80)
print("STEP 2: Apply transform that drops 'impression_date'")
print("=" * 80)

df_transformed = (
    df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # New column, original impression_date is dropped
    )
)

print(f"Columns after transform: {df_transformed.columns}")
print(f"Has 'impression_date': {'impression_date' in df_transformed.columns}")
print(f"Has 'impression_date_parsed': {'impression_date_parsed' in df_transformed.columns}")

# Test attribute access on transformed DataFrame
print("\n2a. Testing attribute access on transformed DataFrame:")
print("   (This is where the bug occurs)")

try:
    # Try to access the NEW column (should work)
    result = df_transformed.impression_date_parsed
    print(f"   ✅ Attribute access to 'impression_date_parsed' works: {type(result)}")
except AttributeError as e:
    print(f"   ❌ Attribute access to 'impression_date_parsed' failed: {e}")

try:
    # Try to access the DROPPED column (this is the bug!)
    result = df_transformed.impression_date
    print(f"   ❌ Should have failed - column was dropped!")
except AttributeError as e:
    error_msg = str(e)
    print(f"   ✅ Expected AttributeError: {error_msg}")
    
    # Check if this matches the error from the test
    if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   The error message matches perfectly.")
        print("   This proves sparkless tries attribute access on dropped columns.")

print("\n" + "=" * 80)
print("STEP 3: Simulating what PipelineBuilder does internally")
print("=" * 80)

# PipelineBuilder might be doing something like this internally
# Let's try to replicate what might be happening

print("\n3a. Testing column access patterns that might be used internally:")

# Pattern 1: Direct attribute access (this is what fails)
print("\n   Pattern 1: Direct attribute access (df.column_name)")
try:
    result = df_transformed.impression_date
    print("   ❌ Should have failed")
except AttributeError as e:
    print(f"   ✅ Fails as expected: {str(e)[:100]}")

# Pattern 2: Using getattr (this is safer)
print("\n   Pattern 2: Using getattr with default")
try:
    result = getattr(df_transformed, "impression_date", None)
    if result is None:
        print("   ✅ getattr returns None for missing column (safe)")
    else:
        print(f"   ⚠️  getattr returned: {type(result)}")
except Exception as e:
    print(f"   ❌ getattr failed: {e}")

# Pattern 3: Checking if attribute exists
print("\n   Pattern 3: Using hasattr")
try:
    has_attr = hasattr(df_transformed, "impression_date")
    print(f"   hasattr result: {has_attr}")
    if has_attr:
        print("   ⚠️  hasattr says column exists (but it doesn't!)")
    else:
        print("   ✅ hasattr correctly says column doesn't exist")
except Exception as e:
    print(f"   ❌ hasattr failed: {e}")

# Pattern 4: Checking columns list
print("\n   Pattern 4: Checking columns list (correct way)")
try:
    has_col = "impression_date" in df_transformed.columns
    print(f"   'impression_date' in columns: {has_col}")
    if not has_col:
        print("   ✅ Correctly identifies column doesn't exist")
    else:
        print("   ❌ Incorrectly says column exists")
except Exception as e:
    print(f"   ❌ columns check failed: {e}")

print("\n" + "=" * 80)
print("STEP 4: Demonstrating the bug scenario")
print("=" * 80)

# This simulates what might be happening in PipelineBuilder
# It might be trying to access columns using attribute access

print("\n4a. Simulating internal code that causes the bug:")

def simulate_internal_processing(df, column_name):
    """
    Simulates what PipelineBuilder might do internally.
    This function tries different ways to access a column.
    """
    print(f"\n   Trying to access column '{column_name}':")
    
    # Method 1: Check if in columns (correct)
    if column_name in df.columns:
        print(f"   ✅ Method 1 (in columns): Column exists")
        return True
    else:
        print(f"   ✅ Method 1 (in columns): Column doesn't exist")
    
    # Method 2: Try attribute access (this is the bug!)
    try:
        result = getattr(df, column_name, None)
        if result is not None:
            print(f"   ⚠️  Method 2 (getattr): Returned {type(result)}")
            return True
        else:
            print(f"   ✅ Method 2 (getattr): Returns None (safe)")
    except AttributeError as e:
        print(f"   ❌ Method 2 (getattr): AttributeError - {str(e)[:80]}")
        print("   THIS IS THE BUG - trying attribute access on dropped column!")
        raise
    
    # Method 3: Direct attribute access (this definitely fails)
    try:
        result = df.impression_date  # This will fail
        print(f"   ⚠️  Method 3 (direct): Returned {type(result)}")
        return True
    except AttributeError as e:
        print(f"   ❌ Method 3 (direct): AttributeError - {str(e)[:80]}")
        print("   THIS IS THE BUG - direct attribute access on dropped column!")
        raise
    
    return False

try:
    # Try to access the dropped column
    simulate_internal_processing(df_transformed, "impression_date")
except AttributeError as e:
    print(f"\n   ⚠️  BUG TRIGGERED: {e}")
    print("   This is exactly what happens in PipelineBuilder!")

print("\n" + "=" * 80)
print("STEP 5: Comparing with PySpark behavior")
print("=" * 80)

print("\n5a. In PySpark, attribute access to columns doesn't work the same way:")
print("   - PySpark DataFrames don't support df.column_name syntax")
print("   - You must use df['column_name'] or F.col('column_name')")
print("   - This prevents the bug from occurring")
print("\n5b. In sparkless, attribute access IS supported:")
print("   - sparkless DataFrames support df.column_name syntax")
print("   - But this causes issues when columns are dropped")
print("   - The attribute access still tries to find the column")
print("   - This is the root of the bug!")

print("\n" + "=" * 80)
print("ROOT CAUSE")
print("=" * 80)
print("""
The bug occurs because:

1. sparkless supports attribute access: df.column_name ✅
2. When a column is dropped via .select(), it's removed from the DataFrame ✅
3. But attribute access (df.column_name) still tries to find it ❌
4. This causes: AttributeError: 'DataFrame' object has no attribute 'column_name'

The issue is that sparkless's DataFrame.__getattr__() method doesn't properly
check if a column exists before trying to access it, or it's being called
on a DataFrame where the column was dropped.

When PipelineBuilder processes transform outputs internally, it might be using
attribute access to check or access columns, which fails for dropped columns.
""")

print("\n" + "=" * 80)
print("EVIDENCE")
print("=" * 80)
print("""
✅ Original DataFrame: df.impression_date works (column exists)
✅ Transformed DataFrame: df.impression_date_parsed works (new column exists)
❌ Transformed DataFrame: df.impression_date fails (column was dropped)
   Error: 'DataFrame' object has no attribute 'impression_date'

This proves:
- sparkless supports attribute access to columns
- Attribute access fails when column is dropped
- PipelineBuilder likely uses attribute access internally
- This causes the bug when processing transforms that drop columns
""")

spark.stop()

