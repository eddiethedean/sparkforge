"""
Direct sparkless code showing F.col() behavior with dropped columns

This demonstrates what happens when F.col() is used with a column name
that was dropped, and how sparkless processes this differently than PySpark.

The bug might be in how sparkless's F.col() or column evaluation works
when the column doesn't exist.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("SPARKLESS F.col() BEHAVIOR WITH DROPPED COLUMNS")
print("=" * 80)
print("\nTesting how sparkless handles F.col() with dropped columns.\n")

spark = SparkSession.builder.appName("sparkless_f_col_bug").getOrCreate()

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

# Apply transform
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

# Now test what happens with F.col() on dropped column
print("\n" + "=" * 80)
print("STEP 3: Testing F.col() with dropped column name")
print("=" * 80)

print("\n3a. Creating F.col() expression for dropped column:")
try:
    # Create the column expression (this should work - it's just creating an expression)
    col_expr = F.col("impression_date")
    print(f"   ✅ F.col('impression_date') created: {type(col_expr)}")
    print(f"   Expression type: {type(col_expr).__name__}")
except Exception as e:
    print(f"   ❌ ERROR creating F.col(): {e}")

print("\n3b. Using F.col() expression in a filter (this is where it might fail):")
try:
    # Try to use the column expression
    filtered = df_transformed.filter(F.col("impression_date").isNotNull())
    count = filtered.count()
    print(f"   ✅ Filter with F.col() worked: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"   ❌ ERROR using F.col() in filter: {error_msg[:200]}")
    
    # Check if this is the attribute error
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE BUG!")
        print("   F.col('impression_date') triggers attribute access internally!")

print("\n3c. Using F.col() expression in select:")
try:
    # Try to select the column
    selected = df_transformed.select(F.col("impression_date"))
    result = selected.collect()
    print(f"   ❌ Should have failed - column doesn't exist!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Expected error: {error_msg[:200]}")
    
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE BUG!")
        print("   F.col('impression_date') in select() triggers attribute access!")

print("\n3d. Testing what happens when F.col() is evaluated:")
print("   What if sparkless evaluates F.col() by trying attribute access?")

# The key insight: maybe when sparkless evaluates F.col("impression_date"),
# it internally tries to do df.impression_date to get the column reference?

try:
    # Try to create a column reference and see what happens
    col_ref = F.col("impression_date")
    
    # Try to use it in a simple operation
    # Maybe sparkless tries to resolve it by accessing df.impression_date?
    test_df = df_transformed.select("impression_id", F.col("impression_date_parsed"))
    result = test_df.collect()
    print(f"   ✅ Using F.col() on existing column works")
    
    # Now try with dropped column
    test_df2 = df_transformed.select("impression_id", F.col("impression_date"))
    result2 = test_df2.collect()
    print(f"   ❌ Should have failed!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error when using F.col() on dropped column: {error_msg[:200]}")
    
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   When F.col('impression_date') is used, sparkless tries df.impression_date")
        print("   This is the root cause - F.col() triggers attribute access!")

print("\n" + "=" * 80)
print("STEP 4: Testing validation rule conversion")
print("=" * 80)

# This is what happens in validation - rules are converted to expressions
print("\n4a. Simulating validation rule conversion:")
print("   Rules: {'impression_date_parsed': ['not_null']}")
print("   This should create: F.col('impression_date_parsed').isNotNull()")

try:
    # This is what validation does
    rule_expr = F.col("impression_date_parsed").isNotNull()
    filtered = df_transformed.filter(rule_expr)
    count = filtered.count()
    print(f"   ✅ Validation expression works: {count} rows")
except Exception as e:
    print(f"   ❌ ERROR: {e}")

print("\n4b. What if validation rules reference the DROPPED column?")
print("   Rules: {'impression_date': ['not_null']}  # This column was dropped!")
print("   This would create: F.col('impression_date').isNotNull()")

try:
    # This is what would happen if rules referenced the dropped column
    rule_expr = F.col("impression_date").isNotNull()
    filtered = df_transformed.filter(rule_expr)
    count = filtered.count()
    print(f"   ❌ Should have failed - column doesn't exist!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Expected error: {error_msg[:200]}")
    
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE BUG!")
        print("   When validation rules reference a dropped column,")
        print("   F.col() triggers attribute access, causing the error!")

print("\n" + "=" * 80)
print("ROOT CAUSE HYPOTHESIS")
print("=" * 80)
print("""
The bug might be that when sparkless evaluates F.col("column_name"):

1. It creates a Column expression ✅
2. But when the expression is evaluated/used, sparkless tries to resolve it
3. It might try: df.column_name (attribute access) to get the column reference
4. If the column was dropped, this raises AttributeError

This would explain:
- Why the error says "'DataFrame' object has no attribute 'impression_date'"
- Why it happens during validation (when F.col() expressions are evaluated)
- Why it works in PySpark (PySpark's F.col() doesn't use attribute access)

The fix would be for sparkless's F.col() evaluation to use:
- df['column_name'] or column lookup by name
- NOT df.column_name (attribute access)
""")

spark.stop()

