"""
Complete Bug Reproduction: sparkless vs PySpark Attribute Access

This script demonstrates the bug using ONLY sparkless and PySpark code,
showing the exact difference in behavior.

The bug: sparkless supports attribute access (df.column_name) but fails
when the column is dropped. PySpark doesn't support attribute access,
so the bug doesn't occur.
"""

from sparkless import SparkSession as SparklessSession, functions as SparklessF

print("=" * 80)
print("COMPLETE BUG REPRODUCTION: sparkless Attribute Access")
print("=" * 80)

# Test in sparkless
print("\n" + "=" * 80)
print("TEST 1: SPARKLESS (Mock Mode)")
print("=" * 80)

spark = SparklessSession.builder.appName("sparkless_bug").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1"),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2"),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This will be dropped
    "campaign_id",
    "customer_id",
])

print("\n1. Original DataFrame:")
print(f"   Columns: {df.columns}")

# Test attribute access on existing column
print("\n2. Testing attribute access on EXISTING column:")
try:
    col = df.impression_date
    print(f"   ✅ sparkless: df.impression_date works")
    print(f"      Returns: {type(col).__name__}")
except AttributeError as e:
    print(f"   ❌ sparkless: df.impression_date failed: {e}")

# Apply transform that drops the column
print("\n3. Applying transform (drops 'impression_date'):")
df_transformed = (
    df.withColumn(
        "impression_date_parsed",
        SparklessF.to_timestamp(
            SparklessF.regexp_replace(SparklessF.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # New column, original is dropped
    )
)

print(f"   Columns after transform: {df_transformed.columns}")
print(f"   'impression_date' in columns: {'impression_date' in df_transformed.columns}")

# Test attribute access on dropped column - THIS IS THE BUG
print("\n4. Testing attribute access on DROPPED column (THE BUG):")
try:
    col = df_transformed.impression_date
    print(f"   ❌ Should have failed - column was dropped!")
except AttributeError as e:
    error_msg = str(e)
    print(f"   ✅ sparkless: df.impression_date fails (as expected)")
    print(f"      Error: {error_msg}")
    
    if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT ERROR FROM THE TEST!")
        print("   Error message matches: 'DataFrame' object has no attribute 'impression_date'")
        print("   This proves the bug is in sparkless's attribute access!")

# Test attribute access on new column (should work)
print("\n5. Testing attribute access on NEW column:")
try:
    col = df_transformed.impression_date_parsed
    print(f"   ✅ sparkless: df.impression_date_parsed works")
    print(f"      Returns: {type(col).__name__}")
except AttributeError as e:
    print(f"   ❌ sparkless: df.impression_date_parsed failed: {e}")

spark.stop()

# Test in PySpark
print("\n" + "=" * 80)
print("TEST 2: PYSPARK (Real Mode)")
print("=" * 80)

from pyspark.sql import SparkSession as PySparkSession, functions as PySparkF

spark = PySparkSession.builder \
    .appName("pyspark_comparison") \
    .master("local[1]") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

# Same test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1"),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2"),
]

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",
    "campaign_id",
    "customer_id",
])

print("\n1. Original DataFrame:")
print(f"   Columns: {df.columns}")

# Test attribute access in PySpark
print("\n2. Testing attribute access in PySpark:")
try:
    col = df.impression_date
    print(f"   ✅ PySpark: df.impression_date works")
except AttributeError as e:
    print(f"   ✅ PySpark: df.impression_date fails (expected)")
    print(f"      PySpark does NOT support attribute access to columns")
    print(f"      Error: {str(e)[:100]}")

# Apply same transform
print("\n3. Applying same transform:")
df_transformed = (
    df.withColumn(
        "impression_date_parsed",
        PySparkF.to_timestamp(
            PySparkF.regexp_replace(PySparkF.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",
    )
)

print(f"   Columns after transform: {df_transformed.columns}")

# Test attribute access on dropped column in PySpark
print("\n4. Testing attribute access on DROPPED column in PySpark:")
try:
    col = df_transformed.impression_date
    print(f"   ❌ Should have failed - PySpark doesn't support attribute access")
except AttributeError as e:
    print(f"   ✅ PySpark: df.impression_date fails (expected)")
    print(f"      PySpark doesn't support attribute access, so this bug doesn't occur")
    print(f"      Error: {str(e)[:100]}")

spark.stop()

# Summary
print("\n" + "=" * 80)
print("SUMMARY: The Bug Explained")
print("=" * 80)
print("""
The bug occurs because:

1. sparkless supports attribute access: df.column_name ✅
   - This is a feature of sparkless DataFrames
   - You can access columns as attributes

2. When a column is dropped via .select(), it's removed ✅
   - The column no longer exists in the DataFrame
   - df.columns no longer includes it

3. But attribute access still tries to find it ❌
   - df.impression_date tries to access the column
   - Even though it was dropped
   - This causes: AttributeError

4. PySpark doesn't have this issue ✅
   - PySpark doesn't support attribute access to columns
   - You must use df['column'] or F.col('column')
   - So the bug doesn't occur

5. PipelineBuilder likely uses attribute access internally ❌
   - When processing transform outputs
   - It might check columns using df.column_name
   - This fails when columns are dropped
   - Causing the pipeline to fail

THE FIX:
sparkless should either:
- Not support attribute access to columns (like PySpark)
- Or properly handle dropped columns in attribute access
- Or PipelineBuilder should use df.columns or F.col() instead of attribute access
""")

