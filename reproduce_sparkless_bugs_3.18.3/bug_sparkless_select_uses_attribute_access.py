"""
EXACT BUG REPRODUCTION: sparkless select() uses attribute access

This demonstrates the exact bug: sparkless's select() method tries to access
columns using attribute access (df.column_name), which fails when columns
are dropped. PySpark's select() doesn't have this issue.

THE BUG: When select() processes column references, sparkless tries df.column_name
instead of checking df.columns first.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("EXACT BUG: sparkless select() Attribute Access")
print("=" * 80)
print("\nThis shows the exact bug using only sparkless code.\n")

spark = SparkSession.builder.appName("sparkless_select_bug").getOrCreate()

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

# Apply transform that drops the column
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
print(f"'impression_date' exists: {'impression_date' in df_transformed.columns}")

# THE BUG: Try to select the dropped column
print("\n" + "=" * 80)
print("STEP 3: THE BUG - select() with dropped column")
print("=" * 80)

print("\n3a. Using select() with string (dropped column):")
try:
    result = df_transformed.select("impression_date")
    count = result.count()
    print(f"   ❌ Should have failed!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  THIS IS THE EXACT BUG!")
        print("   sparkless's select() uses attribute access: df.impression_date")

print("\n3b. Using select() with F.col() (dropped column):")
try:
    result = df_transformed.select(F.col("impression_date"))
    count = result.count()
    print(f"   ❌ Should have failed!")
except Exception as e:
    error_msg = str(e)
    print(f"   ✅ Error: {error_msg[:200]}")
    
    if "has no attribute 'impression_date'" in error_msg:
        print("\n   ⚠️  BUG CONFIRMED!")
        print("   select(F.col()) also triggers attribute access!")

print("\n" + "=" * 80)
print("ROOT CAUSE")
print("=" * 80)
print("""
THE BUG:

sparkless's select() method uses attribute access (df.column_name) to resolve
column references. When a column was dropped:

1. df.columns correctly shows it's gone ✅
2. But select() tries df.column_name ❌
3. This raises: AttributeError: 'DataFrame' object has no attribute 'column_name'

This is in sparkless's select() implementation, not in PipelineBuilder code.

EVIDENCE:
- select("impression_date") → AttributeError (uses df.impression_date)
- select(F.col("impression_date")) → AttributeError (uses df.impression_date)

The fix: sparkless's select() should check df.columns first, not use attribute access.
""")

spark.stop()

