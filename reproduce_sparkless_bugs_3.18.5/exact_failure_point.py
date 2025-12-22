"""
Exact Failure Point: Reproducing the "cannot resolve" error

This script isolates the exact sparkless code that fails by testing
each operation that might reference the dropped column.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("EXACT FAILURE POINT: sparkless Code")
print("=" * 80)
print("\nTesting each operation to find where 'impression_date' is referenced.\n")

spark = SparkSession.builder.appName("failure_point").getOrCreate()

# Create bronze data
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
print(f"  Columns: {bronze_df.columns}")
print(f"  incremental_col: 'impression_date'")

# Apply transform (drops impression_date)
print("\nSTEP 2: Apply transform (drops 'impression_date')")
print("=" * 80)

silver_df = (
    bronze_df.withColumn(
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

print(f"  Columns after transform: {silver_df.columns}")
print(f"  'impression_date' in columns: {'impression_date' in silver_df.columns}")

# Test each operation that might reference impression_date
print("\nSTEP 3: Test operations that might reference 'impression_date'")
print("=" * 80)

# Operation 1: Materialization
print("\n3a. Materialization (df.count()):")
try:
    count = silver_df.count()
    print(f"  ✅ Success: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:150]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("  ⚠️  THIS IS THE ERROR!")

# Operation 2: Cache
print("\n3b. Cache (df.cache()):")
try:
    cached = silver_df.cache()
    count = cached.count()
    print(f"  ✅ Success: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:150]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("  ⚠️  THIS IS THE ERROR!")

# Operation 3: Collect
print("\n3c. Collect (df.collect()):")
try:
    collected = silver_df.collect()
    print(f"  ✅ Success: {len(collected)} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:150]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("  ⚠️  THIS IS THE ERROR!")

# Operation 4: Write to table (this might trigger the error)
print("\n3d. Write to table (df.write.format('delta').mode('overwrite').saveAsTable(...)):")
print("  This is what PipelineBuilder does - might trigger the error")
try:
    # Create schema first
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Try to write (this might reference columns)
    # Note: In sparkless, saveAsTable might not work the same way
    # Let's try to see if the error occurs during write preparation
    writer = silver_df.write.format("delta").mode("overwrite")
    print("  ✅ Writer created")
    
    # Try to save - this might trigger plan evaluation
    try:
        writer.saveAsTable("silver.processed_impressions")
        print("  ✅ Table write successful")
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Write failed: {error_msg[:200]}")
        if "cannot resolve 'impression_date'" in error_msg:
            print("  ⚠️  THIS IS THE EXACT ERROR!")
            print("  Table write operation triggers the error!")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Writer creation failed: {error_msg[:150]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("  ⚠️  THIS IS THE ERROR!")

# Operation 5: Check if there's a reference in the execution plan
print("\n3e. Check execution plan:")
try:
    # Try to get the execution plan - this might show references to impression_date
    plan = silver_df._jdf.toString() if hasattr(silver_df, "_jdf") else "N/A"
    print(f"  Plan: {plan[:200]}")
except Exception as e:
    print(f"  Could not get plan: {e}")

# Operation 6: Try operations that might internally reference columns
print("\n3f. Operations that might reference columns internally:")
operations = [
    ("show()", lambda df: df.show(1)),
    ("head()", lambda df: df.head(1)),
    ("take(1)", lambda df: df.take(1)),
    ("first()", lambda df: df.first()),
]

for op_name, op_func in operations:
    try:
        result = op_func(silver_df)
        print(f"  ✅ {op_name}: Success")
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ {op_name}: {error_msg[:100]}")
        if "cannot resolve 'impression_date'" in error_msg:
            print(f"  ⚠️  {op_name} triggers the error!")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("""
The error 'cannot resolve impression_date' occurs when sparkless tries to
evaluate an operation that references 'impression_date' on a DataFrame
where that column was dropped.

The exact sparkless code that fails is shown above - it's one of these operations:
- df.count() (materialization)
- df.collect() (materialization)
- df.write.saveAsTable() (table write)
- Or some other operation that triggers plan evaluation

The error message shows columns AFTER transform, so the error occurs AFTER
the transform is applied, during one of these operations.
""")

spark.stop()

