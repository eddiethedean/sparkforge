"""
Bug 6: Column resolution error - "unable to find column 'source'"

This reproduces the bug where sparkless cannot resolve a column that was created
in a transform but then referenced in validation or downstream operations.
"""

from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("bug_column_resolution_source").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: Column resolution error - 'unable to find column source'")
print("=" * 80)

# Create test data
data = []
for i in range(10):
    data.append({
        "id": f"ID-{i:03d}",
        "value": i * 10,
    })

df = spark.createDataFrame(data, ["id", "value"])

print("\nSTEP 1: Create DataFrame")
print("=" * 80)
print(f"Columns: {df.columns}")

print("\nSTEP 2: Apply transform that creates 'source' column")
print("=" * 80)

# Transform that creates 'source' column (like in data quality pipeline)
transformed_df = (
    df.agg(
        F.count("*").alias("total_records"),
        F.avg("value").alias("avg_value"),
    )
    .withColumn("source", F.lit("source_a"))  # Create 'source' column
)

print("✅ Transform completed")
print(f"   Columns: {transformed_df.columns}")
print(f"   'source' in columns: {'source' in transformed_df.columns}")

print("\nSTEP 3: Try to select 'source' column (THIS SHOULD WORK)")
print("=" * 80)

try:
    result = transformed_df.select("source", "total_records")
    rows = result.collect()
    print(f"✅ Selection succeeded: {rows}")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Selection failed: {error_msg[:300]}")
    
    if "unable to find column" in error_msg.lower() and "source" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - Transform creates 'source' column with F.lit('source_a')")
        print("  - Column exists in DataFrame.columns")
        print("  - Error: 'unable to find column source'")
        print("  - sparkless cannot resolve the column even though it exists")
        print("=" * 80)
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

print("\nSTEP 4: Try to filter by 'source' column")
print("=" * 80)

try:
    result = transformed_df.filter(F.col("source") == "source_a")
    count = result.count()
    print(f"✅ Filter succeeded: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Filter failed: {error_msg[:300]}")
    
    if "unable to find column" in error_msg.lower() and "source" in error_msg:
        print("\n  ⚠️  Filter also fails with same error")

print("\nSTEP 5: Try to use 'source' in validation")
print("=" * 80)

try:
    # This is what validation does
    result = transformed_df.filter(F.col("source").isNotNull())
    count = result.count()
    print(f"✅ Validation filter succeeded: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Validation filter failed: {error_msg[:300]}")
    
    if "unable to find column" in error_msg.lower() and "source" in error_msg:
        print("\n  ⚠️  Validation also fails with same error")

spark.stop()

