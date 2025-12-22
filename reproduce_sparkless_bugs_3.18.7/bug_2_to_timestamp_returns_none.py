"""
Bug 2: to_timestamp() returns None for all rows

This reproduces the bug where to_timestamp() returns None instead of parsed datetime values.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_to_timestamp_none").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: to_timestamp() returns None for all rows")
print("=" * 80)

# Create test data
data = []
for i in range(10):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
    })

df = spark.createDataFrame(data, ["impression_id", "impression_date"])

print("\nSTEP 1: Original data")
print("=" * 80)
sample = df.select("impression_id", "impression_date").limit(3).collect()
for row in sample:
    print(f"  {row[0]}: {row[1]}")

print("\nSTEP 2: Apply to_timestamp() transform")
print("=" * 80)

result_df = (
    df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select("impression_id", "impression_date", "impression_date_parsed")
)

print("Transform completed")
print("Checking results...")

try:
    results = result_df.collect()
    print(f"\n✅ Collected {len(results)} rows")
    
    none_count = 0
    valid_count = 0
    
    print("\nSample results:")
    for i, row in enumerate(results[:5]):
        parsed_value = row[2]
        if parsed_value is None:
            none_count += 1
            print(f"  Row {i}: {row[0]} -> {row[1]} -> None ❌")
        else:
            valid_count += 1
            print(f"  Row {i}: {row[0]} -> {row[1]} -> {parsed_value} ✅")
    
    print(f"\nSummary:")
    print(f"  Valid (not None): {valid_count}/{len(results)}")
    print(f"  None values: {none_count}/{len(results)}")
    
    if none_count == len(results):
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - to_timestamp() returns None for ALL rows")
        print("  - Expected: Parsed datetime values")
        print("  - Actual: None for all rows")
        print("=" * 80)
    elif none_count > 0:
        print(f"\n⚠️  Partial bug: {none_count}/{len(results)} rows have None values")
    else:
        print("\n✅ No bug: All values parsed correctly")
        
except Exception as e:
    print(f"❌ Error collecting results: {e}")
    import traceback
    traceback.print_exc()

spark.stop()

