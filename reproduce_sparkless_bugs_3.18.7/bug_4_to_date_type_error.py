"""
Bug 4: to_date() requires StringType or DateType input, got TimestampType

This reproduces the bug where to_date() fails when given a TimestampType column.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.appName("bug_to_date_type").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: to_date() type handling error")
print("=" * 80)

# Create test data with timestamp
data = []
for i in range(10):
    data.append({
        "event_id": f"EVT-{i:03d}",
        "event_timestamp": datetime.now(),
    })

df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

print("\nSTEP 1: Create DataFrame with timestamp")
print("=" * 80)
print(f"Columns: {df.columns}")
print(f"Schema: {df.schema}")

print("\nSTEP 2: Parse timestamp (like in streaming pipeline)")
print("=" * 80)

# Parse timestamp (this creates a TimestampType column)
parsed_df = df.withColumn(
    "event_timestamp_parsed",
    F.to_timestamp(F.col("event_timestamp").cast("string"), "yyyy-MM-dd HH:mm:ss")
)

print("✅ Timestamp parsed")
print(f"   Columns: {parsed_df.columns}")

print("\nSTEP 3: Apply to_date() on TimestampType column (THIS TRIGGERS THE BUG)")
print("=" * 80)

try:
    result_df = parsed_df.withColumn(
        "event_date",
        F.to_date(F.col("event_timestamp_parsed"))  # This should work with TimestampType
    )
    
    result = result_df.select("event_id", "event_date").limit(3).collect()
    print(f"✅ to_date() succeeded: {result}")
    
except Exception as e:
    error_msg = str(e)
    print(f"❌ BUG REPRODUCED: {error_msg[:400]}")
    
    if "to_date() requires StringType or DateType" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - to_date() should accept TimestampType (PySpark does)")
        print("  - Error: 'to_date() requires StringType or DateType input, got TimestampType'")
        print("  - Workaround: Cast to string first (but this shouldn't be necessary)")
        print("=" * 80)
        
        print("\nSTEP 4: Test workaround (cast to string first)")
        print("=" * 80)
        try:
            result_df = parsed_df.withColumn(
                "event_date",
                F.to_date(F.col("event_timestamp_parsed").cast("string"))
            )
            result = result_df.select("event_id", "event_date").limit(3).collect()
            print(f"✅ Workaround works: {result}")
        except Exception as e2:
            print(f"❌ Workaround also failed: {e2}")
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

spark.stop()

