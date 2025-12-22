"""
Bug 5: Unsupported function: unix_timestamp

This reproduces the bug where unix_timestamp() is not supported in sparkless.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.appName("bug_unix_timestamp").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: unix_timestamp() unsupported")
print("=" * 80)

# Create test data
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

print("\nSTEP 2: Test unix_timestamp() function (THIS TRIGGERS THE BUG)")
print("=" * 80)

try:
    result_df = df.withColumn(
        "unix_ts",
        F.unix_timestamp(F.col("event_timestamp"))
    )
    
    result = result_df.select("event_id", "unix_ts").limit(3).collect()
    print(f"✅ unix_timestamp() succeeded: {result}")
    
except Exception as e:
    error_msg = str(e)
    print(f"❌ BUG REPRODUCED: {error_msg[:300]}")
    
    if "Unsupported function" in error_msg and "unix_timestamp" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - unix_timestamp() is a standard PySpark function")
        print("  - Error: 'Unsupported function: unix_timestamp'")
        print("  - This function should be supported in sparkless")
        print("=" * 80)
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

spark.stop()

