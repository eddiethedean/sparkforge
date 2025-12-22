"""
Investigate: Bug Still Exists in sparkless 3.18.7

The bug manifests differently but is still present.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_still_exists").getOrCreate()

# Create test data (150 rows)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile", "tablet"][i % 3],
    })

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("STEP 1: Apply transform")
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

print(f"✅ Transform completed")

# Check if to_timestamp worked
print("\nSTEP 2: Check to_timestamp() results")
print("=" * 80)

try:
    sample = silver_df.select("impression_id", "impression_date", "impression_date_parsed").limit(5).collect()
    print("Sample rows (checking if impression_date_parsed is None):")
    for row in sample:
        print(f"  impression_id={row[0]}, impression_date_parsed={row[2]}")
except Exception as e:
    print(f"❌ Error: {e}")
    if "cannot resolve 'impression_date'" in str(e):
        print("  ⚠️  BUG STILL EXISTS: cannot resolve 'impression_date'")
    import traceback
    traceback.print_exc()

# Test simple validation (this triggers the bug)
print("\nSTEP 3: Test simple validation (triggers the bug)")
print("=" * 80)

try:
    # This is what validation does - filter with isNotNull
    valid_df = silver_df.filter(F.col("impression_id").isNotNull())
    count = valid_df.count()
    print(f"✅ Validation filter worked: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Validation filter failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n  ⚠️  BUG CONFIRMED: cannot resolve 'impression_date'")
        print("  The bug still exists in sparkless 3.18.7!")
        print("  It occurs when validating columns after a transform that drops columns.")
    import traceback
    traceback.print_exc()

# Test if to_timestamp is the issue
print("\nSTEP 4: Test to_timestamp() directly")
print("=" * 80)

try:
    # Test to_timestamp on a simple DataFrame
    test_data = [("2024-01-15T10:30:45",)]
    test_df = spark.createDataFrame(test_data, ["date_str"])
    
    result_df = test_df.withColumn(
        "parsed",
        F.to_timestamp(F.col("date_str"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    
    result = result_df.collect()
    print(f"✅ to_timestamp() test: {result}")
    print(f"   Parsed value: {result[0][1]}")
    
    if result[0][1] is None:
        print("  ⚠️  to_timestamp() returns None - this is a separate bug!")
except Exception as e:
    print(f"❌ to_timestamp() test failed: {e}")

spark.stop()

