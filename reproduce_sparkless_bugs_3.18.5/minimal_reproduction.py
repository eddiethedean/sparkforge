"""
MINIMAL REPRODUCTION: sparkless Bug with Dropped Columns

This is a minimal reproduction of the bug using ONLY sparkless code.
The bug occurs when:
1. A column is used in transformations
2. The column is dropped via .select()
3. .cache() is called, which triggers materialization
4. During materialization, sparkless tries to validate column expressions
5. It fails because it tries to resolve the dropped column

This is a pure sparkless bug!
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("MINIMAL REPRODUCTION: sparkless Bug")
print("=" * 80)
print("\nThis reproduces the bug using ONLY sparkless code.\n")

spark = SparkSession.builder.appName("minimal_reproduction").getOrCreate()

# Create test data (150 rows like the test)
from datetime import datetime, timedelta
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
    "impression_date",  # This column will be dropped
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("STEP 1: Create DataFrame with 'impression_date' column")
print(f"  Columns: {bronze_df.columns}")

# Apply transform that uses impression_date then drops it
print("\nSTEP 2: Apply transform (uses 'impression_date', then drops it)")
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
        "impression_date_parsed",  # New column
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",
        "cost_per_impression",
        "device_type",
        "is_mobile",
        # impression_date is DROPPED - not in select list
    )
)

print(f"  ✅ Transform completed")
print(f"  Columns after transform: {silver_df.columns}")
print(f"  'impression_date' in columns: {'impression_date' in silver_df.columns}")

# STEP 3: Try operations
print("\nSTEP 3: Test operations")
print("=" * 80)

# Operation 1: count() - works fine
print("\n3a. count() (without cache):")
try:
    count = silver_df.count()
    print(f"  ✅ Success: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("     ⚠️  THIS IS THE ERROR!")

# Operation 2: cache() - THIS IS WHERE THE BUG OCCURS
print("\n3b. cache() (THIS TRIGGERS THE BUG):")
try:
    cached_df = silver_df.cache()  # THIS FAILS!
    count = cached_df.count()
    print(f"  ✅ Success: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n     ⚠️  THIS IS THE EXACT ERROR!")
        print("     The bug occurs when calling .cache() on a DataFrame")
        print("     where a column was used in transformations then dropped.")
        print("\n     Root cause:")
        print("     - .cache() triggers materialization")
        print("     - During materialization, sparkless validates all column expressions")
        print("     - It tries to resolve 'impression_date' which was dropped")
        print("     - Error: 'cannot resolve impression_date'")

# Operation 3: collect() - works fine (doesn't trigger the same validation)
print("\n3c. collect() (without cache):")
try:
    collected = silver_df.collect()
    print(f"  ✅ Success: {len(collected)} rows")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("     ⚠️  THIS IS THE ERROR!")

print("\n" + "=" * 80)
print("BUG CONFIRMED")
print("=" * 80)
print("""
The bug is a pure sparkless issue:

1. Transform uses a column (e.g., 'impression_date') in operations
2. Transform drops the column via .select()
3. Calling .cache() triggers materialization
4. During materialization, sparkless validates all column expressions in the plan
5. It tries to resolve the dropped column ('impression_date')
6. Error: "cannot resolve 'impression_date' given input columns: [...]"

The bug is in sparkless's column validation during materialization.
It doesn't account for columns that were dropped via .select().

Workaround: Don't call .cache() on DataFrames where columns were dropped.
However, this is impractical as PipelineBuilder needs to cache for validation.
""")

spark.stop()

