"""
Pure sparkless Reproduction - Attempting to reproduce the bug

This script tries to reproduce the bug using ONLY sparkless code,
without PipelineBuilder, to isolate the issue.
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("PURE sparkless REPRODUCTION")
print("=" * 80)
print("\nTesting if the bug can be reproduced with pure sparkless code.\n")

spark = SparkSession.builder.appName("pure_sparkless_reproduction").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
]

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This will be dropped
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

print("STEP 1: Create bronze DataFrame")
print(f"  Columns: {bronze_df.columns}")

# Apply transform that uses impression_date then drops it
print("\nSTEP 2: Apply transform (uses impression_date, then drops it)")
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

# Test various operations
print("\nSTEP 3: Test operations that might trigger the error")
print("=" * 80)

operations = [
    ("count()", lambda df: df.count()),
    ("cache() + count()", lambda df: df.cache().count()),
    ("collect()", lambda df: len(df.collect())),
    ("show()", lambda df: df.show(1)),
    ("head()", lambda df: len(df.head(1))),
    ("take(1)", lambda df: len(df.take(1))),
]

all_passed = True
for op_name, op_func in operations:
    try:
        result = op_func(silver_df)
        print(f"  ✅ {op_name}: Success")
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ {op_name}: {error_msg[:150]}")
        if "cannot resolve 'impression_date'" in error_msg:
            print(f"     ⚠️  THIS IS THE EXACT ERROR!")
            all_passed = False
        else:
            all_passed = False

# Test write operation
print("\nSTEP 4: Test write operation")
print("=" * 80)
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    writer = silver_df.write.format("delta").mode("overwrite")
    writer.saveAsTable("silver.test_table")
    print("  ✅ write.saveAsTable(): Success")
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ write.saveAsTable(): {error_msg[:200]}")
    if "cannot resolve 'impression_date'" in error_msg:
        print("     ⚠️  THIS IS THE EXACT ERROR!")
        all_passed = False
    else:
        all_passed = False

print("\n" + "=" * 80)
if all_passed:
    print("RESULT: All operations succeeded with pure sparkless code")
    print("=" * 80)
    print("\nThe bug does NOT reproduce with pure sparkless code.")
    print("The error only occurs when using PipelineBuilder.")
    print("\nThis suggests:")
    print("1. The bug is triggered by something specific in PipelineBuilder's execution flow")
    print("2. Or there's a specific context/state that PipelineBuilder creates")
    print("3. Or the bug requires a specific combination of operations")
else:
    print("RESULT: Bug reproduced with pure sparkless code!")
    print("=" * 80)

spark.stop()

