"""
Bug 3: invalid series dtype: expected String, got datetime[μs]

This reproduces the exact bug from the streaming hybrid pipeline test where
unified_analytics fails with:
  "invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed`"

This occurs when using to_date() on a TimestampType column.

Version: sparkless 3.19.0
Issue: Related to Issue #165
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_to_date_datetime").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: to_date() on TimestampType fails")
print("=" * 80)
print("This reproduces the exact bug from test_streaming_hybrid_pipeline.py")
print()

# Create test data similar to streaming pipeline
print("STEP 1: Create test data (event data)")
print("-" * 80)
data = []
for i in range(100):
    data.append({
        "event_id": f"EVT-{i:08d}",
        "user_id": f"USER-{i % 50:04d}",
        "event_timestamp": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "event_type": ["click", "view", "purchase"][i % 3],
        "product_id": f"PROD-{i % 20:03d}",
    })

bronze_df = spark.createDataFrame(data, [
    "event_id",
    "user_id",
    "event_timestamp",
    "event_type",
    "product_id",
])

print(f"✅ Created DataFrame with {bronze_df.count()} rows")

# Apply EXACT transform from test (unified_batch_transform)
print("\nSTEP 2: Parse timestamp (EXACT from test)")
print("-" * 80)
print("Transform: unified_batch_transform")
print("  1. Parses event_timestamp to event_timestamp_parsed (TimestampType)")
print()

silver_df = (
    bronze_df.withColumn(
        "event_timestamp_clean",
        F.regexp_replace(F.col("event_timestamp"), r"\.\d+", ""),
    )
    .withColumn(
        "event_timestamp_parsed",
        F.to_timestamp(F.col("event_timestamp_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    .drop("event_timestamp_clean")
    .withColumn("data_source", F.lit("batch"))
    .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", True).otherwise(False))
    .withColumn("is_mobile", F.lit(False))
    .withColumn("event_value", F.lit(0.0))
    .withColumn("session_id", F.lit("session_1"))
    .select(
        "event_id",
        "user_id",
        "event_timestamp_parsed",  # This is TimestampType
        "event_type",
        "product_id",
        "data_source",
        "is_purchase",
        "is_mobile",
        "event_value",
        "session_id",
    )
)

print(f"✅ Transform completed")
print(f"   Columns: {silver_df.columns}")

# Check column type
print("\nSTEP 3: Check column type")
print("-" * 80)
try:
    schema = silver_df.schema
    for field in schema.fields:
        if field.name == "event_timestamp_parsed":
            print(f"   {field.name}: {field.dataType}")
            if "Timestamp" in str(field.dataType):
                print("   ✅ Column is TimestampType (as expected)")
except Exception as e:
    print(f"   ⚠️  Could not check schema: {e}")

# Apply EXACT transform from test (unified_analytics_transform)
print("\nSTEP 4: Apply to_date() on TimestampType (THIS TRIGGERS THE BUG)")
print("-" * 80)
print("Transform: unified_analytics_transform")
print("  1. Uses to_date() on event_timestamp_parsed (TimestampType)")
print("  2. Groups by metric_date")
print()
print("Expected: to_date() should accept TimestampType (PySpark does)")
print("Actual: sparkless expects StringType")
print()

try:
    gold_df = (
        silver_df.withColumn(
            "metric_date",
            F.to_date(F.col("event_timestamp_parsed")),  # ❌ Error here
        )
        .groupBy("metric_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("total_users"),
        )
    )
    
    print("✅ to_date() succeeded")
    result = gold_df.limit(5).collect()
    print(f"   Sample results: {result}")
    
except Exception as e:
    error_msg = str(e)
    print(f"❌ BUG REPRODUCED: {error_msg[:400]}")
    
    if "invalid series dtype" in error_msg.lower() and "expected" in error_msg.lower() and "String" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED: invalid series dtype: expected String, got datetime[μs]")
        print("=" * 80)
        print("This matches the error in test_streaming_hybrid_pipeline.py:")
        print("  'invalid series dtype: expected `String`, got `datetime[μs]`")
        print("   for series with name `event_timestamp_parsed`'")
        print("\nRoot cause:")
        print("  - to_date() in sparkless only accepts StringType or DateType")
        print("  - PySpark's to_date() accepts TimestampType")
        print("  - This is inconsistent behavior")
        print("\nWorkaround (doesn't work):")
        print("  F.to_date(F.col('event_timestamp_parsed').cast('string'))")
        print("  But even this may fail in some cases")
    elif "to_date() requires StringType" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED: to_date() requires StringType or DateType")
        print("=" * 80)
        print("sparkless's to_date() doesn't accept TimestampType")
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

spark.stop()

