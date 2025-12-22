#!/usr/bin/env python3
"""
Sparkless Bug #1: Datetime Type Validation Issue

Reproduces: invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`

This occurs when:
- Creating a datetime column using to_timestamp()
- Attempting to validate that column
- Sparkless expects String type but receives datetime[μs]

Minimal reproduction:
"""
from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace, hour, dayofweek, when, lit

spark = SparkSession.builder.appName("BugRepro1").getOrCreate()

# Create sample data
data = [
    ("imp1", "camp1", "user1", "2024-01-15T10:30:00", "mobile", "channel1", "ad1", 0.01),
    ("imp2", "camp2", "user2", "2024-01-15T11:15:00", "desktop", "channel2", "ad2", 0.02),
]
df = spark.createDataFrame(
    data,
    ["impression_id", "campaign_id", "customer_id", "impression_date", "device_type", "channel", "ad_id", "cost_per_impression"]
)

# Transform: Parse datetime and extract components
transformed = (
    df.withColumn(
        "impression_date_parsed",
        to_timestamp(
            regexp_replace(col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", hour(col("impression_date_parsed")))
    .withColumn("day_of_week", dayofweek(col("impression_date_parsed")))
    .withColumn("is_mobile", when(col("device_type") == "mobile", lit(True)).otherwise(lit(False)))
)

# This should work but fails in sparkless 3.17.10
print("Attempting to validate datetime column...")
try:
    # Try to validate the datetime column
    validation_result = transformed.filter(col("impression_date_parsed").isNotNull())
    count = validation_result.count()
    print(f"✅ Success: {count} rows")
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e).__name__}")
    raise

