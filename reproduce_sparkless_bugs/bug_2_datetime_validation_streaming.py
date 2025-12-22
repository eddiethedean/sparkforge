#!/usr/bin/env python3
"""
Sparkless Bug #2: Datetime Type Validation Issue (Streaming/Batch Events)

Reproduces: invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed`

Same issue as Bug #1 but in a different context (event processing).
"""
from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace, hour, dayofweek, when, lit, coalesce

spark = SparkSession.builder.appName("BugRepro2").getOrCreate()

# Create sample event data
data = [
    ("evt1", "user1", "2024-01-15T10:30:00", "click", "prod1", "sess1", 10.0, "mobile", "batch"),
    ("evt2", "user2", "2024-01-15T11:15:00", "purchase", "prod2", "sess2", 25.0, "desktop", "streaming"),
]
df = spark.createDataFrame(
    data,
    ["event_id", "user_id", "event_timestamp", "event_type", "product_id", "session_id", "amount", "device", "source"]
)

# Transform: Parse datetime
transformed = (
    df.withColumn(
        "event_timestamp_parsed",
        to_timestamp(
            regexp_replace(col("event_timestamp"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", hour(col("event_timestamp_parsed")))
    .withColumn("day_of_week", dayofweek(col("event_timestamp_parsed")))
    .withColumn("is_purchase", when(col("event_type") == "purchase", lit(True)).otherwise(lit(False)))
    .withColumn("event_value", coalesce(col("amount"), lit(0.0)))
)

print("Attempting to validate datetime column...")
try:
    validation_result = transformed.filter(col("event_timestamp_parsed").isNotNull())
    count = validation_result.count()
    print(f"✅ Success: {count} rows")
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e).__name__}")
    raise

