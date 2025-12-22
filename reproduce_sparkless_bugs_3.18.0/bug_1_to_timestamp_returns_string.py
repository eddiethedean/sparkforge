"""
Bug Reproduction: to_timestamp() returns String when Datetime expected

Issue: In sparkless 3.18.0, F.to_timestamp() with regexp_replace().cast("string")
returns a String column when sparkless expects Datetime('μs') in validation contexts.

Error: expected output type 'Datetime('μs')', got 'String'; set `return_dtype` to the proper datatype

This bug occurs when:
1. Using F.to_timestamp() with a pattern that includes regexp_replace().cast("string")
2. The resulting column is used in validation rules or accessed in certain contexts
3. Sparkless expects Datetime('μs') but receives String

Affects:
- Marketing Pipeline (processed_impressions, processed_clicks, processed_conversions)
- Supply Chain Pipeline (processed_orders, processed_shipments, processed_inventory)
- Streaming Hybrid Pipeline (unified_batch_events, unified_streaming_events)
- Data Quality Pipeline (normalized_source_a, normalized_source_b)

Reproduction Steps:
1. Run this script with SPARK_MODE=mock
2. The error occurs when sparkless validates the column type
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create sparkless session
spark = SparkSession.builder.appName("to_timestamp_bug").getOrCreate()

# Create test data with datetime strings (with microseconds)
data = [
    ("2024-01-15T10:30:45.123456",),
    ("2024-01-16T14:20:30.789012",),
    ("2024-01-17T09:15:22.456789",),
]
df = spark.createDataFrame(data, ["date_string"])

print("Original DataFrame:")
df.show(truncate=False)
df.printSchema()

# This is the EXACT pattern used in the failing tests
# Step 1: Remove microseconds with regexp_replace
# Step 2: Cast to string (explicit cast)
# Step 3: Parse with to_timestamp
df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

print("\nTransformed DataFrame:")
df_transformed.show(truncate=False)
df_transformed.printSchema()

# The issue: date_parsed appears as timestamp in schema, but sparkless
# internally treats it as String when validating or accessing in certain contexts
print("\nColumn type check:")
print(f"date_parsed type: {df_transformed.schema['date_parsed'].dataType}")

# Try to use the parsed date column in a way that triggers validation
# This simulates what happens in the pipeline builder when validation rules are applied
try:
    # Using the column in a datetime function should work if it's truly a timestamp
    df_with_hour = df_transformed.withColumn(
        "hour", F.hour(F.col("date_parsed"))
    )
    print("\nWith hour column (should work if date_parsed is timestamp):")
    df_with_hour.show(truncate=False)
    df_with_hour.printSchema()
    
    # This is where the bug manifests - when sparkless validates the type
    # In the actual pipeline, validation rules like {"date_parsed": ["not_null"]}
    # trigger sparkless to check the type, and it finds String instead of Datetime
    print("\nNOTE: In pipeline context with validation rules, sparkless reports:")
    print("ERROR: expected output type 'Datetime('μs')', got 'String'")
    
except Exception as e:
    print(f"\nERROR: {e}")

# Expected: date_parsed should be TimestampType and sparkless should recognize it as Datetime('μs')
# Actual: date_parsed appears as TimestampType in schema, but sparkless treats it as String in validation

