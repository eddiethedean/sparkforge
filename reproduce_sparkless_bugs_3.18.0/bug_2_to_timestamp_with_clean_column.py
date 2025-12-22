"""
Bug Reproduction: to_timestamp() on cleaned column returns String

Issue: In sparkless 3.18.0, F.to_timestamp() on a column that was cleaned
(without explicit cast) also returns String when Datetime expected.

Error: expected output type 'Datetime('μs')', got 'String'; set `return_dtype` to the proper datatype

Affects:
- Healthcare Pipeline (normalize_lab_results_transform, normalize_diagnoses_transform)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create sparkless session
spark = SparkSession.builder.appName("to_timestamp_clean_bug").getOrCreate()

# Create test data with datetime strings
data = [
    ("2024-01-15T10:30:45.123456",),
    ("2024-01-16T14:20:30.789012",),
    ("2024-01-17T09:15:22.456789",),
]
df = spark.createDataFrame(data, ["test_date"])

print("Original DataFrame:")
df.show(truncate=False)
df.printSchema()

# This is the pattern used in healthcare pipeline
# Step 1: Clean the date (remove microseconds)
# Step 2: Parse with to_timestamp (no explicit cast)
df_transformed = (
    df.withColumn(
        "test_date_clean",
        F.regexp_replace(F.col("test_date"), r"\.\d+", ""),  # Remove microseconds
    )
    .withColumn(
        "test_date_parsed",
        F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    .drop("test_date_clean")
)

print("\nTransformed DataFrame:")
df_transformed.show(truncate=False)
df_transformed.printSchema()

# Try to use the parsed date column (this is where validation fails)
try:
    df_with_validation = df_transformed.filter(
        F.col("test_date_parsed").isNotNull()
    )
    print("\nWith validation filter:")
    df_with_validation.show(truncate=False)
except Exception as e:
    print(f"\nERROR: {e}")

# Expected: test_date_parsed should be TimestampType
# Actual: test_date_parsed is StringType in sparkless 3.18.0

