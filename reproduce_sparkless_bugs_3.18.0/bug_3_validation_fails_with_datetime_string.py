"""
Bug Reproduction: Validation fails when datetime column is actually string

Issue: In sparkless 3.18.0, validation rules that check datetime columns fail
because the column is actually a string type, leading to 0% validation rate
and 0 rows processed.

Error: AssertionError: assert 0 > 0 (validation fails, no rows processed)

Affects:
- Healthcare Pipeline (patient_risk_scores step fails validation)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create sparkless session
spark = SparkSession.builder.appName("validation_datetime_bug").getOrCreate()

# Create test data
data = [
    ("P001", "2024-01-15T10:30:45.123456", 75.5),
    ("P002", "2024-01-16T14:20:30.789012", 82.3),
    ("P003", "2024-01-17T09:15:22.456789", 68.9),
]
df = spark.createDataFrame(data, ["patient_id", "test_date", "result_value"])

print("Original DataFrame:")
df.show(truncate=False)
df.printSchema()

# Transform with to_timestamp (which returns string in sparkless 3.18.0)
df_transformed = (
    df.withColumn(
        "test_date_clean",
        F.regexp_replace(F.col("test_date"), r"\.\d+", ""),
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

# Try validation (this is where it fails in healthcare pipeline)
# The validation expects test_date_parsed to be datetime, but it's string
try:
    # Simulate validation rule: not_null on datetime column
    valid_rows = df_transformed.filter(
        F.col("test_date_parsed").isNotNull()
        & F.col("patient_id").isNotNull()
        & F.col("result_value").isNotNull()
    )
    
    print("\nValid rows after validation:")
    print(f"Count: {valid_rows.count()}")
    valid_rows.show(truncate=False)
    
    # In sparkless 3.18.0, this validation may fail because
    # test_date_parsed is string, not datetime
    if valid_rows.count() == 0:
        print("\nERROR: Validation failed - 0 valid rows (expected > 0)")
        print("This is the bug: datetime column is string, validation fails")
    
except Exception as e:
    print(f"\nERROR: {e}")

# Expected: test_date_parsed is TimestampType, validation passes
# Actual: test_date_parsed is StringType, validation may fail or behave incorrectly

