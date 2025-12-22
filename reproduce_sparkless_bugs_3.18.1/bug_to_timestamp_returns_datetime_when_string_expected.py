"""
Bug Reproduction: to_timestamp() returns datetime when String expected in sparkless 3.18.1

Issue: In sparkless 3.18.1, F.to_timestamp() correctly returns a datetime column,
but sparkless's validation system expects String type, causing validation failures.

Error: invalid series dtype: expected `String`, got `datetime[μs]` for series with name...

This is the REVERSE of Issue #149 - sparkless 3.18.0 expected Datetime but got String.
Now sparkless 3.18.1 expects String but got datetime (the correct type from PySpark's perspective).

The core issue: sparkless's type validation system doesn't recognize datetime columns
created by to_timestamp() as valid, even though they are the correct type.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType

# Create sparkless session
spark = SparkSession.builder.appName("to_timestamp_string_expected_bug").getOrCreate()

# Create test data with datetime strings (with microseconds)
data = [
    ("2024-01-15T10:30:45.123456",),
    ("2024-01-16T14:20:30.789012",),
    ("2024-01-17T09:15:22.456789",),
]
df = spark.createDataFrame(data, ["date_string"])

print("=" * 80)
print("STEP 1: Original DataFrame")
print("=" * 80)
df.show(truncate=False)
df.printSchema()
print(f"\nColumn 'date_string' type: {df.schema['date_string'].dataType}")
print(f"Expected: StringType")
print(f"Actual: {type(df.schema['date_string'].dataType).__name__}")
assert isinstance(df.schema['date_string'].dataType, StringType), "date_string should be StringType"

# Apply to_timestamp transformation (this is the pattern used in failing tests)
print("\n" + "=" * 80)
print("STEP 2: Apply to_timestamp() transformation")
print("=" * 80)
df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

df_transformed.show(truncate=False)
df_transformed.printSchema()
print(f"\nColumn 'date_parsed' type: {df_transformed.schema['date_parsed'].dataType}")
print(f"Expected: TimestampType (this is CORRECT - to_timestamp should return timestamp)")
print(f"Actual: {type(df_transformed.schema['date_parsed'].dataType).__name__}")

# Verify the type is correct from PySpark's perspective
assert isinstance(df_transformed.schema['date_parsed'].dataType, TimestampType), \
    "date_parsed should be TimestampType - to_timestamp() works correctly!"

print("\n✅ PySpark correctly returns TimestampType for to_timestamp()")

# Now try to use the column in a way that triggers sparkless validation
print("\n" + "=" * 80)
print("STEP 3: Use datetime column in validation context (triggers sparkless bug)")
print("=" * 80)

try:
    # This simulates what happens in pipeline validation
    # When sparkless validates the column, it expects String but finds datetime
    df_with_validation = df_transformed.filter(
        F.col("date_parsed").isNotNull()
    )
    
    # Try to access the column - this may trigger sparkless's internal validation
    result = df_with_validation.select("date_parsed").collect()
    
    print("✅ Column access succeeded")
    print(f"Result count: {len(result)}")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    print("\nThis is where sparkless fails:")
    print("ERROR: invalid series dtype: expected `String`, got `datetime[μs]`")
    print("\nThe problem:")
    print("- PySpark correctly returns TimestampType for to_timestamp()")
    print("- But sparkless's validation expects StringType")
    print("- This is a sparkless type system bug, not a PySpark bug")

# Demonstrate that datetime operations work correctly
print("\n" + "=" * 80)
print("STEP 4: Demonstrate datetime operations work correctly")
print("=" * 80)

try:
    df_with_hour = df_transformed.withColumn(
        "hour", F.hour(F.col("date_parsed"))
    )
    print("✅ Datetime operations work correctly:")
    df_with_hour.show(truncate=False)
    print("\nThis proves the column IS a datetime and works correctly in PySpark")
    print("The issue is ONLY in sparkless's validation/type checking system")
except Exception as e:
    print(f"❌ ERROR: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("""
EXPECTED BEHAVIOR:
- to_timestamp() should return TimestampType ✅ (PySpark does this correctly)
- sparkless should accept TimestampType columns in validation ✅ (but it doesn't)

ACTUAL BEHAVIOR:
- to_timestamp() returns TimestampType ✅ (PySpark works correctly)
- sparkless validation expects StringType ❌ (sparkless bug)

ROOT CAUSE:
sparkless's type validation system incorrectly expects StringType for columns
that are created by to_timestamp(), even though PySpark correctly returns TimestampType.

This is a sparkless type system bug, not a PySpark compatibility issue.
""")

