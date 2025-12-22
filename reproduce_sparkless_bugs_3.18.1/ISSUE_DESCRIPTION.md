# Bug: to_timestamp() returns datetime but sparkless validation expects String in 3.18.1

## Summary

In sparkless 3.18.1, `F.to_timestamp()` correctly returns a `TimestampType` column (as PySpark does), but sparkless's internal validation system incorrectly expects `StringType`, causing validation failures with the error:

```
invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
```

## Environment

- **sparkless version**: 3.18.1
- **Python version**: 3.11
- **PySpark version**: 3.5.0+

## Expected Behavior

1. `F.to_timestamp()` should return `TimestampType` ✅ (PySpark does this correctly)
2. Sparkless should accept `TimestampType` columns in validation ✅ (but it doesn't)
3. Datetime operations (hour, dayofweek, etc.) should work on the column ✅ (they do)

## Actual Behavior

1. `F.to_timestamp()` returns `TimestampType` ✅ (PySpark works correctly)
2. Sparkless validation expects `StringType` ❌ (sparkless bug)
3. Error thrown: `invalid series dtype: expected `String`, got `datetime[μs]``

## Steps to Reproduce

### Minimal Reproduction

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.appName("to_timestamp_bug").getOrCreate()

# Create test data
data = [("2024-01-15T10:30:45.123456",)]
df = spark.createDataFrame(data, ["date_string"])

# Apply to_timestamp (this pattern is used in failing tests)
df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

# Verify PySpark returns correct type
print(df_transformed.schema['date_parsed'].dataType)  # TimestampType() ✅

# The error occurs when sparkless validates this column in pipeline context
# Error: invalid series dtype: expected `String`, got `datetime[μs]`
```

### Real-World Example (from failing test)

The error occurs in pipeline transforms like this:

```python
def processed_impressions_transform(spark, df, silvers):
    return (
        df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
        .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
        .select("impression_id", "impression_date_parsed", ...)
    )

# When this transform is used in pipeline with validation rules:
builder.add_silver_transform(
    name="processed_impressions",
    source_bronze="raw_impressions",
    transform=processed_impressions_transform,
    rules={
        "impression_date_parsed": ["not_null"],  # This triggers the error
        ...
    },
)
```

## Error Details

**Error Message:**
```
invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
```

**Error Location:**
- Occurs during sparkless's internal validation of transform outputs
- Happens when validation rules reference columns created by `to_timestamp()`
- The error is thrown by sparkless's type checking system, not PySpark

## Verification

### PySpark Behavior (Correct)

```python
# PySpark correctly returns TimestampType
df.schema['date_parsed'].dataType  # TimestampType() ✅

# Datetime operations work correctly
df.withColumn("hour", F.hour(F.col("date_parsed")))  # Works ✅
```

### Sparkless Behavior (Incorrect)

```python
# Sparkless validation expects StringType but finds TimestampType
# Error: invalid series dtype: expected `String`, got `datetime[μs]` ❌
```

## Root Cause Analysis

1. **PySpark is correct**: `to_timestamp()` returns `TimestampType` as expected
2. **The column works**: All datetime operations (hour, dayofweek, etc.) work correctly
3. **Sparkless bug**: Sparkless's internal type validation system incorrectly expects `StringType` for columns created by `to_timestamp()`

This is a **sparkless type inference/validation bug**, not a PySpark compatibility issue.

## Impact

This affects all pipeline transforms that use `to_timestamp()` to parse datetime strings:

- Marketing pipelines (impression dates, click dates, conversion dates)
- Supply chain pipelines (order dates, shipment dates, inventory dates)
- Streaming pipelines (event timestamps)
- Data quality pipelines (transaction dates)
- Healthcare pipelines (test dates, diagnosis dates)

## Related Issues

- **Issue #149**: Reported the reverse problem in 3.18.0 (expected Datetime, got String)
- **This issue**: 3.18.1 fixed #149 but reverted to the original problem (expected String, got datetime)

## Suggested Fix

Sparkless should:
1. Recognize `TimestampType` as valid for columns created by `to_timestamp()`
2. Not expect `StringType` for datetime columns
3. Accept both `TimestampType` and `StringType` in validation contexts, or properly infer the correct type

## Test Files

Reproduction scripts are available in:
- `reproduce_sparkless_bugs_3.18.1/bug_to_timestamp_validation_failure.py`
- `reproduce_sparkless_bugs_3.18.1/bug_to_timestamp_returns_datetime_when_string_expected.py`

## Additional Context

This bug was identified when running tests with sparkless 3.18.1:
- **5 tests fail** with this error pattern
- **All 5 tests pass** with real PySpark (`SPARK_MODE=real`)
- Confirms this is a sparkless compatibility issue, not a test bug

