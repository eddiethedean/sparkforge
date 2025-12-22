# Bug: to_timestamp() returns String when Datetime expected in sparkless 3.18.0

## Summary

In sparkless 3.18.0, `F.to_timestamp()` operations return String columns when sparkless expects Datetime('μs') in validation and type-checking contexts. This causes pipeline execution failures with the error:

```
expected output type 'Datetime('μs')', got 'String'; set `return_dtype` to the proper datatype
```

## Environment

- **sparkless version**: 3.18.0
- **Python version**: 3.8+
- **PySpark version**: 3.5.0+

## Steps to Reproduce

1. Create a DataFrame with datetime strings containing microseconds
2. Use `F.to_timestamp()` with `regexp_replace().cast("string")` pattern
3. Apply validation rules or access the column in contexts that trigger type checking
4. Observe the error: `expected output type 'Datetime('μs')', got 'String'`

## Minimal Reproduction

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("to_timestamp_bug").getOrCreate()

data = [("2024-01-15T10:30:45.123456",)]
df = spark.createDataFrame(data, ["date_string"])

# This pattern causes the issue
df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

# In pipeline context with validation, sparkless reports:
# ERROR: expected output type 'Datetime('μs')', got 'String'
```

## Expected Behavior

`F.to_timestamp()` should return a TimestampType column that sparkless recognizes as Datetime('μs') in all contexts, including validation.

## Actual Behavior

The column appears as TimestampType in the schema, but sparkless treats it as String when validating or type-checking, causing validation failures.

## Impact

This affects multiple pipeline transformations that parse datetime strings:
- Marketing pipelines (impression dates, click dates, conversion dates)
- Supply chain pipelines (order dates, shipment dates, inventory dates)
- Streaming pipelines (event timestamps)
- Data quality pipelines (transaction dates)

## Related

This appears to be a regression or incomplete fix from the datetime handling improvements in 3.18.0. The error message suggests sparkless now expects Datetime but receives String, which is the opposite of the previous issue (Issue #135, #145) where sparkless expected String but received datetime.

## Workaround

None currently known. The issue occurs at the sparkless type system level.

