# to_date() on TimestampType fails with 'NoneType' object has no attribute 'collect'

## Summary

When using `to_date()` on a `TimestampType` column, sparkless fails with `'NoneType' object has no attribute 'collect'`. PySpark's `to_date()` accepts `TimestampType`, but sparkless does not.

## Version

**sparkless 3.19.0**

## Error Message

```
AttributeError: 'NoneType' object has no attribute 'collect'
```

Full traceback:
```
File ".../sparkless/backend/polars/materializer.py", line 654, in materialize
    df_collected = lazy_df.collect()
                   ^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'collect'
```

## Reproduction

### Minimal Reproduction

```python
from sparkless import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data
data = []
for i in range(100):
    data.append({
        "event_id": f"EVT-{i:08d}",
        "event_timestamp": datetime.now().isoformat(),
    })

bronze_df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

# Parse timestamp (creates TimestampType column)
silver_df = (
    bronze_df.withColumn(
        "event_timestamp_parsed",
        F.to_timestamp(F.col("event_timestamp").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    .select("event_id", "event_timestamp_parsed")
)

# Apply to_date() on TimestampType (THIS TRIGGERS THE BUG)
gold_df = (
    silver_df.withColumn(
        "metric_date",
        F.to_date(F.col("event_timestamp_parsed")),  # ❌ Error here
    )
    .groupBy("metric_date")
    .agg(F.count("*").alias("total_events"))
)

result = gold_df.collect()  # ❌ Error here
```

### Full Reproduction Script

See `reproduce_sparkless_bugs_3.19.0/bug_3_to_date_datetime_type_error.py` for a complete reproduction that matches the exact test case from `test_streaming_hybrid_pipeline.py`.

## Expected Behavior

`to_date()` should accept `TimestampType` as input, just like PySpark does. The function should extract the date part from a timestamp.

## Actual Behavior

sparkless throws `AttributeError: 'NoneType' object has no attribute 'collect'` when trying to materialize a DataFrame that uses `to_date()` on a `TimestampType` column. This suggests that:
- `to_date()` on `TimestampType` fails internally
- The operation returns `None` instead of a proper column expression
- Materialization fails when trying to collect the result

## Root Cause Analysis

The error occurs because:
1. **to_date() doesn't accept TimestampType**: sparkless's `to_date()` function only accepts `StringType` or `DateType`, not `TimestampType`
2. **Silent failure**: When `to_date()` is called on `TimestampType`, it fails silently and returns `None`
3. **Materialization error**: When sparkless tries to materialize the DataFrame, it calls `.collect()` on `None`

## Related Issue

This is related to Issue #165, which reported that `to_date()` requires `StringType` or `DateType` input. However, the error message in 3.19.0 is different - it's now a `NoneType` error instead of a clear type error message.

## Impact

This bug affects:
- **Streaming Hybrid Pipeline**: `unified_analytics` step fails
- Any transform that uses `to_date()` on a `TimestampType` column

## Workaround

Cast to string first (but this shouldn't be necessary):

```python
gold_df = (
    silver_df.withColumn(
        "metric_date",
        F.to_date(F.col("event_timestamp_parsed").cast("string")),
    )
)
```

However, this workaround may not work in all cases and adds unnecessary complexity.

## Additional Context

- **Related Issue**: #165 (reported for 3.18.7, still present in 3.19.0 with different error)
- **Test Case**: `tests/builder_tests/test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
- **PySpark Behavior**: PySpark's `to_date()` accepts `TimestampType` directly

## Steps to Reproduce

1. Create a DataFrame with a timestamp string column
2. Parse the timestamp using `to_timestamp()` (creates `TimestampType` column)
3. Use `to_date()` on the `TimestampType` column
4. Try to materialize the DataFrame (e.g., `collect()`, `count()`)
5. Observe `AttributeError: 'NoneType' object has no attribute 'collect'`

