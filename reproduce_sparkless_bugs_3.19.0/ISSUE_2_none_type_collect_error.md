# 'NoneType' object has no attribute 'collect' after transform with to_timestamp()

## Summary

After applying a transform that uses `to_timestamp()`, attempting to materialize the DataFrame (via `count()`, `collect()`, etc.) fails with `'NoneType' object has no attribute 'collect'`.

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
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data
data = []
for i in range(150):
    data.append({
        "lab_id": f"LAB-{i:08d}",
        "test_date": (datetime.now() - timedelta(days=i % 365)).isoformat(),
    })

bronze_df = spark.createDataFrame(data, ["lab_id", "test_date"])

# Transform with to_timestamp()
silver_df = (
    bronze_df.withColumn(
        "test_date_clean",
        F.regexp_replace(F.col("test_date"), r"\.\d+", ""),
    )
    .withColumn(
        "test_date_parsed",
        F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    .drop("test_date_clean")
    .select("lab_id", "test_date_parsed")
)

# Materialize (THIS TRIGGERS THE BUG)
count = silver_df.count()  # ❌ Error here
```

### Full Reproduction Script

See `reproduce_sparkless_bugs_3.19.0/bug_2_none_type_collect_error.py` for a complete reproduction that matches the exact test case from `test_healthcare_pipeline.py`.

## Expected Behavior

The transform should complete successfully and `count()` should return the number of rows (150).

## Actual Behavior

sparkless throws `AttributeError: 'NoneType' object has no attribute 'collect'` when trying to materialize the DataFrame. This suggests that:
- An internal operation returns `None` instead of a DataFrame
- sparkless tries to call `.collect()` on `None`
- The error occurs during materialization, not during the transform itself

## Root Cause Analysis

The error occurs in `sparkless/backend/polars/materializer.py` at line 654, where `lazy_df.collect()` is called but `lazy_df` is `None`. This suggests:

1. **to_timestamp() may return None**: The `to_timestamp()` operation might be failing silently and returning `None` instead of a proper column expression
2. **Column operation failure**: Some column operation in the transform chain is failing and returning `None`
3. **Internal DataFrame materialization issue**: sparkless's internal materialization logic is not handling a failed operation correctly

## Impact

This bug affects:
- **Healthcare Pipeline**: `normalized_labs` and `processed_diagnoses` steps fail
- Any transform that uses `to_timestamp()` followed by materialization

## Workaround

None known. The bug occurs during materialization, which is required for most operations.

## Additional Context

- **Test Case**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
- **Affected Steps**: `normalized_labs`, `processed_diagnoses`
- **Downstream Impact**: Gold steps that depend on these silver steps fail with "Source silver normalized_labs not found in context"

## Steps to Reproduce

1. Create a DataFrame with a timestamp string column
2. Apply a transform that:
   - Cleans the timestamp string (e.g., removes microseconds)
   - Uses `to_timestamp()` to parse it
   - Drops intermediate columns
3. Try to materialize the DataFrame (e.g., `count()`, `collect()`)
4. Observe `AttributeError: 'NoneType' object has no attribute 'collect'`

