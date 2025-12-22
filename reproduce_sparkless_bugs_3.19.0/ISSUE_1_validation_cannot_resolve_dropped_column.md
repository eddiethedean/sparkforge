# Validation fails: "cannot resolve 'impression_date'" when validating after transform that drops column

## Summary

When validating a DataFrame after a transform that uses a column and then drops it, sparkless tries to resolve the dropped column during validation, causing a `SparkColumnNotFoundError`.

## Version

**sparkless 3.19.0**

## Error Message

```
sparkless.core.exceptions.operation.SparkColumnNotFoundError: cannot resolve 'impression_date' given input columns: [impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

## Reproduction

### Minimal Reproduction

```python
from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data (150 rows - bug manifests with larger datasets)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
    })

bronze_df = spark.createDataFrame(data, ["impression_id", "impression_date"])

# Transform that uses impression_date then drops it
silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select("impression_id", "impression_date_parsed")  # impression_date is DROPPED
)

# Validation (THIS TRIGGERS THE BUG)
validation_predicate = F.col("impression_id").isNotNull() & F.col("impression_date_parsed").isNotNull()

valid_df = silver_df.filter(validation_predicate)  # ❌ Error here
count = valid_df.count()
```

### Full Reproduction Script

See `reproduce_sparkless_bugs_3.19.0/bug_1_validation_fails_after_dropping_column.py` for a complete reproduction that matches the exact test case from `test_marketing_pipeline.py`.

## Expected Behavior

Validation should succeed. The validation only references columns that exist (`impression_id` and `impression_date_parsed`), and `impression_date` was already dropped in the transform.

## Actual Behavior

sparkless throws `SparkColumnNotFoundError: cannot resolve 'impression_date'` when trying to validate the DataFrame, even though:
- The validation predicate only references existing columns
- The dropped column (`impression_date`) is not referenced in the validation rules
- Simple operations like `count()`, `cache()`, `collect()` work fine

## Root Cause Analysis

When sparkless validates column expressions during materialization, it appears to:
1. Check all column references in the execution plan
2. Include references from the transform (even though the column was dropped)
3. Try to resolve `impression_date` which was used in the transform but dropped

This suggests that sparkless's column validation is checking the entire execution plan history, not just the current DataFrame schema.

## Impact

This bug affects:
- **Marketing Pipeline**: `processed_impressions` validation fails with 0.0% valid
- **Healthcare Pipeline**: Similar validation failures
- **Supply Chain Pipeline**: Similar validation failures

All tests fail because validation cannot complete when a transform drops a column that was used earlier.

## Workaround

None known. The bug occurs during validation, which is a common operation in data pipelines.

## Additional Context

- **Related Issue**: #163 (reported for 3.18.7, still present in 3.19.0)
- **Test Case**: `tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
- **Dataset Size**: Bug manifests with larger datasets (150+ rows)

## Steps to Reproduce

1. Create a DataFrame with a column (e.g., `impression_date`)
2. Apply a transform that:
   - Uses the column (e.g., `F.col("impression_date")`)
   - Creates a new column from it
   - Drops the original column via `.select()`
3. Apply validation rules that only reference existing columns
4. Call `.filter()` or `.count()` on the validated DataFrame
5. Observe `SparkColumnNotFoundError: cannot resolve 'impression_date'`

