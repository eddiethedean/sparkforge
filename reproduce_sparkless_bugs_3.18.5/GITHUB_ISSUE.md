# Bug: `cannot resolve` error when execution plan references dropped columns

## Summary

When a DataFrame operation drops a column via `.select()`, but the execution plan still contains references to that column from earlier operations, sparkless fails with a `cannot resolve` error during plan evaluation (materialization, validation, or write operations).

## Environment

- **sparkless version**: 3.18.5
- **Python version**: 3.11.13
- **OS**: macOS

## Description

When a transform:
1. Uses a column in operations (e.g., `F.col("impression_date")`)
2. Then drops that column via `.select()` (excluding it from the final column list)

sparkless's execution plan still contains references to the dropped column. When the plan is later evaluated (during `count()`, `collect()`, `write()`, etc.), sparkless tries to resolve ALL column references in the plan, including the dropped column, causing a `cannot resolve` error.

## Steps to Reproduce

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "ad_1", "mobile", 0.05),
    ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "ad_2", "mobile", 0.03),
]

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This column will be dropped
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",
    "device_type",
    "cost_per_impression",
])

# Apply transform that uses impression_date then drops it
silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
    .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
    .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # New column
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",
        "cost_per_impression",
        "device_type",
        "is_mobile",
        # impression_date is DROPPED - not in select list
    )
)

# Verify column was dropped
print(f"Columns: {silver_df.columns}")
print(f"'impression_date' in columns: {'impression_date' in silver_df.columns}")
# Output: False (correct - column was dropped)

# ERROR: Try to materialize/evaluate the DataFrame
try:
    count = silver_df.count()  # This triggers the error
except Exception as e:
    print(f"Error: {e}")
    # Error: cannot resolve 'impression_date' given input columns: 
    # [impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, 
    #  day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]

spark.stop()
```

## Expected Behavior

The DataFrame should materialize/evaluate successfully. The dropped column (`impression_date`) should not be referenced during plan evaluation since it was explicitly excluded via `.select()`.

## Actual Behavior

sparkless raises:
```
cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, 
 day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

The error occurs when sparkless evaluates the execution plan and tries to resolve column references. The plan still contains references to `impression_date` from the earlier `F.regexp_replace(F.col("impression_date"), ...)` operation, even though the column was dropped.

## Root Cause Analysis

1. **Transform creates execution plan**: Operations like `F.regexp_replace(F.col("impression_date"), ...)` add references to `impression_date` in the execution plan
2. **Column is dropped**: `.select()` excludes `impression_date` from the final column list
3. **Plan evaluation**: When the DataFrame is materialized (via `count()`, `collect()`, `write()`, etc.), sparkless evaluates the entire execution plan
4. **Column resolution fails**: sparkless tries to resolve ALL column references in the plan, including `impression_date`, which no longer exists in the DataFrame schema
5. **Error raised**: sparkless raises `cannot resolve 'impression_date'` error

## Impact

This bug affects any pipeline that:
- Uses a column in transformations
- Then drops that column via `.select()`
- Later materializes or evaluates the DataFrame

This is a common pattern in data pipelines where:
- Raw columns are parsed/transformed into new columns
- Original columns are dropped to keep only the transformed versions
- The DataFrame is later materialized for validation or writing

## Workaround

Currently, there is no workaround. The only way to avoid this error is to:
- Not drop columns that were used in transformations, OR
- Avoid using the column in transformations if you plan to drop it

However, both of these workarounds are impractical for real-world data pipelines.

## Additional Context

This bug was discovered when running tests with PipelineBuilder (a data pipeline framework). The same code works correctly in PySpark, suggesting this is a sparkless-specific issue with execution plan handling.

The error occurs consistently when:
- The DataFrame is materialized (`.count()`, `.collect()`)
- The DataFrame is cached (`.cache()` then `.count()`)
- The DataFrame is written to a table (`.write.saveAsTable()`)
- The DataFrame is used in validation operations

## Related Issues

This appears to be related to how sparkless handles execution plans and column resolution. The execution plan should not reference columns that were explicitly dropped via `.select()`.

