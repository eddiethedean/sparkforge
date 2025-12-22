# Bug: select() uses attribute access for dropped columns, causing AttributeError

## Description

sparkless's `select()` method uses attribute access (`df.column_name`) to resolve column references. When a column has been dropped (via a previous `.select()` operation), this causes an `AttributeError` instead of a more appropriate "column not found" error.

## Version

sparkless 3.18.3

## Steps to Reproduce

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("test").getOrCreate()

# Create DataFrame with column
data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1")]
df = spark.createDataFrame(data, ["impression_id", "impression_date", "campaign_id"])

# Apply transform that drops the column
df_transformed = (
    df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select(
        "impression_id",
        "campaign_id",
        "impression_date_parsed",  # New column, original 'impression_date' is dropped
    )
)

# Verify column is dropped
print(f"'impression_date' in columns: {'impression_date' in df_transformed.columns}")
# Output: 'impression_date' in columns: False

# THE BUG: select() tries attribute access
try:
    result = df_transformed.select("impression_date")
except AttributeError as e:
    print(f"Error: {e}")
    # Error: 'DataFrame' object has no attribute 'impression_date'. 
    # Available columns: impression_id, campaign_id, impression_date_parsed
```

## Expected Behavior

When selecting a column that doesn't exist, sparkless should raise an error indicating the column is not found, similar to PySpark's behavior:

```python
# PySpark behavior (expected)
df.select("nonexistent_column")
# Raises: AnalysisException: cannot resolve 'nonexistent_column' given input columns: [...]
```

## Actual Behavior

sparkless raises an `AttributeError` because it uses attribute access (`df.column_name`) internally:

```
AttributeError: 'DataFrame' object has no attribute 'impression_date'. 
Available columns: impression_id, campaign_id, impression_date_parsed
```

## Root Cause

The bug occurs because sparkless's `select()` method uses attribute access to resolve column references:

1. When `select("column_name")` is called, sparkless tries `df.column_name` (attribute access)
2. If the column was dropped, Python's `__getattr__` raises `AttributeError`
3. This is different from checking `df.columns` first, which would allow a more appropriate error

## Impact

This bug affects any code that:
- Drops columns via `.select()` and later tries to reference the dropped column
- Uses transforms that drop columns in data pipelines
- Validates or processes DataFrames where columns may have been dropped

## Comparison with PySpark

PySpark's `select()` doesn't use attribute access - it checks column existence through the schema/columns, so this bug doesn't occur.

## Minimal Reproduction

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("minimal_repro").getOrCreate()

# Create DataFrame
df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

# Drop column via select
df_dropped = df.select("col1")

# Try to select dropped column
df_dropped.select("col2")  # Raises AttributeError
```

## Suggested Fix

The `select()` method should:
1. Check `df.columns` first (not use attribute access)
2. Use column lookup by name instead of `df.column_name`
3. Raise an appropriate "column not found" error instead of `AttributeError`

This would make the behavior consistent with PySpark and handle dropped columns gracefully.

