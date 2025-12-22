# Bug: "cannot resolve" error when referencing dropped columns in select() and filter()

## Description

sparkless raises a "cannot resolve" error when code tries to reference a column that was dropped via `.select()`. While this is technically correct behavior (the column doesn't exist), the error message and behavior differ from PySpark, and the error occurs in scenarios where the column reference might be valid in certain contexts (e.g., validation rules, incremental processing).

## Version

sparkless 3.18.4

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

# THE BUG: Try to reference the dropped column
try:
    result = df_transformed.select("impression_id", "impression_date")
except Exception as e:
    print(f"Error: {e}")
    # Error: cannot resolve 'impression_date' given input columns: 
    # [impression_id, campaign_id, impression_date_parsed]
```

## Expected Behavior

When selecting a column that doesn't exist, sparkless should raise an error indicating the column is not found. However, the error message format and behavior should be consistent with PySpark:

**PySpark behavior**:
```python
df.select("nonexistent_column")
# Raises: AnalysisException: cannot resolve 'nonexistent_column' given input columns: [...]
```

**Expected sparkless behavior**: Similar error message format, consistent with PySpark.

## Actual Behavior

sparkless raises:
```
cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

**Note**: This error is actually **correct** - the column doesn't exist. However, the issue is that:

1. The error occurs in scenarios where code might legitimately need to check if a column exists
2. The error message format differs from PySpark in some contexts
3. The error occurs when using both string column names and `F.col()` expressions

## Impact

This bug affects data pipeline scenarios where:

1. **Transforms drop columns**: A transform uses a column to create a new one, then drops the original via `.select()`
2. **Validation rules reference original columns**: Validation rules might reference the original column name (before transform)
3. **Incremental processing**: Incremental column references might use the original column name from bronze layer, but the column is dropped in silver/gold transforms
4. **Downstream transforms**: Downstream transforms might expect the original column to exist

## Real-World Example

This bug affects the SparkForge PipelineBuilder framework when running tests. The marketing pipeline test fails with this exact error:

```
Step execution failed: cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

**Pipeline scenario**:
1. Bronze step defines `incremental_col="impression_date"`
2. Silver transform creates `impression_date_parsed` from `impression_date`
3. Silver transform drops `impression_date` via `.select()`
4. Later processing (validation, incremental filtering, or downstream transforms) tries to reference `impression_date`
5. sparkless correctly identifies the column doesn't exist, but the error suggests the code is trying to reference it

## Minimal Reproduction

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("minimal_repro").getOrCreate()

# Create DataFrame
df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

# Drop column via select
df_dropped = df.select("col1")

# Try to select dropped column
df_dropped.select("col2")  # Raises: cannot resolve 'col2' given input columns: [col1]
```

## Additional Test Cases

### Test Case 1: Using F.col() with dropped column
```python
df_transformed.select(F.col("impression_date"))
# Raises: cannot resolve 'impression_date' given input columns: [...]
```

### Test Case 2: Using dropped column in filter
```python
df_transformed.filter(F.col("impression_date").isNotNull())
# Raises: unable to find column "impression_date"; valid columns: [...]
```

### Test Case 3: Using dropped column in validation expression
```python
validation_expr = F.col("impression_date").isNotNull()
df_transformed.filter(validation_expr)
# Raises: unable to find column "impression_date"; valid columns: [...]
```

## Comparison with Previous Versions

| Version | Error Pattern | Status |
|---------|---------------|--------|
| 3.18.2 | `to_timestamp()` returns None | ❌ Different bug |
| 3.18.3 | `AttributeError: 'DataFrame' object has no attribute 'impression_date'` | ❌ Attribute access bug (Issue #156) |
| 3.18.4 | `cannot resolve 'impression_date' given input columns: [...]` | ❌ Current bug |

**Note**: The error message change from 3.18.3 to 3.18.4 suggests Issue #156 (select() attribute access) may have been fixed, but the underlying column resolution issue persists.

## Root Cause Analysis

The issue appears to be that:

1. **Column tracking**: When a column is dropped via `.select()`, sparkless correctly removes it from the DataFrame
2. **Column resolution**: When code tries to reference the dropped column, sparkless correctly identifies it doesn't exist
3. **Error context**: The error occurs in scenarios where the column reference might be:
   - From validation rules that reference original column names
   - From incremental processing that uses bronze layer column names
   - From downstream transforms that expect original columns to exist

**The core issue**: While sparkless is technically correct (the column doesn't exist), the error suggests that code is trying to reference a column that was explicitly dropped. This could be:

1. **A logic error in the calling code** (PipelineBuilder, transforms, etc.) - code shouldn't reference dropped columns
2. **A sparkless limitation** - sparkless might need better handling of column name mapping/tracking
3. **A design issue** - transforms that drop columns might need to update column references

## Suggested Fix

The fix depends on the root cause:

### Option 1: Better error messages
If this is expected behavior, improve the error message to be more helpful:
```
Column 'impression_date' was dropped in a previous transform. 
Available columns: [impression_id, impression_date_parsed, ...]
Did you mean 'impression_date_parsed'?
```

### Option 2: Column name mapping/tracking
If transforms drop columns, provide a way to map original column names to new ones:
```python
# Transform could specify column mapping
df_transformed = df.transform(
    column_mapping={"impression_date": "impression_date_parsed"}
)
```

### Option 3: Graceful handling
Allow code to check if a column exists before referencing it:
```python
if "impression_date" in df.columns:
    # Use original column
else:
    # Use mapped column or handle gracefully
```

## Reproduction Scripts

I've created two detailed reproduction scripts that demonstrate this bug:

1. **`bug_cannot_resolve_dropped_column.py`**: Shows the exact error when referencing dropped columns
2. **`bug_incremental_col_reference.py`**: Explores the incremental column reference scenario

Both scripts use only sparkless code (no PipelineBuilder dependencies) and can be run independently.

## Environment

- **sparkless version**: 3.18.4
- **Python version**: 3.11.13
- **OS**: macOS 24.6.0

## Related Issues

- Issue #156: select() uses attribute access for dropped columns (may be fixed in 3.18.4)
- This issue: Column resolution error when referencing dropped columns

## Additional Context

This bug was discovered while testing the SparkForge PipelineBuilder framework. The framework uses sparkless for fast, reliable testing without a full Spark environment. The bug affects 5 pipeline tests that use transforms that drop columns.

The error is technically correct (the column doesn't exist), but the issue is that code is trying to reference dropped columns, suggesting either:
1. A logic error in the calling code
2. A need for better column tracking/mapping in sparkless
3. A design issue with how transforms handle column dropping

