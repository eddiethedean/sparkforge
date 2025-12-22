# Final Bug Root Cause: sparkless select() Uses Attribute Access

## Summary

The bug is in **sparkless's `select()` method**, not in PipelineBuilder code. When `select()` processes column references, it uses attribute access (`df.column_name`) instead of checking `df.columns` first. This fails when columns are dropped.

## The Bug

**Location**: sparkless's `DataFrame.select()` method  
**Root Cause**: Uses attribute access (`df.column_name`) to resolve column references  
**Impact**: Fails when columns are dropped via `.select()` operations

## Evidence

### Direct sparkless Code Reproduction

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("test").getOrCreate()

# Create DataFrame with column
data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1")]
df = spark.createDataFrame(data, ["impression_id", "impression_date", "campaign_id"])

# Apply transform that drops the column
df_transformed = (
    df.withColumn("impression_date_parsed", F.to_timestamp(...))
    .select("impression_id", "campaign_id", "impression_date_parsed")
    # 'impression_date' is now dropped
)

# THE BUG: select() tries attribute access
try:
    result = df_transformed.select("impression_date")
except AttributeError as e:
    # Error: 'DataFrame' object has no attribute 'impression_date'
    # This is the exact error from the test!
```

### Error Message

```
'DataFrame' object has no attribute 'impression_date'. 
Available columns: impression_id, campaign_id, impression_date_parsed, ...
```

This error message format matches exactly what we see in the failing tests.

## Why It Happens

1. **sparkless supports attribute access**: `df.column_name` works when column exists ✅
2. **Column is dropped**: `.select()` removes it from DataFrame ✅
3. **select() uses attribute access**: When processing column references, sparkless tries `df.column_name` ❌
4. **AttributeError raised**: Because column doesn't exist ❌

## Comparison with PySpark

**PySpark**: `select()` doesn't use attribute access - it checks `df.columns` or uses column lookup, so this bug doesn't occur.

**sparkless**: `select()` uses attribute access, causing the bug.

## Reproduction Scripts

1. **`bug_sparkless_select_uses_attribute_access.py`**: Shows the exact bug using only sparkless code
2. **`bug_sparkless_f_col_behavior.py`**: Shows how `F.col()` triggers the same issue
3. **`bug_sparkless_column_access_difference.py`**: Demonstrates the difference in column access patterns

## The Fix

sparkless's `select()` method should:
- Check `df.columns` first (not use attribute access)
- Use column lookup by name instead of `df.column_name`
- Handle dropped columns gracefully

## Impact on PipelineBuilder

PipelineBuilder code is **not** the issue. The bug occurs when:
- A transform drops a column (via `.select()`)
- PipelineBuilder processes the output DataFrame
- sparkless's internal `select()` processing tries to access the dropped column
- This triggers the attribute access bug in sparkless

The fix must be in sparkless's `DataFrame.select()` implementation.

