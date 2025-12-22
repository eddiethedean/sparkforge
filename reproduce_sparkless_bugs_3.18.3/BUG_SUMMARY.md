# Column Tracking Bug in sparkless 3.18.3

## Summary

After fixing the `to_timestamp()` None bug, sparkless 3.18.3 introduced a new column tracking bug where it tries to access dropped columns using attribute access.

## The Bug

**Error Message**:
```
'DataFrame' object has no attribute 'impression_date'. 
Available columns: impression_id, campaign_id, customer_id, impression_date_parsed, 
hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile
```

## Root Cause

The bug occurs when:
1. Transform uses a column (`impression_date`) to create a new column (`impression_date_parsed`)
2. Transform drops the original column via `.select()`
3. **Sparkless internally tries to access `df.impression_date` (attribute access)**
4. Error: `'DataFrame' object has no attribute 'impression_date'`

## Direct Evidence

### Reproduction Script

Run:
```bash
SPARK_MODE=mock python reproduce_sparkless_bugs_3.18.3/bug_column_tracking_pipeline_context.py
```

### What Works

```python
# Transform function works correctly
df_transformed = processed_impressions_transform(spark, df, {})
# ✅ Succeeds
# ✅ Columns are correct
# ✅ Data is correct
```

### What Fails

```python
# Trying to access dropped column via attribute access
result = df_transformed.impression_date
# ❌ AttributeError: 'DataFrame' object has no attribute 'impression_date'
```

## The Issue

The error message format (`'DataFrame' object has no attribute 'impression_date'`) indicates that **something in sparkless is using attribute access** (`df.column_name`) instead of column expressions (`F.col('column_name')` or `df['column_name']`).

This happens in **PipelineBuilder's internal processing**, not in the transform function itself.

## Evidence

| Aspect | Status |
|--------|--------|
| Transform function works | ✅ Correct |
| Transform drops column correctly | ✅ Correct |
| Transformed DataFrame has correct columns | ✅ Correct |
| Sparkless tries attribute access to dropped column | ❌ **BUG** |

## Why This Happens

The bug occurs in PipelineBuilder's internal processing when it:
- Validates the transform output
- Tracks column dependencies
- Processes validation rules
- Optimizes or caches the DataFrame

Somewhere in this process, sparkless tries to access the original column name using attribute access (`df.impression_date`) even though it was dropped.

## Impact

This affects all pipeline transforms that:
1. Use a column to create a new column
2. Drop the original column via `.select()`
3. Are processed by PipelineBuilder

**5 pipeline tests fail** with this pattern.

## Comparison with PySpark

**Same code in PySpark (real mode)**: ✅ Works correctly
- Transform executes successfully
- No attribute access errors
- Pipeline completes successfully

This confirms the bug is in sparkless, not in the transform logic.

## Suggested Fix

sparkless should:
1. Not use attribute access (`df.column_name`) for column references
2. Use column expressions (`F.col('column_name')` or `df['column_name']`) instead
3. Properly track which columns exist after `.select()` operations
4. Check column existence before trying to access them

## Files

- `bug_column_tracking.py` - Basic reproduction
- `bug_column_tracking_pipeline_context.py` - Pipeline context reproduction
- Both demonstrate the attribute access issue

