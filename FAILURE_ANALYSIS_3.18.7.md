# Failure Analysis: sparkless 3.18.7

## Summary

**The original bug is NOT fully fixed** - it still exists but manifests differently.

## Test Results

**Total**: 1788 tests
- ✅ **Passed**: 1713
- ❌ **Failed**: 5
- ⏭️ **Skipped**: 70

## The 5 Failing Tests

### 1. Marketing Pipeline
**Error**: `0 rows processed` in gold step
**Root Cause**: 
- Silver step `processed_impressions`: 0.0% valid (all 150 rows fail validation)
- Bug still exists: `cannot resolve 'impression_date'` when validating after transform
- `to_timestamp()` returns None for all rows (separate issue or related)
- `hour_of_day` is None (depends on `impression_date_parsed`)

### 2. Healthcare Pipeline
**Error**: `'NoneType' object has no attribute 'collect'` and `Source silver normalized_labs not found in context`
**Root Cause**:
- Silver step `normalized_labs` likely fails (similar to marketing pipeline)
- Downstream gold steps can't find the silver step in context

### 3. Supply Chain Pipeline
**Error**: `0 rows processed` in gold step
**Root Cause**:
- Similar to marketing pipeline - validation failures

### 4. Streaming Hybrid Pipeline
**Error**: 
- `to_date() requires StringType or DateType input, got TimestampType(nullable=True)`
- `Unsupported function: unix_timestamp`
**Root Cause**:
- Different issues: type handling and unsupported functions in sparkless

### 5. Data Quality Pipeline
**Error**: `unable to find column "source"` 
**Root Cause**:
- Column resolution issue - trying to reference a column that doesn't exist

## Bug Status: Still Exists

The bug **still exists** in sparkless 3.18.7, but manifests differently:

### When Bug Occurs:
1. ✅ Simple operations (`count()`, `cache()`, `collect()`) - **Work fine**
2. ❌ Selecting dropped column explicitly - **Fails**
3. ❌ Validation after transform that drops columns - **Fails**

### Evidence:
```python
# This works:
silver_df.count()  # ✅ Works

# This fails:
silver_df.select("impression_id", "impression_date")  # ❌ cannot resolve 'impression_date'

# This fails:
silver_df.filter(F.col("impression_id").isNotNull())  # ❌ cannot resolve 'impression_date'
```

The bug occurs when sparkless validates column expressions that reference the dropped column during:
- `.select()` with dropped column
- `.filter()` validation
- Column validation during materialization for validation

## Additional Issues Found

1. **`to_timestamp()` returns None**: All rows have `None` for `impression_date_parsed`
2. **Type comparison error**: "cannot compare string with numeric type (i32)"
3. **Unsupported functions**: `unix_timestamp` not supported
4. **Type handling**: `to_date()` expects StringType but gets TimestampType

## Conclusion

The bug is **partially fixed** - simple operations work, but the bug still exists when:
- Explicitly selecting dropped columns
- Validating after transforms that drop columns

This explains why:
- Standalone reproduction script works (only tests simple operations)
- PipelineBuilder tests fail (uses validation which triggers the bug)

