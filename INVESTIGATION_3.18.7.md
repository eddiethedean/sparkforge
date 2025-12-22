# Investigation: sparkless 3.18.7 Test Failures

## Summary

**Status**: The original bug is **partially fixed** but still exists in certain scenarios.

## Test Results

- ✅ **1713 passed**
- ❌ **5 failed** (same tests as before)
- ⏭️ **70 skipped**

## Bug Status: Partially Fixed

### What Works Now ✅
- Simple operations: `count()`, `cache()`, `collect()` - **All work**
- Filter operations on existing columns - **Work**
- Transform operations that drop columns - **Complete successfully**

### What Still Fails ❌
1. **Explicit selection of dropped columns**: `df.select("dropped_column")` fails
2. **Validation after transforms**: When validation runs on a DataFrame that was transformed (and dropped columns), sparkless tries to resolve the dropped column during validation

## The 5 Failing Tests

### 1. Marketing Pipeline
**Error**: `0.0% valid` (all 150 rows fail validation)
**Root Cause**: 
- Validation fails because sparkless tries to resolve `impression_date` (dropped column) during validation
- `to_timestamp()` returns `None` for all rows (separate issue)
- `hour_of_day` is `None` (depends on `impression_date_parsed`)

**Evidence**:
```
cannot resolve 'impression_date' given input columns: [impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

### 2. Healthcare Pipeline
**Error**: `'NoneType' object has no attribute 'collect'` and `Source silver normalized_labs not found in context`
**Root Cause**: 
- Similar to marketing pipeline - silver step likely fails validation
- Downstream gold steps can't find the failed silver step

### 3. Supply Chain Pipeline
**Error**: `0 rows processed` in gold step
**Root Cause**: Similar validation failures

### 4. Streaming Hybrid Pipeline
**Error**: 
- `to_date() requires StringType or DateType input, got TimestampType(nullable=True)`
- `Unsupported function: unix_timestamp`
**Root Cause**: 
- Different issues: type handling and unsupported functions in sparkless
- Not related to the original "cannot resolve" bug

### 5. Data Quality Pipeline
**Error**: `unable to find column "source"`
**Root Cause**: 
- Column resolution issue - trying to reference a column that doesn't exist
- Different from the original bug

## Root Cause Analysis

### Why Validation Fails

The validation code creates column expressions like:
```python
F.col("impression_id").isNotNull()
F.col("impression_date_parsed").isNotNull()
F.col("hour_of_day").isNotNull()
```

When sparkless validates these expressions during materialization, it appears to:
1. Check all column references in the execution plan
2. Include references from the transform (even though the column was dropped)
3. Try to resolve `impression_date` which was used in the transform but dropped

### Evidence

```python
# This works:
silver_df.count()  # ✅ Works - simple operation

# This fails:
silver_df.select("impression_id", "impression_date")  # ❌ cannot resolve 'impression_date'

# This fails:
silver_df.filter(F.col("impression_id").isNotNull())  # ❌ cannot resolve 'impression_date' (during validation)
```

The bug occurs when:
- A transform uses a column (`impression_date`)
- The transform drops that column (via `.select()`)
- Validation tries to validate the DataFrame
- sparkless validates column expressions and tries to resolve the dropped column

## Additional Issues Found

1. **`to_timestamp()` returns None**: All rows have `None` for `impression_date_parsed`
   - This might be a separate bug or related to the column resolution issue
   
2. **Type comparison error**: "cannot compare string with numeric type (i32)"
   - Occurs when validating `cost_per_impression >= 0`
   - Suggests type handling issues in sparkless

3. **Unsupported functions**: `unix_timestamp` not supported in sparkless

4. **Type handling**: `to_date()` expects StringType but gets TimestampType

## Conclusion

The bug is **partially fixed**:
- ✅ Simple operations work (count, cache, collect)
- ❌ Validation after transforms that drop columns still fails
- ❌ Explicit selection of dropped columns fails

The bug manifests when sparkless validates column expressions and tries to resolve columns that were used in the transform but then dropped.

## Next Steps

1. Create a minimal reproduction showing validation failure
2. Report to sparkless developers with clear reproduction steps
3. Investigate the `to_timestamp()` None issue separately

