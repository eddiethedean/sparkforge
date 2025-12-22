# Investigation Results: sparkless 3.18.5 Test Failures

## Root Cause Identified

The issue is **NOT a sparkless bug** - it's a **PipelineBuilder logic issue**.

### The Problem

1. **Bronze step** defines `incremental_col="impression_date"` (line 69 of test)
2. **Silver step** automatically inherits `source_incremental_col="impression_date"` from bronze step (line 701-712 of builder.py)
3. **Silver transform** drops `impression_date` via `.select()` (line 119-131 of test)
4. **After transform**, something tries to reference `impression_date` on the transformed DataFrame

### Where the Error Occurs

The error happens in the SILVER step `processed_impressions`:
```
❌ Failed SILVER step: processed_impressions (0.19s) - cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

The columns shown are **after the transform** (with `impression_date_parsed` but no `impression_date`).

### Code Flow

1. **Builder sets source_incremental_col** (builder.py:701-712):
   ```python
   source_incremental_col = self.bronze_steps[source_bronze].incremental_col
   # This sets source_incremental_col="impression_date"
   ```

2. **Incremental filtering happens BEFORE transform** (execution.py:1995):
   ```python
   bronze_df = self._filter_incremental_bronze_input(step, bronze_df)
   # This correctly filters the bronze DataFrame using impression_date
   ```

3. **Transform is applied** (execution.py:1998):
   ```python
   return step.transform(self.spark, bronze_df, {})
   # This drops impression_date via .select()
   ```

4. **ERROR: Something tries to use impression_date AFTER transform**
   - The error shows columns AFTER transform
   - Something is trying to reference `impression_date` on the transformed DataFrame

### Possible Causes

1. **Watermark column reference**: If `watermark_col` is set to `"impression_date"`, it might try to use it after transform
2. **Writing/table operations**: Table write operations might try to reference the incremental column
3. **Post-processing**: Some post-processing step might try to use the original column name

### Next Steps

Need to check:
1. Where `source_incremental_col` or `watermark_col` is used AFTER the transform
2. If table write operations reference these columns
3. If there's any post-processing that uses the original column names

### Conclusion

This is **NOT a sparkless bug** - sparkless is correctly identifying that the column doesn't exist. The issue is that PipelineBuilder code is trying to reference a column (`impression_date`) that was explicitly dropped in the transform.

The fix should be in PipelineBuilder to either:
1. Not reference dropped columns
2. Map original column names to new ones
3. Check if columns exist before referencing them

