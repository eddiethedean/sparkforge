# Exact sparkless Code Execution Trail

## Summary

After extensive investigation, I've extracted the exact execution trail from the failing marketing pipeline test. The error occurs during PipelineBuilder execution, specifically in the SILVER step `processed_impressions`.

## Error Message

```
cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

**Key observation**: The columns shown are **AFTER the transform** (with `impression_date_parsed` but no `impression_date`).

## Execution Trail

### Step 1: Bronze Step Configuration
```python
builder.with_bronze_rules(
    name="raw_impressions",
    rules={...},
    incremental_col="impression_date",  # This is stored
)
```

### Step 2: Silver Step Configuration
```python
builder.add_silver_transform(
    name="processed_impressions",
    source_bronze="raw_impressions",
    transform=processed_impressions_transform,  # Drops impression_date
    rules={...},
    table_name="processed_impressions",
)
# Silver step inherits: source_incremental_col="impression_date"
```

### Step 3: Transform Execution
```python
def processed_impressions_transform(spark, df, silvers):
    return (
        df.withColumn("impression_date_parsed", F.to_timestamp(...))
        .withColumn("hour_of_day", F.hour(...))
        .withColumn("day_of_week", F.dayofweek(...))
        .withColumn("is_mobile", F.when(...))
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",  # New column
            # ... other columns
            # impression_date is DROPPED
        )
    )
```

**Result**: Transform completes successfully ✅
- Columns after transform: `[impression_id, ..., impression_date_parsed, ...]`
- `impression_date` is NOT in columns ✅

### Step 4: Materialization
```python
# From execution.py:656-658
output_df = self._ensure_materialized_for_validation(output_df, step.rules)
# This calls: df.cache() and df.count()
```

**Result**: Materialization works in isolation ✅

### Step 5: Validation
```python
# From execution.py:659-665
output_df, _, validation_stats = apply_column_rules(
    output_df,
    step.rules,
    "pipeline",
    step.name,
    functions=self.functions,
)
```

**Result**: Validation works in isolation ✅

### Step 6: Table Write
```python
# From execution.py:673-689
# Write output_df to table: silver.processed_impressions
```

**Result**: Table write works in isolation ✅

## The Mystery

**All individual operations work fine in isolation**, but the error occurs during PipelineBuilder execution. This suggests:

1. **The error happens in a specific context** that we haven't reproduced yet
2. **There's something in the execution flow** that references `impression_date` after the transform
3. **The error might be in sparkless's plan optimization** when processing the DataFrame in a specific context

## Hypothesis

The error likely occurs when:
- sparkless evaluates the DataFrame's execution plan
- The plan still contains references to `impression_date` from earlier operations
- When the plan is materialized/evaluated, sparkless tries to resolve `impression_date`
- But the column was dropped, so it fails

## Exact sparkless Code That Fails

Based on the error message and execution trail, the failing sparkless code is:

```python
# Somewhere in sparkless's execution plan evaluation:
# It tries to resolve 'impression_date' on a DataFrame where:
# - The column was used earlier (in the transform)
# - The column was dropped via .select()
# - But the execution plan still references it

# The exact operation that triggers this is likely:
df.count()  # or df.collect() or df.write.saveAsTable()
# When sparkless evaluates the plan, it tries to resolve all column references
# Including 'impression_date' which no longer exists
```

## Reproduction Scripts Created

1. **`exact_execution_trail.py`**: Basic execution trail (works fine)
2. **`exact_pipelinebuilder_execution.py`**: PipelineBuilder execution (works fine)
3. **`exact_pipelinebuilder_with_error.py`**: Full PipelineBuilder (fails with error)
4. **`manual_step_execution.py`**: Manual step execution (different error)
5. **`exact_failure_point.py`**: Individual operations (all work)

## Conclusion

The error occurs during PipelineBuilder execution, but **we haven't been able to reproduce it with isolated sparkless code**. This suggests:

1. The error is context-specific (parallel execution, multiple steps, etc.)
2. The error might be in sparkless's plan optimization when processing complex pipelines
3. The error might be triggered by a specific combination of operations

The exact sparkless code that fails is likely in:
- Plan evaluation during materialization
- Plan optimization during validation
- Plan evaluation during table write
- Or some other post-processing step

