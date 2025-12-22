# Reproduction Status

## Summary

**The bug does NOT reproduce with pure sparkless code.** All operations work fine when using sparkless directly.

**The error only occurs when using PipelineBuilder.**

## Pure sparkless Code (Works Fine)

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName('test').getOrCreate()

# Create data
data = [('imp_001', '2024-01-15T10:30:45.123456', 'campaign_1', ...)]
bronze_df = spark.createDataFrame(data, ['impression_id', 'impression_date', ...])

# Apply transform (uses impression_date, then drops it)
silver_df = (
    bronze_df.withColumn('impression_date_parsed', F.to_timestamp(...))
    .select('impression_id', 'impression_date_parsed', ...)  # drops impression_date
)

# All these operations work fine:
silver_df.count()  # ✅
silver_df.collect()  # ✅
silver_df.cache().count()  # ✅
silver_df.write.saveAsTable('test')  # ✅
```

## PipelineBuilder Code (Fails)

The same transform fails when executed through PipelineBuilder's execution engine:

```
cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, ...]
```

## Implications

1. **The bug is triggered by something specific in PipelineBuilder's execution flow**
   - PipelineBuilder might create a specific context/state
   - Or perform operations in a specific order
   - Or pass DataFrames through multiple layers

2. **The error message is clearly a sparkless error**
   - `"cannot resolve 'impression_date'"` is a sparkless error
   - So it's still a sparkless issue, but triggered by PipelineBuilder's context

3. **Possible causes:**
   - How sparkless handles execution plans in specific contexts
   - Plan optimization/evaluation when DataFrames are passed through multiple layers
   - A specific interaction between PipelineBuilder's execution flow and sparkless's plan evaluation

## Next Steps

1. Investigate what PipelineBuilder does differently that triggers this issue
2. Check if there's a specific combination of operations that causes the bug
3. Look for any state or context that PipelineBuilder creates that might affect sparkless's plan evaluation

