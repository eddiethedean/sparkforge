# PySpark Comparison: Reproduction Scripts

## Summary

The reproduction scripts work correctly in both sparkless and PySpark. The key difference is that **the actual pipeline test passes in PySpark but fails in sparkless**.

## Reproduction Scripts Behavior

### Both sparkless and PySpark

The reproduction scripts (`bug_column_tracking.py` and `bug_column_tracking_pipeline_context.py`) show the **same behavior** in both modes:

```python
# Trying to access dropped column via attribute access
result = df_transformed.impression_date
# Both show: AttributeError: 'DataFrame' object has no attribute 'impression_date'
```

**This is correct behavior** - you shouldn't be able to access a dropped column via attribute access.

## Actual Pipeline Test Behavior

### sparkless 3.18.3 (Mock Mode) - ❌ FAILS

```bash
SPARK_MODE=mock python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution
# Result: FAILED
# Error: 'DataFrame' object has no attribute 'impression_date'
```

### PySpark (Real Mode) - ✅ PASSES

```bash
SPARK_MODE=real python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution
# Result: PASSED ✅
```

## Key Finding

**The reproduction scripts demonstrate expected behavior** (attribute access to dropped columns fails, which is correct).

**The actual pipeline test reveals the bug** - sparkless's PipelineBuilder internally tries to access dropped columns using attribute access, causing the pipeline to fail. PySpark's PipelineBuilder doesn't have this issue.

## Root Cause

The bug is **not in the transform logic** (which works correctly in both modes).

The bug is in **sparkless's PipelineBuilder internal processing**:
- When processing/validating transform outputs
- It tries to access original column names using attribute access
- This fails when columns were dropped via `.select()`
- PySpark's PipelineBuilder doesn't have this issue

## Evidence

| Aspect | sparkless 3.18.3 | PySpark |
|--------|-------------------|---------|
| Transform function works | ✅ | ✅ |
| Attribute access to dropped column fails | ✅ (expected) | ✅ (expected) |
| **Pipeline test passes** | ❌ **FAILS** | ✅ **PASSES** |

## Conclusion

The reproduction scripts correctly demonstrate that attribute access to dropped columns fails (which is expected). The actual bug is that **sparkless's PipelineBuilder tries to do this internally**, causing pipeline failures, while **PySpark's PipelineBuilder doesn't have this issue**.

This confirms:
1. ✅ The transform code is correct
2. ✅ The reproduction scripts show expected behavior
3. ❌ The bug is in sparkless's PipelineBuilder internal processing
4. ✅ PySpark's PipelineBuilder works correctly

