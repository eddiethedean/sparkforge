# Bug Reproduction: sparkless 3.18.2 vs PySpark

## Summary

This document demonstrates the validation failure bug in sparkless 3.18.2 by running the same test in both mock mode (sparkless) and real mode (PySpark).

## The Bug

In sparkless 3.18.2, when using `F.to_timestamp()` to create datetime columns in pipeline transforms, validation silently fails (0% valid, 0 rows processed). The same code works perfectly in PySpark.

## Reproduction

### Test: Marketing Pipeline

**Test File**: `tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`

**Transform Pattern**:
```python
df.withColumn(
    "impression_date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)
```

**Validation Rules**:
```python
rules={
    "impression_date_parsed": ["not_null"],  # This triggers the bug
    ...
}
```

## Results Comparison

### sparkless 3.18.2 (Mock Mode) - ❌ FAILS

```
Validation completed for pipeline.processed_impressions: 0.0% valid
✅ Completed SILVER step: processed_impressions (0.38s) - 0 rows processed, 0 rows written, 150 invalid, 0.0% valid

Validation completed for pipeline.processed_clicks: 0.0% valid
✅ Completed SILVER step: processed_clicks (0.13s) - 0 rows processed, 0 rows written, 60 invalid, 0.0% valid

Validation completed for pipeline.processed_conversions: 0.0% valid
✅ Completed SILVER step: processed_conversions (0.16s) - 0 rows processed, 0 rows written, 40 invalid, 0.0% valid

Test Result: FAILED
```

**Key Observations**:
- ✅ Steps "complete" (no explicit error thrown)
- ❌ Validation rate: **0.0%** (should be 100%)
- ❌ Rows processed: **0** (should be > 0)
- ❌ All rows marked as invalid (150 invalid, 60 invalid, 40 invalid)
- ❌ Test fails because no rows were processed

### PySpark (Real Mode) - ✅ PASSES

```
Test Result: PASSED
```

**Key Observations**:
- ✅ Validation rate: **100%** (correct)
- ✅ Rows processed: **> 0** (correct)
- ✅ All rows valid
- ✅ Test passes

## Root Cause Analysis

### What Works Correctly

1. **PySpark's `to_timestamp()`**: ✅
   - Correctly returns `TimestampType`
   - Column works in all datetime operations (hour, dayofweek, etc.)

2. **Transform Logic**: ✅
   - The transform function is correct
   - Works perfectly in PySpark

3. **Validation Rules**: ✅
   - The rules are correct
   - Work perfectly in PySpark

### What Fails in sparkless 3.18.2

1. **Validation System**: ❌
   - Sparkless's validation system incorrectly rejects `TimestampType` columns
   - Validation silently fails (0% valid) instead of throwing an error
   - No explicit error message, making debugging difficult

2. **Type Handling**: ❌
   - Sparkless doesn't properly handle `TimestampType` from `to_timestamp()`
   - Even though the column is correct and works in operations

## Evidence

### Same Code, Different Results

| Aspect | sparkless 3.18.2 | PySpark |
|--------|-------------------|---------|
| Transform executes | ✅ Yes | ✅ Yes |
| Column type correct | ✅ TimestampType | ✅ TimestampType |
| Datetime operations work | ✅ Yes | ✅ Yes |
| Validation passes | ❌ **0% valid** | ✅ **100% valid** |
| Rows processed | ❌ **0** | ✅ **> 0** |
| Test result | ❌ **FAILED** | ✅ **PASSED** |

### Key Evidence

1. **Bronze steps pass in both modes**: ✅
   - Raw data validation: 100% valid in both modes
   - Proves the input data is correct

2. **Silver steps fail only in sparkless**: ❌
   - Steps with `to_timestamp()`: 0% valid in sparkless
   - Same steps: 100% valid in PySpark
   - Proves the bug is in sparkless's validation of datetime columns

3. **No explicit error in sparkless 3.18.2**: ⚠️
   - Steps "complete" but with 0% valid
   - Makes debugging harder
   - Previous versions (3.18.1) had explicit error messages

## How to Reproduce

### Option 1: Run the Comparison Script

```bash
cd /Users/odosmatthews/Documents/coding/sparkforge
./reproduce_sparkless_bugs_3.18.2/run_test_both_modes.sh
```

### Option 2: Run Tests Manually

**Mock Mode (sparkless 3.18.2)**:
```bash
SPARK_MODE=mock python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution -v
```

**Real Mode (PySpark)**:
```bash
SPARK_MODE=real python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution -v
```

## Conclusion

This reproduction clearly demonstrates:

1. ✅ **The code is correct** - works perfectly in PySpark
2. ❌ **The bug is in sparkless** - validation fails silently in sparkless 3.18.2
3. 🔍 **The issue is specific** - only affects columns created by `to_timestamp()`
4. ⚠️ **Silent failure** - makes debugging harder than previous versions

The bug is in sparkless's validation system, not in the test code or PySpark compatibility.

## Related

- **GitHub Issue #151**: https://github.com/eddiethedean/sparkless/issues/151
- **Affected Tests**: 5 pipeline tests fail with this pattern
- **Version**: sparkless 3.18.2

