# Test Failures Analysis - 5 Builder Tests

## Summary

All 5 failing tests are in `tests/builder_tests/` and are **100% sparkless 3.17.10 compatibility issues**. 

**CRITICAL FINDING**: All 5 tests **PASS in real mode (PySpark)**, confirming:
- ✅ Test code is correct
- ✅ Framework is working properly
- ❌ Failures are exclusively due to sparkless 3.17.10 limitations

## Failure Patterns

### 1. **Healthcare Pipeline** (`test_healthcare_pipeline.py`)
**Error**: `AssertionError: assert 0 > 0` - `patient_risk_scores` has 0 rows processed

**Root Cause**: 
- Silver steps `normalized_labs` and `processed_diagnoses` failed validation (0.0% valid)
- Both produced 0 rows, causing downstream gold step to have no data
- The validation rules are too strict or the data transformations are incompatible with sparkless

**Log Evidence**:
```
✅ Completed SILVER step: normalized_labs (0.04s) - 0 rows processed, 0 rows written, 150 invalid, 0.0% valid
✅ Completed SILVER step: processed_diagnoses (0.04s) - 0 rows processed, 0 rows written, 120 invalid, 0.0% valid
```

### 2. **Marketing Pipeline** (`test_marketing_pipeline.py`)
**Error**: `invalid series dtype: expected String, got datetime[μs] for series with name impression_date_parsed`

**Root Cause**: 
- Sparkless expects string types for certain operations
- The transform creates `impression_date_parsed` as a datetime, but sparkless validation/operations expect strings
- This is a **sparkless 3.17.10 compatibility issue**

**Log Evidence**:
```
ERROR - ❌ Failed SILVER step: processed_impressions (0.18s) - invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
```

### 3. **Streaming Hybrid Pipeline** (`test_streaming_hybrid_pipeline.py`)
**Error**: `invalid series dtype: expected String, got datetime[μs] for series with name event_timestamp_parsed`

**Root Cause**: 
- Same issue as marketing pipeline
- Both `unified_batch_events` and `unified_streaming_events` fail with datetime type issues
- **sparkless 3.17.10 compatibility issue**

**Log Evidence**:
```
ERROR - ❌ Failed SILVER step: unified_streaming_events (2.49s) - invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed`
ERROR - ❌ Failed SILVER step: unified_batch_events (2.50s) - invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed`
```

### 4. **Supply Chain Pipeline** (`test_supply_chain_pipeline.py`)
**Error**: `'DataFrame' object has no attribute 'snapshot_date'. Available columns: inventory_id, product_id, warehouse_id, snapshot_date_parsed, ...`

**Root Cause**: 
- **Sparkless 3.17.10 compatibility issue**: Sparkless handles column references differently than PySpark
- The transform works correctly in PySpark but fails in sparkless due to internal dataframe handling differences
- **PASSES in real mode** - confirms this is sparkless-specific

**Log Evidence**:
```
ERROR - ❌ Failed SILVER step: processed_inventory (0.19s) - 'DataFrame' object has no attribute 'snapshot_date'. Available columns: inventory_id, product_id, warehouse_id, snapshot_date_parsed, ...
```

### 5. **Data Quality Pipeline** (`test_data_quality_pipeline.py`)
**Error**: `unable to find column "transaction_date_parsed"; valid columns: ["id", "customer_id", "date", "amount", ...]`

**Root Cause**: 
- **Sparkless 3.17.10 compatibility issue**: Sparkless may not be properly handling column transformations in the same way PySpark does
- The transform creates `transaction_date_parsed` correctly, but sparkless validation sees the original column structure
- **PASSES in real mode** - confirms this is sparkless-specific

**Log Evidence**:
```
ERROR - ❌ Failed SILVER step: normalized_source_b (2.47s) - unable to find column "transaction_date_parsed"; valid columns: ["id", "customer_id", "date", "amount", "status", "category", "region", "quality_score", "quality_status", "has_null_customer", "has_invalid_amount", "has_empty_category"]
```

## Common Themes

**ALL 5 TESTS ARE SPARKLESS 3.17.10 COMPATIBILITY ISSUES** - All tests pass in real mode (PySpark)

1. **Datetime Type Handling** (3 tests):
   - Sparkless expects string types for datetime columns in certain operations
   - PySpark handles datetime types natively, sparkless requires explicit casting
   - Tests: Marketing, Streaming Hybrid, Healthcare (validation on datetime columns)

2. **Column Reference Handling** (2 tests):
   - Sparkless handles column transformations and references differently than PySpark
   - Internal dataframe structure differences cause validation/transformation issues
   - Tests: Supply Chain, Data Quality

## Real Mode Test Results

**All 5 tests PASS in real mode (PySpark):**
- ✅ Healthcare Pipeline: **PASSED** (23.56s)
- ✅ Marketing Pipeline: **PASSED** (18.47s)
- ✅ Streaming Hybrid Pipeline: **PASSED** (16.67s)
- ✅ Supply Chain Pipeline: **PASSED** (16.19s)
- ✅ Data Quality Pipeline: **PASSED** (15.50s)

This confirms that:
- Test code is correct and well-written
- Framework functionality is working properly
- All failures are sparkless 3.17.10 compatibility issues

## Recommendations

### Immediate Fixes Needed (Sparkless Compatibility):

1. **For datetime issues** (Marketing, Streaming Hybrid, Healthcare):
   - Cast datetime columns to strings for sparkless compatibility: `.withColumn("date_str", F.col("date_parsed").cast("string"))`
   - Or create a compatibility layer that handles datetime types automatically for sparkless
   - Consider adding sparkless-specific validation rules that work with datetime types

2. **For column reference issues** (Supply Chain, Data Quality):
   - May need to adjust how sparkless handles column transformations
   - Consider adding explicit column casting or intermediate steps for sparkless
   - May require sparkless-specific workarounds in the transform functions

### Long-term Considerations:

- **Create a sparkless compatibility layer** that automatically handles:
  - Datetime to string conversions where needed
  - Column reference handling differences
  - Validation rule adjustments for sparkless
  
- **Consider marking these tests as "pyspark_compat"** if they're meant to test PySpark-specific features
- **Document sparkless limitations** in test documentation
- **Consider creating separate test suites** for sparkless vs PySpark if compatibility becomes too complex

## Impact Assessment

- **Severity**: Low - These are sparkless compatibility issues, not framework bugs
- **Scope**: Limited to 5 integration tests in builder_tests when using sparkless 3.17.10
- **Framework Health**: Excellent - Framework works correctly with PySpark (all tests pass)
- **Test Code Health**: Excellent - All test code is correct (all tests pass in real mode)
- **Action Required**: 
  - Sparkless 3.17.10 compatibility fixes needed
  - OR: Mark these tests as PySpark-only if they test PySpark-specific features
  - OR: Create sparkless compatibility layer to handle type/column differences automatically

## Conclusion

These are **NOT test bugs or framework issues**. They are **sparkless 3.17.10 compatibility limitations**. The framework and test code are both correct. The failures occur because sparkless has different type handling and column reference behavior compared to PySpark.

## GitHub Issues Created

All bugs have been reported to the sparkless project:

1. **[Issue #135](https://github.com/eddiethedean/sparkless/issues/135)**: Datetime columns cause SchemaError - expected String, got datetime[μs]
2. **[Issue #136](https://github.com/eddiethedean/sparkless/issues/136)**: Column rename/transform not reflected in validation - column mismatch error
3. **[Issue #137](https://github.com/eddiethedean/sparkless/issues/137)**: Validation rules fail on datetime columns (0% valid rate)
4. **[Issue #138](https://github.com/eddiethedean/sparkless/issues/138)**: Column reference error after drop() - DataFrame attribute error
5. **[Issue #139](https://github.com/eddiethedean/sparkless/issues/139)**: Validation system incompatible with datetime column operations (umbrella issue)

## Reproduction Scripts

Minimal reproduction scripts have been created in `reproduce_sparkless_bugs/`:
- `bug_1_datetime_validation.py` - Reproduces Issue #135
- `bug_2_datetime_validation_streaming.py` - Reproduces Issue #135 (streaming context)
- `bug_3_validation_on_datetime.py` - Reproduces Issue #137
- `bug_4_column_reference_after_transform.py` - Reproduces Issue #138
- `bug_5_column_rename_validation_mismatch.py` - Reproduces Issue #136

**Options:**
1. Wait for sparkless fixes (issues reported)
2. Fix sparkless compatibility in tests (cast datetimes, adjust column handling) as temporary workaround
3. Mark tests as PySpark-only (`@pytest.mark.pyspark_compat`) if they test PySpark-specific features
4. Create a compatibility layer to abstract sparkless differences

