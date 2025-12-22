# sparkless 3.18.1 Test Results

## Summary

**sparkless 3.18.1 still has 5 failures, but with a different error pattern.**

### Test Results

| Version | Passed | Failed | Skipped | Status |
|---------|--------|--------|---------|--------|
| **3.18.0** | 1713 | 5 | 70 | ❌ 5 failures |
| **3.18.1** | **1713** | **5** | 70 | ❌ **Same 5 failures, different error** |

**Result**: No improvement - same 5 tests still failing, but error message changed.

## Error Pattern Change

### sparkless 3.18.0 Error:
```
expected output type 'Datetime('μs')', got 'String'; set `return_dtype` to the proper datatype
```
- **Issue**: Sparkless expected Datetime but got String

### sparkless 3.18.1 Error:
```
invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
```
- **Issue**: Sparkless expects String but got datetime (reverted to original problem)

## Analysis

The error pattern has **reverted** in 3.18.1:
- **3.18.0**: Expected Datetime, got String
- **3.18.1**: Expected String, got datetime (back to original issue)

This suggests:
1. sparkless 3.18.0 tried to fix the datetime handling but overcorrected
2. sparkless 3.18.1 reverted the change, bringing back the original issue
3. The core problem remains: `F.to_timestamp()` type handling is inconsistent

## Affected Tests (Same 5)

1. **Marketing Pipeline** (`test_marketing_pipeline.py`)
   - Error: `invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed``
   - Steps: `processed_impressions`, `processed_clicks`, `processed_conversions`

2. **Supply Chain Pipeline** (`test_supply_chain_pipeline.py`)
   - Error: Similar datetime type mismatch
   - Steps: `processed_orders`, `processed_shipments`, `processed_inventory`

3. **Streaming Hybrid Pipeline** (`test_streaming_hybrid_pipeline.py`)
   - Error: `invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed``
   - Steps: `unified_batch_events`, `unified_streaming_events`

4. **Data Quality Pipeline** (`test_data_quality_pipeline.py`)
   - Error: Similar datetime type mismatch
   - Steps: `normalized_source_a`, `normalized_source_b`

5. **Healthcare Pipeline** (`test_healthcare_pipeline.py`)
   - Error: `0.0% valid` - validation fails, 0 rows processed
   - Step: `patient_risk_scores` (downstream effect)

## Root Cause

The issue is the same as before: `F.to_timestamp()` creates datetime columns, but sparkless expects String in certain validation contexts. The error message format changed, but the underlying type mismatch problem persists.

## Comparison with Previous Versions

| Version | Error Pattern | Status |
|---------|---------------|--------|
| 3.17.11 | `invalid series dtype: expected String, got datetime[μs]` | ❌ 19 failures |
| 3.18.0 | `expected output type 'Datetime('μs')', got 'String'` | ❌ 5 failures (reversed) |
| 3.18.1 | `invalid series dtype: expected `String`, got `datetime[μs]`` | ❌ 5 failures (reverted) |

## Conclusion

**sparkless 3.18.1 did not fix the datetime type handling issue.** The error message format changed, but the core problem remains: `F.to_timestamp()` type handling is inconsistent with sparkless's validation system.

The bug reported in Issue #149 (https://github.com/eddiethedean/sparkless/issues/149) is still present, though the error message format has changed.

## Next Steps

1. ✅ Bug already reported (Issue #149)
2. ⏳ Update issue #149 with 3.18.1 findings
3. ⏳ Wait for sparkless team to provide a proper fix

