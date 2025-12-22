# sparkless 3.18.5 Test Results

## Summary

**sparkless 3.18.5 still has the same 5 test failures.**

### Test Results

| Version | Passed | Failed | Skipped | Status |
|---------|--------|--------|---------|--------|
| **3.18.4** | 1713 | 5 | 70 | ❌ 5 failures |
| **3.18.5** | **1713** | **5** | 70 | ❌ **Same 5 failures** |

**Result**: The same 5 tests still fail with sparkless 3.18.5.

## Affected Tests (Same 5)

1. **Marketing Pipeline** - `processed_impressions` step fails
   - Error: `cannot resolve 'impression_date' given input columns: [...]`
   - Column `impression_date` was dropped, but code tries to reference it

2. **Supply Chain Pipeline** - `processed_inventory` step fails
   - Error: `cannot resolve 'snapshot_date' given input columns: [...]`
   - Column `snapshot_date` was dropped, but code tries to reference it

3. **Healthcare Pipeline** - `normalized_labs` and `processed_diagnoses` steps fail
   - Error: `'NoneType' object has no attribute 'collect'`
   - Different error pattern - suggests a different issue

4. **Data Quality Pipeline** - `data_quality_metrics` step fails
   - Error: `unable to find column "source"; valid columns: [...]`
   - Column `source` is missing

5. **Streaming Hybrid Pipeline** - Similar column resolution issues

## Comparison with Previous Versions

| Version | Error Pattern | Status |
|---------|---------------|--------|
| 3.18.2 | `to_timestamp()` returns None | ❌ 5 failures |
| 3.18.3 | AttributeError: `'DataFrame' object has no attribute 'impression_date'` | ❌ 5 failures |
| 3.18.4 | `cannot resolve 'impression_date' given input columns: [...]` | ❌ 5 failures |
| 3.18.5 | `cannot resolve 'impression_date' given input columns: [...]` | ❌ 5 failures (same) |

## Analysis

sparkless 3.18.5 shows **no change** from 3.18.4:
- Same 5 tests fail
- Same error patterns
- Same column resolution issues

This suggests that Issue #158 (column resolution bug) has not been addressed in 3.18.5.

## Conclusion

sparkless 3.18.5 does not fix the column resolution issues reported in Issue #158. The same 5 tests continue to fail with the same error patterns as 3.18.4.

