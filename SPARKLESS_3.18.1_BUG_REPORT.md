# sparkless 3.18.1 Bug Report

## Summary

Created a new GitHub issue (#151) for the datetime type validation bug in sparkless 3.18.1.

## Issue Details

**GitHub Issue**: https://github.com/eddiethedean/sparkless/issues/151

**Title**: "to_timestamp() returns datetime but validation expects String in 3.18.1"

**Status**: OPEN

## Bug Description

### The Problem

In sparkless 3.18.1, `F.to_timestamp()` correctly returns a `TimestampType` column (as PySpark does), but sparkless's internal validation system incorrectly expects `StringType`, causing validation failures.

### Error Message

```
invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
```

### Expected vs Actual

| Aspect | Expected | Actual | Status |
|--------|----------|--------|--------|
| `to_timestamp()` return type | `TimestampType` | `TimestampType` | ✅ Correct |
| Sparkless validation expectation | `TimestampType` | `StringType` | ❌ Bug |
| Datetime operations | Work | Work | ✅ Correct |

## Root Cause

**PySpark is correct**: `to_timestamp()` returns `TimestampType` as expected.

**Sparkless bug**: Sparkless's internal type validation system incorrectly expects `StringType` for columns created by `to_timestamp()`.

This is a **sparkless type inference/validation bug**, not a PySpark compatibility issue.

## Comparison with Previous Versions

| Version | Error Pattern | Status |
|---------|---------------|--------|
| 3.17.11 | `expected String, got datetime[μs]` | ❌ 19 failures |
| 3.18.0 | `expected Datetime('μs'), got String` | ❌ 5 failures (reversed) |
| 3.18.1 | `expected String, got datetime[μs]` | ❌ 5 failures (reverted) |

**Analysis**:
- Issue #149 (3.18.0) was fixed - sparkless now correctly recognizes datetime columns
- But sparkless 3.18.1 reverted the validation logic, bringing back the original problem
- The core issue: sparkless's type validation doesn't properly handle `TimestampType` from `to_timestamp()`

## Affected Tests (5 total)

1. **Marketing Pipeline** - `processed_impressions`, `processed_clicks`, `processed_conversions`
2. **Supply Chain Pipeline** - `processed_orders`, `processed_shipments`, `processed_inventory`
3. **Streaming Hybrid Pipeline** - `unified_batch_events`, `unified_streaming_events`
4. **Data Quality Pipeline** - `normalized_source_a`, `normalized_source_b`
5. **Healthcare Pipeline** - `patient_risk_scores` (downstream validation failure)

## Reproduction Scripts

Created comprehensive reproduction scripts:

1. **`bug_to_timestamp_validation_failure.py`**
   - Mimics the exact transform pattern from failing tests
   - Shows PySpark works correctly
   - Demonstrates where sparkless validation fails

2. **`bug_to_timestamp_returns_datetime_when_string_expected.py`**
   - Step-by-step demonstration
   - Shows expected vs actual behavior
   - Proves the column works correctly in PySpark

## Issue Quality Improvements

Compared to Issue #149, this issue includes:

1. ✅ **Clear expected vs actual behavior table**
2. ✅ **Step-by-step reproduction with verification**
3. ✅ **Root cause analysis distinguishing PySpark (correct) vs sparkless (bug)**
4. ✅ **Demonstration that datetime operations work correctly**
5. ✅ **Comparison with previous versions showing the regression**
6. ✅ **Minimal reproduction code**
7. ✅ **Real-world example from failing tests**

## Next Steps

1. ✅ Issue #151 created and reported
2. ⏳ Wait for sparkless team to address the type validation bug
3. ⏳ Once fixed, verify all 5 tests pass

## Conclusion

The bug report is more comprehensive and clearly demonstrates:
- PySpark works correctly ✅
- The issue is in sparkless's validation system ❌
- The fix needed: sparkless should accept `TimestampType` from `to_timestamp()`

This should help the sparkless developers understand and fix the issue more quickly.

