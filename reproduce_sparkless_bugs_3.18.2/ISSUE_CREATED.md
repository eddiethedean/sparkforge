# GitHub Issue Created Successfully

## Issue Details

**Issue #153**: https://github.com/eddiethedean/sparkless/issues/153

**Title**: "to_timestamp() returns None for all values in 3.18.2"

**Status**: OPEN

**Label**: bug

## Issue Summary

This issue reports the root cause of the validation failures in sparkless 3.18.2:

**The Bug**: `to_timestamp()` returns `None` for all values, even though the schema correctly shows `TimestampType`.

**Impact**: Causes silent validation failures (0% valid, 0 rows processed) in all pipelines that use `to_timestamp()` to parse datetime strings.

## Key Points in the Issue

1. **Root Cause Identified**: Not a type mismatch - `to_timestamp()` returns `None` for all values
2. **Minimal Reproduction**: Direct sparkless code showing the bug
3. **Evidence**: Schema shows `TimestampType` but all values are `None`
4. **Comparison**: Same code works correctly in PySpark
5. **Impact**: 5 pipeline tests fail with this pattern

## Related Files

- `GITHUB_ISSUE.md` - The issue body that was posted
- `bug_direct_sparkless_validation.py` - Direct reproduction code
- `BUG_ROOT_CAUSE.md` - Detailed root cause analysis
- `REPRODUCTION_SUMMARY.md` - Complete reproduction guide

## Next Steps

1. ✅ Issue #153 created and reported
2. ⏳ Wait for sparkless team to address the bug
3. ⏳ Once fixed, verify all 5 tests pass

## Previous Issues

- **Issue #149**: Reported type mismatch in 3.18.0 (expected Datetime, got String)
- **Issue #151**: Reported type mismatch in 3.18.1 (expected String, got datetime)
- **Issue #153**: Reports the actual root cause - `to_timestamp()` returns `None` for all values

Issue #153 supersedes the previous issues with the correct root cause identification.

