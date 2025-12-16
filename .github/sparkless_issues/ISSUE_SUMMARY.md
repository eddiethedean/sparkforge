# Sparkless 3.17.0 Bug Report Summary

## Overview

This document summarizes confirmed bugs in sparkless 3.17.0 that affect compatibility with PySpark. All bugs have been validated by testing equivalent code in both sparkless and PySpark.

## Confirmed Bugs

### Bug #1: Combined AND Expressions Treated as Column Names

**Issue**: Combined `ColumnOperation` expressions using `&` (AND) operator are incorrectly treated as column names in `DataFrame.filter()`.

**Impact**: High - Breaks common filtering patterns used in data validation and quality checks.

**Status**: ✅ Confirmed (validated with PySpark)

**Details**: See [issue_1_combined_and_expressions.md](./issue_1_combined_and_expressions.md)

**Example**:
```python
# This fails in sparkless 3.17.0:
df.filter(F.col('a').isNotNull() & F.col('b') > 0)
# Error: unable to find column "a IS NOT NULL"

# This works in PySpark:
df.filter(F.col('a').isNotNull() & F.col('b') > 0)  # ✅ Works
```

---

### Bug #2: Combined OR Expressions Treated as Column Names

**Issue**: Combined `ColumnOperation` expressions using `|` (OR) operator are incorrectly treated as column names in `DataFrame.filter()`.

**Impact**: High - Breaks common filtering patterns, especially for OR logic which cannot be easily worked around.

**Status**: ✅ Confirmed (validated with PySpark)

**Details**: See [issue_2_combined_or_expressions.md](./issue_2_combined_or_expressions.md)

**Example**:
```python
# This fails in sparkless 3.17.0:
df.filter(F.col('a').isNotNull() | F.col('b') > 0)
# Error: unable to find column "a IS NOT NULL"

# This works in PySpark:
df.filter(F.col('a').isNotNull() | F.col('b') > 0)  # ✅ Works
```

---

## Root Cause Analysis

Both bugs appear to stem from the same root cause: when `ColumnOperation` objects are combined using boolean operators (`&` or `|`), the resulting combined expression's string representation is being incorrectly used as a column name in the `filter()` method, rather than being evaluated as a filter expression.

**Key Observations**:
1. Single expressions work correctly: `df.filter(F.col('a').isNotNull())` ✅
2. Individual expressions work: Both `expr1` and `expr2` can be used individually ✅
3. Only combined expressions fail: `expr1 & expr2` or `expr1 | expr2` ❌
4. The combined expression is still a `ColumnOperation` object, but its string representation is being misused

## Validation Process

Each bug was validated using the following process:

1. **Reproduce in sparkless**: Create minimal test case that demonstrates the issue
2. **Test in PySpark**: Run equivalent code in PySpark to confirm expected behavior
3. **Compare results**: Verify that PySpark works correctly while sparkless fails
4. **Document**: Create detailed bug report with steps to reproduce

## Test Results

| Test Case | sparkless 3.17.0 | PySpark | Status |
|-----------|------------------|---------|--------|
| Single expression | ✅ Works | ✅ Works | Not a bug |
| Combined AND (`&`) | ❌ Fails | ✅ Works | **Bug #1** |
| Combined OR (`\|`) | ❌ Fails | ✅ Works | **Bug #2** |

## Workarounds

### For AND Operations

Filter sequentially instead of combining:
```python
# Instead of:
df.filter(expr1 & expr2)

# Use:
df.filter(expr1).filter(expr2)
```

**Note**: This workaround is implemented in SparkForge's validation code.

### For OR Operations

No simple workaround exists. Sequential filtering changes semantics (AND vs OR). Potential alternatives:
- Use `functions.when()` chains (not a drop-in replacement)
- Manually construct SQL expressions (complex and error-prone)

## Impact on SparkForge

These bugs affect SparkForge's data validation functionality, which commonly uses combined expressions for multi-column validation rules. A workaround has been implemented that filters sequentially for AND operations, but OR operations remain problematic.

## Next Steps

1. ✅ **Bug #1 and #2**: Fixed in sparkless 3.17.1 - Workarounds removed from SparkForge
2. 🔴 **Bug #3**: Open issue - Monitor for fix in future sparkless releases
3. Update SparkForge's compatibility layer when Bug #3 is fixed
4. Consider contributing fixes if the root cause is identified

## Files

- [Issue #1: Combined AND expressions](./issue_1_combined_and_expressions.md)
- [Issue #2: Combined OR expressions](./issue_2_combined_or_expressions.md)
- [Validation script](./validate_bugs.py) - Run to validate bugs
- [README](./README.md) - Overview of all bugs

