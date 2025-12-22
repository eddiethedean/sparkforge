# GitHub Issue Created: sparkless Column Resolution Bug

## Issue Details

**Repository**: eddiethedean/sparkless  
**Issue Number**: #158  
**URL**: https://github.com/eddiethedean/sparkless/issues/158  
**Title**: Bug: 'cannot resolve' error when referencing dropped columns in select() and filter()

## Bug Summary

sparkless raises a "cannot resolve" error when code tries to reference a column that was dropped via `.select()`. While this is technically correct behavior (the column doesn't exist), the error occurs in scenarios where the column reference might be valid in certain contexts (e.g., validation rules, incremental processing).

## Key Points

### The Bug
- Error: `cannot resolve 'impression_date' given input columns: [...]`
- Occurs when referencing columns that were dropped via `.select()`
- Affects both string column names and `F.col()` expressions

### Real-World Impact
- Affects SparkForge PipelineBuilder framework tests
- 5 pipeline tests fail with this error
- Occurs in scenarios where:
  1. Transforms drop columns after creating new ones
  2. Validation rules reference original column names
  3. Incremental processing uses bronze layer column names
  4. Downstream transforms expect original columns

### Version Comparison
- **3.18.3**: `AttributeError: 'DataFrame' object has no attribute 'impression_date'` (Issue #156)
- **3.18.4**: `cannot resolve 'impression_date' given input columns: [...]` (Current issue)

The error message change suggests Issue #156 may have been fixed, but the underlying column resolution issue persists.

## Reproduction Scripts

Two detailed reproduction scripts were created:

1. **`bug_cannot_resolve_dropped_column.py`**: Shows the exact error when referencing dropped columns
2. **`bug_incremental_col_reference.py`**: Explores the incremental column reference scenario

Both scripts use only sparkless code (no PipelineBuilder dependencies) and can be run independently.

## Root Cause Analysis

The issue appears to be that:
1. Column tracking: When a column is dropped via `.select()`, sparkless correctly removes it
2. Column resolution: When code tries to reference the dropped column, sparkless correctly identifies it doesn't exist
3. Error context: The error occurs in scenarios where the column reference might be from validation rules, incremental processing, or downstream transforms

**The core issue**: While sparkless is technically correct (the column doesn't exist), the error suggests that code is trying to reference a column that was explicitly dropped. This could be:
1. A logic error in the calling code
2. A sparkless limitation in column name mapping/tracking
3. A design issue with how transforms handle column dropping

## Suggested Fixes

1. **Better error messages**: More helpful error messages suggesting alternative column names
2. **Column name mapping/tracking**: Provide a way to map original column names to new ones
3. **Graceful handling**: Allow code to check if a column exists before referencing it

## Related Issues

- Issue #156: select() uses attribute access for dropped columns (may be fixed in 3.18.4)
- Issue #158: Column resolution error when referencing dropped columns (current issue)

