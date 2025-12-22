# GitHub Issue Created: sparkless select() Attribute Access Bug

## Issue Details

**Repository**: eddiethedean/sparkless  
**Issue Number**: #156  
**URL**: https://github.com/eddiethedean/sparkless/issues/156  
**Title**: Bug: select() uses attribute access for dropped columns, causing AttributeError

## Bug Summary

sparkless's `select()` method uses attribute access (`df.column_name`) to resolve column references. When a column has been dropped (via a previous `.select()` operation), this causes an `AttributeError` instead of a more appropriate "column not found" error.

## Root Cause

The bug occurs because sparkless's `select()` method uses attribute access to resolve column references:

1. When `select("column_name")` is called, sparkless tries `df.column_name` (attribute access)
2. If the column was dropped, Python's `__getattr__` raises `AttributeError`
3. This is different from checking `df.columns` first, which would allow a more appropriate error

## Evidence

- Direct sparkless code reproduction shows the bug
- Error message: `'DataFrame' object has no attribute 'impression_date'`
- Both `select("column")` and `select(F.col("column"))` trigger the bug
- PySpark doesn't have this issue (doesn't use attribute access)

## Reproduction Scripts

1. `bug_sparkless_select_uses_attribute_access.py` - Main reproduction
2. `bug_sparkless_f_col_behavior.py` - Shows F.col() behavior
3. `bug_sparkless_column_access_difference.py` - Column access patterns

## Suggested Fix

The `select()` method should:
1. Check `df.columns` first (not use attribute access)
2. Use column lookup by name instead of `df.column_name`
3. Raise an appropriate "column not found" error instead of `AttributeError`

## Related

This bug affects the PipelineBuilder tests that drop columns in transforms, causing:
- `test_marketing_pipeline.py` to fail
- Other pipeline tests that use `.select()` to drop columns

