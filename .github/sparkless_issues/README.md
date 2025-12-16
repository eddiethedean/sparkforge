# Sparkless Bug Reports

This directory contains bug reports for sparkless 3.17.0 that have been validated against PySpark to confirm they are sparkless-specific issues.

## Bugs Found

1. **[Issue #1: Combined AND expressions](./issue_1_combined_and_expressions.md)**
   - Combined `ColumnOperation` expressions with `&` operator are treated as column names
   - **Status**: ✅ Fixed in sparkless 3.17.1
   - **GitHub Issue**: [#108](https://github.com/eddiethedean/sparkless/issues/108)

2. **[Issue #2: Combined OR expressions](./issue_2_combined_or_expressions.md)**
   - Combined `ColumnOperation` expressions with `|` operator are treated as column names
   - **Status**: ✅ Fixed in sparkless 3.17.1
   - **GitHub Issue**: [#109](https://github.com/eddiethedean/sparkless/issues/109)

3. **[Issue #3: Table visibility after append](./issue_3_table_visibility_after_append.md)**
   - Table data not visible after append write operations
   - **Status**: 🟡 Partially Fixed in 3.17.2 (simple cases work, parquet format still broken)
   - **Severity**: High (breaks logging and data persistence)
   - **GitHub Issue**: [#112](https://github.com/eddiethedean/sparkless/issues/112)

4. **[Issue #4: Parquet format table visibility (LogWriter)](./issue_4_logwriter_table_visibility.md)**
   - Table data not visible after append write via parquet format tables
   - **Status**: 🔴 Open - Confirmed bug, validated with PySpark
   - **Severity**: High (breaks LogWriter and any parquet format table usage)
   - **GitHub Issue**: [#114](https://github.com/eddiethedean/sparkless/issues/114)
   - **Note**: This is the remaining case from issue #3 - parquet format tables specifically

## Validation Process

Each bug has been validated by:
1. Reproducing the issue in sparkless 3.17.0
2. Testing the equivalent code in PySpark to confirm it works correctly
3. Documenting the expected vs actual behavior
4. Providing minimal reproducible examples

## Test Results Summary

| Test Case | sparkless 3.17.0 | sparkless 3.17.1 | sparkless 3.17.2 | PySpark | Status |
|-----------|------------------|------------------|------------------|---------|--------|
| Single expression (`F.col('a').isNotNull()`) | ✅ Works | ✅ Works | ✅ Works | ✅ Works | Not a bug |
| Combined AND (`expr1 & expr2`) | ❌ Fails | ✅ Works | ✅ Works | ✅ Works | **Bug #1** - Fixed |
| Combined OR (`expr1 \| expr2`) | ❌ Fails | ✅ Works | ✅ Works | ✅ Works | **Bug #2** - Fixed |
| Simple append (no format) | ❌ Fails | ❌ Fails | ✅ Works | ✅ Works | **Bug #3** - Fixed |
| Parquet format append | ❌ Fails | ❌ Fails | ❌ Fails | ✅ Works | **Bug #4** - Open |

## Next Steps

These issues should be reported to the sparkless GitHub repository:
- Repository: https://github.com/eddiethedean/sparkless
- Issues should be created with the content from the respective markdown files

