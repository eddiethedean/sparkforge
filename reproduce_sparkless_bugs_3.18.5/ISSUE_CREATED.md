# GitHub Issue Created: sparkless Execution Plan Bug

## Issue Details

- **Repository**: eddiethedean/sparkless
- **Issue Number**: #160
- **Title**: Bug: cannot resolve error when execution plan references dropped columns
- **URL**: https://github.com/eddiethedean/sparkless/issues/160

## Bug Summary

When a DataFrame operation drops a column via `.select()`, but the execution plan still contains references to that column from earlier operations, sparkless fails with a `cannot resolve` error during plan evaluation.

## Root Cause

1. Transform creates execution plan with operations on a column (e.g., `F.col("impression_date")`)
2. Transform drops that column via `.select()` (excluding it from final column list)
3. Later, when plan is evaluated (during `count()`, `collect()`, `write()`, etc.), sparkless tries to resolve ALL column references in the plan
4. It tries to resolve the dropped column, which no longer exists
5. sparkless raises: `"cannot resolve 'impression_date' given input columns: [...]"`

## Reproduction

The issue includes:
- Clear description of the bug
- Minimal reproduction code
- Expected vs actual behavior
- Root cause analysis
- Impact assessment
- Environment details

## Related Files

- `GITHUB_ISSUE.md` - Full issue content
- `final_exact_reproduction.py` - Reproduction script
- `EXACT_SPARKLESS_CODE_TRAIL.md` - Detailed execution trail analysis

## Next Steps

Monitor the issue for:
- Acknowledgment from sparkless maintainers
- Questions or requests for additional information
- Fix or workaround suggestions
- Resolution timeline

