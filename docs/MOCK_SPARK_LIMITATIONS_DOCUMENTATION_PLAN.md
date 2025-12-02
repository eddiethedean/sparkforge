# Plan: Comprehensive Mock-Spark Limitations Documentation

> **Note**: With mock-spark 3.7+, most limitations described in this plan have been resolved. Mock-spark 3.7+ uses a Polars backend instead of DuckDB, which resolved threading issues and improved compatibility. This plan is kept for historical reference.

## Overview

This plan outlines the creation of a comprehensive document that catalogs all known mock-spark limitations encountered in the SparkForge codebase. The document will provide detailed context, impact assessment, reproduction steps, and actionable suggestions for mock-spark developers.

## Document Structure

### 1. Executive Summary
- Brief overview of mock-spark's role in SparkForge
- High-level categorization of limitations
- Overall compatibility assessment
- Recommendations for users

### 2. Limitation Categories

#### Category A: DataFrame Creation and Schema Handling
#### Category B: Column Name and Metadata Issues
#### Category C: Parallel Execution and Threading
#### Category D: Type System and Python Compatibility
#### Category E: SQL Function Compatibility
#### Category F: Delta Lake and Advanced Features
#### Category G: Performance and Resource Management

## Detailed Limitation Documentation Template

For each limitation, document:

1. **Limitation ID**: Unique identifier (e.g., `MOCK-001`)
2. **Title**: Clear, concise description
3. **Severity**: Critical / High / Medium / Low
4. **Category**: Which category it belongs to
5. **Affected Versions**: mock-spark versions where this occurs
6. **Python Versions**: Python versions affected
7. **Backend**: DuckDB / Polars / Both
8. **Description**: Detailed explanation of the limitation
9. **Root Cause**: Technical explanation of why this occurs
10. **Reproduction Steps**: Step-by-step code to reproduce
11. **Error Messages**: Actual error messages encountered
12. **Impact Assessment**: 
    - How many tests are affected
    - Which features are impacted
    - Workarounds required
13. **Current Workarounds**: What SparkForge does to work around it
14. **Suggested Fixes for Mock-Spark**: Concrete suggestions for mock-spark developers
15. **Test Cases**: Minimal reproduction code
16. **Related Issues**: Links to GitHub issues, discussions, etc.

## Phase 1: Research and Collection (2-3 hours)

### 1.1 Search Codebase for Limitations

**Tasks:**
- [ ] Search for all `pytest.skip` calls related to mock-spark
- [ ] Find all `ColumnNotFoundError` occurrences
- [ ] Identify all tuple-to-dict conversion workarounds
- [ ] Locate threading/parallel execution workarounds
- [ ] Find Python 3.8 compatibility patches
- [ ] Search for `createDataFrame` monkey-patching
- [ ] Find Delta Lake skip conditions
- [ ] Locate datetime parsing workarounds
- [ ] Find regexp_replace workarounds
- [ ] Search for AnalysisException workarounds

**Tools:**
- `grep` for patterns
- `codebase_search` for semantic search
- Review test files for skip reasons
- Review conftest.py for workarounds

### 1.2 Categorize Limitations

**Categories to identify:**
- DataFrame creation issues
- Column name/metadata loss
- Threading/parallel execution
- Type system incompatibilities
- SQL function differences
- Delta Lake limitations
- Performance issues
- Python version compatibility

### 1.3 Collect Context

For each limitation, collect:
- Exact error messages
- Code snippets that trigger it
- Workaround implementations
- Test files that demonstrate it
- Related documentation

## Phase 2: Documentation Creation (3-4 hours)

### 2.1 Create Main Document

**File**: `docs/MOCK_SPARK_LIMITATIONS_COMPREHENSIVE.md`

**Sections:**

1. **Introduction**
   - Purpose of document
   - How to use this document
   - Version information
   - Contributing guidelines

2. **Quick Reference Table**
   - Table of all limitations with severity and category
   - Links to detailed sections

3. **Detailed Limitations** (one section per limitation)

4. **Workarounds Summary**
   - Quick reference for common workarounds
   - Code examples

5. **Recommendations for Mock-Spark Developers**
   - Prioritized list of fixes
   - Technical implementation suggestions
   - Test cases to add

6. **Migration Guide**
   - How to work around limitations
   - Best practices
   - When to use PySpark instead

### 2.2 Create Individual Limitation Sections

For each limitation, include:

```markdown
## MOCK-XXX: [Title]

**Severity**: [Critical/High/Medium/Low]  
**Category**: [Category Name]  
**Affected Versions**: mock-spark X.Y.Z+  
**Python Versions**: 3.8, 3.9+  
**Backend**: DuckDB / Polars / Both

### Description
[Detailed explanation of what doesn't work]

### Root Cause
[Technical explanation of why this limitation exists]

### Reproduction
```python
# Minimal reproduction code
```

### Error Messages
```
[Actual error output]
```

### Impact
- **Tests Affected**: X tests
- **Features Impacted**: [List]
- **Workarounds Required**: [Description]

### Current Workarounds
[What SparkForge does to work around this]

### Suggested Fixes for Mock-Spark Developers
1. [Specific technical suggestion]
2. [Alternative approach]
3. [Long-term solution]

### Test Cases
[Minimal test code that should pass]

### Related
- GitHub Issue: [if applicable]
- Discussion: [if applicable]
```

## Phase 3: Specific Limitations to Document

### 3.1 DataFrame Creation Issues

**MOCK-001: Tuple Data with StructType Schema**
- **Issue**: `createDataFrame(tuple_data, StructType)` loses column names
- **Error**: `ColumnNotFoundError: unable to find column "X"; valid columns: ["column_0", "column_1", ...]`
- **Root Cause**: mock-spark doesn't properly map StructType field names to tuple data
- **Workaround**: Convert tuples to dicts before calling `createDataFrame`
- **Fix Suggestion**: mock-spark should automatically map schema field names to tuple positions

**MOCK-002: createDataFrame Schema Argument Position**
- **Issue**: PySpark 3.5+ expects schema as positional, mock-spark expects keyword
- **Error**: `PySparkTypeError: [NOT_LIST_OR_NONE_OR_STRUCT]`
- **Root Cause**: Signature differences between PySpark versions and mock-spark
- **Workaround**: Try-catch with both calling patterns
- **Fix Suggestion**: mock-spark should accept both positional and keyword schema arguments

**MOCK-003: PySpark 3.5+ StructType Bug**
- **Issue**: PySpark 3.5+ has a bug with `createDataFrame(dict_data, StructType)`
- **Error**: `PySparkTypeError: [NOT_LIST_OR_NONE_OR_STRUCT]`
- **Note**: This is a PySpark bug, not mock-spark, but affects compatibility testing
- **Workaround**: Skip affected tests with real PySpark
- **Fix Suggestion**: N/A (PySpark issue)

### 3.2 Column Name and Metadata Issues

**MOCK-004: Column Names Lost During Filter Operations**
- **Issue**: After filtering with `F.col("column_name")`, column names become `column_0`, `column_1`, etc.
- **Error**: `ColumnNotFoundError: unable to find column "user_id"; valid columns: ["column_0", "column_1", "column_2"]`
- **Root Cause**: mock-spark's Polars backend loses column metadata during lazy evaluation
- **Workaround**: Skip tests that require column name preservation after filtering
- **Fix Suggestion**: 
  - Preserve column names through lazy evaluation chain
  - Ensure `filter()` operations maintain schema metadata
  - Add column name validation in materialization step

**MOCK-005: Column Metadata Not Preserved in Transformations**
- **Issue**: Column names/metadata lost in complex transformation chains
- **Error**: Similar to MOCK-004
- **Root Cause**: Schema propagation issues in Polars lazy evaluation
- **Workaround**: Use dict-based DataFrames and avoid complex transformations
- **Fix Suggestion**: Implement proper schema tracking through transformation pipeline

### 3.3 Parallel Execution and Threading

**MOCK-006: DuckDB Thread Isolation**
- **Issue**: DuckDB connections in worker threads don't see schemas from main thread
- **Error**: `TableNotFoundError` or silent failures
- **Root Cause**: DuckDB connection-level isolation; schemas are per-connection
- **Workaround**: Disable parallel execution for mock-spark (`parallel.enabled=False`)
- **Fix Suggestion**:
  - Implement connection pooling with shared schema visibility
  - Add schema propagation mechanism across connections
  - Provide thread-safe schema creation API

**MOCK-007: Segmentation Faults in Parallel Tests**
- **Issue**: pytest-xdist parallel execution causes segfaults with mock-spark
- **Error**: Segmentation fault (core dumped)
- **Root Cause**: DuckDB thread safety issues + process-level isolation problems
- **Workaround**: Reduce parallel workers (`-n 4` instead of `-n 10`) or disable parallel execution
- **Fix Suggestion**:
  - Fix thread safety in DuckDB backend
  - Add proper connection management for multi-process scenarios
  - Implement process-level schema sharing mechanism

**MOCK-008: Schema Visibility Race Conditions**
- **Issue**: Schemas created in one thread not immediately visible to others
- **Error**: `SchemaNotFoundError` or `TableNotFoundError`
- **Root Cause**: Race conditions in schema creation and catalog updates
- **Workaround**: Pre-create all schemas in main thread before parallel execution
- **Fix Suggestion**:
  - Add schema synchronization mechanism
  - Implement retry logic with exponential backoff
  - Provide atomic schema creation API

### 3.4 Type System and Python Compatibility

**MOCK-009: Python 3.8 TypeAlias Compatibility**
- **Issue**: `typing.TypeAlias` is not subscriptable in Python 3.8
- **Error**: `TypeError: 'ABCMeta' object is not subscriptable`
- **Root Cause**: mock-spark uses Python 3.9+ type features in Python 3.8
- **Workaround**: SparkForge ships `sitecustomize.py` patch
- **Fix Suggestion**:
  - Use `typing_extensions.TypeAlias` for Python 3.8 compatibility
  - Add `__class_getitem__` to ABCs for Python 3.8
  - Document minimum Python version clearly

**MOCK-010: collections.abc.Mapping Generic Support**
- **Issue**: `collections.abc.Mapping[str, int]` unavailable in Python 3.8
- **Error**: Import errors or type checking failures
- **Root Cause**: Generic ABCs require Python 3.9+
- **Workaround**: `sitecustomize.py` patch adds `__class_getitem__`
- **Fix Suggestion**: Use `typing_extensions` for Python 3.8 support

**MOCK-011: DataFrame Type Checking**
- **Issue**: `isinstance(df, DataFrame)` fails because mock-spark DataFrame != PySpark DataFrame
- **Error**: Type checking failures in mypy/type checkers
- **Root Cause**: Different class hierarchies
- **Workaround**: Use structural typing (`hasattr` checks) or Protocol
- **Fix Suggestion**: 
  - Implement common base class or Protocol
  - Provide type stubs that align with PySpark types
  - Support structural typing patterns

### 3.5 SQL Function Compatibility

**MOCK-012: regexp_replace SQL Syntax**
- **Issue**: DuckDB doesn't support PySpark's `regexp_replace` SQL syntax
- **Error**: `Parser Error: syntax error at or near "regexp_replace"`
- **Root Cause**: SQL syntax differences between Spark SQL and DuckDB SQL
- **Workaround**: Pre-process data or use alternative string functions
- **Fix Suggestion**:
  - Implement `regexp_replace` function in DuckDB backend
  - Translate PySpark regexp_replace to DuckDB-compatible SQL
  - Add function compatibility layer

**MOCK-013: Datetime Parsing Functions**
- **Issue**: `F.to_timestamp`/`F.to_date` fail with string columns
- **Error**: `polars.exceptions.SchemaError` or `ComputeError`
- **Root Cause**: Polars backend doesn't handle string-to-timestamp conversion like PySpark
- **Workaround**: Use string slicing or pre-process timestamps
- **Fix Suggestion**:
  - Support explicit format strings in `to_date`/`to_timestamp`
  - Improve string-to-timestamp conversion in Polars backend
  - Add compatibility helpers for common datetime patterns

**MOCK-014: Window Functions**
- **Issue**: Window functions require JVM in PySpark, not available in mock-spark
- **Error**: Various window function errors
- **Root Cause**: Window functions not fully implemented in mock-spark
- **Workaround**: Skip tests requiring window functions
- **Fix Suggestion**: Implement window function support in Polars/DuckDB backend

### 3.6 Delta Lake and Advanced Features

**MOCK-015: Delta Lake Operations**
- **Issue**: Delta Lake operations not supported in mock-spark
- **Error**: `DeltaTableNotFoundError` or similar
- **Root Cause**: Delta Lake is PySpark-specific feature
- **Workaround**: Skip Delta Lake tests, use PySpark for Delta operations
- **Fix Suggestion**: 
  - Consider implementing basic Delta Lake support using DuckDB
  - Or clearly document as unsupported feature
  - Provide feature detection API

**MOCK-016: Delta Schema Evolution**
- **Issue**: Delta schema evolution (`mergeSchema=True`) had regressions in mock-spark 3.0
- **Error**: Schema evolution failures
- **Root Cause**: Implementation bugs in schema evolution logic
- **Workaround**: Test with both backends, skip if issues
- **Fix Suggestion**: Add regression tests for schema evolution scenarios

### 3.7 AnalysisException Compatibility

**MOCK-017: AnalysisException Constructor**
- **Issue**: mock-spark's `AnalysisException` requires `stackTrace` parameter
- **Error**: `TypeError: __init__() missing 1 required positional argument: 'stackTrace'`
- **Root Cause**: Different constructor signatures
- **Workaround**: Always pass `None` as second argument
- **Fix Suggestion**: Make `stackTrace` optional with default `None` to match PySpark

### 3.8 Performance and Resource Management

**MOCK-018: Memory Usage in Parallel Execution**
- **Issue**: Higher memory usage with parallel execution
- **Root Cause**: Multiple DuckDB connections, no connection pooling
- **Workaround**: Disable parallel execution
- **Fix Suggestion**: Implement connection pooling and resource management

**MOCK-019: Test Execution Speed**
- **Issue**: Some operations slower than PySpark in certain scenarios
- **Root Cause**: DuckDB/Polars overhead for certain operations
- **Workaround**: Acceptable for testing, use PySpark for production
- **Fix Suggestion**: Optimize common operations, add performance benchmarks

## Phase 4: Implementation Details

### 4.1 Document Format

**Markdown Structure:**
- Use consistent heading levels
- Include code blocks with syntax highlighting
- Add tables for quick reference
- Include diagrams where helpful
- Link to related documentation

### 4.2 Code Examples

**For each limitation, include:**
1. **Failing Code**: Code that demonstrates the issue
2. **Error Output**: Actual error messages
3. **Workaround Code**: How SparkForge works around it
4. **Suggested Fix**: What mock-spark should do
5. **Test Case**: Minimal test that should pass

### 4.3 Prioritization

**Priority Levels:**
- **P0 (Critical)**: Blocks core functionality, affects many tests
- **P1 (High)**: Significant impact, requires workarounds
- **P2 (Medium)**: Moderate impact, has workarounds
- **P3 (Low)**: Minor impact, easy to work around

### 4.4 Suggested Fixes Format

For each limitation, provide:
1. **Quick Fix**: Minimal change to improve situation
2. **Proper Fix**: Complete solution addressing root cause
3. **Long-term Solution**: Architectural improvement
4. **Test Cases**: What should be tested
5. **Breaking Changes**: If any, how to handle

## Phase 5: Additional Sections

### 5.1 Compatibility Matrix

**Table showing:**
- mock-spark version
- Python version
- Backend (DuckDB/Polars)
- Which limitations apply
- Workaround availability

### 5.2 Migration Guide

**For users switching between PySpark and mock-spark:**
- What to expect
- Code changes needed
- Feature availability
- Performance differences

### 5.3 Best Practices

**Recommendations:**
- When to use mock-spark vs PySpark
- How to write compatible code
- Testing strategies
- CI/CD considerations

### 5.4 Contributing Back

**How SparkForge can help:**
- Provide test cases
- Contribute fixes
- Report issues
- Collaborate on improvements

## Phase 6: Maintenance Plan

### 6.1 Update Process

**When to update:**
- New limitation discovered
- mock-spark version released
- Workaround improved
- Fix confirmed

### 6.2 Version Tracking

**Track:**
- When limitation was first documented
- mock-spark versions affected
- When workaround was added
- When fix was confirmed (if applicable)

### 6.3 Testing

**Regular checks:**
- Verify limitations still exist
- Test workarounds still work
- Check if fixes are available
- Update status

## Success Criteria

1. ✅ All known limitations documented with full context
2. ✅ Each limitation has reproduction steps
3. ✅ Workarounds clearly explained
4. ✅ Actionable suggestions for mock-spark developers
5. ✅ Easy to navigate and find relevant information
6. ✅ Maintained and updated as needed

## Timeline Estimate

- **Phase 1 (Research)**: 2-3 hours
- **Phase 2 (Documentation)**: 3-4 hours
- **Phase 3 (Review)**: 1 hour
- **Phase 4 (Polish)**: 1 hour
- **Total**: 7-9 hours

## Deliverables

1. **Main Document**: `docs/MOCK_SPARK_LIMITATIONS_COMPREHENSIVE.md`
2. **Quick Reference**: Table of all limitations
3. **Test Cases**: Minimal reproduction code for each limitation
4. **Upstream Report**: Formatted suggestions for mock-spark developers

