# Mock-Spark Upgrade Plan for PySpark Compatibility

**Version:** 1.0  
**Date:** 2025-10-29  
**Status:** Planning  
**Goal:** Achieve true drop-in replacement compatibility with PySpark

## Executive Summary

This document outlines a comprehensive plan to upgrade mock-spark to become a true drop-in replacement for PySpark. Based on extensive testing with 5 realistic data pipeline scenarios, we've identified critical compatibility gaps that prevent seamless swapping between PySpark and mock-spark imports.

**Current Status:** ~88% compatibility (60/68 tests passing)  
**Target Status:** 95%+ compatibility with zero code changes required

## Current Compatibility Assessment

### What Works Well ✅

- Basic DataFrame operations: `select()`, `filter()`, `groupBy()`, `agg()`
- Simple functions: `when()`, `col()`, `lit()`, `count()`, `sum()`, `avg()`
- Incremental processing with simple transforms
- Basic schema creation and table operations
- Simple aggregations and window functions

### Critical Compatibility Gaps ❌

Based on test failures and code adaptations required:

1. **Datetime Parsing** (HIGH PRIORITY)
2. **String Concatenation** (HIGH PRIORITY)  
3. **Boolean Expression Evaluation** (MEDIUM PRIORITY)
4. **Date Arithmetic** (MEDIUM PRIORITY)
5. **Column Lifecycle Management** (HIGH PRIORITY)
6. **CTE Optimization** (MEDIUM PRIORITY)
7. **Type Coercion** (LOW PRIORITY)

---

## Issue 1: Datetime Parsing Incompatibility

### Problem

PySpark's `to_timestamp()` accepts flexible ISO 8601 formats including microseconds:
```python
# PySpark - WORKS
F.to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
```

Mock-spark's DuckDB backend fails with microseconds:
```
Parser Error: syntax error at or near "T"
```

### Evidence from Tests

**Test:** `tests/builder_tests/test_healthcare_pipeline.py`
- **Line 153-157:** Required workaround:
  ```python
  F.regexp_replace(F.col("test_date"), r"\.\d+", "")  # Remove microseconds
  F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss")
  ```

**Impact:** Found in 5/5 realistic scenarios requiring datetime parsing adaptations.

### Recommended Fix

**Priority:** HIGH  
**Effort:** Medium (2-3 days)

1. **Enhance `to_timestamp()` in mock-spark:**
   - Accept fractional seconds pattern `[.SSSSSS]` in format string
   - Auto-strip microseconds from ISO 8601 strings before parsing
   - Or: Accept microseconds in format string and handle internally

2. **Implementation approach:**
   ```python
   # In mock_spark/functions.py
   def to_timestamp(col, format=None):
       # Pre-process format string to handle [.SSSSSS]
       # Strip microseconds from input if present and format expects them
       # Call DuckDB's strptime with cleaned format
   ```

3. **DuckDB compatibility:**
   - DuckDB's `strptime()` may need wrapper to handle flexible format strings
   - Consider preprocessing input strings to normalize format

4. **Testing requirements:**
   - Test with microseconds: `"2025-10-29T10:30:45.123456"`
   - Test without microseconds: `"2025-10-29T10:30:45"`
   - Test with nanoseconds: `"2025-10-29T10:30:45.123456789"`
   - Test PySpark format patterns: `"yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"`

---

## Issue 2: String Concatenation

### Problem

PySpark's `concat_ws()` and simple string `+` have different semantics:

```python
# PySpark - WORKS
F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
# OR
F.col("first_name") + F.lit(" ") + F.col("last_name")
```

Mock-spark errors:
```
Binder Error: No function matches the given name and argument types '+(VARCHAR, STRING_LITERAL)'
```

### Evidence from Tests

**Test:** `tests/builder_tests/test_healthcare_pipeline.py`
- **Line 113:** Had to replace `concat_ws` with `+` operator
- **Line 113:** Still requires type casting workarounds

**Impact:** String concatenation broken in 3/5 scenarios.

### Recommended Fix

**Priority:** HIGH  
**Effort:** Low (1-2 days)

1. **Add `concat_ws()` function:**
   ```python
   # In mock_spark/functions.py
   def concat_ws(sep, *cols):
       # Convert to DuckDB CONCAT_WS equivalent
       # Handle Column objects and literals
   ```

2. **Fix string `+` operator:**
   - Ensure proper type coercion for VARCHAR + LITERAL
   - Add explicit CAST operations in SQL generation
   - Or: Use DuckDB's string concatenation operator `||`

3. **Implementation:**
   ```python
   # When generating SQL for col + lit:
   # Instead of: col1 + ' '
   # Generate: col1 || ' '
   # Or: CONCAT(col1, ' ')
   ```

4. **Testing:**
   - `concat_ws(" ", col1, col2)`
   - `col1 + lit(" ") + col2`
   - `concat(col1, lit(" "), col2)`
   - Type coercion edge cases

---

## Issue 3: Boolean Expression Evaluation

### Problem

PySpark allows direct boolean operations that evaluate to boolean columns:
```python
# PySpark - WORKS
F.col("value") < F.col("min") | F.col("value") > F.col("max")  # Results in boolean
```

Mock-spark requires explicit `when()` chains:
```python
# Mock-spark - REQUIRED
F.when(F.col("value") < F.col("min"), True)
 .when(F.col("value") > F.col("max"), True)
 .otherwise(False)
```

### Evidence from Tests

**Test:** `tests/builder_tests/test_healthcare_pipeline.py`
- **Lines 162-176:** Complex boolean logic required `when()` chains
- **Line 237:** Boolean comparison needed explicit `when().otherwise()`

**Impact:** Affects any conditional logic creation.

### Recommended Fix

**Priority:** MEDIUM  
**Effort:** Medium (2-3 days)

1. **Enhance boolean operator support:**
   - Make `|` (OR) and `&` (AND) work on boolean columns in SQL generation
   - Convert to SQL: `(condition1) OR (condition2)`

2. **Automatic boolean column creation:**
   - When `col == value` is used in `withColumn()`, automatically create boolean
   - Detect boolean context (if used in aggregation with sum/count)

3. **Implementation:**
   ```python
   # In Column __or__ method:
   def __or__(self, other):
       # Check if both are boolean expressions
       # Generate: (left) OR (right) in SQL
       # Return Column with boolean type
   ```

4. **Testing:**
   - `(col1 < val1) | (col1 > val2)`
   - `(col1 == val1) & (col2 == val2)`
   - Boolean columns in aggregations
   - Boolean columns in filters

---

## Issue 4: Date Arithmetic

### Problem

PySpark's `datediff()` accepts strings and auto-converts:
```python
# PySpark - WORKS (strings auto-converted)
F.datediff(F.col("date1"), F.col("date2"))  # Works even if strings
```

Mock-spark requires explicit date conversion:
```python
# Mock-spark - REQUIRED
F.datediff(
    F.to_date(F.col("date1"), "yyyy-MM-dd"),
    F.to_date(F.col("date2"), "yyyy-MM-dd")
)
```

### Evidence from Tests

**Test:** `tests/builder_tests/test_healthcare_pipeline.py`
- **Lines 105-110:** Required explicit `to_date()` before `datediff()`
- **Test:** `tests/builder_tests/test_supply_chain_pipeline.py`
- **Lines 152-155:** Required date type conversion

**Impact:** All date calculations need extra steps.

### Recommended Fix

**Priority:** MEDIUM  
**Effort:** Low (1 day)

1. **Auto-convert string dates in `datediff()`:**
   - Detect if input columns are string type
   - Auto-apply `to_date()` if format can be inferred
   - Or: Accept format parameter in `datediff()`

2. **Implementation:**
   ```python
   def datediff(end_date, start_date, date_format=None):
       # If columns are string type and format provided:
       #   end_date = to_date(end_date, date_format)
       #   start_date = to_date(start_date, date_format)
       # Call DuckDB DATEDIFF
   ```

3. **Testing:**
   - `datediff()` with date types (should work now)
   - `datediff()` with string types (needs auto-conversion)
   - `datediff()` with mixed types
   - Format inference from common patterns

---

## Issue 5: Column Lifecycle Management

### Problem

Columns created in silver transforms aren't automatically available for gold transforms unless explicitly included in validation rules. This suggests columns aren't persisted correctly to intermediate tables.

**Error Pattern:**
```
Binder Error: Referenced column "test_date_parsed" not found in FROM clause!
Available columns: ['lab_id', 'patient_id', 'test_date', ...]
```

### Evidence from Tests

**Test:** `tests/builder_tests/test_healthcare_pipeline.py`
- Multiple "column not found" errors despite columns being created
- Required adding all created columns to `rules` dict to persist them

**Impact:** All complex multi-step pipelines fail.

### Recommended Fix

**Priority:** HIGH  
**Effort:** High (3-5 days)

1. **Automatic column persistence:**
   - Ensure all columns created in `withColumn()` are included in DataFrame schema
   - When writing to table, persist all columns (not just validated ones)
   - Fix schema inference from DataFrame operations

2. **Validation rules should filter, not restrict:**
   - Validation should filter rows, not filter columns in output
   - Current behavior: Only columns in `rules` are persisted
   - Expected: All columns from transform are persisted; rules validate data

3. **Implementation investigation needed:**
   - Check how `add_silver_transform()` handles column selection
   - Verify table write operations include all columns
   - Ensure CTEs don't drop columns unexpectedly

4. **Testing:**
   - Create column in silver transform
   - Use that column in gold transform
   - Verify column exists in intermediate table
   - Test with/without explicit validation rules

---

## Issue 6: CTE Optimization Failures

### Problem

Mock-spark's DuckDB backend fails to optimize complex CTEs, falling back to table-per-operation with warnings.

**Warning Pattern:**
```
CTE optimization failed, falling back to table-per-operation:
(duckdb.duckdb.ParserException) Parser Error: syntax error at or near "T"
```

### Evidence from Tests

All 6 failing mock-spark main pipeline tests involve complex CTE chains with 4-5 chained `withColumn()` operations.

### Recommended Fix

**Priority:** MEDIUM  
**Effort:** High (5-7 days, requires DuckDB expertise)

1. **Improve CTE SQL generation:**
   - Fix datetime format string issues in CTE generation
   - Handle complex nested transformations better
   - Optimize CTE dependency analysis

2. **Alternative: Better fallback strategy:**
   - If CTE optimization fails, use materialized views
   - Or: Break complex transforms into simpler steps automatically
   - Provide clear error messages with suggestions

3. **DuckDB compatibility:**
   - Review DuckDB version compatibility
   - Consider DuckDB-specific SQL optimizations
   - Test with latest DuckDB version

4. **Testing:**
   - Complex transforms with 5+ chained operations
   - Nested CTEs
   - Multiple column creations in sequence
   - Performance comparison: CTE vs. table-per-operation

---

## Issue 7: Type Coercion Edge Cases

### Problem

Various type coercion issues appear in edge cases, particularly with string operations and literal values.

### Evidence

- String concatenation type mismatches
- Numeric operations with mixed types
- Date/string conversions

### Recommended Fix

**Priority:** LOW  
**Effort:** Medium (2-3 days)

1. **Comprehensive type coercion:**
   - Implement PySpark-like implicit type conversions
   - Handle common type mismatches automatically
   - Add explicit CAST when needed

2. **Testing:**
   - All type combinations in common operations
   - Literal + Column operations
   - String + Numeric edge cases

---

## Implementation Roadmap

### Phase 1: Critical Fixes (Weeks 1-2)
**Goal:** Fix HIGH priority issues to achieve 90%+ compatibility

1. **Week 1:**
   - Fix datetime parsing (Issue #1)
   - Fix string concatenation (Issue #2)
   - Add comprehensive tests for both

2. **Week 2:**
   - Fix column lifecycle management (Issue #5)
   - Validate with test suite
   - Document workarounds for remaining issues

### Phase 2: Important Improvements (Weeks 3-4)
**Goal:** Address MEDIUM priority issues

3. **Week 3:**
   - Fix boolean expression evaluation (Issue #3)
   - Fix date arithmetic (Issue #4)
   - Expand test coverage

4. **Week 4:**
   - Improve CTE optimization (Issue #6)
   - Performance testing
   - Compatibility documentation

### Phase 3: Polish & Edge Cases (Week 5)
**Goal:** Handle LOW priority issues and edge cases

5. **Week 5:**
   - Type coercion improvements (Issue #7)
   - Edge case testing
   - Final compatibility validation

---

## Testing Strategy

### Test Suite

**Existing Tests to Fix:**
1. `tests/builder_tests/test_healthcare_pipeline.py::test_complete_healthcare_pipeline_execution`
2. `tests/builder_tests/test_supply_chain_pipeline.py::test_complete_supply_chain_pipeline_execution`
3. `tests/builder_tests/test_marketing_pipeline.py::test_complete_marketing_pipeline_execution`
4. `tests/builder_tests/test_data_quality_pipeline.py::test_complete_data_quality_pipeline_execution`
5. `tests/builder_tests/test_streaming_hybrid_pipeline.py::test_complete_streaming_hybrid_pipeline_execution`

### Compatibility Test Matrix

Create dedicated compatibility tests:

```python
# tests/compatibility/test_datetime_parsing.py
def test_to_timestamp_with_microseconds():
    """Test that to_timestamp handles microseconds like PySpark."""
    
def test_to_timestamp_without_microseconds():
    """Test that to_timestamp handles timestamps without microseconds."""

# tests/compatibility/test_string_concatenation.py
def test_concat_ws():
    """Test concat_ws compatibility."""
    
def test_string_plus_operator():
    """Test string + operator compatibility."""

# And so on...
```

### Regression Prevention

1. **Test both engines:**
   - Run same tests with PySpark and mock-spark
   - Compare results to ensure identical behavior

2. **Compatibility validation:**
   - Create test that runs identical code with both engines
   - Verify same results

3. **Performance benchmarks:**
   - Track mock-spark performance improvements
   - Ensure fixes don't degrade performance

---

## Success Criteria

### Phase 1 Success (Week 2)
- ✅ All datetime parsing tests pass without workarounds
- ✅ All string concatenation tests pass
- ✅ Column persistence works correctly
- ✅ 90%+ of realistic pipeline tests pass

### Phase 2 Success (Week 4)
- ✅ Boolean expressions work without explicit `when()` chains
- ✅ Date arithmetic works without explicit conversions
- ✅ CTE optimization warnings reduced by 80%
- ✅ 95%+ of realistic pipeline tests pass

### Phase 3 Success (Week 5)
- ✅ All type coercion edge cases handled
- ✅ Zero code changes required to swap engines
- ✅ 100% of incremental tests pass
- ✅ 95%+ of main pipeline tests pass

---

## Contributing Guidelines

### For Mock-Spark Contributions

1. **Issue Priority:**
   - Focus on HIGH priority issues first
   - Reference this document when submitting PRs

2. **Test Requirements:**
   - Include PySpark compatibility tests
   - Demonstrate identical behavior with PySpark
   - Show before/after code comparisons

3. **Documentation:**
   - Update compatibility notes
   - Document any remaining limitations
   - Provide migration examples

### For Pipeline-Builder Testing

1. **Test Suite Maintenance:**
   - Keep PySpark and mock-spark tests in sync
   - Document any engine-specific workarounds
   - Flag compatibility issues immediately

2. **Feedback Loop:**
   - Report failures to mock-spark team
   - Provide minimal reproducible examples
   - Track compatibility improvements

---

## Appendix A: Specific Failure Patterns

### Pattern 1: Datetime Parsing
```
Error: Parser Error: syntax error at or near "T"
Location: to_timestamp() with microseconds
Workaround: regexp_replace to strip microseconds first
Tests: All 5 realistic scenarios
```

### Pattern 2: String Operations
```
Error: No function matches '+(VARCHAR, STRING_LITERAL)'
Location: String concatenation operations
Workaround: Use explicit type casting or different approach
Tests: Healthcare, Supply Chain scenarios
```

### Pattern 3: Column Not Found
```
Error: Referenced column "X" not found in FROM clause
Location: Gold transforms reading from silver tables
Workaround: Add column to validation rules
Tests: All 6 failing main pipeline tests
```

### Pattern 4: Boolean Evaluation
```
Error: Column type mismatch or evaluation failure
Location: Boolean condition creation
Workaround: Use when().otherwise() chains
Tests: Healthcare, Supply Chain, Marketing scenarios
```

### Pattern 5: CTE Optimization
```
Warning: CTE optimization failed, falling back to table-per-operation
Location: Complex multi-step transforms
Impact: Performance degradation, sometimes failures
Tests: All 6 failing main pipeline tests
```

---

## Appendix B: Test Evidence Summary

### Test Results (as of 2025-10-29)

**Total Tests:** 68
- **Passing:** 60 (88%)
- **Failing:** 8 (12%)

**PySpark Tests:** 10/10 passing ✅
- All main pipeline tests pass
- All incremental tests pass
- 2 logging tests fail (Delta Lake config, non-blocking)

**Mock-Spark Tests:** 50/58 passing (86%)
- ✅ All 5 incremental tests pass
- ❌ 6 main pipeline tests fail (complex gold transforms)
- ❌ Some integration edge cases

### Failure Breakdown by Issue

1. **Column Lifecycle:** 6 failures (all main pipeline tests)
2. **Datetime Parsing:** Required in all 5 scenarios (workarounds present)
3. **CTE Optimization:** 6 warnings (performance impact)
4. **String Concatenation:** 3 scenarios require workarounds
5. **Boolean Evaluation:** 3 scenarios require workarounds
6. **Date Arithmetic:** 2 scenarios require workarounds

---

## References

- **Mock-Spark Repository:** [Link to mock-spark GitHub]
- **PySpark Documentation:** https://spark.apache.org/docs/latest/api/python/
- **DuckDB Documentation:** https://duckdb.org/docs/
- **Pipeline-Builder Tests:** `tests/builder_tests/` and `tests/builder_pyspark_tests/`

---

## Changelog

**2025-10-29 - v1.0**
- Initial plan document created
- Comprehensive issue analysis from 5 realistic scenarios
- 60 test passes, 8 failures documented
- Roadmap and success criteria defined

