# Mock-Spark CTE Optimization Fix Plan

**Document Purpose**: Technical plan for fixing CTE optimization failures in mock-spark that prevent complex transforms with `withColumn()` from working correctly

**Mock-Spark Version**: 2.16.1 (current)  
**Target Version**: 2.17.0 (planned)  
**Date**: November 4, 2025  
**Issue**: CTE optimization failures prevent 6 builder tests from passing in sparkforge

## Executive Summary

Mock-spark's CTE (Common Table Expression) optimization is failing when transforms create new columns via `withColumn()`. The optimization attempts to combine multiple operations into a single SQL query using CTEs, but fails because:

1. **Column Visibility**: Columns created by `withColumn()` in earlier CTEs aren't visible to subsequent CTEs
2. **Type Inference**: String concatenation operations are incorrectly typed (attempts to cast strings to FLOAT)
3. **Fallback Limitations**: The fallback to table-per-operation still has issues with column references

**Impact**: 6 builder tests failing (0.6% of test suite), preventing complex pipeline transforms from working correctly in mock-spark mode.

**Solution**: Fix CTE optimization to properly handle column propagation, type inference, and sequential `withColumn()` operations.

---

## Problem Analysis

### Issue #1: Column Visibility in CTEs

**Error Pattern**:
```
Binder Error: Referenced column "risk_level" not found in FROM clause!
Candidate bindings: "diagnosis_date", "diagnosis_code", "diagnosis_name", "diagnosis_id"
```

**Root Cause**: When multiple `withColumn()` operations are chained:
```python
df.withColumn("risk_level", F.when(...).otherwise(...))
  .withColumn("is_chronic", F.when(F.col("status") == "chronic", True).otherwise(False))
```

The CTE optimization creates multiple CTEs, but columns created in earlier CTEs aren't visible in subsequent CTE contexts. The SQL generation looks like:
```sql
WITH temp_table_0 AS (
  SELECT ..., CASE WHEN ... END AS risk_level FROM ...
),
temp_table_1 AS (
  SELECT ..., risk_level, CASE WHEN ... END AS is_chronic 
  FROM temp_table_0  -- ❌ risk_level should be available but isn't
)
```

**Expected Behavior**: Columns created in earlier CTEs should be available in subsequent CTEs via proper table aliasing and column propagation.

### Issue #2: Type Conversion Errors

**Error Pattern**:
```
Conversion Error: Could not convert string 'Patient0 LastName0' to FLOAT when casting from source column full_name
```

**Root Cause**: When string concatenation is used:
```python
df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
```

DuckDB's type inference for the concatenation result is incorrect. The SQL generated:
```sql
((temp_table_0."first_name" || ' ') || last_name) AS full_name
```

DuckDB attempts to cast the result to FLOAT instead of STRING.

**Expected Behavior**: String concatenation operations should preserve STRING type, not be inferred as FLOAT.

### Issue #3: Missing Columns After Transform

**Error Pattern**:
```
Column 'diagnosis_date_parsed' not found. Available columns: ['diagnosis_id', 'patient_id', 'diagnosis_date', ...]
```

**Root Cause**: When transforms chain `withColumn()` followed by `drop()`:
```python
df.withColumn("test_date_clean", F.regexp_replace(...))
  .withColumn("test_date_parsed", F.to_timestamp(F.col("test_date_clean"), ...))
  .drop("test_date_clean")
```

The `drop()` operation removes the intermediate column, but when the result is used in subsequent operations, the schema isn't properly updated, causing "column not found" errors.

**Expected Behavior**: Schema should be updated after each operation (withColumn, drop) to reflect current column state.

---

## Proposed Solutions

### Solution #1: Fix CTE Column Propagation

**Location**: `mock_spark/dataframe/lazy.py` (CTE optimization logic)

**Changes**:
1. **Track Column Dependencies**: When building CTEs, maintain a mapping of columns available at each CTE level
2. **Proper Table Aliasing**: Ensure each CTE references the previous CTE with explicit table alias
3. **Column Visibility Check**: Before adding a column to a CTE, verify all referenced columns exist in the previous CTE's output

**Implementation Steps**:
```python
# Pseudo-code for fix
class CTEBuilder:
    def __init__(self):
        self.cte_columns = {}  # Map CTE name -> list of available columns
        self.cte_counter = 0
    
    def add_with_column(self, df, column_name, expression):
        # Get previous CTE name
        prev_cte = f"temp_table_{self.cte_counter - 1}" if self.cte_counter > 0 else "base_table"
        
        # Get available columns from previous CTE
        available_columns = self.cte_columns.get(prev_cte, df.columns)
        
        # Verify expression only references available columns
        referenced_columns = self._extract_column_references(expression)
        missing_columns = set(referenced_columns) - set(available_columns)
        if missing_columns:
            # Fall back to table-per-operation or raise clear error
            raise ColumnNotFoundException(f"Columns {missing_columns} not available in {prev_cte}")
        
        # Create new CTE with all previous columns + new column
        new_cte = f"temp_table_{self.cte_counter}"
        new_columns = available_columns + [column_name]
        self.cte_columns[new_cte] = new_columns
        
        # Generate SQL: SELECT ... FROM prev_cte
        sql = f"""
        WITH {new_cte} AS (
            SELECT {', '.join(available_columns)}, {expression} AS {column_name}
            FROM {prev_cte}
        )
        """
        self.cte_counter += 1
        return sql
```

**Files to Modify**:
- `mock_spark/dataframe/lazy.py` - CTE optimization logic
- `mock_spark/dataframe/column.py` - Column reference extraction

### Solution #2: Fix String Concatenation Type Inference

**Location**: `mock_spark/functions.py` or `mock_spark/dataframe/column.py` (type inference)

**Changes**:
1. **Explicit Type Casting**: When generating SQL for `concat()` operations, explicitly cast result as STRING
2. **Type Propagation**: Ensure concatenation operations always return STRING type, not inferred numeric

**Implementation Steps**:
```python
# In concat() function or column type inference
def concat(*cols):
    # Generate SQL with explicit STRING cast
    concat_expr = " || ".join([f"CAST({col.to_sql()} AS VARCHAR)" for col in cols])
    # Return column with explicit STRING type
    return Column(concat_expr, data_type=StringType())
```

**Alternative**: Fix DuckDB type inference by ensuring proper quoting and type hints in SQL generation.

**Files to Modify**:
- `mock_spark/functions/concat.py` or equivalent
- `mock_spark/dataframe/column.py` - Type inference logic
- SQL generation to include explicit type casts for string operations

### Solution #3: Fix Schema Tracking After Transform Operations

**Location**: `mock_spark/dataframe/lazy.py` (schema management)

**Changes**:
1. **Schema Update After Each Operation**: When `drop()`, `withColumn()`, or other schema-altering operations occur, immediately update the DataFrame's schema
2. **Column List Synchronization**: Ensure `df.columns` and the actual schema are always in sync

**Implementation Steps**:
```python
class LazyDataFrame:
    def withColumn(self, colName, col):
        # ... existing logic ...
        # After creating new DataFrame with column added:
        new_schema = self._update_schema_add_column(colName, col.infer_type())
        return LazyDataFrame(new_schema, ...)
    
    def drop(self, *cols):
        # ... existing logic ...
        # After dropping columns:
        new_schema = self._update_schema_remove_columns(cols)
        return LazyDataFrame(new_schema, ...)
    
    def _update_schema_remove_columns(self, cols_to_remove):
        """Update schema to reflect dropped columns."""
        remaining_fields = [
            field for field in self.schema.fields 
            if field.name not in cols_to_remove
        ]
        return StructType(remaining_fields)
```

**Files to Modify**:
- `mock_spark/dataframe/lazy.py` - Schema update methods
- `mock_spark/types.py` - Schema manipulation utilities

---

## Implementation Plan

### Phase 1: Column Visibility (Priority: High)

**Estimated Time**: 2-3 days

1. **Day 1**: Implement column dependency tracking in CTE builder
   - Add `CTEBuilder` class with column tracking
   - Modify CTE generation to include column availability checks
   - Add unit tests for column visibility

2. **Day 2**: Fix CTE SQL generation
   - Ensure proper table aliasing between CTEs
   - Verify all referenced columns exist before generating SQL
   - Add integration tests with complex transforms

3. **Day 3**: Testing and edge cases
   - Test with nested transforms (multiple withColumn chains)
   - Test with column references across CTEs
   - Fix any edge cases discovered

**Success Criteria**: 
- All 6 failing builder tests pass
- No CTE optimization warnings for column visibility

### Phase 2: Type Inference (Priority: Medium)

**Estimated Time**: 1-2 days

1. **Day 1**: Fix string concatenation type inference
   - Modify `concat()` function to return explicit STRING type
   - Add explicit CAST in SQL generation for string operations
   - Test with various string concatenation patterns

2. **Day 2**: Test and refine
   - Verify no more type conversion errors
   - Test edge cases (empty strings, NULL handling)
   - Performance check (no regression)

**Success Criteria**:
- No more "Conversion Error: Could not convert string to FLOAT" errors
- String operations consistently return STRING type

### Phase 3: Schema Tracking (Priority: Medium)

**Estimated Time**: 1 day

1. **Half Day**: Implement schema update logic
   - Add `_update_schema_add_column()` and `_update_schema_remove_columns()` methods
   - Ensure schema is updated after each transform operation
   - Test with drop() + withColumn() combinations

2. **Half Day**: Testing
   - Test schema consistency after transforms
   - Verify `df.columns` matches actual schema
   - Fix any inconsistencies

**Success Criteria**:
- Schema always matches current column state
- No "column not found" errors when columns should exist

### Phase 4: Integration and Testing (Priority: High)

**Estimated Time**: 1-2 days

1. **Day 1**: Full integration testing
   - Run sparkforge test suite against fixed mock-spark
   - Verify all 6 builder tests pass
   - Check for regressions in other tests

2. **Day 2**: Performance and edge cases
   - Benchmark CTE optimization vs. table-per-operation
   - Test with very large datasets
   - Test with complex nested transforms

**Success Criteria**:
- All sparkforge tests pass
- No performance regressions
- CTE optimization works correctly for all tested patterns

---

## Testing Strategy

### Unit Tests for Mock-Spark

1. **Column Visibility Tests**:
   ```python
   def test_cte_column_propagation():
       df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
       result = df.withColumn("col1", F.lit(1)).withColumn("col2", F.col("col1") + 1)
       assert "col2" in result.columns
       assert result.collect()[0]["col2"] == 2
   ```

2. **Type Inference Tests**:
   ```python
   def test_string_concatenation_type():
       df = spark.createDataFrame([("John", "Doe")], ["first", "last"])
       result = df.withColumn("full", F.concat(F.col("first"), F.lit(" "), F.col("last")))
       assert result.schema["full"].dataType == StringType()
       assert result.collect()[0]["full"] == "John Doe"
   ```

3. **Schema Tracking Tests**:
   ```python
   def test_drop_and_with_column():
       df = spark.createDataFrame([(1, "a", "b")], ["id", "col1", "col2"])
       result = df.drop("col1").withColumn("new_col", F.col("id") * 2)
       assert "col1" not in result.columns
       assert "new_col" in result.columns
       assert "col2" in result.columns
   ```

### Integration Tests with SparkForge

Run the 6 failing builder tests:
1. `test_data_quality_pipeline.py::test_complete_data_quality_pipeline_execution`
2. `test_healthcare_pipeline.py::test_complete_healthcare_pipeline_execution`
3. `test_iot_pipeline.py::test_anomaly_detection_pipeline`
4. `test_marketing_pipeline.py::test_complete_marketing_pipeline_execution`
5. `test_streaming_hybrid_pipeline.py::test_complete_streaming_hybrid_pipeline_execution`
6. `test_supply_chain_pipeline.py::test_complete_supply_chain_pipeline_execution`

### Regression Tests

Ensure existing functionality still works:
- Basic DataFrame operations
- Simple transforms without CTE optimization
- Table-per-operation fallback
- Other mock-spark features

---

## Code Locations (Mock-Spark Repository)

Based on error messages and mock-spark structure, changes will likely be needed in:

1. **`mock_spark/dataframe/lazy.py`**:
   - Lines ~150-200: CTE optimization logic
   - Materialization and SQL generation

2. **`mock_spark/functions/__init__.py` or function-specific files**:
   - `concat()` function implementation
   - Type inference for string operations

3. **`mock_spark/dataframe/column.py`**:
   - Column reference extraction
   - Type inference logic

4. **`mock_spark/types.py`**:
   - Schema manipulation utilities
   - Type system

---

## Risk Assessment

### Low Risk Changes
- Adding explicit type casts for string operations (well-isolated change)
- Schema update methods (additive, doesn't break existing code)

### Medium Risk Changes
- Column dependency tracking (could affect existing CTE optimization paths)
- CTE SQL generation changes (could break edge cases)

### Mitigation Strategies
1. **Feature Flag**: Add a flag to enable/disable new CTE optimization (default: enabled)
2. **Fallback**: Always fall back to table-per-operation if CTE optimization fails
3. **Comprehensive Testing**: Test against sparkforge's full test suite before release
4. **Version Bump**: Release as 2.17.0 (minor version) to allow rollback if issues occur

---

## Success Metrics

### Primary Metrics
- ✅ All 6 failing sparkforge builder tests pass
- ✅ No CTE optimization warnings for column visibility
- ✅ Zero type conversion errors for string operations
- ✅ Schema consistency: `df.columns` always matches actual schema

### Secondary Metrics
- Performance: CTE optimization should be faster than table-per-operation (when it works)
- Test Coverage: 90%+ coverage for new CTE optimization code
- Documentation: Update mock-spark docs with CTE optimization limitations/features

---

## Timeline

**Total Estimated Time**: 5-8 days

- **Week 1**: Phases 1-2 (Column Visibility + Type Inference) - 4-5 days
- **Week 2**: Phases 3-4 (Schema Tracking + Integration) - 2-3 days

**Target Release**: mock-spark 2.17.0 by end of November 2025

---

## Open Questions

1. **Performance Impact**: Will the new column tracking add significant overhead? (Likely minimal, but should benchmark)
2. **Backward Compatibility**: Do any existing tests rely on the current (broken) CTE behavior? (Should check mock-spark's test suite)
3. **DuckDB Version**: Are type inference issues specific to certain DuckDB versions? (May need to test across versions)
4. **Alternative Approaches**: Should we consider disabling CTE optimization entirely and always use table-per-operation? (Not recommended - loses performance benefit)

---

## References

- **SparkForge Test Failures**: See `MOCK_SPARK_IMPROVEMENTS.md` for detailed failure analysis
- **Mock-Spark CTE Warnings**: UserWarning messages indicate when CTE optimization fails
- **DuckDB Documentation**: Type system and SQL generation guidelines
- **PySpark Behavior**: Reference implementation for correct transform behavior

---

## Contact

For questions or collaboration on this fix, please contact:
- SparkForge maintainers (via GitHub issues)
- Mock-spark maintainers (if separate project)

**Test Suite Available**: Full sparkforge test suite (1,542 tests) available for validation
