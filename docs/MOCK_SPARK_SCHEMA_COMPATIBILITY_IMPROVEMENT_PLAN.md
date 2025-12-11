# Mock-Spark Schema Compatibility Improvement Plan

## Overview

This document outlines improvements for mock-spark to achieve full PySpark compatibility regarding schema handling and validation. These improvements are critical to ensure that tests passing with mock-spark will also pass with real PySpark in production environments.

**Status**: ✅ **IMPLEMENTED** in mock-spark 3.10.4  
**Priority**: HIGH  
**Impact**: Prevents false positives in tests that would fail in production  
**Implemented Version**: 3.10.4

---

## Problem Statement

Mock-spark currently exhibits lenient behavior in two critical areas where PySpark is strict:

1. **Empty DataFrame Schema Inference**: Mock-spark allows creating empty DataFrames with column name lists, while PySpark requires explicit schema definitions.
2. **Union Schema Compatibility**: Mock-spark allows unioning DataFrames with incompatible schemas, while PySpark strictly enforces schema compatibility.

These differences cause tests to pass with mock-spark but fail with real PySpark, creating false confidence in test results.

---

## Issue 1: Empty DataFrame Schema Inference

### Current Behavior

**Mock-spark:**
```python
# This succeeds with mock-spark
empty_df = spark.createDataFrame([], ['col1', 'col2'])
# Result: DataFrame with inferred StringType for all columns
```

**PySpark:**
```python
# This fails with PySpark
empty_df = spark.createDataFrame([], ['col1', 'col2'])
# Error: ValueError: can not infer schema from empty dataset
```

### Root Cause

Mock-spark's `createDataFrame` method accepts column name lists for empty DataFrames and automatically infers types (typically defaulting to `StringType`). PySpark's `createDataFrame` requires explicit schema when the dataset is empty because there's no data to infer types from.

### Impact

- Tests that create empty DataFrames with column name lists pass with mock-spark but fail with PySpark
- Developers may write code that works in tests but fails in production
- False confidence in edge case handling (empty data scenarios)

### Required Changes

#### 1. Update `createDataFrame` Method

**Location**: `mock_spark/session.py` (or equivalent)

**Current Implementation** (pseudo-code):
```python
def createDataFrame(self, data, schema=None):
    if not data and isinstance(schema, list):
        # Current: Infer types from column names
        inferred_schema = StructType([
            StructField(col, StringType(), True) for col in schema
        ])
        return DataFrame(data, inferred_schema)
    # ... rest of implementation
```

**Required Implementation**:
```python
def createDataFrame(self, data, schema=None):
    if not data:  # Empty dataset
        if schema is None:
            raise ValueError("can not infer schema from empty dataset")
        elif isinstance(schema, list):
            # PySpark behavior: Require explicit StructType for empty DataFrames
            raise ValueError(
                "can not infer schema from empty dataset. "
                "Please provide a StructType schema instead of a column name list."
            )
        elif isinstance(schema, StructType):
            # Valid: Explicit schema provided
            return DataFrame(data, schema)
        else:
            raise TypeError(f"schema must be StructType, got {type(schema)}")
    # ... rest of implementation for non-empty data
```

#### 2. Update Documentation

Add clear documentation that empty DataFrames require explicit `StructType` schemas, matching PySpark's behavior.

### Test Cases

```python
def test_empty_dataframe_with_column_names_raises_error():
    """Test that empty DataFrame creation with column names raises ValueError."""
    spark = SparkSession("test")
    
    # Should raise ValueError
    with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
        spark.createDataFrame([], ['col1', 'col2'])

def test_empty_dataframe_with_structtype_succeeds():
    """Test that empty DataFrame creation with StructType succeeds."""
    spark = SparkSession("test")
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", IntegerType(), True)
    ])
    
    df = spark.createDataFrame([], schema)
    assert df.columns == ['col1', 'col2']
    assert df.count() == 0

def test_empty_dataframe_with_none_schema_raises_error():
    """Test that empty DataFrame creation with None schema raises ValueError."""
    spark = SparkSession("test")
    
    with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
        spark.createDataFrame([], None)
```

### Migration Path

1. **Phase 1**: Add deprecation warning when column name list is used with empty data
   - Warning: "Creating empty DataFrame with column names is deprecated. Use StructType schema instead."
   - Still allow the operation but warn users

2. **Phase 2**: Change to error (breaking change)
   - Raise `ValueError` matching PySpark's exact error message
   - Update all internal mock-spark tests to use `StructType`

3. **Phase 3**: Update documentation and examples
   - All examples should use `StructType` for empty DataFrames

---

## Issue 2: Union Schema Compatibility

### Current Behavior

**Mock-spark:**
```python
# This succeeds with mock-spark (incorrectly)
df1 = spark.createDataFrame([('a', 1, 'x')], ['col1', 'col2', 'col3'])
df2 = spark.createDataFrame([('b',)], ['col1'])
result = df1.union(df2)  # Succeeds, but should fail
# Result: DataFrame with columns ['col1', 'col2', 'col3']
```

**PySpark:**
```python
# This fails with PySpark (correctly)
df1 = spark.createDataFrame([('a', 1, 'x')], ['col1', 'col2', 'col3'])
df2 = spark.createDataFrame([('b',)], ['col1'])
result = df1.union(df2)
# Error: AnalysisException: Union can only be performed on tables with the same 
# number of columns, but the first table has 3 columns and the second table has 1 columns
```

### Root Cause

Mock-spark's `union` method does not validate schema compatibility before performing the union operation. It may be padding missing columns or silently handling schema mismatches, which PySpark does not allow.

### Impact

- Tests that union DataFrames with different schemas pass with mock-spark but fail with PySpark
- Pipeline code that relies on union operations may work in tests but fail in production
- Schema validation issues are hidden during testing

### Required Changes

#### 1. Add Schema Validation to `union` Method

**Location**: `mock_spark/dataframe.py` (or equivalent)

**Current Implementation** (pseudo-code):
```python
def union(self, other):
    # Current: No schema validation
    return DataFrame(
        self._data + other._data,
        self._schema  # Uses first DataFrame's schema
    )
```

**Required Implementation**:
```python
def union(self, other):
    """
    Union this DataFrame with another DataFrame.
    
    Raises:
        AnalysisException: If DataFrames have incompatible schemas
    """
    # Validate schema compatibility
    self_schema = self.schema
    other_schema = other.schema
    
    # Check column count
    if len(self_schema.fields) != len(other_schema.fields):
        raise AnalysisException(
            f"Union can only be performed on tables with the same number of columns, "
            f"but the first table has {len(self_schema.fields)} columns and "
            f"the second table has {len(other_schema.fields)} columns"
        )
    
    # Check column types and names
    for i, (self_field, other_field) in enumerate(zip(self_schema.fields, other_schema.fields)):
        if self_field.name != other_field.name:
            raise AnalysisException(
                f"Union can only be performed on tables with compatible column names. "
                f"Column {i} name mismatch: '{self_field.name}' vs '{other_field.name}'"
            )
        
        # Type compatibility check (allowing some type promotions)
        if not _are_types_compatible(self_field.dataType, other_field.dataType):
            raise AnalysisException(
                f"Union can only be performed on tables with compatible column types. "
                f"Column '{self_field.name}' type mismatch: "
                f"{self_field.dataType} vs {other_field.dataType}"
            )
    
    # Perform union if schemas are compatible
    return DataFrame(
        self._data + other._data,
        self_schema
    )

def _are_types_compatible(type1, type2):
    """
    Check if two types are compatible for union operations.
    
    PySpark allows some type promotions (e.g., IntegerType -> LongType),
    but generally requires exact matches or compatible numeric types.
    """
    # Exact match
    if type1 == type2:
        return True
    
    # Numeric type compatibility (simplified - PySpark has more complex rules)
    numeric_types = (IntegerType, LongType, FloatType, DoubleType)
    if isinstance(type1, numeric_types) and isinstance(type2, numeric_types):
        # Allow numeric promotions (this is simplified - PySpark has specific rules)
        return True
    
    # String types are generally compatible
    if isinstance(type1, StringType) and isinstance(type2, StringType):
        return True
    
    return False
```

#### 2. Update `unionByName` Method

The `unionByName` method should also validate schema compatibility, but it allows column reordering. Ensure it validates:
- All columns from both DataFrames exist
- Column types are compatible
- Nullable flags are compatible (union result is nullable if either input is nullable)

#### 3. Add `unionAll` Alias

Ensure `unionAll` (alias for `union`) has the same validation.

### Test Cases

```python
def test_union_with_different_column_counts_raises_error():
    """Test that union with different column counts raises AnalysisException."""
    spark = SparkSession("test")
    
    df1 = spark.createDataFrame([('a', 1, 'x')], ['col1', 'col2', 'col3'])
    df2 = spark.createDataFrame([('b',)], ['col1'])
    
    with pytest.raises(AnalysisException, match="same number of columns"):
        df1.union(df2)

def test_union_with_different_column_names_raises_error():
    """Test that union with different column names raises AnalysisException."""
    spark = SparkSession("test")
    
    df1 = spark.createDataFrame([('a', 1)], ['col1', 'col2'])
    df2 = spark.createDataFrame([('b', 2)], ['col1', 'col3'])
    
    with pytest.raises(AnalysisException, match="compatible column names"):
        df1.union(df2)

def test_union_with_compatible_schemas_succeeds():
    """Test that union with compatible schemas succeeds."""
    spark = SparkSession("test")
    
    df1 = spark.createDataFrame([('a', 1)], ['col1', 'col2'])
    df2 = spark.createDataFrame([('b', 2)], ['col1', 'col2'])
    
    result = df1.union(df2)
    assert result.count() == 2
    assert result.columns == ['col1', 'col2']

def test_union_with_compatible_numeric_types_succeeds():
    """Test that union with compatible numeric types succeeds."""
    spark = SparkSession("test")
    from mock_spark.spark_types import IntegerType, LongType
    
    schema1 = StructType([
        StructField("value", IntegerType(), True)
    ])
    schema2 = StructType([
        StructField("value", LongType(), True)
    ])
    
    df1 = spark.createDataFrame([(1,)], schema1)
    df2 = spark.createDataFrame([(2,)], schema2)
    
    # Should succeed (numeric type promotion allowed)
    result = df1.union(df2)
    assert result.count() == 2
```

### Migration Path

1. **Phase 1**: Add validation with deprecation warning
   - Log warning when incompatible schemas are unioned
   - Still allow the operation but warn users

2. **Phase 2**: Change to error (breaking change)
   - Raise `AnalysisException` matching PySpark's exact error message
   - Update all internal mock-spark tests

3. **Phase 3**: Update documentation
   - Document schema compatibility requirements
   - Provide examples of compatible vs incompatible unions

---

## Issue 3: Validation Column Filtering Impact on Union

### Additional Context

While not directly a mock-spark issue, the interaction between validation column filtering and union operations reveals the importance of schema compatibility:

**Scenario:**
1. Two silver steps produce DataFrames with different validation rules
2. Validation filters columns based on rules (only columns with rules are kept)
3. Gold step tries to union the filtered DataFrames
4. If schemas don't match, union fails

**Example:**
```python
# Silver step 1: Rules for ['user_id', 'value', 'action'] -> 3 columns
# Silver step 2: Rules for ['user_id'] -> 1 column
# Gold step: Union of both -> FAILS (3 columns vs 1 column)
```

This is actually correct behavior (PySpark correctly rejects it), but mock-spark's lenient union allows it to pass, hiding the issue.

### Recommendation

The union schema validation fix (Issue 2) will automatically catch these cases, making tests fail early and forcing developers to fix validation rules.

---

## Implementation Priority

1. **HIGH**: Issue 2 (Union Schema Compatibility) - Most critical for catching real-world bugs
2. **HIGH**: Issue 1 (Empty DataFrame Schema) - Important for edge case testing
3. **MEDIUM**: Documentation updates and migration guides

---

## Testing Strategy

### Unit Tests

Create comprehensive unit tests for both issues:
- Test all error cases match PySpark's exact error messages
- Test all success cases work identically to PySpark
- Test edge cases (null schemas, empty schemas, etc.)

### Integration Tests

Test with real-world scenarios:
- Pipeline validation tests that create empty DataFrames
- Pipeline union operations with various schema combinations
- Cross-validation with PySpark to ensure identical behavior

### Backward Compatibility

- Provide migration guide for existing code
- Consider a compatibility flag (deprecated) for lenient behavior
- Clear deprecation timeline

---

## Expected Outcomes

After implementing these changes:

1. ✅ Tests that pass with mock-spark will pass with PySpark
2. ✅ Tests that fail with mock-spark will fail with PySpark (same errors)
3. ✅ Error messages match PySpark exactly
4. ✅ Developers catch schema issues during testing, not in production
5. ✅ Mock-spark becomes a reliable proxy for PySpark behavior

---

## Related Issues

- Schema evolution compatibility
- Catalog synchronization
- Aggregated DataFrame registration
- Type inference accuracy

---

## References

- PySpark Documentation: [DataFrame.union()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html)
- PySpark Source: `pyspark/sql/dataframe.py` - `union()` method
- PySpark Source: `pyspark/sql/session.py` - `createDataFrame()` method

---

## Contact

For questions or clarifications about this improvement plan, please contact the mock-spark development team or open an issue in the mock-spark repository.

