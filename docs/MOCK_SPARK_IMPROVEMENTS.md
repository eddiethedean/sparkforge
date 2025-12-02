# Mock-Spark Usage Analysis and Improvement Recommendations

> **Note**: With mock-spark 3.7+, many of the limitations described in this document have been fixed. The threading issues were resolved by switching from DuckDB to Polars backend, and tuple-to-dict conversion is no longer needed. This document is kept for historical reference and to document the improvements that were made.

## Executive Summary

After examining mock-spark usage across the codebase (tests and source), we've identified several areas where mock-spark could be used more effectively. The main issues are:

1. **Duplicated monkey-patching logic** for `createDataFrame` tuple handling
2. **Bypassing the compat layer** in tests (direct `mock_spark.functions` imports)
3. **Inconsistent engine detection** (hasattr checks vs `is_mock_spark()`)
4. **Type checking issues** (isinstance vs hasattr for DataFrame compatibility)
5. **Schema handling differences** not abstracted properly

## Current State Analysis

### 1. Monkey-Patching createDataFrame

**Location:** `tests/conftest.py` (3 duplicate implementations)

**Issue:** The same tuple-to-dict conversion logic is duplicated in:
- `_create_mock_spark_session()`
- `isolated_spark_session()` fixture
- `mock_spark_session()` fixture

**Current Code Pattern:**
```python
def createDataFrame_wrapper(data, schema=None, **kwargs):
    """Wrapper to convert tuples to dicts when schema is provided."""
    if schema is not None and data and isinstance(data, list) and len(data) > 0:
        if isinstance(data[0], tuple):
            # Complex logic to extract column names...
            data = [dict(zip(column_names, row)) for row in data]
    return original_createDataFrame(data, schema)
```

**Problems:**
- Code duplication (3x)
- Complex logic scattered across fixtures
- Hard to maintain and test
- Not reusable in source code

### 2. Direct mock_spark.functions Imports in Tests

**Location:** `tests/unit/test_validation.py` (10+ occurrences)

**Issue:** Tests directly import `mock_spark.functions` instead of using the compat layer:

```python
# Current (bypasses compat layer)
import mock_spark.functions as mock_functions
functions = mock_functions

# Should use
from pipeline_builder.functions import get_default_functions
functions = get_default_functions()
```

**Problems:**
- Bypasses the abstraction layer
- Tests don't verify compat layer works correctly
- Harder to switch between engines
- Inconsistent with source code patterns

### 3. Inconsistent Engine Detection

**Location:** Multiple files

**Current Patterns:**
```python
# Pattern 1: hasattr check (fragile)
is_pyspark = hasattr(spark, 'sparkContext') and hasattr(spark.sparkContext, '_jsc')

# Pattern 2: Using compat layer (better)
from pipeline_builder.compat import is_mock_spark
if is_mock_spark():
    # mock-spark specific code
```

**Problems:**
- `hasattr` checks are fragile and can break
- Not using the centralized `is_mock_spark()` function
- Inconsistent detection logic across codebase

### 4. Type Checking Issues

**Location:** `tests/integration/test_step_execution.py`, `pipeline_builder/pipeline/runner.py`

**Issue:** `isinstance(..., DataFrame)` checks fail because mock-spark DataFrames don't match PySpark DataFrame types.

**Current Workaround:**
```python
# Fragile workaround
assert hasattr(context["bronze_data"], "count")
```

**Problems:**
- Duck typing instead of proper type checking
- Less clear intent
- Potential runtime errors if object doesn't have expected methods

### 5. Schema Handling Differences

**Location:** `tests/integration/test_parallel_execution.py`

**Issue:** PySpark and mock-spark handle `createDataFrame` with schemas differently:
- PySpark: Prefers tuple data with list schema
- Mock-spark: Prefers dict data with StructType

**Current Pattern:**
```python
is_pyspark = hasattr(spark, 'sparkContext') and hasattr(spark.sparkContext, '_jsc')
if is_pyspark:
    data = [(i, name, value, timestamp)]  # Tuples
    schema = ["id", "name", "value", "timestamp"]  # List
else:
    data = [{"id": i, "name": name, ...}]  # Dicts
    schema = StructType([...])  # StructType
```

**Problems:**
- Duplicated logic in multiple places
- Not abstracted into a helper function
- Hard to maintain consistency

## Recommendations

### 1. Centralize createDataFrame Tuple Handling

**Create:** `pipeline_builder/compat_helpers.py`

```python
def create_dataframe_compat(spark: SparkSession, data, schema=None):
    """
    Create DataFrame with compatibility for both PySpark and mock-spark.
    
    Handles tuple-to-dict conversion automatically for mock-spark.
    """
    from .compat import is_mock_spark
    
    if is_mock_spark() and schema is not None and data:
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], tuple):
            # Convert tuples to dicts for mock-spark
            if isinstance(schema, list):
                column_names = schema
            elif hasattr(schema, 'fieldNames'):
                column_names = schema.fieldNames()
            elif hasattr(schema, 'fields'):
                column_names = [field.name for field in schema.fields]
            else:
                column_names = None
            
            if column_names:
                data = [dict(zip(column_names, row)) for row in data]
    
    return spark.createDataFrame(data, schema)
```

**Benefits:**
- Single source of truth
- Reusable in both tests and source code
- Easier to test and maintain
- Consistent behavior

### 2. Use Compat Layer Consistently in Tests

**Refactor:** All test files to use `get_default_functions()` instead of direct imports

**Before:**
```python
import mock_spark.functions as mock_functions
functions = mock_functions
```

**After:**
```python
from pipeline_builder.functions import get_default_functions
functions = get_default_functions()
```

**Benefits:**
- Tests verify compat layer works
- Easier to switch engines
- Consistent with source code
- Better abstraction

### 3. Standardize Engine Detection

**Create:** Helper function in `pipeline_builder/compat.py`

```python
def detect_spark_type(spark: SparkSession) -> str:
    """
    Detect if spark session is PySpark or mock-spark.
    
    Returns: 'pyspark' or 'mock'
    """
    if is_mock_spark():
        return 'mock'
    if hasattr(spark, 'sparkContext') and hasattr(spark.sparkContext, '_jsc'):
        return 'pyspark'
    # Fallback detection
    return 'unknown'
```

**Refactor:** Replace all `hasattr` checks with this function

**Benefits:**
- Centralized detection logic
- More reliable
- Easier to update if detection needs change

### 4. Improve DataFrame Type Checking

**Create:** Protocol-based type checking

```python
from typing import Protocol

class DataFrameLike(Protocol):
    """Protocol for DataFrame-like objects."""
    def count(self) -> int: ...
    def columns(self) -> list[str]: ...
    def filter(self, condition): ...
    # Add other common methods
```

**Use:**
```python
def is_dataframe_like(obj) -> bool:
    """Check if object is DataFrame-like."""
    return hasattr(obj, 'count') and hasattr(obj, 'columns') and hasattr(obj, 'filter')
```

**Benefits:**
- Clear intent
- Type-safe
- Works with both PySpark and mock-spark
- Better IDE support

### 5. Abstract Schema Handling

**Create:** Helper function for creating test DataFrames

```python
def create_test_dataframe(spark: SparkSession, data, schema):
    """
    Create DataFrame with automatic schema/data format handling.
    
    Automatically converts between tuple/list and dict formats
    based on the engine being used.
    """
    from .compat import is_mock_spark, create_dataframe_compat
    
    return create_dataframe_compat(spark, data, schema)
```

**Benefits:**
- Single function for all DataFrame creation
- Handles differences automatically
- Consistent API

### 6. Enhance Compat Layer

**Add to:** `pipeline_builder/compat.py`

```python
def get_functions_from_session(spark: SparkSession) -> FunctionsProtocol:
    """
    Get functions object from a spark session.
    
    This ensures we get the right functions object (PySpark or mock-spark)
    that matches the session.
    """
    if is_mock_spark():
        try:
            import mock_spark.functions as mock_functions
            return mock_functions
        except ImportError:
            pass
    return get_default_functions()
```

**Benefits:**
- Ensures functions match the session
- No manual detection needed
- Works in both tests and source code

## Implementation Priority

### High Priority (Immediate Benefits)

1. **Centralize createDataFrame tuple handling** - Eliminates duplication
2. **Use compat layer in tests** - Better abstraction, easier maintenance
3. **Standardize engine detection** - More reliable, consistent

### Medium Priority (Quality Improvements)

4. **Improve DataFrame type checking** - Better type safety
5. **Abstract schema handling** - Cleaner API

### Low Priority (Nice to Have)

6. **Enhance compat layer** - Additional convenience functions

## Migration Path

1. **Phase 1:** Create helper functions in `compat_helpers.py`
2. **Phase 2:** Refactor tests to use helpers and compat layer
3. **Phase 3:** Update source code to use helpers
4. **Phase 4:** Remove duplicate monkey-patching code
5. **Phase 5:** Add comprehensive tests for compat helpers

## Testing Strategy

- Create unit tests for all compat helpers
- Test with both PySpark and mock-spark
- Verify backward compatibility
- Ensure no performance regression

## Expected Benefits

1. **Reduced Code Duplication:** ~200 lines of duplicate code eliminated
2. **Better Maintainability:** Single source of truth for compatibility logic
3. **Improved Test Quality:** Tests verify compat layer works correctly
4. **Easier Engine Switching:** No code changes needed to switch engines
5. **Better Type Safety:** Protocol-based type checking
6. **Cleaner API:** Abstracted differences between engines

## Conclusion

Mock-spark is being used effectively, but there are opportunities to:
- Reduce duplication
- Improve abstraction
- Standardize patterns
- Enhance type safety

The recommended changes will make the codebase more maintainable and easier to work with, while maintaining full backward compatibility.

