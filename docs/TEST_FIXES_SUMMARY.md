# Test Files Fixed for Mock-Spark vs Real PySpark Compatibility

## Summary

Fixed **15 test files** to work with both mock-spark and real PySpark by replacing hardcoded mock_spark imports with conditional imports based on `SPARK_MODE` environment variable.

## Fixed Files

1. `tests/unit/test_validation.py` - Fixed module-level and fixture-level type imports
2. `tests/unit/test_execution_final_coverage.py` - Fixed hardcoded type imports
3. `tests/unit/test_writer_core_simple.py` - Fixed type imports and exception handling, added conditional logic for `.storage` API
4. `tests/unit/test_execution_engine_simple.py` - Fixed AnalysisException import and inline type imports
5. `tests/unit/test_trap_1_silent_exception_handling.py` - Fixed inline type imports
6. `tests/unit/test_execution_comprehensive.py` - Fixed module-level type imports
7. `tests/unit/test_execution_100_coverage.py` - Fixed module-level type imports
8. `tests/unit/test_validation_simple.py` - Fixed module-level type imports
9. `tests/unit/test_validation_enhanced_simple.py` - Fixed module-level type imports
10. `tests/unit/test_validation_standalone.py` - Fixed module-level type imports
11. `tests/unit/test_writer_comprehensive.py` - Fixed module-level type imports
12. `tests/unit/test_edge_cases.py` - Fixed type and exception imports
13. `tests/unit/test_validation_enhanced.py` - Fixed module-level type imports
14. `tests/unit/test_validation_mock.py` - Fixed module-level type imports
15. `tests/unit/test_sparkforge_working.py` - Fixed module-level type imports

## Pattern Applied

All files now follow this pattern:

```python
import os

# Import types based on SPARK_MODE
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.types import StructType, StructField, StringType, ...
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark import StructType, StructField, StringType, ...
    from mock_spark.errors import AnalysisException
```

## Key Issues Fixed

1. **Type Compatibility**: PySpark's `createDataFrame()` is strict about type objects and won't accept mock-spark types when running with real PySpark
2. **Exception Types**: Different exception class names between mock-spark and PySpark (AnalysisException, etc.)
3. **Mock-Specific APIs**: Tests using `.storage` API now have conditional logic or skip in real mode

## Files Already Compatible

These files already had conditional imports based on SPARK_MODE:
- `test_execution_write_mode.py`
- `test_pipeline_runner_write_mode.py`
- `test_bronze_rules_column_validation.py`
- `test_trap_4_broad_exception_catching.py`
- `test_validation_additional_coverage.py`
- `test_pipeline_builder_basic.py`
- `test_models_simple.py`
- `test_models_new.py`
- `test_trap_2_missing_object_creation.py`

## Testing

All fixed files should now work with:
- `SPARK_MODE=mock` (mock-spark - default)
- `SPARK_MODE=real` (real PySpark + Delta Lake)

## Root Causes Identified

### 1. Hardcoded Type Imports
**Problem:** Tests imported types from `mock_spark` without checking `SPARK_MODE`

**Example:**
```python
# ❌ Wrong
from mock_spark import StructType, StructField, StringType
```

**Solution:**
```python
# ✅ Correct
import os
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.types import StructType, StructField, StringType
else:
    from mock_spark import StructType, StructField, StringType
```

### 2. Type Incompatibility
**Problem:** PySpark's `createDataFrame()` won't accept mock-spark type objects when running with real PySpark

**Solution:** Always use types that match the `spark_session` type

### 3. Exception Type Differences
**Problem:** Exception classes are in different modules

**Solution:**
```python
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark.errors import AnalysisException
```

### 4. Mock-Specific APIs
**Problem:** Mock-spark has APIs like `.storage` that don't exist in real PySpark

**Solution:** Use conditional logic:
```python
if spark_mode == "real":
    # Use PySpark SQL commands
    spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")
else:
    # Use mock-spark storage API
    spark_session.storage.create_schema("test_schema")
```

## Next Steps

1. Run all tests with both `SPARK_MODE=mock` and `SPARK_MODE=real` to verify fixes
2. Monitor for any remaining failures and fix them using the same patterns
3. Update test documentation to reflect these patterns

## Related Documentation

See also:
- `docs/MOCK_VS_REAL_SPARK_DIFFERENCES.md` - Detailed guide on differences between mock-spark and real PySpark

