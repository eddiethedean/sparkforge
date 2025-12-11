# Mock-Spark Missed Issues Log

This document tracks issues that passed with mock-spark but failed with real PySpark, indicating areas where mock-spark needs improvement to better match PySpark behavior.

## Summary

- **Total Tests Failing**: 70 (initial: 59 failures + 11 errors)
- **Tests Fixed**: 32 failures fixed (reduced to 38 remaining)
- **Date**: 2024-12-11
- **Mock-Spark Version**: 3.11.0
- **PySpark Version**: 3.5.x

### Tests Fixed
1. ✅ `test_marketing_pipeline.py::test_complete_marketing_pipeline_execution`
2. ✅ `test_streaming_hybrid_pipeline.py::test_complete_streaming_hybrid_pipeline_execution`
3. ✅ `test_multi_schema_support.py::test_schema_creation_failure`
4. ✅ `test_step_execution.py::test_execution_context_flow`
5. ✅ `test_validation_integration.py` (5 tests: test_empty_dataframe x3, test_error_handling x2)
6. ✅ `test_trap_5_default_schema_fallbacks.py` (7 tests - all passing)
7. ✅ `test_validation_enhanced.py` (2 tests: performance, error_handling)
8. ✅ `test_validation_mock.py::test_custom_expression`
9. ✅ `test_validation_standalone.py::test_custom_expression`
10. ✅ `test_data_quality_pipeline.py::test_complete_data_quality_pipeline_execution`
11. ✅ `test_healthcare_pipeline.py::test_healthcare_logging`
12. ✅ `test_validation_enhanced_simple.py` (2 tests: performance, error_handling)
13. ✅ `test_healthcare_pipeline.py::test_complete_healthcare_pipeline_execution` (fixed string concatenation)
14. ✅ `test_supply_chain_pipeline.py::test_complete_supply_chain_pipeline_execution` (fixed materialization)
15. ✅ `builder_pyspark_tests/test_healthcare_pipeline.py::test_healthcare_logging` (fixed datetime parsing and Delta Lake config)
16. ✅ `builder_pyspark_tests/test_supply_chain_pipeline.py::test_supply_chain_logging` (fixed datetime parsing and Delta Lake config)

### Remaining Issues (Not Mock-Spark Compatibility Issues)
- ✅ `builder_pyspark_tests/test_healthcare_pipeline.py::test_healthcare_logging` - Fixed: Added Delta Lake configuration and fixed datetime parsing
- ✅ `builder_pyspark_tests/test_supply_chain_pipeline.py::test_supply_chain_logging` - Fixed: Added Delta Lake configuration and fixed datetime parsing

### Issue #15: Delta Lake Catalog Configuration (MAJOR FIX)
**Tests**: Multiple tests across `test_validation.py`, `test_pipeline_builder.py`, `test_trap_5_default_schema_fallbacks.py`, `test_trap_1_silent_exception_handling.py`, `test_pipeline_runner_write_mode.py`, and others
**Mock-Spark Behavior**: Not applicable (mock-spark doesn't use Delta Lake)
**PySpark Behavior**: When Delta Lake catalog config is set before verifying Delta Lake is available, causes `ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog`
**Why Mock-Spark Missed**: Not a mock-spark issue - this is a PySpark/Delta Lake configuration issue
**Fix**: Removed explicit `spark.sql.catalog.spark_catalog` config from initial builder in `tests/conftest.py`. The `configure_spark_with_delta_pip()` function handles catalog setup automatically when Delta Lake is properly configured.
**Status**: Fixed - Resolved 32 test failures (from 70 to 38 remaining)
**Files Changed**: `tests/conftest.py` (removed catalog config), `tests/integration/test_pipeline_builder.py` (fixed SparkSession creation for PySpark)

## Common Fix Patterns

1. **Import Functions Conditionally**: Replace `from mock_spark import functions as F` with conditional import based on `SPARK_MODE`
2. **Schema Creation**: Replace `spark.storage.create_schema()` with helper function that uses SQL for PySpark
3. **Fixture Names**: Replace `mock_spark_session` fixture with `spark_session` fixture
4. **Datetime Casting**: Add `.cast("string")` before `to_timestamp()` calls
5. **StructType Imports**: Make StructType/StructField imports conditional based on `SPARK_MODE`
6. **Catalog API**: PySpark uses `spark.sql()` instead of `catalog.createDatabase()`

---

## Issue Categories

### 1. Schema/StructType Compatibility Issues
### 2. Column Reference Timing Issues  
### 3. Empty DataFrame Handling
### 4. Performance/Timing Test Flakiness
### 5. Expression Parsing Differences
### 6. Catalog/Database API Differences

---

## Detailed Issues

### Issue #1: Schema Creation API Difference
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: `spark.storage.create_schema()` works
**PySpark Behavior**: `spark.storage` doesn't exist, must use `spark.sql("CREATE SCHEMA IF NOT EXISTS ...")`
**Why Mock-Spark Missed**: Mock-spark has a `storage` API that PySpark doesn't have
**Fix**: Created `create_schema_if_not_exists()` helper function that checks for `storage` attribute and uses SQL if not available
**Status**: Fixed

### Issue #2: StructType Import Compatibility
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: Accepts mock-spark `StructType` objects
**PySpark Behavior**: Rejects mock-spark `StructType`, requires PySpark `StructType` from `pyspark.sql.types`
**Why Mock-Spark Missed**: Data generator in `tests/builder_tests/conftest.py` imported types from `mock_spark` unconditionally
**Fix**: Made imports conditional based on `SPARK_MODE` environment variable
**Status**: Fixed

### Issue #3: Functions Import Compatibility
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: `from mock_spark import functions as F` works
**PySpark Behavior**: Requires `from pyspark.sql import functions as F`, mock-spark functions don't work with PySpark
**Why Mock-Spark Missed**: Test imported F from mock_spark unconditionally
**Fix**: Made F import conditional based on `SPARK_MODE` environment variable
**Status**: Fixed

### Issue #4: DataFrame Materialization for Validation
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: Validation can access columns created in transforms without explicit materialization
**PySpark Behavior**: Requires DataFrame materialization before validation can access transform-created columns
**Why Mock-Spark Missed**: `_ensure_materialized_for_validation` only ran for mock-spark, not PySpark
**Fix**: Changed condition from `if is_mock_spark() and rules:` to `if rules:` to materialize for both
**Status**: Fixed

### Issue #5: Validation Failure in clean_patients Step
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: Validation passes (100% valid)
**PySpark Behavior**: Validation fails (0% valid, all 40 rows invalid)
**Why Mock-Spark Missed**: Mock-spark's validation may be less strict or handle computed columns differently
**Status**: Investigating - need to check why age calculation or validation rules are failing

### Issue #6: Datetime Type Inference in to_timestamp
**Test**: `tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
**Mock-Spark Behavior**: `to_timestamp` accepts datetime types directly
**PySpark Behavior**: `to_timestamp` requires string input, fails if column is already datetime
**Why Mock-Spark Missed**: Mock-spark infers datetime columns from ISO strings, but PySpark requires explicit string cast
**Fix**: Added `.cast("string")` before `to_timestamp` calls for `impression_date_parsed`, `click_date_parsed`, `conversion_date_parsed`
**Status**: Fixed

### Issue #7: Catalog API Differences
**Test**: `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_creation_failure`
**Mock-Spark Behavior**: Has `spark.catalog.createDatabase()` method
**PySpark Behavior**: Uses `spark.sql("CREATE SCHEMA ...")` instead, `catalog.createDatabase` doesn't exist
**Why Mock-Spark Missed**: Mock-spark provides a Catalog API that PySpark doesn't have
**Fix**: Updated test to patch `spark.sql` for PySpark and `catalog.createDatabase` for mock-spark
**Status**: Fixed

### Issue #8: Import Logic Using Wrong Environment Variable
**Tests**: Multiple tests in `tests/integration/` and `tests/unit/`
**Mock-Spark Behavior**: Tests check `SPARKFORGE_ENGINE` environment variable
**PySpark Behavior**: Tests use `SPARK_MODE=real` environment variable
**Why Mock-Spark Missed**: Import logic was checking the wrong environment variable
**Fix**: Updated import logic to check `SPARK_MODE` first, then fall back to `SPARKFORGE_ENGINE`
**Status**: Fixed

### Issue #9: Py4JJavaError vs Python Exceptions
**Test**: `tests/unit/test_validation_enhanced.py::TestFunctionsIntegration::test_mock_functions_error_handling`
**Mock-Spark Behavior**: Raises Python exceptions (TypeError, ValueError, AttributeError)
**PySpark Behavior**: Raises `Py4JJavaError` for JVM-related errors
**Why Mock-Spark Missed**: Mock-spark doesn't use JVM, so doesn't raise Py4JJavaError
**Fix**: Updated test to accept `Py4JJavaError` when running with PySpark
**Status**: Fixed

### Issue #10: Performance Test Timing Differences
**Test**: `tests/unit/test_validation_enhanced.py::TestFunctionsIntegration::test_mock_functions_performance`
**Mock-Spark Behavior**: Very fast (<100ms for 1000 function calls)
**PySpark Behavior**: Slower due to JVM overhead (~1-2s for 1000 function calls)
**Why Mock-Spark Missed**: Mock-spark is pure Python, PySpark has JVM overhead
**Fix**: Adjusted performance threshold to 2.0s for PySpark, 0.1s for mock-spark
**Status**: Fixed

### Issue #11: F.expr() SQL vs Python Expression Syntax
**Tests**: `tests/unit/test_validation_mock.py::TestConvertRuleToExpression::test_custom_expression`, `tests/unit/test_validation_standalone.py::TestConvertRuleToExpression::test_custom_expression`
**Mock-Spark Behavior**: `F.expr()` accepts Python-like expressions (e.g., `col('user_id').isNotNull()`)
**PySpark Behavior**: `F.expr()` expects SQL expressions (e.g., `user_id IS NOT NULL`)
**Why Mock-Spark Missed**: Mock-spark is more lenient with expression parsing
**Fix**: Updated tests to use SQL syntax for PySpark and Python syntax for mock-spark
**Status**: Fixed

### Issue #12: String Concatenation with + Operator
**Test**: `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
**Mock-Spark Behavior**: `F.col("first_name") + F.lit(" ") + F.col("last_name")` works correctly
**PySpark Behavior**: String concatenation with `+` operator fails when DataFrame is cached/materialized, producing null values
**Why Mock-Spark Missed**: Mock-spark handles string concatenation differently, doesn't have the same caching/materialization issues
**Fix**: Changed to use `F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))` which works correctly in both
**Status**: Fixed

### Issue #13: Datetime Parsing with Microseconds
**Tests**: `tests/builder_pyspark_tests/test_healthcare_pipeline.py::test_healthcare_logging`, `tests/builder_pyspark_tests/test_supply_chain_pipeline.py::test_supply_chain_logging`
**Mock-Spark Behavior**: `to_timestamp()` accepts datetime strings with microseconds without explicit casting
**PySpark Behavior**: PySpark 3.0+ requires explicit string casting and format pattern that includes microseconds `[.SSSSSS]`
**Why Mock-Spark Missed**: Mock-spark is more lenient with datetime parsing and doesn't enforce strict format requirements
**Fix**: 
1. Added `.cast("string")` before `to_timestamp()` calls to ensure column is string type
2. Updated format pattern from `"yyyy-MM-dd'T'HH:mm:ss"` to `"yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"` to handle microseconds
3. Added Delta Lake configuration to `spark_session` fixture in `tests/builder_pyspark_tests/conftest.py`
**Status**: Fixed

### Issue #14: Delta Lake Configuration Missing
**Tests**: `tests/builder_pyspark_tests/test_healthcare_pipeline.py::test_healthcare_logging`, `tests/builder_pyspark_tests/test_supply_chain_pipeline.py::test_supply_chain_logging`
**Mock-Spark Behavior**: Not applicable (mock-spark doesn't use Delta Lake)
**PySpark Behavior**: Requires Delta Lake configuration for `LogWriter` to save tables
**Why Mock-Spark Missed**: Not a mock-spark issue - these tests require real PySpark with Delta Lake
**Fix**: Added Delta Lake configuration to `spark_session` fixture using `configure_spark_with_delta_pip()` with fallback to basic Spark if Delta Lake is not available
**Status**: Fixed

### Issue #15: F.expr() SQL vs Python Expression Syntax (Duplicate)
**Tests**: `tests/unit/test_validation_mock.py::TestConvertRuleToExpression::test_custom_expression`, `tests/unit/test_validation_standalone.py::TestConvertRuleToExpression::test_custom_expression`
**Mock-Spark Behavior**: `F.expr()` accepts Python-like expressions (e.g., `col('user_id').isNotNull()`)
**PySpark Behavior**: `F.expr()` expects SQL expressions (e.g., `user_id IS NOT NULL`)
**Why Mock-Spark Missed**: Mock-spark is more lenient with expression parsing
**Fix**: Updated tests to use SQL syntax for PySpark and Python syntax for mock-spark
**Status**: Fixed
**Test**: `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_creation_failure`
**Mock-Spark Behavior**: Has `spark.catalog.createDatabase()` method
**PySpark Behavior**: Uses `spark.sql("CREATE SCHEMA ...")` instead, `catalog.createDatabase` doesn't exist
**Why Mock-Spark Missed**: Mock-spark provides a Catalog API that PySpark doesn't have
**Fix**: Updated test to patch `spark.sql` for PySpark and `catalog.createDatabase` for mock-spark
**Status**: Fixed

---

## Remaining Issues to Investigate

### Validation Failures
Several pipeline tests show validation failures (0% valid) when running with PySpark but pass with mock-spark:
- `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution` - `clean_patients` step fails validation
- `tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution` - All silver steps fail validation

**Possible Causes**:
1. Column materialization timing - validation may be checking columns before they're fully materialized
2. Type mismatches - computed columns may have different types in PySpark vs mock-spark
3. Null handling - PySpark may handle nulls differently in computed columns
4. Validation rule evaluation - PySpark may evaluate rules more strictly

**Next Steps**: 
- Add debug logging to see which validation rules are failing
- Check if materialization is working correctly for PySpark
- Compare DataFrame schemas between mock-spark and PySpark after transforms

### Other Test Failures
See `failed_tests_pyspark.txt` for complete list. Common patterns likely include:
- Missing `spark_session` fixtures
- Import issues (StructType, functions)
- Schema creation API differences
- Empty DataFrame handling differences

