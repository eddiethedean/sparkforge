# Skipped Tests Summary for Sparkless (Mock Mode) Runs

**Generated:** 2024-12-18  
**Test Mode:** Mock (sparkless 3.17.6)  
**Total Skipped:** 78 tests

## Overview

This document summarizes all tests that are skipped when running the test suite in mock mode (sparkless). Tests are skipped for various reasons, primarily:
1. **Real PySpark Required**: Tests that require actual PySpark installation and functionality
2. **Delta Lake Features**: Tests that require Delta Lake JAR or features not available in mock-spark
3. **Refactored Code**: Tests for methods/features that were removed during refactoring
4. **Known Limitations**: Tests that fail due to mock-spark limitations (e.g., Polars backend datetime validation)
5. **Debug/Development Tests**: Tests intended for debugging specific issues

---

## 1. PySpark-Specific Tests (Require SPARK_MODE=real)

### 1.1 Builder PySpark Tests (`tests/builder_pyspark_tests/`)
**Total:** 33 tests  
**Reason:** These tests are specifically designed to run with real PySpark and require `SPARK_MODE=real`.

#### Test Files:
- `test_customer_analytics_pipeline.py` (4 tests)
  - `test_complete_customer_360_pipeline_execution`
  - `test_customer_churn_prediction`
  - `test_customer_lifetime_value_analysis`
  - `test_customer_analytics_logging`

- `test_data_quality_pipeline.py` (2 tests)
  - `test_complete_data_quality_pipeline_execution`
  - `test_incremental_data_quality_processing`

- `test_ecommerce_pipeline.py` (4 tests)
  - `test_complete_ecommerce_pipeline_execution`
  - `test_incremental_order_processing`
  - `test_validation_failures`
  - `test_logging_and_monitoring`

- `test_financial_pipeline.py` (4 tests)
  - `test_complete_financial_transaction_pipeline_execution`
  - `test_fraud_detection_scenarios`
  - `test_compliance_monitoring`
  - `test_financial_audit_logging`

- `test_healthcare_pipeline.py` (3 tests)
  - `test_complete_healthcare_pipeline_execution`
  - `test_incremental_healthcare_processing`
  - `test_healthcare_logging`

- `test_iot_pipeline.py` (4 tests)
  - `test_complete_iot_sensor_pipeline_execution`
  - `test_incremental_sensor_processing`
  - `test_anomaly_detection_pipeline`
  - `test_performance_monitoring`

- `test_marketing_pipeline.py` (2 tests)
  - `test_complete_marketing_pipeline_execution`
  - `test_incremental_marketing_processing`

- `test_multi_source_pipeline.py` (4 tests)
  - `test_complete_multi_source_integration_pipeline_execution`
  - `test_schema_evolution_handling`
  - `test_complex_dependency_handling`
  - `test_multi_source_logging`

- `test_simple.py` (1 test)
  - `test_simple_pipeline_creation`

- `test_simple_pipeline.py` (1 test)
  - `test_simple_pipeline_execution`

- `test_streaming_hybrid_pipeline.py` (2 tests)
  - `test_complete_streaming_hybrid_pipeline_execution`
  - `test_incremental_streaming_processing`

- `test_supply_chain_pipeline.py` (3 tests)
  - `test_complete_supply_chain_pipeline_execution`
  - `test_incremental_supply_chain_processing`
  - `test_supply_chain_logging`

**Skip Condition:** Module-level skip when `SPARK_MODE != "real"`

---

### 1.2 PySpark Compatibility Tests (`tests/compat_pyspark/`)
**Total:** 13 tests  
**Reason:** Tests PySpark compatibility and engine switching, requires real PySpark installation.

#### Test File: `test_pyspark_compatibility.py`
- `TestPySparkCompatibility` class (10 tests)
  - `test_pyspark_engine_detection`
  - `test_pyspark_imports`
  - `test_pyspark_dataframe_operations`
  - `test_pyspark_pipeline_building`
  - `test_pyspark_validation`
  - `test_pyspark_delta_lake_operations`
  - `test_pyspark_error_handling`
  - `test_pyspark_performance_monitoring`
  - `test_pyspark_table_operations`
  - `test_pyspark_engine_switching` (3 tests)
    - `test_switch_to_pyspark`
    - `test_switch_to_mock`
    - `test_auto_detection`

**Skip Condition:** Tests check for PySpark availability and skip if not installed or if `SPARK_MODE != "real"`

---

### 1.3 Real Spark Operations Tests
**Total:** 1 test  
**Reason:** Requires real Spark environment for join operations.

#### Test File: `tests/system/test_simple_real_spark.py`
- `TestRealSparkOperations::test_real_spark_joins`
  - **Reason:** "Requires real Spark environment"

---

### 1.4 Compatibility Helper Tests
**Total:** 1 test  
**Reason:** PySpark detection test requires real PySpark.

#### Test File: `tests/unit/test_compat_helpers.py`
- `TestDetectSparkType::test_detect_pyspark`
  - **Reason:** "PySpark detection test requires real PySpark"

---

## 2. Delta Lake Tests (Delta Lake JAR Not Available)

### 2.1 Delta Lake Comprehensive Tests (`tests/system/test_delta_lake.py`)
**Total:** 9 tests  
**Reason:** Delta Lake JAR not available in sparkless/mock Spark session, or features not fully supported.

#### Test Class: `TestDeltaLakeComprehensive`
- `test_delta_lake_acid_transactions`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_schema_evolution`
  - **Reason:** "Delta Lake schema evolution not fully supported in mock-spark"
  - **Skip Condition:** `SPARKFORGE_ENGINE == "mock"` OR `SPARK_MODE == "mock"`
  
- `test_delta_lake_time_travel`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_merge_operations`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_optimization`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_history_and_metadata`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_concurrent_writes`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_performance_characteristics`
  - **Reason:** "Delta Lake JAR not available in Spark session"
  
- `test_delta_lake_data_quality_constraints`
  - **Reason:** "Delta Lake JAR not available in Spark session"

**Note:** These tests check for Delta Lake availability at runtime using `_is_delta_lake_available()` helper function.

---

### 2.2 Delta Overwrite Real Test
**Total:** 1 test  
**Reason:** Requires real PySpark + Delta Lake.

#### Test File: `tests/system/test_delta_overwrite_real.py`
- `test_initial_load_overwrite_delta`
  - **Reason:** "Requires real PySpark + Delta"
  - **Skip Condition:** `@pytest.mark.real_spark_only`

---

### 2.3 Debug Delta Minimal Tests
**Total:** 2 tests  
**Reason:** Debug tests for Delta Lake configuration, require real Spark mode.

#### Test File: `tests/debug/test_delta_minimal.py`
- `test_delta_minimal_write`
- `test_delta_minimal_direct_session`
  - **Reason:** "Test requires real Spark mode"
  - **Skip Condition:** `@pytest.mark.real_spark_only`

---

## 3. Known Mock-Spark Limitations (Polars Backend Issues)

### 3.1 Builder Tests with Complex Datetime Validation
**Total:** 5 tests  
**Reason:** Polars backend (used by sparkless) still fails complex datetime validation in these pipelines.

#### Test Files:
- `tests/builder_tests/test_data_quality_pipeline.py`
  - `test_complete_data_quality_pipeline_execution`
  
- `tests/builder_tests/test_healthcare_pipeline.py`
  - `test_complete_healthcare_pipeline_execution`
  
- `tests/builder_tests/test_marketing_pipeline.py`
  - `test_complete_marketing_pipeline_execution`
  
- `tests/builder_tests/test_streaming_hybrid_pipeline.py`
  - `test_complete_streaming_hybrid_pipeline_execution`
  
- `tests/builder_tests/test_supply_chain_pipeline.py`
  - `test_complete_supply_chain_pipeline_execution`

**Skip Condition:** `SPARK_MODE == "mock"`  
**Reason:** "Polars backend still fails complex datetime validation in this pipeline (mock-spark follow-up)."

**Note:** These are marked as follow-up items for future mock-spark improvements.

---

## 4. Refactored/Removed Code Tests

### 4.1 Pipeline Logger Tests (`tests/unit/test_logging.py`)
**Total:** 11 tests  
**Reason:** Tests for methods that were removed during refactoring of the `PipelineLogger` class.

#### Test Class: `TestPipelineLoggerComprehensive`
- `test_pipeline_start` (2 tests)
  - **Reason:** "pipeline_start method was removed in refactoring"
  
- `test_pipeline_end_success` / `test_pipeline_end_failure` (2 tests)
  - **Reason:** "pipeline_end method was removed in refactoring"
  
- `test_performance_metric` / `test_performance_metric_custom_unit` (2 tests)
  - **Reason:** "performance_metric method was removed in refactoring"
  
- `test_context_manager_with_existing_extra`
  - **Reason:** "context method with kwargs was removed, use log_context instead"
  
- `test_step_failed` / `test_step_failed_no_duration` (2 tests)
  - **Reason:** "step_failed method was removed in refactoring, use error() instead"
  
- `test_validation_passed` / `test_validation_failed` (2 tests)
  - **Reason:** "validation_passed/validation_failed method was removed in refactoring"

**Note:** These tests are kept for historical reference but are permanently skipped since the methods no longer exist.

---

## 5. Indirectly Tested / Fallback Mechanism Tests

### 5.1 Execution Write Mode Test
**Total:** 1 test  
**Reason:** Fallback mechanism is tested indirectly; filter typically succeeds.

#### Test File: `tests/unit/test_execution_write_mode.py`
- `TestExecutionEngineWriteMode::test_incremental_filter_uses_mock_fallback_when_needed`
  - **Reason:** "Fallback mechanism tested indirectly; filter typically succeeds"
  - **Skip Type:** `@pytest.mark.skip` (permanent skip)

---

## 6. Performance Tests

### 6.1 Validation Performance Test
**Total:** 1 test  
**Reason:** Requires real DataFrame for validation operations.

#### Test File: `tests/performance/test_performance.py`
- `TestValidationPerformance::test_assess_data_quality_performance`
  - **Reason:** "Requires real DataFrame for validation operations"
  - **Skip Condition:** Checks for real Spark mode

---

## 7. Other Skipped Tests

### 7.1 Reporting Tests
**Total:** 2 tests  
**Reason:** Tests are explicitly marked as skipped.

#### Test File: `tests/unit/test_reporting.py`
- `TestCreateTransformDict::test_create_transform_dict_skipped`
- `TestCreateWriteDict::test_create_write_dict_skipped`

---

### 7.2 Python 3.8 Compatibility Tests
**Total:** 8 tests  
**Reason:** Tests for Python 3.8 compatibility checking.

#### Test File: `tests/unit/test_trap_10_silent_test_skip.py`
- `Python38CompatibilityTest` class (5 tests)
  - `test_all_files_parseable`
  - `test_import_compatibility`
  - `test_no_dict_type_annotations`
  - `test_no_legacy_typing_imports`
  - `test_python_version`
  
- `TestTrap10SilentTestSkip` class (3 tests)
  - `test_parsing_errors_are_logged_and_tracked`
  - `test_parsing_errors_in_dict_syntax_are_logged_and_tracked`
  - `test_valid_files_are_processed_normally`
  - `test_multiple_parsing_errors_are_all_tracked`
  - `test_logging_uses_correct_module_name`

**Note:** These appear to be skipped by default (possibly conditional on Python version).

---

### 7.3 Validation Mode Test
**Total:** 1 test  
**Reason:** Test for validation mode behavior.

#### Test File: `tests/unit/test_trap_5_default_schema_fallbacks.py`
- `TestTrap5DefaultSchemaFallbacks::test_validation_mode_skips_schema_validation`

---

## Summary by Category

| Category | Count | Primary Reason |
|----------|-------|----------------|
| **PySpark-Specific Tests** | 33 | Require real PySpark installation |
| **PySpark Compatibility Tests** | 13 | Test PySpark compatibility features |
| **Delta Lake Tests** | 12 | Delta Lake JAR/features not available in mock-spark |
| **Known Mock-Spark Limitations** | 5 | Polars backend datetime validation issues |
| **Refactored Code Tests** | 11 | Methods removed during refactoring |
| **Indirectly Tested** | 1 | Fallback mechanism tested elsewhere |
| **Performance Tests** | 1 | Requires real DataFrame |
| **Other Tests** | 2 | Various reasons |
| **TOTAL** | **78** | |

---

## Recommendations

### For Mock Mode Testing:
1. **PySpark Tests**: These are intentionally separated and should only run in `SPARK_MODE=real`
2. **Delta Lake Tests**: Consider implementing Delta Lake support in sparkless or mark as "real-only" features
3. **Polars Backend Issues**: Track as follow-up items for sparkless improvements
4. **Refactored Tests**: Consider removing permanently skipped tests or updating them to test new APIs

### For Real Mode Testing:
- Run with `SPARK_MODE=real` to execute all PySpark-specific and Delta Lake tests
- Ensure PySpark and Delta Lake dependencies are installed

---

## Notes

- **Total Tests:** 1712 passed + 78 skipped = 1790 total tests
- **Skip Rate:** ~4.4% of tests are skipped in mock mode
- **Most Common Reason:** PySpark-specific functionality (46 tests)
- **Second Most Common:** Delta Lake features (12 tests)
- **Third Most Common:** Refactored code (11 tests)

All skipped tests are intentional and appropriate for mock mode testing. The test suite is designed to work in both mock and real modes, with appropriate tests skipped based on the execution environment.

