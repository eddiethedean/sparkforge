# Unskipped Tests for Sparkless Bug Detection

**Date:** 2024-12-18  
**Purpose:** Unskip tests that sparkless should theoretically be able to handle, even if features aren't fully implemented yet. This will help identify bugs that need to be reported to sparkless.

## Changes Made

### 1. Delta Lake Schema Evolution Test
**File:** `tests/system/test_delta_lake.py`
- **Removed:** `@pytest.mark.skipif` decorator that skipped the test in mock mode
- **Test:** `test_delta_lake_schema_evolution`
- **Reason:** Sparkless should be able to handle basic Delta operations, even if schema evolution isn't fully supported. This will help identify what actually fails.

### 2. Builder Tests with Polars Backend Issues (5 tests)
**Files:**
- `tests/builder_tests/test_marketing_pipeline.py`
- `tests/builder_tests/test_data_quality_pipeline.py`
- `tests/builder_tests/test_healthcare_pipeline.py`
- `tests/builder_tests/test_streaming_hybrid_pipeline.py`
- `tests/builder_tests/test_supply_chain_pipeline.py`

- **Removed:** `@pytest.mark.skipif` decorators that skipped tests in mock mode
- **Tests:**
  - `test_complete_marketing_pipeline_execution`
  - `test_complete_data_quality_pipeline_execution`
  - `test_complete_healthcare_pipeline_execution`
  - `test_complete_streaming_hybrid_pipeline_execution`
  - `test_complete_supply_chain_pipeline_execution`
- **Reason:** These tests were skipped due to "Polars backend still fails complex datetime validation". By unskipping them, we can see the actual failures and report specific bugs to sparkless.

### 3. Performance Test
**File:** `tests/performance/test_performance.py`
- **Removed:** `pytest.skip("Requires real DataFrame for validation operations")`
- **Test:** `test_assess_data_quality_performance`
- **Changed:** Now uses `spark_session` fixture to create a sparkless DataFrame and test `assess_data_quality` function
- **Reason:** Sparkless DataFrames should work with validation functions. This will help identify any sparkless-specific issues.

### 4. Debug Delta Tests (2 tests)
**File:** `tests/debug/test_delta_minimal.py`
- **Removed:** `@pytest.mark.real_spark_only` markers
- **Tests:**
  - `test_delta_minimal_write`
  - `test_delta_minimal_direct_session`
- **Changed:** 
  - Removed `from pyspark.sql import SparkSession` import (not needed in mock mode)
  - Updated `test_delta_minimal_direct_session` to use `_create_mock_spark_session()` instead of `_create_real_spark_session()`
- **Reason:** These debug tests can help identify Delta Lake issues in sparkless.

## Expected Outcomes

When running tests in mock mode, we expect to see:
1. **New test failures** that reveal sparkless bugs
2. **Specific error messages** that can be used to create bug reports
3. **Better understanding** of what sparkless can and cannot handle

## Tests That Remain Skipped

The following tests remain skipped (intentionally):
- **PySpark-specific tests** (`builder_pyspark_tests/`) - These require real PySpark
- **PySpark compatibility tests** (`compat_pyspark/`) - These test PySpark-specific features
- **Refactored code tests** (`test_logging.py`) - Methods were removed
- **Delta Lake tests with runtime checks** - These check if Delta is actually available (kept for safety)

## Next Steps

1. Run tests in mock mode: `SPARK_MODE=mock pytest tests/ -n 10`
2. Collect failure information
3. Document specific sparkless bugs found
4. Create GitHub issues for sparkless with reproduction cases

