# PySpark Test Failure Analysis Report

**Date**: December 10, 2025  
**Test Suite**: sparkforge pipeline_builder  
**Mock-Spark Version**: 3.10.4  
**PySpark Version**: 3.5.x

## Executive Summary

### Test Results Comparison

| Metric | Mock-Spark | PySpark | Difference |
|--------|------------|---------|------------|
| **Total Tests** | 1766 | 1766 | 0 |
| **Passed** | 1740 (98.5%) | 1345 (76.1%) | -395 (-22.4%) |
| **Failed** | 0 (0.0%) | 108 (6.1%) | +108 |
| **Errors** | 0 (0.0%) | 115 (6.5%) | +115 |
| **Skipped** | 26 (1.5%) | 198 (11.2%) | +172 |

**Key Finding**: 223 tests (108 failures + 115 errors) pass with mock-spark but fail with PySpark, representing a **12.6% false positive rate** in the test suite.

### Critical Issues Identified

1. **SparkContext Dependency**: 63+ tests fail because PySpark requires an active SparkContext to create column expressions, while mock-spark does not.
2. **Fixture/Setup Issues**: 25+ tests fail due to SparkContext initialization problems in test fixtures.
3. **Column Reference Timing**: 32+ tests fail because columns created in transforms are not immediately available for validation in PySpark.
4. **Function API Differences**: Tests using `F.current_date()` and similar functions behave differently between mock-spark and PySpark.
5. **Type System Differences**: Mock-spark is more lenient with type conversions and column references than PySpark.

---

## Failure Statistics by Category

### Category Breakdown

| Category | Count | Percentage | Severity |
|----------|-------|------------|----------|
| **SparkContext/JVM Errors** | 63 | 28.3% | HIGH |
| **Fixture/Setup Errors** | 25 | 11.2% | HIGH |
| **Column Reference Errors** | 32 | 14.3% | MEDIUM |
| **Function API Differences** | 8 | 3.6% | MEDIUM |
| **Validation Test Issues** | 95 | 42.6% | MEDIUM |
| **Other/Unclassified** | 0 | 0.0% | - |

**Total Failures**: 223 (108 failed + 115 errors)

---

## Detailed Failure Analysis

### Category 1: SparkContext/JVM Errors

**Count**: 63 tests  
**Severity**: HIGH  
**Impact**: Tests cannot create PySpark column expressions without an active SparkContext.

#### Root Cause

PySpark's `functions.col()` and other column creation functions require an active `SparkContext` to access the JVM. When tests call `F.col()` at module level or in test setup before a SparkSession is created, PySpark fails with:

```
AttributeError: 'NoneType' object has no attribute '_jvm'
```

Mock-spark does not have this requirement because it doesn't use a JVM - column expressions are created directly in Python.

#### Why Mock-Spark Didn't Catch It

Mock-spark's `F.col()` implementation doesn't require a SparkContext, so these tests pass. This is a **fundamental architectural difference** between mock-spark and PySpark.

#### Example Failures

```python
# tests/integration/test_validation_integration.py:543
def test_validate_gold_steps(self):
    validator = UnifiedValidator()
    rules = {"id": [F.col("id").isNotNull()]}  # ❌ Fails: No SparkContext
    # ...
```

```python
# tests/unit/test_models_new.py:292
def test_bronze_step_creation(self):
    rules = {"id": [F.col("id").isNotNull()]}  # ❌ Fails: No SparkContext
    # ...
```

#### Affected Test Files

- `tests/integration/test_validation_integration.py` (multiple tests)
- `tests/unit/test_models_new.py` (all tests)
- `tests/unit/test_models_simple.py` (all tests)
- `tests/integration/test_execution_engine.py` (multiple tests)
- `tests/integration/test_step_execution.py` (multiple tests)

#### Recommendations

1. **Immediate Fix**: Move all `F.col()` calls inside test methods after SparkSession is created.
2. **Test Pattern**: Use fixtures that provide both SparkSession and functions together.
3. **Mock-Spark Improvement**: Consider adding a warning when `F.col()` is called without a session to catch this pattern.

---

### Category 2: Fixture/Setup Errors

**Count**: 25 tests  
**Severity**: HIGH  
**Impact**: Tests fail during setup, preventing execution.

#### Root Cause

Some test fixtures or setup code attempt to create PySpark objects (like `WriterConfig`) that require a `SparkContext` parameter, but the context is not available or not properly initialized.

#### Error Pattern

```
TypeError: __init__() missing 1 required positional argument: 'sparkContext'
```

#### Why Mock-Spark Didn't Catch It

Mock-spark objects don't require SparkContext initialization, so these setup issues don't manifest.

#### Example Failures

- `tests/integration/test_write_mode_integration.py` (all tests)
- `tests/system/test_multi_schema_support.py` (multiple tests)
- `tests/system/test_bronze_no_datetime.py` (multiple tests)
- `tests/unit/test_execution_write_mode.py` (all tests)
- `tests/unit/test_pipeline_runner_write_mode.py` (all tests)

#### Recommendations

1. **Fix Fixtures**: Ensure all fixtures that create PySpark objects properly initialize SparkContext.
2. **Lazy Initialization**: Use lazy initialization patterns for objects that require SparkContext.
3. **Test Isolation**: Ensure each test properly sets up and tears down SparkContext.

---

### Category 3: Column Reference Errors

**Count**: 32 tests  
**Severity**: MEDIUM  
**Impact**: Pipeline execution fails because columns created in transforms are not immediately available.

#### Root Cause

In PySpark, when you create a column in a transform (e.g., `withColumn("new_col", ...)`), the column is not immediately available for use in validation rules or subsequent operations until the DataFrame is materialized. Mock-spark is more lenient and allows immediate access to newly created columns.

#### Error Patterns

1. **Column Not Found**:
   ```
   unable to find column "diagnosis_date_parsed"; valid columns: [...]
   ```

2. **Type Mismatch**:
   ```
   invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`
   ```

#### Why Mock-Spark Didn't Catch It

Mock-spark's lazy evaluation and schema tracking is more permissive. It allows:
- Immediate access to columns created in transforms
- More flexible type conversions
- Less strict schema validation during transform execution

#### Example Failures

```python
# tests/builder_tests/test_healthcare_pipeline.py
def normalize_lab_results_transform(spark, bronze_df, silvers):
    return (
        bronze_df
        .withColumn("test_date_parsed", 
                   F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"))
        # Column created here
    )

# Validation rules reference the new column
rules={"test_date_parsed": ["not_null"]}  # ❌ Column not available yet in PySpark
```

#### Affected Test Files

- `tests/builder_tests/test_healthcare_pipeline.py`
- `tests/builder_tests/test_marketing_pipeline.py`
- `tests/builder_tests/test_streaming_hybrid_pipeline.py`
- `tests/builder_tests/test_supply_chain_pipeline.py`

#### Recommendations

1. **Materialize DataFrames**: Call `.cache()` or `.collect()` before referencing new columns in validation.
2. **Separate Transform and Validation**: Don't reference transform-created columns in the same step's validation rules.
3. **Mock-Spark Improvement**: Make mock-spark's column availability behavior match PySpark's stricter model.

---

### Category 4: Function API Differences

**Count**: 8 tests  
**Severity**: MEDIUM  
**Impact**: Function calls that work in mock-spark fail in PySpark.

#### Root Cause

Some PySpark functions have different APIs or requirements than their mock-spark equivalents.

#### Example: `F.current_date()`

```python
# tests/builder_tests/test_healthcare_pipeline.py:110
F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
```

**Error**: `'DataFrame' object has no attribute 'current_date()'`

**Issue**: In PySpark, `F.current_date()` is a function call, not a DataFrame method. The code may be calling it incorrectly, or mock-spark allows a different pattern.

#### Why Mock-Spark Didn't Catch It

Mock-spark's function implementations may be more lenient or have different signatures than PySpark.

#### Recommendations

1. **Function Signature Audit**: Compare all function calls between mock-spark and PySpark.
2. **API Documentation**: Ensure mock-spark functions match PySpark signatures exactly.
3. **Test Fixes**: Update tests to use correct PySpark function patterns.

---

### Category 5: Validation Test Issues

**Count**: 95 tests  
**Severity**: MEDIUM  
**Impact**: Validation-related tests fail due to SparkContext or function call issues.

#### Root Cause

Many validation tests combine multiple issues:
- Calling `F.col()` without SparkContext (Category 1)
- Using validation functions that require active session
- Testing validation logic that depends on PySpark-specific behavior

#### Affected Test Files

- `tests/integration/test_validation_integration.py` (most tests)
- `tests/unit/test_validation_standalone.py` (multiple tests)
- `tests/unit/test_validation_mock.py` (multiple tests)
- `tests/unit/test_validation_enhanced.py` (all tests)
- `tests/unit/test_validation_enhanced_simple.py` (all tests)
- `tests/unit/test_validation_additional_coverage.py` (multiple tests)

#### Recommendations

1. **Refactor Validation Tests**: Ensure all validation tests properly initialize SparkSession before using functions.
2. **Test Isolation**: Each validation test should be independent and properly set up its environment.
3. **Mock Functions**: Consider using mock functions in unit tests to avoid SparkContext dependency.

---

## Mock-Spark Limitations Identified

### 1. SparkContext Independence

**Issue**: Mock-spark doesn't require SparkContext for column expressions.  
**Impact**: HIGH - 63+ tests have false positives.  
**Priority**: HIGH  
**Recommendation**: Add validation/warning when functions are called without an active session.

### 2. Column Availability Timing

**Issue**: Mock-spark allows immediate access to columns created in transforms.  
**Impact**: MEDIUM - 32+ tests have false positives.  
**Priority**: MEDIUM  
**Recommendation**: Make column availability behavior match PySpark's stricter model.

### 3. Type System Leniency

**Issue**: Mock-spark is more lenient with type conversions and schema validation.  
**Impact**: MEDIUM - Several tests pass incorrectly.  
**Priority**: MEDIUM  
**Recommendation**: Implement stricter type checking to match PySpark behavior.

### 4. Function API Differences

**Issue**: Some function signatures or behaviors differ from PySpark.  
**Impact**: LOW-MEDIUM - 8+ tests affected.  
**Priority**: MEDIUM  
**Recommendation**: Audit and align all function APIs with PySpark.

---

## Test Suite Issues

### 1. Module-Level Function Calls

**Problem**: Tests call `F.col()` and other functions at module level or in class setup.  
**Impact**: 63+ tests fail with PySpark.  
**Fix**: Move all function calls inside test methods after SparkSession creation.

### 2. Missing SparkContext Initialization

**Problem**: Some fixtures don't properly initialize SparkContext for PySpark.  
**Impact**: 25+ tests fail during setup.  
**Fix**: Ensure all fixtures that create PySpark objects properly initialize SparkContext.

### 3. Column Reference Timing

**Problem**: Tests reference columns immediately after creation in transforms.  
**Impact**: 32+ tests fail with PySpark.  
**Fix**: Materialize DataFrames or separate transform and validation steps.

### 4. Test Isolation

**Problem**: Some tests depend on shared state or don't properly clean up.  
**Impact**: Tests may pass in isolation but fail when run together.  
**Fix**: Ensure proper test isolation and cleanup.

---

## Recommendations

### Immediate Actions (High Priority)

1. **Fix SparkContext Issues** (63 tests)
   - Move all `F.col()` calls inside test methods
   - Ensure SparkSession is created before any function calls
   - Add helper fixtures that provide functions with session

2. **Fix Fixture Issues** (25 tests)
   - Review and fix all fixtures that create PySpark objects
   - Ensure proper SparkContext initialization
   - Add fixture validation

3. **Fix Column Reference Issues** (32 tests)
   - Materialize DataFrames before validation
   - Separate transform and validation logic
   - Update validation rules to not reference transform-created columns

### Mock-Spark Improvements (Medium Priority)

1. **Add SparkContext Validation**
   - Warn when functions are called without active session
   - Help catch test patterns that won't work with PySpark

2. **Stricter Column Availability**
   - Match PySpark's column availability behavior
   - Require DataFrame materialization for new columns

3. **Type System Alignment**
   - Implement stricter type checking
   - Match PySpark's type conversion rules

4. **Function API Audit**
   - Compare all function signatures with PySpark
   - Ensure exact API compatibility

### Testing Strategy Improvements

1. **PySpark Compatibility Testing**
   - Run all tests with PySpark in CI/CD
   - Fail builds if PySpark tests fail
   - Track PySpark vs mock-spark test results

2. **Test Patterns**
   - Document best practices for PySpark-compatible tests
   - Create test templates that work with both engines
   - Add linting rules to catch common issues

3. **Documentation**
   - Document known differences between mock-spark and PySpark
   - Provide migration guides for tests
   - Create troubleshooting guides

### Documentation Updates

1. **Testing Guide**
   - Add section on PySpark compatibility
   - Document common pitfalls
   - Provide examples of correct test patterns

2. **Mock-Spark Limitations**
   - Document known behavioral differences
   - Provide workarounds
   - Set expectations for developers

---

## Conclusion

The analysis reveals that **12.6% of tests (223 tests) pass with mock-spark but fail with PySpark**, indicating significant behavioral differences between the two engines. The primary issues are:

1. **SparkContext dependency** (63 tests) - PySpark requires active SparkContext for function calls
2. **Fixture/Setup problems** (25 tests) - Improper SparkContext initialization
3. **Column reference timing** (32 tests) - Mock-spark is more lenient with column availability
4. **Validation test issues** (95 tests) - Combination of above issues in validation tests
5. **Function API differences** (8 tests) - Some functions behave differently

**Key Takeaway**: While mock-spark 3.10.4 has made significant improvements (empty DataFrame schemas, union validation), there are still fundamental architectural differences that cause false positives in tests. The test suite needs updates to be PySpark-compatible, and mock-spark could benefit from stricter validation to catch these patterns earlier.

**Next Steps**:
1. Fix the 223 failing tests to be PySpark-compatible
2. Implement mock-spark improvements to catch these patterns
3. Add PySpark testing to CI/CD pipeline
4. Update documentation with best practices

---

## Appendix: Detailed Failure List

### SparkContext/JVM Errors (63 tests)

<details>
<summary>Click to expand full list</summary>

- `tests/integration/test_validation_integration.py::TestUnifiedValidator::test_validate_bronze_steps`
- `tests/integration/test_validation_integration.py::TestUnifiedValidator::test_validate_silver_steps`
- `tests/integration/test_validation_integration.py::TestUnifiedValidator::test_validate_gold_steps`
- `tests/integration/test_validation_integration.py::TestUnifiedValidator::test_validate_dependencies`
- `tests/integration/test_validation_integration.py::TestConvertRuleToExpression::test_not_null_rule`
- `tests/integration/test_validation_integration.py::TestConvertRuleToExpression::test_positive_rule`
- `tests/integration/test_validation_integration.py::TestConvertRuleToExpression::test_non_negative_rule`
- `tests/integration/test_validation_integration.py::TestConvertRuleToExpression::test_non_zero_rule`
- `tests/integration/test_validation_integration.py::TestConvertRuleToExpression::test_unknown_rule`
- `tests/integration/test_validation_integration.py::TestConvertRulesToExpressions::test_string_rules_conversion`
- `tests/integration/test_validation_integration.py::TestConvertRulesToExpressions::test_mixed_rules_conversion`
- `tests/integration/test_validation_integration.py::TestAndAllRules::test_single_column_single_rule`
- `tests/integration/test_validation_integration.py::TestAndAllRules::test_single_column_multiple_rules`
- `tests/integration/test_validation_integration.py::TestAndAllRules::test_multiple_columns`
- `tests/integration/test_validation_integration.py::TestAndAllRules::test_complex_rules`
- `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_step_silver_without_dependencies`
- `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_step_silver_without_transform`
- `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_step_gold_without_dependencies`
- `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_step_gold_without_transform`
- `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_pipeline_with_different_step_types`
- `tests/integration/test_step_execution.py::TestStepExecutionFlow::test_execution_context_flow`
- `tests/unit/test_models_new.py::TestBronzeStep::test_bronze_step_creation`
- `tests/unit/test_models_new.py::TestBronzeStep::test_bronze_step_validation`
- `tests/unit/test_models_new.py::TestBronzeStep::test_bronze_step_invalid_name`
- `tests/unit/test_models_new.py::TestSilverStep::test_silver_step_creation`
- `tests/unit/test_models_new.py::TestSilverStep::test_silver_step_validation`
- `tests/unit/test_models_new.py::TestSilverStep::test_silver_step_invalid_source_bronze`
- `tests/unit/test_models_new.py::TestSilverStep::test_silver_step_invalid_transform`
- `tests/unit/test_models_new.py::TestSilverStep::test_silver_step_invalid_table_name`
- `tests/unit/test_models_new.py::TestGoldStep::test_gold_step_creation`
- `tests/unit/test_models_new.py::TestGoldStep::test_gold_step_validation`
- `tests/unit/test_models_new.py::TestGoldStep::test_gold_step_invalid_transform`
- `tests/unit/test_models_new.py::TestGoldStep::test_gold_step_invalid_table_name`
- `tests/unit/test_models_new.py::TestGoldStep::test_gold_step_invalid_source_silvers`
- `tests/unit/test_models_simple.py::TestBronzeStep::test_bronze_step_creation`
- `tests/unit/test_models_simple.py::TestBronzeStep::test_bronze_step_validation`
- `tests/unit/test_models_simple.py::TestBronzeStep::test_bronze_step_invalid_name`
- `tests/unit/test_models_simple.py::TestSilverStep::test_silver_step_creation`
- `tests/unit/test_models_simple.py::TestSilverStep::test_silver_step_validation`
- `tests/unit/test_models_simple.py::TestSilverStep::test_silver_step_invalid_source_bronze`
- `tests/unit/test_models_simple.py::TestSilverStep::test_silver_step_invalid_transform`
- `tests/unit/test_models_simple.py::TestSilverStep::test_silver_step_invalid_table_name`
- `tests/unit/test_models_simple.py::TestGoldStep::test_gold_step_creation`
- `tests/unit/test_models_simple.py::TestGoldStep::test_gold_step_validation`
- `tests/unit/test_models_simple.py::TestGoldStep::test_gold_step_invalid_transform`
- `tests/unit/test_models_simple.py::TestGoldStep::test_gold_step_invalid_table_name`
- `tests/unit/test_models_simple.py::TestGoldStep::test_gold_step_invalid_source_silvers`
- (Additional validation test failures...)

</details>

### Fixture/Setup Errors (25 tests)

<details>
<summary>Click to expand full list</summary>

- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_incremental_pipeline_preserves_data`
- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_initial_pipeline_overwrites_data`
- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_write_mode_consistency_across_pipeline_runs`
- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_mixed_pipeline_modes_have_correct_write_modes`
- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_write_mode_regression_prevention`
- `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::test_log_writer_receives_correct_write_mode`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_bronze_rules_without_schema`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_silver_rules_with_schema`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_silver_transform_with_schema`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_gold_transform_with_schema`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_validation_success`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_validation_failure`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_get_effective_schema`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_creation`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_creation_failure`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_cross_schema_pipeline`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_mixed_schema_usage`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_schema_validation_integration`
- `tests/system/test_multi_schema_support.py::TestMultiSchemaSupport::test_backward_compatibility`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_bronze_step_without_incremental_col`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_silver_step_creation`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_gold_step_creation`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_pipeline_validation`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_pipeline_creation`
- `tests/system/test_bronze_no_datetime.py::TestBronzeNoDatetime::test_dataframe_operations`
- (Additional write mode and execution tests...)

</details>

### Column Reference Errors (32 tests)

<details>
<summary>Click to expand full list</summary>

- `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
- `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_healthcare_logging`
- `tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
- `tests/builder_tests/test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
- `tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution`
- `tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_supply_chain_logging`
- `tests/builder_tests/test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution`
- (Additional pipeline tests with column reference issues...)

</details>

---

**Report Generated**: December 10, 2025  
**Analysis Tool**: Automated test failure analysis  
**Next Review**: After implementing recommended fixes

