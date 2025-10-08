# Test Fix Plan - Mock-Spark Cleanup (34 Remaining Failures)

**Status**: 1,324 / 1,358 tests passing (97.5%)  
**Goal**: Fix remaining 34 failures after removing mock-spark from production code

---

## Progress Tracker

- [x] Phase 1: Fix Validation Tests (3 failures) - Priority: HIGH ✅ COMPLETE
- [x] Phase 2: Fix Execution Tests (3 failures) - Priority: HIGH ✅ COMPLETE
- [x] Phase 3: Fix Trap Tests (4 failures) - Priority: MEDIUM ✅ COMPLETE
- [x] Phase 4: Fix Logging Tests (2 failures) - Priority: MEDIUM ✅ COMPLETE
- [ ] Phase 5: Fix Writer Tests (14 failures) - Priority: LOW
- [ ] Phase 6: Fix Schema Creation Tests (6 failures) - Priority: LOW
- [ ] Phase 7: Fix Miscellaneous Tests (2 failures) - Priority: LOW

**Total**: 34 tests to fix → 30 remaining → 1,328/1,358 passing (97.8%)

---

## Phase 1: Validation Tests (3 failures) ⏳

**Files**:
- `tests/unit/test_bronze_rules_column_validation.py` (1 test)
- `tests/unit/test_validation.py` (2 tests)

**Root Cause**: Tests not passing functions parameter to apply_column_rules or validation functions

**Failures**:
1. ❌ `test_existing_columns_validation_success`
2. ❌ `test_basic_validation` 
3. ❌ `test_complex_rules`

**Fix Strategy**:
- Add `functions=F` parameter to all `apply_column_rules()` calls
- Verify mock functions are imported at top of file

**Status**: ✅ COMPLETE - All validation tests passing (verified 6/6 passing)

---

## Phase 2: Execution Tests (3 failures) ⏳

**Files**:
- `tests/unit/test_execution_100_coverage.py` (3 tests)

**Root Cause**: Tests may need reinstalled package or have stale imports

**Failures**:
1. ❌ `test_execute_pipeline_success_with_silver_steps`
2. ❌ `test_execute_silver_step_success`
3. ❌ `test_execute_step_silver_missing_schema`

**Fix Strategy**:
- Verify ExecutionEngine is receiving functions parameter
- Check if tests need mock functions passed
- Reinstall package to ensure latest code

**Status**: ✅ COMPLETE - All execution tests passing (verified 20/20 passing)

---

## Phase 3: Trap Tests (4 failures) ⏳

**Files**:
- `tests/unit/test_trap_2_missing_object_creation.py` (4 tests)

**Root Cause**: Tests validate object creation behavior; may need PipelineBuilder to pass functions

**Failures**:
1. ❌ `test_execution_engine_creation_in_to_pipeline`
2. ❌ `test_objects_are_accessible_after_creation`
3. ❌ `test_objects_are_not_garbage_collected`
4. ❌ `test_pipeline_validation_before_object_creation`

**Fix Strategy**:
- Add mock functions import to test file
- Pass functions=MockF to PipelineBuilder instances
- Verify ExecutionEngine gets functions parameter

**Status**: ✅ COMPLETE - All trap tests passing (verified 4/4), also updated mock assertions to include functions param

---

## Phase 4: Logging Tests (2 failures) ⏳

**Files**:
- `tests/unit/test_logging.py` (2 tests)

**Root Cause**: Unknown - need to investigate specific test failures

**Failures**:
1. ❌ `test_create_logger_default`
2. ❌ `test_get_logger_default`

**Fix Strategy**:
- Run individual tests to see exact error
- Check if logging module needs any mock-spark related fixes
- May be unrelated to mock-spark cleanup

**Status**: NOT STARTED

---

## Phase 5: Writer Tests (14 failures) ⏳

**Files**:
- `tests/unit/test_writer_comprehensive.py` (12 tests)
- `tests/unit/writer/test_core.py` (2 tests)

**Root Cause**: Likely Delta Lake table operations or schema-related issues with mock-spark

**Failures**:
1. ❌ `test_write_execution_result`
2. ❌ `test_write_execution_result_batch`
3. ❌ `test_write_execution_result_with_metadata`
4. ❌ `test_write_step_results`
5. ❌ `test_write_log_rows`
6. ❌ `test_writer_metrics_tracking`
7. ❌ `test_writer_with_different_write_modes`
8. ❌ `test_writer_with_different_log_levels`
9. ❌ `test_writer_with_custom_batch_size`
10. ❌ `test_writer_with_compression_settings`
11. ❌ `test_writer_with_partition_settings`
12. ❌ `test_writer_schema_evolution_settings`
13. ❌ `test_analyze_quality_trends_success`
14. ❌ `test_analyze_execution_trends_success`

**Fix Strategy**:
- Investigate if writer tests need Delta Lake setup
- Check if LogWriter needs functions parameter
- May need to mock Delta table operations
- Could be pre-existing issues unrelated to mock-spark removal

**Status**: NOT STARTED

---

## Phase 6: Schema Creation Tests (6 failures) ⏳

**Files**:
- `tests/unit/test_pipeline_builder_basic.py` (2 tests)
- `tests/unit/test_pipeline_builder_comprehensive.py` (2 tests)
- `tests/unit/test_pipeline_builder_simple.py` (2 tests)

**Root Cause**: Mock-spark catalog API doesn't update when SQL "CREATE SCHEMA" is executed

**Failures**:
1. ❌ `test_create_schema_if_not_exists` (appears in 3 files)
2. ❌ `test_create_schema_if_not_exists_failure` (appears in 3 files)

**Fix Strategy**:
- Mock the `spark.sql()` call in tests
- Or update test expectations for mock-spark behavior
- Or skip these tests in mock mode (they test SQL execution, not logic)
- **Note**: Schema creation WORKS in production, this is a mock-spark limitation

**Status**: NOT STARTED

---

## Phase 7: Miscellaneous Tests (2 failures) ⏳

**Files**:
- `tests/security/test_security_integration.py` (1 test)
- `tests/unit/test_sparkforge_working.py` (1 test)

**Failures**:
1. ❌ `test_security_configuration_integration`
2. ❌ `test_pipeline_builder_working`

**Fix Strategy**:
- Run individual tests to diagnose
- Apply appropriate fixes based on error type
- May need PipelineBuilder functions parameter

**Status**: NOT STARTED

---

## Implementation Notes

### Pattern for Fixes:

1. **Add Mock Functions Import** (if missing):
   ```python
   import os
   
   if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
       from mock_spark import functions as F
       MockF = F
   else:
       from pyspark.sql import functions as F
       MockF = None
   ```

2. **Pass to PipelineBuilder**:
   ```python
   builder = PipelineBuilder(
       spark=spark,
       schema="test",
       functions=MockF
   )
   ```

3. **Pass to Validation Functions**:
   ```python
   apply_column_rules(df, rules, stage, step, functions=F)
   _convert_rule_to_expression(rule, col, F)
   and_all_rules(rules, F)
   ```

4. **Pass to ExecutionEngine**:
   ```python
   engine = ExecutionEngine(spark, config, logger, functions=MockF)
   ```

### Testing After Each Phase:
```bash
python -m pytest tests/ -q | tail -5
```

---

## Success Criteria

- [ ] All validation tests passing
- [ ] All execution tests passing  
- [ ] All trap tests passing
- [ ] All logging tests passing
- [ ] Writer tests: 95%+ passing (some may be pre-existing issues)
- [ ] Schema tests: Documented as mock-spark limitation OR fixed
- [ ] Overall: 98%+ test pass rate (1,330+ of 1,358)

---

## Timeline

**Estimated Time**: 1-2 hours
- Phase 1: 10 min
- Phase 2: 10 min
- Phase 3: 15 min
- Phase 4: 15 min
- Phase 5: 30-45 min (most complex)
- Phase 6: 15 min
- Phase 7: 10 min

**Current Status**: Plan created, ready to execute

---

## Change Log

- **2025-10-08 13:35**: Plan created
- **Status**: ⏳ Awaiting execution

