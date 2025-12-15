## Mock-Spark Test Run Report (`pytest -n 10` / `-n -1`, `SPARK_MODE=mock`)

Date: 2025-12-15  
Command:

```bash
cd /Users/odosmatthews/Documents/coding/sparkforge
source activate_env.sh
SPARK_MODE=mock SPARKFORGE_ENGINE=mock python -m pytest tests -n 10 -v
```

### Summary (latest runs: `-n 10`, `-n -1`, and serial)

- **Serial command (no xdist)**:

  ```bash
  cd /Users/odosmatthews/Documents/coding/sparkforge
  source activate_env.sh
  SPARK_MODE=mock SPARKFORGE_ENGINE=mock python -m pytest tests -v
  ```

  - **Collected**: 1,766 tests  
  - The run progressed further than the parallel runs but still exited with a non-zero status (signal 134) once it reached the same problematic areas (schema evolution, write modes, validation, mock storage/Delta behaviors). Pytest did not reach a clean final summary line before termination.

- **Parallel command (`-n -1`, auto workers)**:

  ```bash
  cd /Users/odosmatthews/Documents/coding/sparkforge
  source activate_env.sh
  SPARK_MODE=mock SPARKFORGE_ENGINE=mock python -m pytest tests -n -1 -v
  ```

- **Total collected**: 1,766 tests  
- **Result**: Run aborted by `xdist` due to repeated worker crashes  
- **xdist status**: `xdist: maximum crashed workers reached: 40`  
- **Reported failing tests at time of abort**: ~90 (very similar set to the `-n 10` run; see buckets below)

As with the `-n 10` run, these numbers reflect the state **when the run stopped**, not a clean, converged result.

### High-level failure buckets

From the short summary at the end of the `-n 10` and `-n -1` runs, failures fall broadly into these categories (the specific test names listed below are representative; the exact set at abort time is very similar between runs):

- **Schema evolution & write-mode behavior**
  - `tests/system/test_schema_evolution_without_override.py::TestSchemaEvolutionWithoutOverride::*`
  - `tests/integration/test_write_mode_integration.py::TestWriteModeIntegration::*`
  - `tests/unit/test_execution_write_mode.py::TestExecutionEngineWriteMode::*`
  - `tests/unit/test_pipeline_runner_write_mode.py::TestPipelineRunnerWriteMode::*`
  - `tests/unit/test_writer_comprehensive.py::TestWriterComprehensive::test_writer_schema_evolution_settings`
  - These are exercising:
    - Silver/Gold overwrite vs append in different execution modes (initial, incremental, full-refresh).
    - Consistency between the engine’s logical write mode and the actual Spark writer mode.
    - End-to-end behavior when schema changes across runs (esp. Silver/Gold, Delta vs parquet).

- **Delta Lake semantics & performance**
  - `tests/system/test_delta_lake.py::TestDeltaLakeComprehensive::*`
  - Failures cover:
    - `test_delta_lake_merge_operations`
    - Optimization and performance characteristics
    - Data quality constraints and history/metadata expectations
  - In mock-spark mode, these rely on the mock’s emulation of Delta behavior, and some expectations (merge semantics, optimization behavior, or history APIs) are not being met.

- **Real-Spark–style operations running under mock-spark**
  - `tests/system/test_simple_real_spark.py::TestRealSparkOperations::*`
  - `tests/system/test_dataframe_access.py::TestDataFrameAccess::test_dataframe_operations`
  - These tests assert behaviors (e.g. metadata operations, performance, error handling) originally validated against real PySpark + Delta; under mock-spark, the emulated engine diverges:
    - DataFrame operations and metadata APIs behave differently.
    - Performance/metrics semantics don’t align with real Spark.

- **Validation & data-quality behavior (mock vs real)**
  - `tests/system/test_utils.py::TestDataValidation::*`
  - `tests/system/test_utils.py::TestDataTransformationUtilities::*`
  - `tests/system/test_utils.py::TestPerformanceWithRealData::*`
  - `tests/unit/test_validation.py::*` and `tests/unit/test_validation_standalone.py::*`
  - `tests/integration/test_validation_integration.py::TestAssessDataQuality::test_dataframe_with_rules`
  - These cover:
    - `apply_column_rules` (basic, multiple columns, complex rules).
    - `assess_data_quality` metrics and thresholds.
    - Behavior on large/complex datasets.
  - The mock-spark execution path is producing different counts, rule application behavior, or error types than the tests expect.

- **Mock-spark edge cases & storage APIs**
  - `tests/unit/test_edge_cases.py::TestEdgeCases::*` (several tests including `test_empty_dataframe_operations`, `test_null_value_handling`, `test_large_dataset_operations`, `test_complex_schema_operations`, `test_error_conditions`, `test_concurrent_operations`, `test_memory_management`, `test_session_edge_cases`, `test_storage_edge_cases`)
  - `tests/unit/test_sparkforge_working.py::TestSparkForgeWorking::*`
  - These tests explicitly probe:
    - DataFrame creation and operations with tricky schemas / data (empty, null-heavy, large datasets, complex types).
    - Error behavior (e.g. invalid schemas, nonexistent tables, invalid column references).
    - Mock-spark’s storage API (schema/table creation, DuckDB-backed limits, etc.).
  - In this run, a number of these edge-case guarantees are not being met in mock mode (e.g. exceptions not raised where expected, catalog/storage divergence, or DataFrame behaviors differing from real Spark).

- **End-to-end pipeline + logging with mixed modes**
  - `tests/system/test_full_pipeline_with_logging_variations.py::*`
  - `tests/system/test_complete_pipeline.py::TestCompletePipeline::test_bronze_to_silver_to_gold_pipeline`
  - Builder tests with logging:
    - `tests/builder_tests/test_customer_analytics_pipeline.py::TestCustomerAnalyticsPipeline::test_customer_analytics_logging`
    - `tests/builder_tests/test_financial_pipeline.py::TestFinancialPipeline::test_financial_audit_logging`
    - `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_healthcare_logging`
    - `tests/builder_tests/test_multi_source_pipeline.py::TestMultiSourcePipeline::test_multi_source_logging`
    - `tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_supply_chain_logging`
    - `tests/builder_tests/test_ecommerce_pipeline.py::TestEcommercePipeline::test_logging_and_monitoring`
  - These require the mock engine to emulate:
    - Correct write modes per layer and per run (initial/incremental).
    - Delta-like schema evolution behavior.
    - Detailed logging/metrics that match real Spark behavior.
  - Failures indicate discrepancies between the mock engine’s current behavior and the expectations derived from real-Spark runs.

- **Execution engine & mode integration**
  - `tests/integration/test_execution_engine.py::TestExecutionEngine::test_execute_pipeline_success`
  - `tests/unit/test_execution_100_coverage.py::*` and `tests/unit/test_execution_comprehensive.py::*`
  - `tests/unit/test_trap_1_silent_exception_handling.py::*`
  - These tests validate:
    - Correct step execution and mode handling (initial, incremental, validation-only).
    - Error propagation and logging behavior.
    - That certain “trap” scenarios (silent exception handling, defaults, fallbacks) behave as guarded by the tests.
  - Under mock-spark, the engine + mock DF behavior diverges from expectations (e.g. rows processed, write modes, or validation rates).

### Observed systemic issues in mock-spark mode

Based on the failure list and the logged output (especially around `ExecutionEngine` and `apply_column_rules`), the main systemic themes are:

1. **Write-mode semantics under mock-spark**
   - Many failures centre around whether Silver/Gold steps use `append` vs `overwrite` in:
     - Initial vs incremental vs full-refresh modes.
     - End-to-end pipeline execution (`PipelineRunner`, Builder tests, system tests).
   - The engine’s write-mode decisions and the mock engine’s behavior are not always aligned with what the tests assert (which are written against the intended production semantics).

2. **Schema evolution & table behavior in the mock engine**
   - Tests expecting Delta-like schema evolution (especially in INITIAL runs without explicit `schema_override`) fail under mock-spark.
   - In some cases, the mock storage/catalog doesn’t handle DELETE / overwrite / recreate in the same way as real Delta/Spark, leading to:
     - Column-count mismatches.
     - Tables not being updated as expected.

3. **Validation and data-quality metrics**
   - A wide swath of `validation` tests (unit, integration, system) fail in mock mode, indicating:
     - Differences in how filters are applied.
     - Differences in counts of valid/invalid rows.
     - Potential discrepancies in how rules are combined or applied across columns.

4. **Mock-spark storage & catalog APIs**
   - Storage-related tests (especially in `TestEdgeCases` and `TestSparkForgeWorking`) highlight:
     - Differences in schema/table existence checks.
     - Edge-case handling around long names, complex identifiers, and catalog sync.

5. **Real-Spark–style tests executed under mock-spark**
   - Some tests in `tests/system/test_simple_real_spark.py` and related modules are fundamentally oriented around **real PySpark + Delta** behavior and performance characteristics.
   - In mock mode, these are expected to behave differently, and the failures reflect that divergence rather than a simple bug.

### Notes and next steps

- This report is **observational only**: no code changes were made in response to these mock-spark failures.
- For future improvement work, the failure buckets above can be used to:
  - Decide which areas of the mock engine to improve (e.g. write modes, validation semantics, storage API surface).
  - Decide which tests should be:
    - Strictly real-Spark only.
    - Engine-aware (different expectations for mock vs real).
    - Or explicitly skipped in mock mode when they are intended to validate real-Spark-only behavior.


