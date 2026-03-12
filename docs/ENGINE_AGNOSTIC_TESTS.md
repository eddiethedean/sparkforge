# Engine-agnostic tests: removing PySpark vs sparkless branches

Sparkless is intended to provide the same methods, objects, and functions as PySpark. Tests should not branch on `SPARK_MODE` or engine type for F (functions) or types; they should use the configured engine via `pipeline_builder.compat`.

## Pattern

**Instead of:**
```python
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType
else:
    from sparkless.sql import functions as F  # type: ignore[import]
    from sparkless.spark_types import StringType, StructField, StructType  # type: ignore[import]
```

**Use:**
```python
from pipeline_builder.compat import F, types

StringType = types.StringType
StructField = types.StructField
StructType = types.StructType
# use F and types.* everywhere; same code runs in mock and real mode
```

For functions-only:
```python
from pipeline_builder.compat import F
# or: from pipeline_builder.functions import get_default_functions; F = get_default_functions()
```

Root `conftest.py` already calls `configure_engine(...)` at import time based on `SPARK_MODE`, so `F` and `types` from `pipeline_builder.compat` are always the right engine.

## When to keep engine-specific logic

- **Infrastructure**: Tests that require a real JVM/Delta JAR, real PostgreSQL, or a real cluster should keep their skip (e.g. `@pytest.mark.skipif(not HAS_DELTA_PYTHON, ...)` or "Requires real PySpark + Delta").
- **Detection tests**: Tests that explicitly assert "we are using PySpark" vs "we are using mock" (e.g. `test_compat_helpers.py` detection tests) can keep a conditional skip or branch if the test’s purpose is to verify detection.

## When to remove engine-specific logic

- **Module-level skip "designed for mock only" / "real only"**: Remove if the test only uses `spark_session` (or `mock_spark_session`) and F/types from compat. The same test can run in both modes.
- **Per-test branches that only choose `pyspark.sql.functions` vs `sparkless.sql.functions`**: Replace with `F` from compat.
- **Per-test branches that only choose `pyspark.sql.types` vs `sparkless.spark_types`**: Replace with `types` from compat (e.g. `types.StructType`, `types.StringType`).

## Files already refactored (use compat, no engine branches for F/types)

- `tests/unit/test_validation_simple.py` – compat types; removed mock-only skip.
- `tests/unit/test_validation_standalone.py` – compat F + types; removed mock-only skip and all per-test F branches.
- `tests/system/test_utils.py` – compat F + types.
- `tests/builder_tests/conftest.py` – compat types.
- `tests/integration/test_validation_integration.py` – compat F + types.
- `tests/system/test_multi_schema_support.py` – compat F + types.

## Files still with engine-specific imports or branches

These still have `if SPARK_MODE == "real": pyspark ... else: sparkless` (or similar) for F or types. Refactor them to use `from pipeline_builder.compat import F, types` and, where applicable, remove mock-only/real-only skips.

- `tests/unit/test_models_new.py`
- `tests/unit/test_execution_engine_simple.py`
- `tests/unit/test_validation_enhanced.py`
- `tests/unit/test_validation.py`
- `tests/unit/test_validation_mock.py`
- `tests/unit/test_validation_additional_coverage.py`
- `tests/unit/test_validation_only_guide_examples.py`
- `tests/unit/test_validation_edge_cases.py`
- `tests/unit/test_validation_enhanced_simple.py`
- `tests/unit/test_trap_1_silent_exception_handling.py`
- `tests/unit/test_trap_4_broad_exception_catching.py`
- `tests/unit/test_pipeline_builder_comprehensive.py`
- `tests/unit/test_pipeline_runner_write_mode.py`
- `tests/unit/test_writer_core_simple.py`
- `tests/unit/test_writer_comprehensive.py`
- `tests/unit/test_sparkforge_working.py`
- `tests/unit/test_working_examples.py`
- `tests/unit/test_models_simple.py`
- `tests/unit/test_execution_final_coverage.py`
- `tests/unit/test_execution_100_coverage.py`
- `tests/unit/test_execution_comprehensive.py`
- `tests/unit/test_execution_write_mode.py`
- `tests/unit/test_edge_cases.py`
- `tests/unit/test_bronze_rules_column_validation.py`
- `tests/unit/test_compat_helpers.py` (detection tests may keep conditional logic)
- `tests/unit/execution/test_step_executors.py`
- `tests/unit/sql_source/test_sql_source_execution.py`
- `tests/unit/sql_source/test_sql_source_builder.py`
- `tests/integration/test_execution_engine.py`
- `tests/integration/test_execution_engine_new.py`
- `tests/integration/test_write_mode_integration.py`
- `tests/integration/test_step_execution.py`
- `tests/integration/test_pipeline_execution.py`
- `tests/integration/test_pipeline_builder.py`
- `tests/integration/test_stepwise_execution.py`
- `tests/system/test_simple_real_spark.py`
- `tests/system/test_bronze_no_datetime.py`
- `tests/system/test_auto_infer_source_bronze.py`
- `tests/system/test_full_pipeline_with_logging.py`
- `tests/system/test_schema_evolution_without_override.py`
- `tests/system/test_edge_case_workflows.py`
- `tests/system/test_dataframe_access.py`
- `tests/system/system_test_helpers.py`
- `tests/builder_tests/test_data_quality_pipeline.py`
- `tests/builder_tests/test_healthcare_pipeline.py`
- `tests/builder_tests/test_marketing_pipeline.py`
- `tests/builder_pyspark_tests/conftest.py`
- `tests/builder_pyspark_tests/test_*.py` (multiple)
- `tests/test_type_imports_helper.py`
- `tests/test_helpers/data_generators.py`
- `tests/integration/conftest.py`
- `tests/security/security_tests.py`

Run `grep -r "SPARK_MODE\|sparkless\.\|pyspark\.sql" tests --include="*.py"` to find any remaining conditional engine logic.
