---
name: sparkless-4-pyspark-aligned-tests
overview: Align remaining tests with sparkless 4.0.1 and real PySpark behavior so the suite no longer depends on mock-only APIs or internals.
todos:
  - id: baseline-failures
    content: Re-run pytest -n 10 in .venv39-s4 and categorize failures into read-only mocks, engine config, expressions, data validation, storage, and imports.
    status: pending
  - id: refactor-readonly-mock-tests
    content: Refactor tests that patch core Spark methods (sql, count, createDatabase) to use mocks at the abstraction layer or helper SparkSessions instead.
    status: pending
  - id: fix-engine-config-usage
    content: Update guide/example tests and scripts to use real Spark sessions plus configure_engine so engine_config recognizes the session type.
    status: pending
  - id: rewrite-expression-internal-tests
    content: Rewrite validation/expression tests to assert on DataFrame behavior or outputs instead of internals like expr dunder methods or classes.
    status: pending
  - id: align-createDataFrame-tests
    content: Update tests to respect stricter createDataFrame argument/row shape rules or explicitly assert the new errors when testing bad inputs.
    status: pending
  - id: normalize-sparkless-exception-imports
    content: Sweep and correct all sparkless exception imports to use current public modules and contracts.
    status: pending
  - id: fix-writer-storage-dependencies
    content: Refactor writer/storage code and related tests so they no longer require SparkSession.storage, only PySpark-compatible APIs.
    status: pending
  - id: finalize-test-suite
    content: Iteratively re-run pytest -n 10 and, when green, document any intentional skips or environment-dependent tests.
    status: pending
isProject: false
---

### Goal

Ensure all tests exercise logic and behavior that match real PySpark (or its deliberate sparkless mirror), removing reliance on mock-only attributes, internal implementation details, or deprecated sparkless APIs so that the test suite is stable under sparkless 4.0.1 and real Spark.

### Plan

- **1. Re-baseline test failures**
  - Run `pytest -n 10` in `.venv39-s4` and capture the current summary of failures and errors.
  - Categorize each failing test into one of these buckets:
    - Read-only method mocking (attempting to overwrite `sql`, `count`, `createDatabase`, etc.).
    - Engine configuration / session type issues ("Unknown Spark session type: builtins").
    - Expression internals vs behavior (tests asserting on dunder methods or concrete expr classes).
    - Stricter `createDataFrame` and argument validation (row shape, data type, etc.).
    - Writer/storage integration still expecting `SparkSession.storage` or mock-only helpers.
    - Any remaining sparkless import/API drift.
- **2. Fix tests that patch core Spark methods directly**
  - Identify tests that use `patch.object(df, "count", ...)`, `patch.object(spark, "sql", ...)`, or `patch.object(catalog, "createDatabase", ...)` on real PySpark/sparkless objects.
  - Refactor these tests to be PySpark-aligned by:
    - Using helper mocks from `tests/test_helpers/mocks.py` (mock `SparkSession` / `DataFrame` with `count`/`sql` as normal mock attributes) and passing these into the functions under test, **or**
    - Patching at the abstraction layer (e.g., `StorageManager.get_table_info`, `QueryBuilder.*`) instead of the engine methods.
  - Keep tests focused on verifying how pipeline code reacts to errors, not on how PySpark itself is implemented.
- **3. Fix engine configuration and custom session tests**
  - For guide/example tests and scripts that construct their own “SparkSessions” (e.g., guide tests, stepwise guide scripts), align them with real engine behavior:
    - Use the shared `spark_session` fixture from `tests/conftest.py` (configured for sparkless or real Spark) where possible.
    - Call `pipeline_builder.engine_config.configure_engine(...)` exactly once per session to register functions/types/exceptions before building pipelines.
    - For any custom/mocked sessions, explicitly call `configure_engine(functions=..., types=..., analysis_exception=...)` so `engine_config` doesn’t see an unknown `builtins` session type.
- **4. Make expression tests assert behavior, not internals**
  - Locate tests that assert on expression internals (e.g., `hasattr(expr, "__and__")` or specific expr class names).
  - Rewrite them to:
    - Construct minimal DataFrames, apply the generated expressions/rules, and assert on observable outcomes (row counts, filtered sets, or resulting metrics).
    - Optionally assert on generated SQL strings only if they are part of the public contract and stable across PySpark and sparkless.
  - Document in comments that only behavioral compatibility with PySpark is guaranteed, not specific expr implementation details.
- **5. Align tests with stricter `createDataFrame` semantics**
  - For tests that fail with new sparkless 4 errors like `LENGTH_SHOULD_BE_THE_SAME` or `TypeError: data must be a list`:
    - If the test is meant to represent a valid user workflow, fix the test data so it matches PySpark’s expectations (rows all same length, `data` is list or DataFrame, schemas are correct).
    - If the test is supposed to exercise bad inputs, update it to explicitly `pytest.raises(...)` the new, stricter error (and optionally match key parts of the message) to lock in the contract.
- **6. Refine sparkless exception imports and usage**
  - Sweep tests for sparkless exception imports (`AnalysisException`, `IllegalArgumentException`, etc.).
  - Update all imports to use the current public modules (e.g., `sparkless.sql.utils`, `sparkless.errors`) and avoid relying on deprecated top-level attributes.
  - Where possible, assert on exception **types and messages** in a way that remains valid for both PySpark’s and sparkless’s representations.
- **7. Fix writer/storage integration to avoid `SparkSession.storage`**
  - Trace integration failures in write-mode tests to their source in writer/storage code.
  - Refactor the writer/storage layer to:
    - Use standard PySpark-style APIs (`spark.sql`, `spark.catalog`, `DataFrame.write.saveAsTable`, etc.) for schema/table management, and
    - Confine any remaining mock-only helpers to clearly guarded code paths (e.g., `if engine_name == "mock"`) that never execute under real Spark.
  - Update associated integration tests to assert on table existence, contents, and metrics (via SQL/catalog) instead of assuming a `storage` attribute on the session.
- **8. Iterate test runs and finalize**
  - After implementing each cluster of fixes above, re-run `pytest -n 10` in `.venv39-s4` and cross off resolved failure groups.
  - Continue until:
    - All non-skipped tests pass, or
    - Any remaining failures are explicitly documented as expected (e.g., requiring external dependencies or environments not present locally).
  - Optionally run a smaller subset (e.g. guide and dependency tests) under `SPARK_MODE=real` to validate changes against an actual PySpark runtime.

