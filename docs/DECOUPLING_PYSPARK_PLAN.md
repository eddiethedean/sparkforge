# Sparkforge PySpark Decoupling Plan

Version: 1.0
Status: In Progress

## Objective
Enable Sparkforge to run on either PySpark or mock-spark without changing user code by introducing a compatibility layer and refactoring imports to use it.

## Approach Overview
- Add `pipeline_builder.compat` that exposes `SparkSession`, `DataFrame`, `Column`, `functions as F`, and `types`.
- Resolve engine via env var `SPARKFORGE_ENGINE={pyspark|mock}` → PySpark (if importable) → mock-spark.
- Refactor library imports to use `pipeline_builder.compat` instead of `pyspark.*` directly.
- Provide safe fallbacks for persistence features when running on mock-spark.

## Compat Layer (pipeline_builder/compat.py)
- Exports: `SparkSession`, `DataFrame`, `Column`, `F` (functions), `types` (StructType, etc.).
- Helpers: `is_mock_spark()`, `compat_name()`, `require_pyspark(msg)`.
- Shims:
  - `desc`, `col`, `lit`, `current_timestamp` → fall back to string-expr or simple placeholders in mock.

## Refactors
- Replace direct PySpark imports across:
  - `pipeline_builder/pipeline/*`
  - `pipeline_builder/validation/*`
  - `pipeline_builder/writer/*`
  - `pipeline_builder/table_operations.py`
  - `pipeline_builder/functions.py`, `pipeline_builder/types.py`, `pipeline_builder/models/types.py`
- Rule: import from `pipeline_builder.compat` (e.g., `from pipeline_builder.compat import SparkSession, DataFrame, F`).

## Writer/Storage Behavior
- Mock mode policy:
  - Default: no-op persistence with structured results and warnings via logger.
  - Config switch `WriterConfig.enable_mock_persistence=False` → raise `WriterUnsupportedInMock` with suggestions.
- Filtering/ordering:
  - PySpark: `col()/lit()/desc()`.
  - Mock: string expressions and `orderBy("created_at", ascending=False)`.

## Validation and Functions
- Default to compat `F` when `functions` is not provided.
- Keep injection of `MockFunctions` supported for tests.

## Testing Strategy
- Fixtures: use dict-row DataFrames (mock-spark friendly); provide `mock_functions`.
- Markers:
  - Keep `unit`, `integration`, `system`.
  - Add `requires_pyspark` for tests that depend on real Spark/Delta.
- CI Matrix:
  - Mock-only job: install `mock-spark`, run `pytest -m "not requires_pyspark"`.
  - PySpark job: install PySpark + Delta, run full suite.
- Coverage:
  - Collect coverage separately per job and combine artifacts.
- Regression tests:
  - Compat resolution (env override, fallback).
  - Writer mock policy (no-op vs hard-fail) with messages.
  - Validation defaults to compat `F`.
  - Filtering/ordering parity in mock.

## Docs & Examples
- Update examples to prefer `from pipeline_builder.compat import F`.
- Add "Mock Mode" section: engine selection, limitations, and recommended patterns.

## Milestones & Acceptance
- M1: Compat module + refactor pipeline/validation; mock-only tests green.
- M2: Writer mock policy + tests green in mock job.
- M3: PySpark job green; docs/examples updated; combined coverage ≥ current baseline.

## Risks & Mitigations
- PySpark-specific edge cases (e.g., window functions):
  - Mark `requires_pyspark`; provide simplified mock equivalents when feasible.
- Divergent writer semantics in mock:
  - Clear logs/warnings; explicit config to hard-fail.

## Rollout
- Default behavior unchanged for PySpark users.
- `SPARKFORGE_ENGINE` documented for explicit control.

---

## Progress Log

- [x] Added `pipeline_builder/compat.py` with engine selection, exports, and shims.
- [x] Refactored `validation/data_validation.py` to import `Column, DataFrame` from compat.
- [x] Simplified `functions.get_default_functions()` to return compat `F`.
- [x] Refactored `pipeline/builder.py` to use compat `DataFrame, SparkSession, F`.
- [x] Adjusted validation select to pass Column objects for mock-spark.
- [ ] Refactor pipeline builder to compat imports.
- [ ] Refactor writer modules to compat + mock-safe paths.
- [ ] Update examples/docs to prefer compat `F`.
- [ ] Add `requires_pyspark` marks where needed and configure CI matrix.

