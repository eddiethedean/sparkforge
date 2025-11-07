# Mock-Spark 3.1.0 Follow-Up Tasks (Python 3.8 Baseline)

Despite the successful migration to `mock-spark==3.1.0`, a few gaps remain before the developer experience is fully frictionless on Python 3.8. This document tracks the outstanding work, open questions, and references for future iterations.

## 1. Datetime Handling Consistency
- **Current state:** System tests fall back to `F.substring` to derive `event_date` because `F.to_date` against string inputs can still yield Polars `SchemaError` / `ComputeError` (missing format).
- **Goal:** Adopt the official compat helpers (e.g. `mock_spark.compat.datetime.to_date_str`) or update our transformation utilities so that both real Spark and mock-spark return typed dates without manual string slicing.
- **Actions:**
  - Evaluate whether upstream plans to accept format strings in native `F.to_date` again or document a preferred migration path.
  - Prototype a helper in `pipeline_builder.compat` that gracefully handles both engines; remove string slicing from tests once the helper is stable.
  - Ensure downstream pipelines consuming these dates are tolerant of proper `date`/`timestamp` types.

## 2. Python 3.8 Compatibility Shim Maintenance
- **Current state:** `sitecustomize.py` patches `typing.TypeAlias`, adds `__class_getitem__` on collections ABCs, and stubs `mock_spark.backend.duckdb`. This keeps mock-spark 3.1.0 importing cleanly on Python 3.8.
- **Goal:** Keep the shim lean, well-documented, and exercised by automated tests while 3.8 remains the baseline.
- **Actions:**
  - Add a lightweight unit test ensuring `typing.TypeAlias`, `collections.abc.Mapping[str, int]`, and `importlib.import_module("mock_spark.compat.datetime")` behave as expected under the shim.
  - Monitor mock-spark release notes for upstream fixes; when they ship native support for Python 3.8, simplify or remove the shim.
  - Revisit the shim once the project upgrades to Python 3.10+, at which point most of these patches become unnecessary.

## 3. Upstream Collaboration & Tracking
- **Current state:** 3.1.0 shipped without detailed release notes; improvements were inferred via diffing and smoke tests.
- **Goal:** Engage with the mock-spark maintainers so that future changes (especially around Polars backend behaviour and datetime parsing) are visible early.
- **Actions:**
  - Open issues summarising our remaining pain points (date parsing, Python 3.8 patches) and ask about roadmap timelines.
  - Subscribe to repository releases or watch relevant discussions to avoid surprise regressions.

## 4. Documentation & Communication
- Ensure contributors know about:
  - The required `sitecustomize.py` shim for Python 3.8.
  - The current datetime workaround.
  - The fact that Delta schema evolution now passes and should remain enabled in tests.
- Consider adding a “mock-spark working notes” section to the contributor guide or README that links to this document and the main upgrade notes.

## Next Review Checkpoint
- Reassess after the next mock-spark release or after we trial the compat helpers in production tests.
- Update this document once the datetime workaround or 3.8 shim is no longer required.

