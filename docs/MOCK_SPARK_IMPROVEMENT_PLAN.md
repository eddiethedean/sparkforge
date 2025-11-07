# Mock-Spark 3.1.0 Upgrade Notes

mock-spark 3.1.0 incorporates the fixes we flagged after moving to 3.0.0. This document records what changed, what we verified, and the cleanups we performed in SparkForge.

## Summary of Upstream Fixes
- **Delta schema evolution restored:** `mergeSchema` appends now succeed against the Polars backend. We re-enabled the full Delta Lake test suite without xfails.
- **Polars backend refinements:** Storage/materialization remains Polars-based with fewer type assertion crashes during import (TypeAlias / collections generics now shimmed via `sitecustomize.py`).
- **Date/time behaviour partially improved:** 3.1.0 adds helpers (`mock_spark.compat.datetime`) but direct `F.to_date` calls against string columns still surface Polars parsing issues in our workload; we continue to format dates manually in system tests.

## SparkForge Changes for 3.1.0
- Bumped dependency pins (`mock` and `test` extras) to `mock-spark==3.1.0` in `pyproject.toml`.
- Added `sitecustomize.py` to keep Python 3.8 compatible (patches `typing.TypeAlias`, generic ABC subscriptions, and stubs the optional DuckDB backend).
- Kept pragmatic string-based date helpers in system tests (`substring` fallback) because Polars still rejects our timestamp format without extra shims.
- Deleted the old duplicate `builder_tests/` package to resolve pytest import mismatches.
- Re-enabled the Delta schema evolution assertion in `tests/system/test_delta_lake.py`.
- Ran `pytest -n 8` to confirm the full suite (including Delta tests) passes with 3.1.0.

## Checklist
- [x] Upgrade local environment (`pip install mock-spark==3.1.0`).
- [x] Ensure compatibility shims load before `mock_spark` imports.
- [x] Validate full pytest suite (with Delta Lake) passes under 3.1.0.
- [x] Document the upgrade and close the open workstreams from the 3.0.0 plan.

## Follow-up
- Track upstream guidance on the new compat helpers (`to_date_str` / `to_timestamp_str`) and re-evaluate once they handle mixed string/timestamp inputs without manual fallbacks.
- Remove the Python 3.8 shims once we move the project baseline to 3.10+.

