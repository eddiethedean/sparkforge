# Mock-Spark → SparkForge Compatibility Plan (for upstream)

This note is aimed at mock-spark maintainers. It summarizes the remaining friction points we see when running SparkForge on `mock-spark==3.1.0` under Python 3.8, along with concrete suggestions to close the gap.

## Key Pain Points

### 1. Datetime Handling in Polars Backend
- **Symptoms:** `F.to_timestamp`/`F.to_date` on string columns (e.g. `"2024-01-01 10:00:00"`) frequently raise `polars.exceptions.SchemaError` or `ComputeError`.
- **Impact:** High; forces us to keep tests skipped or resort to string slicing (`substring`) in system tests.
- **Suggested Fix:**
  - Permit explicit format strings in `F.to_date`/`F.to_timestamp` (similar to PySpark).
  - Expand `mock_spark.compat.datetime.to_date_str`/`to_timestamp_str` so they convert strings → timestamps internally before formatting, without type errors on Polars.
  - Provide explicit guidance/examples for typical Spark transformations (order of operations, format strings) to avoid Polars failures.

### 2. Python 3.8 Compatibility
- **Symptoms:** Import fails without workarounds because `typing.TypeAlias` is non-subscriptable and `collections.abc.Mapping[str, int]` is unavailable.
- **Current Workaround:** SparkForge ships a `sitecustomize.py` patch that monkey-patches `typing.TypeAlias`, adds `__class_getitem__` to ABCs, and stubs `mock_spark.backend.duckdb`.
- **Suggested Fix:**
  - Vendor a lightweight 3.8 fallback directly in mock-spark (e.g. reuse `typing_extensions.TypeAlias` and register generics).
  - Guard optional DuckDB imports more defensively (e.g. try/except around `import mock_spark.backend.duckdb.storage`).
  - Document the minimum supported Python more clearly; if 3.8 support is strategic, consider a CI job covering it.

### 3. Loss of Delta Schema Evolution
- **Symptoms:** `DeltaTable.forPath(...).toDF().write.option("mergeSchema","true").mode("append")` now succeeds (thanks!), but earlier 3.0 regressions slipped through undetected.
- **Request:** Keep regression tests that mimic real Delta append scenarios, ideally running against both DuckDB and Polars backends.
  - If schema evolution still has edge cases, expose feature flags we can query to decide whether to `xfail` in mocks.

### 4. Backend Selection & Visibility
- **Observation:** Automatic Polars fallback is great, but we need insight into which backend is active to reason about behaviour.
- **Request:**
  - Provide a public `mock_spark.backend.get_active_backend()` or similar.
  - Emit clear warnings when the backend switches (e.g. DuckDB missing) so users can diagnose skips/regressions quickly.

### 5. Release Notes & Migration Guides
- **Issue:** 3.0.0 and 3.1.0 lacked detailed changelogs; we diffed packages to understand breaking changes.
- **Request:** Publish migration notes with each major release covering:
  - Added/removed exports.
  - Backend defaults (DuckDB → Polars).
  - Known compatibility gaps (datetime parsing, Python version nuances).

## Suggested Action Items for Mock-Spark
| Priority | Area | Action |
| --- | --- | --- |
| 🔴 | Datetime parsing | Allow format strings on `to_date`/`to_timestamp`; extend compat helpers to accept strings safely. |
| 🔴 | Python 3.8 | Ship built-in shims for `TypeAlias`/generics; guard DuckDB imports. |
| 🟠 | Backend introspection | Add API to query active backend/type; log backend selection. |
| 🟡 | Testing | Add regression coverage for datetime conversions and Delta schema evolution on Polars. |
| 🟢 | Documentation | Provide versioned release notes & migration guides. |

## How We Can Help
- Happy to contribute sample datasets or minimal reproduction scripts (datetime pipelines, failing tests).
- Can run preliminary builds or branches in our CI to confirm cross-compatibility.

## Contact
- SparkForge maintains this document; reach out via our issue tracker if more detail would help. We’d love to collaborate on closing these gaps.

