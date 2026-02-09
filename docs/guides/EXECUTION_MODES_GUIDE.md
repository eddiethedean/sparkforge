# Execution Modes Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Bronze Source Wiring](#bronze-source-wiring)
3. [Initial Load](#initial-load)
4. [Incremental Load](#incremental-load)
5. [Full Refresh](#full-refresh)
6. [Validation Only](#validation-only)
7. [Typical Workflow](#typical-workflow)
8. [Result Object](#result-object)
9. [Related Guides](#related-guides)

---

## Introduction

After building a pipeline with `builder.to_pipeline()`, you run it with one of four modes: **initial load**, **incremental**, **full refresh**, or **validation only**. Each mode changes whether tables are overwritten or appended and whether data is written at all. This guide describes each mode, how to wire bronze sources, and how to read the result.

## Bronze Source Wiring

For steps defined with `with_bronze_rules`, you supply input DataFrames at run time via `bronze_sources`: a dict mapping **bronze step name** → **DataFrame**.

```python
result = pipeline.run_initial_load(
    bronze_sources={
        "raw_events": events_df,   # key must match bronze step name
        "raw_orders": orders_df,   # one entry per in-memory bronze step
    }
)
```

- Keys must match the `name` you used in `with_bronze_rules`.
- If a bronze step is defined with **SQL source** (`with_bronze_sql_source`), the framework reads from the database at run time; you do **not** pass that step name in `bronze_sources`. See [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md).

## Initial Load

**When to use:** First-time load, full refresh of existing data, or rebuilding tables from scratch.

**Behavior:** Overwrites existing silver and gold Delta tables with the result of this run.

**API:** `pipeline.run_initial_load(bronze_sources={...})`

```python
result = pipeline.run_initial_load(
    bronze_sources={"raw_events": source_df}
)

if result.status.value == "completed":
    print(f"Bronze rows processed: {result.bronze_results['raw_events']['rows_processed']}")
    print(f"Silver rows written: {result.silver_results['processed_events']['rows_written']}")
    print(f"Gold rows written: {result.gold_results['user_metrics']['rows_written']}")
```

## Incremental Load

**When to use:** Daily or hourly updates; process only new or changed data.

**Behavior:** Appends to existing silver and gold tables. The framework filters bronze data using the `incremental_col` you set on the bronze step (e.g. only rows with `timestamp` greater than the max already processed).

**API:** `pipeline.run_incremental(bronze_sources={...})`

```python
# New data only (e.g. last hour)
incremental_df = spark.createDataFrame(new_rows, ["user_id", "action", "timestamp", "value"])

result = pipeline.run_incremental(
    bronze_sources={"raw_events": incremental_df}
)

if result.status.value == "completed":
    print(f"New rows processed: {result.bronze_results['raw_events']['rows_processed']}")
```

You must have defined `incremental_col` on the bronze step for incremental filtering to apply.

## Full Refresh

**When to use:** Reprocess all data (e.g. after schema changes or data corrections) but keep the same API as initial load.

**Behavior:** Same as initial load — overwrites silver and gold tables with the result of this run.

**API:** `pipeline.run_full_refresh(bronze_sources={...})`

```python
result = pipeline.run_full_refresh(
    bronze_sources={"raw_events": full_source_df}
)
```

## Validation Only

**When to use:** Test validation rules, run quality checks without writing, or develop/debug without touching tables.

**Behavior:** Runs validation on bronze (and downstream steps) but does **not** write to any Delta tables.

**API:** `pipeline.run_validation_only(bronze_sources={...})`

```python
result = pipeline.run_validation_only(
    bronze_sources={"raw_events": source_df}
)

print(f"Validation rate: {result.bronze_results['raw_events']['validation_rate']}")
```

## Typical Workflow

Common pattern: one initial load to backfill, then repeated incremental loads.

```python
# One-time: load historical data
initial_result = pipeline.run_initial_load(
    bronze_sources={"raw_events": historical_df}
)
if initial_result.status.value != "completed":
    raise RuntimeError("Initial load failed")

# Scheduled (e.g. hourly): append new data
incremental_result = pipeline.run_incremental(
    bronze_sources={"raw_events": new_data_df}
)
```

## Result Object

The object returned by `run_initial_load`, `run_incremental`, `run_full_refresh`, or `run_validation_only` is a **PipelineReport** (or compatible type). Use it as follows:

| Attribute | Description |
|-----------|-------------|
| `result.status` | Enum; use `result.status.value` for `"completed"` or `"failed"`. |
| `result.bronze_results` | Dict[step_name, info]. `info` has `rows_processed`, `validation_rate`, `duration`, `status`, etc. |
| `result.silver_results` | Dict[step_name, info]. `info` has `rows_written`, `rows_processed`, `validation_rate`, `duration`, etc. |
| `result.gold_results` | Dict[step_name, info]. Same shape as silver. |
| `result.metrics` | Aggregates: `total_steps`, `successful_steps`, `failed_steps`, `total_rows_processed`, `total_rows_written`, `total_duration`, etc. |
| `result.duration_seconds` | Total pipeline run time in seconds. |
| `result.errors` | List of error messages if any step failed. |

Example:

```python
result = pipeline.run_initial_load(bronze_sources={"raw_events": df})

print(result.status.value)           # "completed" or "failed"
print(result.duration_seconds)       # float
print(result.bronze_results["raw_events"]["rows_processed"])
print(result.bronze_results["raw_events"]["validation_rate"])
print(result.silver_results["processed_events"]["rows_written"])
print(result.metrics.total_rows_written)
```

## Related Guides

- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) — Define bronze, silver, and gold steps.
- [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md) — Bronze (and silver/gold) steps that read from SQL; no `bronze_sources` entry needed for those.
- [Common Workflows](./COMMON_WORKFLOWS_GUIDE.md) — Multi-source and initial+incremental patterns.
- [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md) — Log pipeline results (e.g. append result to LogWriter).
