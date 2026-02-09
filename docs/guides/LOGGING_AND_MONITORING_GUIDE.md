# Logging and Monitoring Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Setting Up LogWriter](#setting-up-logwriter)
3. [Logging Pipeline Runs](#logging-pipeline-runs)
4. [What Gets Logged](#what-gets-logged)
5. [Querying Logs](#querying-logs)
6. [Analytics and Trends](#analytics-and-trends)
7. [Related Guides](#related-guides)

---

## Introduction

The **LogWriter** writes pipeline execution results to a Delta Lake table so you can track runs, step-level metrics, validation rates, and durations. This guide covers setup, appending pipeline results, querying logs, and using built-in analytics—all within the guides directory.

## Setting Up LogWriter

LogWriter is part of SparkForge; no extra install is needed. Use the simplified API: pass `schema` and `table_name` when creating the writer.

```python
from pipeline_builder.writer import LogWriter

# Recommended: schema + table_name
log_writer = LogWriter(
    spark=spark,
    schema="my_schema",
    table_name="pipeline_logs",
)
```

- **schema:** The Delta schema (database) where the log table lives. It should match your pipeline schema or a dedicated logging schema.
- **table_name:** Name of the log table (e.g. `pipeline_logs`). The table is created on first write if it does not exist.

The log table is stored at `{schema}.{table_name}` (e.g. `my_schema.pipeline_logs`).

## Logging Pipeline Runs

After running a pipeline, pass the **result** (PipelineReport) to `append()`. You can optionally set a `run_id` to identify the run.

```python
# Run pipeline
result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

# Log the result
log_result = log_writer.append(result, run_id="initial_20240201")

if log_result.get("success", True):
    print(f"Logged {log_result.get('rows_written', 0)} rows to {log_result.get('table_fqn', '')}")
```

Use the same writer for initial and incremental runs; each call to `append()` adds rows to the log table.

```python
# Initial load
result1 = pipeline.run_initial_load(bronze_sources={"raw_events": historical_df})
log_writer.append(result1, run_id="initial_20240201")

# Incremental
result2 = pipeline.run_incremental(bronze_sources={"raw_events": new_data_df})
log_writer.append(result2, run_id="incremental_20240201_12")
```

## What Gets Logged

For each `append(report, run_id)` call, the writer writes:

- **One row per pipeline run** (or equivalent): run_id, pipeline_id, execution_id, mode (initial/incremental/full_refresh/validation_only), status, duration, total rows processed/written, start/end time.
- **One row per step** (bronze, silver, gold): run_id, step name, phase (bronze/silver/gold), duration, rows_processed, rows_written, validation_rate, status, etc.

So a pipeline with one bronze, one silver, and one gold step produces multiple log rows per run (one summary plus one per step, or similar depending on implementation). You can filter or aggregate by `run_id`, `phase`, or `step_name`.

## Querying Logs

**Display logs in the console:**

```python
# Show recent logs (default limit)
log_writer.show_logs()

# Show last N rows
log_writer.show_logs(limit=50)
```

**Query with Spark:** The log table is at `{schema}.{table_name}`. Use it like any Delta table:

```python
# DataFrame of all log rows
logs_df = spark.table("my_schema.pipeline_logs")

# Recent runs
from pyspark.sql import functions as F
recent = logs_df.orderBy(F.col("start_time").desc()).limit(100)

# By phase
bronze_logs = logs_df.filter(F.col("phase") == "bronze")

# By run_id
run_logs = logs_df.filter(F.col("run_id") == "initial_20240201")
```

If your LogWriter uses a storage manager that exposes `query_logs(limit=..., filters=...)`, you can use that for filtered queries; otherwise use `spark.table(...)` as above.

## Analytics and Trends

LogWriter provides helpers to analyze logged data over a time window (e.g. last N days).

**Quality trends** (e.g. validation rates over time):

```python
quality = log_writer.analyze_quality_trends(days=30)
# Use quality dict: validation rates, success rates, etc.
```

**Execution trends** (durations, throughput, success/failure):

```python
execution = log_writer.analyze_execution_trends(days=30)
# Use execution dict: durations, counts, trends
```

**Quality anomalies** (unusual validation or failure patterns):

```python
anomalies = log_writer.detect_quality_anomalies()
# Use anomalies dict to inspect detected issues
```

Exact keys and structure of these return values depend on the framework version; see the method docstrings or type hints. The log table must already have data (from `append()`) for trends and anomalies to be meaningful.

## Related Guides

- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — Result object (status, bronze_results, etc.) that you pass to `append()`.
- [Getting Started](./GETTING_STARTED_GUIDE.md) — Running your first pipeline to produce a result to log.
- [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) — If the log table is missing or writes fail.
