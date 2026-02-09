# Building Pipelines Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Medallion Architecture](#medallion-architecture)
3. [Define Bronze Steps](#define-bronze-steps)
4. [Define Silver Steps](#define-silver-steps)
5. [Define Gold Steps](#define-gold-steps)
6. [Validate and Build](#validate-and-build)
7. [Related Guides](#related-guides)

---

## Introduction

A pipeline is built by defining **steps** (Bronze, Silver, Gold), then calling `validate_pipeline()` and `to_pipeline()`. This guide covers the core builder API: `with_bronze_rules`, `add_silver_transform`, and `add_gold_transform`. For SQL-backed steps and validation-only steps, see the related guides below.

**Prerequisites:** Engine configured (see [Getting Started](./GETTING_STARTED_GUIDE.md)). You need a `SparkSession`, a schema name, and (for transforms) validation rules. Rule syntax is covered in [Validation Rules](./VALIDATION_RULES_GUIDE.md).

## Medallion Architecture

- **Bronze:** Raw data. You validate it; the framework does not persist bronze (it stays in-memory or is read from a SQL source at run time). Bronze feeds silver.
- **Silver:** Cleaned, transformed data. Each silver step reads from one bronze (or from a SQL source) and optionally from prior silver tables. Output is validated and written to Delta tables.
- **Gold:** Aggregated, business-ready data. Each gold step reads from one or more silver tables (or from a SQL source), aggregates, and writes to Delta after validation.

Steps are executed in dependency order. You define the graph; the runner executes in the correct sequence.

## Define Bronze Steps

Bronze steps validate incoming raw data. Data is either supplied at run time via `bronze_sources` (DataFrame per bronze step name) or read from a SQL database (see [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md)).

**Method:** `with_bronze_rules(name, rules, incremental_col=None, ...)`

- **name:** Step name; must be unique. This is the key you use in `bronze_sources` when running (unless the step is a SQL source).
- **rules:** Dict mapping column names to lists of PySpark column expressions (or string rules; see [Validation Rules](./VALIDATION_RULES_GUIDE.md)). Rows that fail are dropped; validation rate is reported.
- **incremental_col:** Optional. Column used for incremental processing (e.g. timestamp); the runner uses it when you call `run_incremental()`.

Example:

```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

builder.with_bronze_rules(
    name="raw_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "value": [F.col("value") > 0],
    },
    incremental_col="timestamp",
)
```

You can add multiple bronze steps; each needs a unique name and its own entry in `bronze_sources` at run time (unless it is a SQL source step).

## Define Silver Steps

Silver steps transform bronze (or SQL-source) data and write to Delta.

**Method:** `add_silver_transform(name, source_bronze, transform, rules, table_name, ...)`

- **name:** Unique step name; used to reference this silver in gold steps or in `prior_silvers`.
- **source_bronze:** Name of the bronze step that provides the input DataFrame.
- **transform:** Function `(spark, bronze_df, prior_silvers) -> DataFrame`. Return the cleaned/transformed DataFrame; only columns with validation rules are kept.
- **rules:** Dict mapping column names to lists of expressions (or string rules). Include rules for every column you want in the output.
- **table_name:** Delta table name (within the pipeline schema) where the result is written.

Example:

```python
def silver_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("action").isNotNull()).select(
        "user_id", "action", "timestamp", "value"
    )

builder.add_silver_transform(
    name="processed_events",
    source_bronze="raw_events",
    transform=silver_transform,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "value": [F.col("value") > 0],
    },
    table_name="silver_processed_events",
)
```

**Important:** Include validation rules for all columns you want to preserve. Columns without rules are filtered out. For existing silver tables that you only validate and reuse, use [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) (`with_silver_rules`).

## Define Gold Steps

Gold steps aggregate silver (or SQL-source) data and write to Delta.

**Method:** `add_gold_transform(name, transform, rules, table_name, source_silvers, ...)`

- **name:** Unique step name.
- **transform:** Function `(spark, silvers) -> DataFrame`. `silvers` is a dict of step name → DataFrame (e.g. `silvers["processed_events"]`).
- **rules:** Validation rules for the gold output columns.
- **table_name:** Delta table name for the gold output.
- **source_silvers:** List of silver step names this gold step depends on.

Example:

```python
def gold_transform(spark, silvers):
    return silvers["processed_events"].groupBy("user_id").agg(
        F.count("*").alias("event_count"),
        F.sum("value").alias("total_value"),
    )

builder.add_gold_transform(
    name="user_metrics",
    transform=gold_transform,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_count": [F.col("event_count") > 0],
        "total_value": [F.col("total_value").isNotNull()],
    },
    table_name="gold_user_metrics",
    source_silvers=["processed_events"],
)
```

For existing gold tables that you only validate and reuse, use [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) (`with_gold_rules`).

## Validate and Build

After adding all steps:

```python
validation_errors = builder.validate_pipeline()
if validation_errors:
    for msg in validation_errors:
        print(msg)
    # Fix configuration and re-validate
    exit(1)

pipeline = builder.to_pipeline()
```

- **validate_pipeline():** Checks step names, dependencies, and configuration. Returns a list of error messages (empty if valid).
- **to_pipeline():** Returns a runnable pipeline (e.g. for `run_initial_load`, `run_incremental`; see [Execution Modes](./EXECUTION_MODES_GUIDE.md)).

## Related Guides

- [Getting Started](./GETTING_STARTED_GUIDE.md) — Install and first run.
- [Validation Rules](./VALIDATION_RULES_GUIDE.md) — Rule syntax, string rules, and helpers.
- [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) — `with_silver_rules` and `with_gold_rules` for existing tables.
- [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md) — Bronze/silver/gold steps that read from JDBC or SQLAlchemy.
- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — How to run the pipeline (initial, incremental, full refresh, validation-only).
