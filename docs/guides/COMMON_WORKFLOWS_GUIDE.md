# Common Workflows Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Multi-Source Pipeline](#multi-source-pipeline)
3. [Initial Then Incremental Pattern](#initial-then-incremental-pattern)
4. [Mixed Steps: In-Memory, SQL, and Validation-Only](#mixed-steps-in-memory-sql-and-validation-only)
5. [Edge Cases](#edge-cases)
6. [Related Guides](#related-guides)

---

## Introduction

This guide covers common pipeline patterns: multiple bronze sources, initial load followed by incremental runs, mixing in-memory bronze with SQL-source and validation-only steps, and handling edge cases. For run modes and result handling, see [Execution Modes](./EXECUTION_MODES_GUIDE.md).

## Multi-Source Pipeline

You can define multiple bronze steps and supply a DataFrame for each at run time. Keys in `bronze_sources` must match bronze step names.

**Example: users, orders, and products**

```python
# Bronze steps
builder.with_bronze_rules(name="users", rules={"user_id": ["not_null"]})
builder.with_bronze_rules(name="orders", rules={"order_id": ["not_null"], "user_id": ["not_null"]})
builder.with_bronze_rules(name="products", rules={"product_id": ["not_null"]})

# Silver steps (each can use one or more bronzes via prior_silvers if needed)
builder.add_silver_transform(
    name="clean_orders",
    source_bronze="orders",
    transform=lambda spark, df, silvers: df.filter(F.col("order_id").isNotNull()),
    rules={"order_id": [F.col("order_id").isNotNull()], "user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_orders",
)
# ... add more silver/gold as needed

pipeline = builder.to_pipeline()

# Run: pass one DataFrame per bronze step name
result = pipeline.run_initial_load(
    bronze_sources={
        "users": users_df,
        "orders": orders_df,
        "products": products_df,
    }
)
```

The framework runs steps in dependency order. Each bronze step receives the DataFrame you provide under its name.

## Initial Then Incremental Pattern

Typical production pattern: one initial load to backfill, then repeated incremental loads (e.g. daily or hourly).

```python
# One-time: historical data
initial_result = pipeline.run_initial_load(
    bronze_sources={"raw_events": historical_df}
)
if initial_result.status.value != "completed":
    raise RuntimeError("Initial load failed")

# Scheduled: new data only
incremental_result = pipeline.run_incremental(
    bronze_sources={"raw_events": new_data_df}
)
```

Requirements:

- The bronze step must have `incremental_col` set (e.g. a timestamp or date column) so the runner can filter to new records.
- Tables must already exist from the initial load (or a previous run) for incremental to append correctly.

See [Execution Modes](./EXECUTION_MODES_GUIDE.md) for full behavior of `run_initial_load` and `run_incremental`.

## Mixed Steps: In-Memory, SQL, and Validation-Only

You can combine:

- **In-memory bronze:** `with_bronze_rules` + DataFrame in `bronze_sources`
- **SQL-source bronze/silver/gold:** `with_bronze_sql_source`, `with_silver_sql_source`, `with_gold_sql_source` — data is read from a JDBC or SQLAlchemy source at run time; no DataFrame entry for those steps
- **Validation-only silver/gold:** `with_silver_rules`, `with_gold_rules` — validate an existing table and expose it to downstream transforms via `prior_silvers` / `prior_golds`

Example idea:

- Bronze A: in-memory (`bronze_sources={"A": df}`)
- Bronze B: SQL source (no entry in `bronze_sources`)
- Silver S1: transform from Bronze A
- Silver S2: validation-only existing table (no transform)
- Gold G: reads from S1 and S2 (e.g. `silvers["S1"]`, `prior_silvers["S2"]` or as exposed by the API)

For SQL source steps, see [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md). For validation-only steps, see [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md).

## Edge Cases

**Empty DataFrame:** Running with an empty DataFrame is valid. The pipeline completes; step row counts and written rows will be zero. No need to special-case; validation and writes still run.

**No new data in incremental:** If the incremental DataFrame is empty or all rows are filtered out by `incremental_col`, the run still completes. Downstream silver/gold steps may write zero new rows (append with no new data).

**Missing columns after transform:** If your transform drops a column that still has validation rules, the framework typically drops that rule and may emit a warning. Include rules only for columns that your transform actually outputs (see [Validation Rules](./VALIDATION_RULES_GUIDE.md) and [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md)).

**Schema changes:** If you change the schema of a table (e.g. add/remove columns), a full refresh may be needed. Use `run_full_refresh` with a full dataset to overwrite tables with the new schema. See [Execution Modes](./EXECUTION_MODES_GUIDE.md).

**Initial load overwrite:** Each `run_initial_load` overwrites target silver and gold tables. You do not get “append” behavior; use `run_incremental` for that.

For more failure modes and fixes (engine not configured, validation failures, Delta/table errors), see [Troubleshooting](./TROUBLESHOOTING_GUIDE.md).

## Related Guides

- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — Initial, incremental, full refresh, validation-only.
- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) — Defining bronze, silver, and gold steps.
- [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md) — Bronze/silver/gold from JDBC or SQLAlchemy.
- [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) — `with_silver_rules` and `with_gold_rules`.
- [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) — Common issues and resolutions.
