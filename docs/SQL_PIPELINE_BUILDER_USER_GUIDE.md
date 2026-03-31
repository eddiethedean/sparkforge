# SQL Pipeline Builder User Guide

This guide explains how to build and run Bronze -> Silver -> Gold pipelines with `sql_pipeline_builder` using:

- **Moltres** for rules and transformation functions (lazy DataFrame API that compiles to SQL)
- **SQLAlchemy** for sessions and ORM models (execution + table creation/materialization)

Moltres docs: https://moltres.readthedocs.io/en/latest/

## What It Is

`sql_pipeline_builder` is the SQLAlchemy-based sibling of the Spark pipeline framework. It gives you:

- A fluent builder API for Bronze, Silver, and Gold steps
- Expression-based validation using Moltres rules
- Dependency-aware step ordering
- Initial and incremental execution modes
- Table creation and write management for Silver and Gold outputs

## Prerequisites

- Python 3.8+
- Moltres
- SQLAlchemy
- A SQLAlchemy-compatible database and driver (SQLite, Postgres, MySQL, etc.)

Install from this repository:

```bash
pip install -e .
```

Optional extras:

```bash
pip install -e ".[sql]"
```

## Core Concepts

### Bronze

- Reads source queries from `bronze_sources`
- Applies validation rules
- Does not write output tables

### Silver

- Transforms from one Bronze source
- Re-validates transformed rows
- Writes to a target table

### Gold

- Transforms from one or more Silver outputs
- Re-validates transformed rows
- Writes to a target table

## Minimal End-to-End Example

```python
from sqlalchemy import Column, Integer, String, create_engine, func, literal, select
from sqlalchemy.orm import Session, declarative_base

from moltres import Database, col
from sql_pipeline_builder import SqlPipelineBuilder

Base = declarative_base()


class UserEvent(Base):
    __tablename__ = "user_events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    event_type = Column(String)
    value = Column(Integer)
    event_date = Column(String)


class CleanEvent(Base):
    __tablename__ = "clean_events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    event_type = Column(String)
    value = Column(Integer)
    event_date = Column(String)


class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    event_date = Column(String, primary_key=True)
    total_events = Column(Integer)
    unique_users = Column(Integer)


engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)
session = Session(engine)
db = Database.from_engine(engine)

builder = SqlPipelineBuilder(session=session, schema="analytics")

builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [col("user_id").is_not_null()],
        "value": [col("value") > 0],
    },
    incremental_col="event_date",
    model_class=UserEvent,
)


def clean_events(session, bronze_df, silvers):
    return bronze_df.select().where(col("value") >= 10)


builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={"user_id": [col("user_id").is_not_null()]},
    table_name="clean_events",
    model_class=CleanEvent,
)


def daily_metrics(session, silvers):
    clean_df = silvers["clean_events"]
    stmt = clean_df.to_sqlalchemy().subquery()
    return select(
        literal("daily").label("event_date"),
        select(func.count()).select_from(stmt).scalar_subquery().label("total_events"),
        literal(0).label("unique_users"),
    )


builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={"event_date": []},
    table_name="daily_metrics",
    source_silvers=["clean_events"],
    model_class=DailyMetric,
)

pipeline = builder.to_pipeline()
source_df = db.table("user_events").select()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

print(result.success)
```

## API Workflow

1. Create a SQLAlchemy `Session`
2. Instantiate `SqlPipelineBuilder(session=..., schema=...)`
3. Define Bronze rules with `with_bronze_rules(...)`
4. Add Silver transforms with `add_silver_transform(...)`
5. Add Gold transforms with `add_gold_transform(...)`
6. Build with `to_pipeline()`
7. Run with `run_initial_load(...)` or `run_incremental(...)`

## Required Inputs and Contracts

### `model_class` requirements

Silver and Gold steps require `model_class` with SQLAlchemy `__table__` metadata so destination tables can be created/recreated.

### Primary key requirement

`to_pipeline()` validates that all Silver and Gold models define at least one primary key.

### Rules requirement

Rules must be dictionaries of Moltres expressions. Example:

```python
rules = {
    "email": [col("email").is_not_null()],
    "age": [col("age").between(18, 65)],
}
```

## Execution Modes

### Initial load

```python
result = pipeline.run_initial_load(bronze_sources={"events": session.query(UserEvent)})
```

- Silver writes in overwrite mode
- Gold writes in overwrite mode

### Incremental

```python
new_rows = session.query(UserEvent).filter(UserEvent.event_date == "2026-03-23")
result = pipeline.run_incremental(bronze_sources={"events": new_rows})
```

- Silver writes in append mode
- Gold still writes in overwrite mode

## Write Behavior and Table Lifecycle

- Silver tables are created from model metadata if missing
- Silver tables are dropped/recreated on initial runs
- Gold tables are dropped/recreated on every run
- Data writes use SQLAlchemy-backed table operations

## Logging with LogWriter

`sql_pipeline_builder` exposes a `LogWriter` facade with the same naming pattern as `pipeline_builder`.

```python
from sql_pipeline_builder import LogWriter
from pipeline_builder_base.writer.models import WriteMode

# Reuse the same SQLAlchemy session used by the pipeline
log_writer = LogWriter(
    session=session,
    schema="monitoring",
    table_name="pipeline_logs",
)

# High-level API: write an ExecutionResult directly
write_result = log_writer.write_execution_result(
    execution_result=result,
    run_mode="initial",
    metadata={"source": "user-guide"},
    mode=WriteMode.APPEND,
)
print(write_result["rows_written"])
```

Notes:

- `LogWriter` is an alias over the SQL writer implementation and behaves the same as `SqlLogWriter`.
- Preferred methods are `write_execution_result(...)`, `create_table(...)`, and `append(...)`.
- `write_log_rows(...)` remains available for pre-built row payloads.
- `write_execution_result(...)` generates a new `run_id` if you do not pass one.

### Create-vs-append pattern

```python
# Initial run: create/replace the logging table
log_writer.create_table(result, run_mode="initial")

# Later runs: append new execution rows
next_result = pipeline.run_incremental(bronze_sources={"events": new_rows})
log_writer.append(next_result, run_mode="incremental")
```

## Dependency Ordering

Execution is dependency-aware and sequential:

- Bronze before dependent Silver
- Silver before dependent Gold

This gives deterministic runs and predictable debugging.

## Common Errors and Fixes

- `Bronze source 'X' not found in context`
  - Add the step key to `bronze_sources` when running
- `Silver step 'X' requires a model_class`
  - Pass `model_class=YourOrmModel` in `add_silver_transform(...)`
- `Gold step 'X' requires a model_class`
  - Pass `model_class=YourOrmModel` in `add_gold_transform(...)`
- Primary-key validation error at `to_pipeline()`
  - Add `primary_key=True` to at least one model column

## Testing Recommendations

- Use SQLite in-memory for fast unit tests
- Keep one fixture for session lifecycle per test
- Validate both success and failure paths
- Assert deterministic step names, counts, and error messages

## Related Docs

- [`docs/src_sql_pipeline_builder_README.md`](./src_sql_pipeline_builder_README.md)
- [`docs/src_pipeline_builder_base_README.md`](./src_pipeline_builder_base_README.md)
- [`docs/guides/README.md`](./guides/README.md)
