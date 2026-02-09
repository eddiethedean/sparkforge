# SQL Source Steps Guide: with_bronze_sql_source, with_silver_sql_source, with_gold_sql_source

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Overview](#overview)
3. [SQL Source Types: JdbcSource and SqlAlchemySource](#sql-source-types-jdbcsource-and-sqlalchemysource)
4. [with_bronze_sql_source](#with_bronze_sql_source)
5. [with_silver_sql_source](#with_silver_sql_source)
6. [with_gold_sql_source](#with_gold_sql_source)
7. [Running Pipelines with SQL Source Steps](#running-pipelines-with-sql-source-steps)
8. [Use Cases](#use-cases)
9. [Examples](#examples)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Introduction

The `with_bronze_sql_source`, `with_silver_sql_source`, and `with_gold_sql_source` methods let you define pipeline steps whose input is read from a SQL database into Spark DataFrames. They are the **SQL alternatives** to `with_bronze_rules`, `with_silver_rules`, and `with_gold_rules`: instead of reading from Delta Lake or in-memory DataFrames, data is read from a JDBC or SQLAlchemy-backed database at run time.

### Key Benefits

- **Ingest from SQL databases**: Use JDBC (PySpark) or SQLAlchemy + pandas to read tables or queries into Spark DataFrames.
- **Two interchangeable source types**: **JdbcSource** (no extra Python deps; JDBC driver JAR on classpath) and **SqlAlchemySource** (optional `[sql]` extra: sqlalchemy, pandas).
- **Same validation and write flow**: SQL-source steps use the same rules, validation, and Delta write logic as rules-based steps.
- **Mixed pipelines**: Combine SQL-source steps with in-memory bronze, transform-based silver/gold, and validation-only steps in one pipeline.

---

## Overview

### What are SQL Source Steps?

SQL source steps are pipeline steps that:

- Read data from a SQL database (table or query) via **JdbcSource** or **SqlAlchemySource**.
- Apply the same validation rules as rules-based steps.
- Write results to Delta Lake (silver/gold) or feed into downstream steps (bronze).
- Do **not** require a DataFrame in `bronze_sources` (bronze) or a transform function (silver/gold).

### When to Use

Use SQL source steps when:

1. **Data lives in a SQL database**: You want to ingest from PostgreSQL, MySQL, SQL Server, or any JDBC/SQLAlchemy-supported database.
2. **No file or Delta source**: The source of truth is a relational database, not files or Delta tables.
3. **Bronze from DB**: You need a bronze step whose data comes from a SQL table or query (no `bronze_sources` entry).
4. **Silver/Gold from DB**: You want a silver or gold table populated directly from a SQL source, with validation and Delta write, without a transform.

### Architecture

```
SQL Database (JDBC or SQLAlchemy)
         │
         ▼
  read_sql_source(source, spark)
         │
         ▼
  Spark DataFrame
         │
         ├── Bronze: context[step.name] (runner resolves before execute)
         ├── Silver: executor reads → validate → write to Delta
         └── Gold:  executor reads → validate → write to Delta
```

- **Build time**: You call `with_bronze_sql_source`, `with_silver_sql_source`, or `with_gold_sql_source` with a `sql_source` (JdbcSource or SqlAlchemySource).
- **Run time**: The runner (bronze) or executor (silver/gold) calls the unified reader, which dispatches on source type and returns a Spark DataFrame. That DataFrame is then validated and written like any other step.

---

## SQL Source Types: JdbcSource and SqlAlchemySource

You pass **one** of these as the `sql_source` argument to any of the three builder methods. The same API works for both.

### JdbcSource

Reads via **PySpark** `spark.read.jdbc()`. No extra Python dependencies; the JDBC driver JAR for your database must be on the Spark application classpath.

| Parameter     | Type            | Required | Description |
|--------------|-----------------|----------|-------------|
| `url`        | str             | Yes      | JDBC URL (e.g. `jdbc:postgresql://host:5432/db`). |
| `properties` | Dict[str, str]  | Yes      | Connection properties (e.g. `user`, `password`). |
| `table`      | str, optional   | One of table/query | Table identifier (optionally `schema.table`). |
| `query`      | str, optional   | One of table/query | Subquery for Spark JDBC (e.g. `"(SELECT * FROM t WHERE x > 1) AS q"`). |
| `driver`     | str, optional   | No       | JDBC driver class name; stored in `properties["driver"]` when reading. |

**Exactly one** of `table` or `query` must be set; both or neither raises `ValidationError`.

```python
from pipeline_builder.sql_source import JdbcSource

source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="public.orders",
    properties={"user": "etl", "password": "secret"},
    driver="org.postgresql.Driver",  # optional
)
```

For **query**, Spark expects a subquery alias:

```python
JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
    properties={"user": "etl", "password": "secret"},
)
```

### SqlAlchemySource

Reads via **SQLAlchemy** and **pandas** (`read_sql` / `read_sql_table`), then `spark.createDataFrame()`. Requires optional extra: `pip install pipeline_builder[sql]`.

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `url`     | str, optional | One of url/engine | SQLAlchemy URL (e.g. `postgresql://user:pass@host:5432/db`). |
| `engine`  | Any, optional  | One of url/engine | Existing `sqlalchemy.engine.Engine` instance. |
| `table`   | str, optional  | One of table/query | Table name for `pandas.read_sql_table()`. |
| `query`   | str, optional  | One of table/query | Raw SQL for `pandas.read_sql()`. |
| `schema`  | str, optional  | No       | Database schema for table reads (e.g. `"public"` for PostgreSQL). |

**Exactly one** of `url` or `engine`; **exactly one** of `table` or `query`.

```python
from pipeline_builder.sql_source import SqlAlchemySource

# URL
source = SqlAlchemySource(
    url="postgresql://etl:secret@dbhost:5432/warehouse",
    table="orders",
    schema="public",
)

# Or existing engine
from sqlalchemy import create_engine
engine = create_engine("postgresql://...")
source = SqlAlchemySource(engine=engine, table="orders", schema="public")
```

**Recommended import:** `from pipeline_builder.sql_source import JdbcSource, SqlAlchemySource`.

---

## with_bronze_sql_source

Adds a **bronze** step that reads from a SQL database. No DataFrame is required in `bronze_sources`; the runner resolves the step by calling the unified reader before execution. Validation rules are **required and non-empty**, same as `with_bronze_rules`.

### Method Signature

```python
def with_bronze_sql_source(
    self,
    *,
    name: str,
    sql_source: Union[JdbcSource, SqlAlchemySource],
    rules: Dict[str, List[Union[str, Column]]],
    incremental_col: Optional[str] = None,
    schema: Optional[str] = None,
    description: Optional[str] = None,
) -> PipelineBuilder
```

### Parameters

- **name** (str, required): Unique identifier for this bronze step.
- **sql_source** (JdbcSource or SqlAlchemySource, required): Connection and table/query to read from.
- **rules** (Dict, required): Validation rules; must be non-empty. PySpark Column expressions or string rules (e.g. `["not_null"]`, `["gt", 0]`).
- **incremental_col** (str, optional): Column used for incremental processing.
- **schema** (str, optional): Schema for the step; defaults to builder schema.
- **description** (str, optional): Step description.

### Returns

- `PipelineBuilder`: Self for method chaining.

### Example: JDBC Bronze

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
from pipeline_builder.sql_source import JdbcSource

# spark = your SparkSession; ensure the engine is configured (e.g. at app startup)
F = get_default_functions()

orders_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="public.orders",
    properties={"user": "etl", "password": "secret"},
)

builder = PipelineBuilder(spark=spark, schema="analytics")
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "amount": [F.col("amount") > 0],
    },
    incremental_col="updated_at",
)
```

### Example: SqlAlchemy Bronze

```python
from pipeline_builder.sql_source import SqlAlchemySource

orders_source = SqlAlchemySource(
    url="postgresql://etl:secret@dbhost:5432/warehouse",
    table="orders",
    schema="public",
)
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "amount": [F.col("amount") > 0],
    },
)
```

### Output

- The bronze step has `sql_source` set; it does not require an entry in `bronze_sources`.
- At run time, the runner calls the unified reader for each bronze step with `sql_source` and puts the DataFrame in `context[step.name]`.
- Downstream silver/gold steps can use this bronze like any other (e.g. `source_bronze="orders"`).

---

## with_silver_sql_source

Adds a **silver** step that reads from a SQL database, validates, and writes to a Delta table. No `source_bronze` or transform function; the executor loads data via the unified reader.

### Method Signature

```python
def with_silver_sql_source(
    self,
    *,
    name: str,
    sql_source: Union[JdbcSource, SqlAlchemySource],
    table_name: str,
    rules: Dict[str, List[Union[str, Column]]],
    schema: Optional[str] = None,
    description: Optional[str] = None,
    optional: bool = False,
) -> PipelineBuilder
```

### Parameters

- **name** (str, required): Unique identifier for this silver step.
- **sql_source** (JdbcSource or SqlAlchemySource, required): Connection and table/query to read from.
- **table_name** (str, required): Target Delta table name (without schema).
- **rules** (Dict, required): Validation rules; must be non-empty.
- **schema** (str, optional): Schema for writing; defaults to builder schema.
- **description** (str, optional): Step description.
- **optional** (bool, optional): If True, SQL read failure does not fail the step; empty DataFrame is used.

### Returns

- `PipelineBuilder`: Self for method chaining.

### Example

```python
inventory_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="inventory",
    properties={"user": "etl", "password": "secret"},
)
builder.with_silver_sql_source(
    name="inventory_snapshot",
    sql_source=inventory_source,
    table_name="inventory_snapshot",
    rules={
        "sku": [F.col("sku").isNotNull()],
        "qty": [F.col("qty") >= 0],
    },
)
```

### Output

- Silver step with `source_bronze=""`, `transform=None`, `sql_source` set.
- At execution, the silver executor reads from `sql_source`, validates, and writes to the Delta table `schema.table_name`.

---

## with_gold_sql_source

Adds a **gold** step that reads from a SQL database, validates, and writes to a Delta table. No `source_silvers` or transform function.

### Method Signature

```python
def with_gold_sql_source(
    self,
    *,
    name: str,
    sql_source: Union[JdbcSource, SqlAlchemySource],
    table_name: str,
    rules: Dict[str, List[Union[str, Column]]],
    schema: Optional[str] = None,
    description: Optional[str] = None,
    optional: bool = False,
) -> PipelineBuilder
```

### Parameters

- **name** (str, required): Unique identifier for this gold step.
- **sql_source** (JdbcSource or SqlAlchemySource, required): Connection and table/query to read from.
- **table_name** (str, required): Target Delta table name (without schema).
- **rules** (Dict, required): Validation rules; must be non-empty.
- **schema** (str, optional): Schema for writing; defaults to builder schema.
- **description** (str, optional): Step description.
- **optional** (bool, optional): If True, SQL read failure does not fail the step; empty DataFrame is used.

### Returns

- `PipelineBuilder`: Self for method chaining.

### Example

```python
revenue_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
    properties={"user": "etl", "password": "secret"},
)
builder.with_gold_sql_source(
    name="revenue_by_region",
    sql_source=revenue_source,
    table_name="revenue_by_region",
    rules={
        "region": [F.col("region").isNotNull()],
        "revenue": [F.col("revenue") >= 0],
    },
)
```

---

## Running Pipelines with SQL Source Steps

- **Bronze SQL steps**: Do **not** pass a DataFrame in `bronze_sources` for those steps. The runner resolves them via the unified reader before execution.
- **Mixed bronze**: If you have both SQL-source bronze and rules-based bronze, pass `bronze_sources` only for the rules-based steps:

```python
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load()  # No bronze_sources needed for SQL bronze steps

# Or with mixed: SQL bronze "orders" + in-memory bronze "events"
result = pipeline.run_initial_load(bronze_sources={"events": events_df})
```

- **Silver/Gold SQL steps**: No extra arguments; the executor loads from `sql_source` when the step has `sql_source` set.

---

## Use Cases

### Use Case 1: Bronze from Database, Silver/Gold Transforms

Ingest orders from PostgreSQL into bronze, then transform and aggregate in silver and gold:

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
from pipeline_builder.sql_source import JdbcSource

F = get_default_functions()
builder = PipelineBuilder(spark=spark, schema="analytics")

orders_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="orders",
    properties={"user": "etl", "password": "secret"},
)
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={"order_id": [F.col("order_id").isNotNull()], "amount": [F.col("amount") > 0]},
    incremental_col="updated_at",
)

def clean_orders_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("amount") > 0)

builder.add_silver_transform(
    name="clean_orders",
    source_bronze="orders",
    transform=clean_orders_transform,
    rules={"order_id": [F.col("order_id").isNotNull()], "amount": [F.col("amount") > 0]},
    table_name="clean_orders",
)

def revenue_transform(spark, silvers):
    return silvers["clean_orders"].groupBy("order_id").agg(F.sum("amount").alias("total"))

builder.add_gold_transform(
    name="daily_revenue",
    transform=revenue_transform,
    rules={"order_id": [F.col("order_id").isNotNull()], "total": [F.col("total") >= 0]},
    table_name="daily_revenue",
    source_silvers=["clean_orders"],
)

# Run: no bronze_sources needed for "orders"
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load()
```

### Use Case 2: Silver and Gold Directly from SQL

Read from different SQL tables into silver and gold with no transforms:

```python
from pipeline_builder.sql_source import JdbcSource

inventory_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="inventory",
    properties={"user": "etl", "password": "secret"},
)
builder.with_silver_sql_source(
    name="inventory_snapshot",
    sql_source=inventory_source,
    table_name="inventory_snapshot",
    rules={"sku": [F.col("sku").isNotNull()]},
)

revenue_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
    properties={"user": "etl", "password": "secret"},
)
builder.with_gold_sql_source(
    name="revenue_by_region",
    sql_source=revenue_source,
    table_name="revenue_by_region",
    rules={"region": [F.col("region").isNotNull()], "revenue": [F.col("revenue") >= 0]},
)
```

### Use Case 3: Mixed SQL and Delta/In-Memory

Combine SQL-source steps with rules-based and transform-based steps:

```python
orders_jdbc = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="orders",
    properties={"user": "etl", "password": "secret"},
)
inv_jdbc = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="inventory",
    properties={"user": "etl", "password": "secret"},
)

# Bronze: one from SQL, one from in-memory
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_jdbc,
    rules={"order_id": [F.col("order_id").isNotNull()]},
)
builder.with_bronze_rules(
    name="events",
    rules={"event_id": [F.col("event_id").isNotNull()]},
    incremental_col="ts",
)

# Silver: one from SQL, one from transform
builder.with_silver_sql_source(
    name="inventory_snapshot",
    sql_source=inv_jdbc,
    table_name="inventory_snapshot",
    rules={"sku": [F.col("sku").isNotNull()]},
)
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df,
    rules={"event_id": [F.col("event_id").isNotNull()]},
    table_name="clean_events",
)

# Run with bronze_sources only for "events" (orders comes from SQL)
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": events_df})
```

---

## Examples

### Complete Example: JDBC Bronze, Silver, and Gold

Ensure the engine is configured (e.g. at application startup or in tests via conftest). Then:

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
from pipeline_builder.sql_source import JdbcSource

# spark = your SparkSession (e.g. SparkSession.builder.appName("JdbcPipeline").getOrCreate())
F = get_default_functions()

orders_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="public.orders",
    properties={"user": "etl", "password": "secret"},
)

builder = PipelineBuilder(spark=spark, schema="analytics")
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "amount": [F.col("amount") > 0],
    },
    incremental_col="updated_at",
)

inventory_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="inventory",
    properties={"user": "etl", "password": "secret"},
)
builder.with_silver_sql_source(
    name="inventory_snapshot",
    sql_source=inventory_source,
    table_name="inventory_snapshot",
    rules={"sku": [F.col("sku").isNotNull()], "qty": [F.col("qty") >= 0]},
)

revenue_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
    properties={"user": "etl", "password": "secret"},
)
builder.with_gold_sql_source(
    name="revenue_by_region",
    sql_source=revenue_source,
    table_name="revenue_by_region",
    rules={"region": [F.col("region").isNotNull()], "revenue": [F.col("revenue") >= 0]},
)

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load()
print(f"Status: {result.status.value}")
```

### Example: SqlAlchemySource (optional [sql] extra)

Requires `pip install pipeline_builder[sql]`. Use the same `builder` and `F` as in the complete example above:

```python
from pipeline_builder.sql_source import SqlAlchemySource

orders_source = SqlAlchemySource(
    url="postgresql://etl:secret@dbhost:5432/warehouse",
    table="orders",
    schema="public",
)
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={"order_id": [F.col("order_id").isNotNull()], "amount": [F.col("amount") > 0]},
)
# Same with_silver_sql_source / with_gold_sql_source; then pipeline.run_initial_load()
```

### Example: String Rules

String rules work the same as in `with_bronze_rules` / `with_silver_rules` / `with_gold_rules`. Use a `sql_source` (e.g. `inventory_source` from the silver example above) and `builder`:

```python
inventory_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="inventory",
    properties={"user": "etl", "password": "secret"},
)
builder.with_silver_sql_source(
    name="inventory_snapshot",
    sql_source=inventory_source,
    table_name="inventory_snapshot",
    rules={
        "sku": ["not_null"],
        "qty": ["gte", 0],
    },
)
```

---

## Best Practices

### 1. Credentials

Do not hardcode passwords. Use environment variables or a secrets manager:

```python
import os

JdbcSource(
    url=os.environ["JDBC_URL"],
    table="orders",
    properties={
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    },
)
```

### 2. Reuse Source Instances

You can reuse the same `JdbcSource` or `SqlAlchemySource` for multiple steps if the connection and table/query are the same:

```python
orders_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="orders",
    properties={"user": "etl", "password": "secret"},
)
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={"order_id": [F.col("order_id").isNotNull()]},
)
# Reuse for another step if needed
builder.with_silver_sql_source(
    name="orders_snapshot",
    sql_source=orders_source,
    table_name="orders_snapshot",
    rules={"order_id": [F.col("order_id").isNotNull()]},
)
```

### 3. Empty Results

If the SQL table or query returns 0 rows, the step still runs: you get an empty DataFrame, validation runs, and (for silver/gold) an empty table is written. No special handling is required.

### 4. Optional Steps

Use `optional=True` for silver/gold SQL steps when the source might be missing or fail (e.g. optional lookup table). The step will not fail the pipeline; it produces an empty DataFrame and continues.

### 5. JDBC Driver on Classpath

For **JdbcSource**, ensure the JDBC driver JAR is on the Spark classpath (e.g. `spark-submit --jars /path/to/postgresql.jar`). If you see a driver-related error, the message should point you to adding the JAR.

### 6. SqlAlchemySource Dependencies

For **SqlAlchemySource**, install the optional extra: `pip install pipeline_builder[sql]`. If sqlalchemy or pandas is missing at run time, you will get a clear error suggesting `pip install pipeline_builder[sql]`.

---

## Troubleshooting

### Error: JdbcSource requires exactly one of 'table' or 'query'

**Cause:** Both `table` and `query` were set, or neither.

**Solution:** Set exactly one of them:

```python
# Table only
JdbcSource(
    url="jdbc:postgresql://host:5432/db",
    table="orders",
    properties={"user": "u", "password": "p"},
)

# Query only (subquery alias for Spark JDBC)
JdbcSource(
    url="jdbc:postgresql://host:5432/db",
    query="(SELECT * FROM orders WHERE x > 1) AS q",
    properties={"user": "u", "password": "p"},
)
```

### Error: SqlAlchemySource requires exactly one of 'url' or 'engine'

**Cause:** Both `url` and `engine` were provided, or neither.

**Solution:** Use either a URL or an existing engine:

```python
SqlAlchemySource(url="postgresql://user:pass@host:5432/db", table="orders")
# or
SqlAlchemySource(engine=my_engine, table="orders")
```

### Error: SqlAlchemySource requires pandas / sqlalchemy

**Cause:** SqlAlchemySource was used but the `[sql]` extra is not installed.

**Solution:** Install the extra: `pip install pipeline_builder[sql]`.

### Error: JDBC driver class not found

**Cause:** The JDBC driver JAR for your database is not on the Spark classpath.

**Solution:** Add the JAR when submitting the application, e.g.:

```bash
spark-submit --jars /path/to/postgresql-42.x.x.jar your_app.py
```

Or set `spark.jars` in your Spark config.

### Error: rules must be non-empty

**Cause:** `with_bronze_sql_source`, `with_silver_sql_source`, or `with_gold_sql_source` was called with empty `rules={}`.

**Solution:** Provide at least one rule per step, same as for `with_bronze_rules` / `with_silver_rules` / `with_gold_rules`:

```python
rules={
    "id": [F.col("id").isNotNull()],
    "amount": [F.col("amount") > 0],
}
```

### Bronze step not found in context

**Cause:** You passed `bronze_sources` for a step that has `sql_source` (runner resolves those automatically), or the runner did not run (e.g. wrong pipeline/steps).

**Solution:** For bronze steps defined with `with_bronze_sql_source`, do **not** add them to `bronze_sources`. Only pass DataFrames for bronze steps that were defined with `with_bronze_rules` and need an in-memory source.

---

## Summary

- **`with_bronze_sql_source`**: Bronze step that reads from a SQL database; no DataFrame in `bronze_sources`. Rules required.
- **`with_silver_sql_source`**: Silver step that reads from SQL, validates, and writes to Delta. No transform or source_bronze.
- **`with_gold_sql_source`**: Gold step that reads from SQL, validates, and writes to Delta. No transform or source_silvers.
- **JdbcSource**: Uses `spark.read.jdbc()`; no extra Python deps; JDBC driver JAR on classpath.
- **SqlAlchemySource**: Uses SQLAlchemy + pandas; install `pipeline_builder[sql]`.
- **Mixed pipelines**: You can combine SQL-source steps with rules-based and transform-based steps; pass `bronze_sources` only for in-memory bronze steps.

For more information, see [Building Pipelines](BUILDING_PIPELINES_GUIDE.md) and [Validation-Only Steps](VALIDATION_ONLY_STEPS_GUIDE.md).
