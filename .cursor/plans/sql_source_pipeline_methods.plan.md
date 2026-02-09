---
name: SQL source pipeline methods
overview: "Add three PipelineBuilder methods (with_bronze_sql_source, with_silver_sql_source, with_gold_sql_source) that define steps whose input is read from a SQL database into DataFrames. Support two interchangeable sql_source types: JdbcSource (PySpark JDBC, no extra deps) and SqlAlchemySource (SQLAlchemy + pandas, optional extra)."
todos: []
isProject: false
---

# SQL source pipeline builder methods

## Goal

Add SQL-backed alternatives to the existing rules-based step methods so pipelines can ingest from SQL databases and use the resulting DataFrames in the same Bronze/Silver/Gold flow. **Two interchangeable sql_source types** (pass either to `with_*_sql_source`): **JdbcSource** (PySpark `spark.read.jdbc()`, no extra deps; JDBC driver JAR on classpath) and **SqlAlchemySource** (SQLAlchemy connection + pandas `read_sql` → `spark.createDataFrame`, optional `[sql]` extra).

- **with_bronze_sql_source** – SQL alternative to **with_bronze_rules**: bronze step whose data is read from a SQL table (no DataFrame in `bronze_sources`). **Requires non-empty validation rules** exactly like `with_bronze_rules` (same rules parameter, same validation and error handling).
- **with_silver_sql_source** – SQL alternative to **with_silver_rules**: silver step that reads from a SQL table, validates, and writes to Delta (no `source_bronze` / transform).
- **with_gold_sql_source** – SQL alternative to **with_gold_rules**: gold step that reads from a SQL table, validates, and writes to Delta (no `source_silvers` / transform).

## Architecture

```mermaid
flowchart LR
  subgraph sql_sources [SQL sources]
    DB_JDBC[(JDBC DB)]
    DB_SA[(Any DB)]
  end
  subgraph resolution [At run time]
    JDBC[spark.read.jdbc]
    SA[SQLAlchemy + pandas]
    SparkDF[Spark DataFrame]
  end
  subgraph pipeline [Existing pipeline]
    Bronze[Bronze executor]
    Silver[Silver executor]
    Gold[Gold executor]
  end
  DB_JDBC --> JDBC
  DB_SA --> SA
  JDBC --> SparkDF
  SA --> SparkDF
  SparkDF --> Bronze
  SparkDF --> Silver
  SparkDF --> Gold
```



- **Build time**: New builder methods create existing step types with an optional **sql_source** (either **JdbcSource** or **SqlAlchemySource**); same API for both.
- **Run time**: Steps with sql_source are resolved: **JdbcSource** → `spark.read.jdbc()`; **SqlAlchemySource** → SQLAlchemy + `pandas.read_sql()` → `spark.createDataFrame()`. The resulting DataFrame is fed into the same validation/write logic.

## Key files to touch


| Area                       | Files                                                                                                                                                                                             |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQL source models + reader | New: [src/pipeline_builder/sql_source/](src/pipeline_builder/) – `JdbcSource` and `SqlAlchemySource` configs + unified reader that dispatches on type (JDBC vs SQLAlchemy)                        |
| Step models                | [src/pipeline_builder/models/steps.py](src/pipeline_builder/models/steps.py) – add optional `sql_source: Union[JdbcSource, SqlAlchemySource]` to BronzeStep, SilverStep, GoldStep                 |
| Builder API                | [src/pipeline_builder/pipeline/builder.py](src/pipeline_builder/pipeline/builder.py) – add `with_bronze_sql_source`, `with_silver_sql_source`, `with_gold_sql_source` (accept either source type) |
| Context resolution         | [src/pipeline_builder/pipeline/runner.py](src/pipeline_builder/pipeline/runner.py) – before `execute_pipeline`, resolve SQL-backed bronze steps into `context` via reader                         |
| Bronze executor            | [src/pipeline_builder/step_executors/bronze.py](src/pipeline_builder/step_executors/bronze.py) – no change if context is pre-filled                                                               |
| Silver executor            | [src/pipeline_builder/step_executors/silver.py](src/pipeline_builder/step_executors/silver.py) – when `step.sql_source` is set, call reader (JDBC or SQLAlchemy) and use result as input          |
| Gold executor              | [src/pipeline_builder/step_executors/gold.py](src/pipeline_builder/step_executors/gold.py) – when `step.sql_source` is set, call reader and use result as input                                   |
| Dependencies               | JdbcSource: no new Python deps (JDBC driver JAR on classpath). SqlAlchemySource: optional extra `[sql]` (sqlalchemy, pandas).                                                                     |


## Example usage

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pipeline_builder.sql_source import JdbcSource  # or from pipeline_builder.sql_source.models
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JdbcPipeline").getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# Define JDBC source: table name (optionally schema.table) or a query
# Prefer env vars for credentials: properties={"user": os.environ["DB_USER"], "password": os.environ["DB_PASSWORD"]}
orders_source = JdbcSource(
    url="jdbc:postgresql://dbhost:5432/warehouse",
    table="public.orders",
    properties={"user": "etl", "password": "secret"},
)

# Bronze: read from JDBC, validate with rules (no DataFrame in bronze_sources)
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

# Silver: read from another JDBC table, validate, write to Delta
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

# Gold: read from JDBC (query or table), validate, write to Delta
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

# Run: no bronze_sources needed for JDBC-backed bronze step
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load()  # or run_initial_load(bronze_sources={}) to be explicit
print(f"Status: {result.status.value}")
```

**Notes:** For **JdbcSource**, ensure the JDBC driver JAR is on the Spark classpath. String rules work the same as in `with_bronze_rules` / `with_silver_rules` / `with_gold_rules`.

**Alternate: SqlAlchemySource** (same builder methods; pass `SqlAlchemySource` instead of `JdbcSource`; requires `pip install pipeline_builder[sql]`):

```python
from pipeline_builder.sql_source import SqlAlchemySource

# SQLAlchemy URL (or pass an existing engine); table or query
orders_source = SqlAlchemySource(
    url="postgresql://etl:secret@dbhost:5432/warehouse",  # or engine=my_engine
    table="orders",  # or query="SELECT * FROM orders WHERE created_at > '2024-01-01'"
)
builder.with_bronze_sql_source(
    name="orders",
    sql_source=orders_source,
    rules={"order_id": [F.col("order_id").isNotNull()], "amount": [F.col("amount") > 0]},
)
# Same with_silver_sql_source / with_gold_sql_source; run_initial_load() as above
```

## 1. SQL source models and reader

- **Two source types** (both usable as `sql_source` in `with_*_sql_source`):
  - **JdbcSource**:
    - **url**: JDBC URL (e.g. `jdbc:postgresql://host:5432/db`). Required.
    - **table** or **query**: Table identifier (optionally `schema.table`) or a query string (subquery alias form for Spark JDBC). Exactly one required.
    - **properties**: Dict for JDBC connection (e.g. `user`, `password`). Optional: driver class, partition column, etc.
    - **Reader path**: `spark.read.jdbc(url=..., table=..., properties=...)` → Spark DataFrame. No extra Python deps; JDBC driver JAR must be on Spark classpath.
  - **SqlAlchemySource**:
    - **url** or **engine**: SQLAlchemy URL string (e.g. `postgresql://user:pass@host/db`) or an existing `sqlalchemy.engine.Engine`. One required.
    - **table** or **query**: Table name or raw SQL query string. Exactly one required. For table use `pandas.read_sql_table()`; for query use `pandas.read_sql()`.
    - **schema** (optional): Database schema name for table reads (e.g. `"public"` for PostgreSQL). Passed to `pandas.read_sql_table(..., schema=schema)`. Omit for query-based reads.
    - **Reader path**: Create engine from URL if needed → `pandas.read_sql_table(table, schema=schema, con=engine)` or `pandas.read_sql(query, con=engine)` → `spark.createDataFrame(pandas_df)`. Requires optional extra `[sql]`: sqlalchemy, pandas; lazy import and raise clear error if missing when SqlAlchemySource is used.
- **Unified reader**: One function (e.g. `read_sql_source(source, spark)`) that accepts `Union[JdbcSource, SqlAlchemySource]`, dispatches on type (e.g. `isinstance(source, JdbcSource)` vs `SqlAlchemySource`), and returns a Spark DataFrame. Runner and step executors call this; they do not care which source type was passed.
- **Location**: New module `pipeline_builder.sql_source` with `models.py` (JdbcSource, SqlAlchemySource) and `reader.py` (unified read function).

## 2. Step model extensions

- In [src/pipeline_builder/models/steps.py](src/pipeline_builder/models/steps.py):
  - **BronzeStep**: Add `sql_source: Optional[Union[JdbcSource, SqlAlchemySource]] = None`. When set, no DataFrame need be provided in `bronze_sources`; resolved via unified reader. **Rules remain required and non-empty** (same as today).
  - **SilverStep**: Add `sql_source: Optional[Union[JdbcSource, SqlAlchemySource]] = None`. When set, `source_bronze=""`, `transform=None`; executor calls unified reader and uses result as input.
  - **GoldStep**: Add `sql_source: Optional[Union[JdbcSource, SqlAlchemySource]] = None`. When set, `source_silvers` empty/None; executor calls unified reader and uses result as input.
- Keep validation logic consistent: e.g. require `rules` and (for silver/gold) `table_name`; require exactly one of in-memory source (bronze_sources / source_bronze / source_silvers) or `sql_source` where applicable.

## 3. Builder methods

In [src/pipeline_builder/pipeline/builder.py](src/pipeline_builder/pipeline/builder.py):

- **with_bronze_sql_source**(name, sql_source, rules, incremental_col=None, schema=None, description=None)
  - **Require non-empty validation rules** as with `with_bronze_rules`: validate name, no duplicate, validate schema if provided, **validate that `rules` is non-empty** (raise `ValidationError` or `StepError` if empty, with same messaging as `with_bronze_rules`), convert rules (reuse `_convert_rules_to_expressions`), build `BronzeStep(name=..., rules=..., incremental_col=..., schema=..., sql_source=sql_source)`, store in `bronze_steps`, update creation order, return self.
- **with_silver_sql_source**(name, sql_source, table_name, rules, schema=None, description=None, optional=False)
  - Same validation pattern; create `SilverStep` with `source_bronze=""`, `transform=None`, `existing=False`, `sql_source=sql_source`, `table_name=table_name`, `rules=...`, `schema=...`, `optional=...`. Append to `silver_steps` and creation order.
- **with_gold_sql_source**(name, sql_source, table_name, rules, schema=None, description=None, optional=False)
  - Create `GoldStep` with `source_silvers=None` (or []), `transform=None`, `existing=False`, `sql_source=sql_source`, `table_name=...`, `rules=...`, `schema=...`, `optional=...`. Append to `gold_steps` and creation order.

Reuse existing helpers (`_check_duplicate_step_name`, `_validate_schema`, `_get_effective_schema`, `_convert_rules_to_expressions`) and docstrings that reference "SQL alternative to with_bronze_rules / with_silver_rules / with_gold_rules".

## 4. Context resolution and execution

- **Runner** ([src/pipeline_builder/pipeline/runner.py](src/pipeline_builder/pipeline/runner.py)): When building `context` for `run_pipeline` (and similarly for `run_initial_load` / `run_incremental`):
  - For each bronze step with `sql_source` not already in `bronze_sources`, call the **unified reader** (dispatches on JdbcSource vs SqlAlchemySource) to get a DataFrame and set `context[step.name] = that_df`.
  - Keeps the contract that bronze steps always have their DataFrame in `context` before the execution engine runs.
- **Silver executor** ([src/pipeline_builder/step_executors/silver.py](src/pipeline_builder/step_executors/silver.py)): If `step.sql_source` is set, call the unified reader to get a DataFrame, then treat that as the input (validation/write as for a silver step with no transform). Skip `step.source_bronze in context` and transform call.
- **Gold executor** ([src/pipeline_builder/step_executors/gold.py](src/pipeline_builder/step_executors/gold.py)): If `step.sql_source` is set, call the unified reader and use the result as the single input, then validate and write.
- **Dependency order**: Existing analyzer in [pipeline_builder_base/dependencies/analyzer.py](src/pipeline_builder_base/dependencies/analyzer.py) already skips adding a bronze dependency when `source_bronze` is falsy and skips silver dependencies when `source_silvers` is falsy. So SQL-backed silver and gold steps will get correct execution order without changes.

## 5. Dependencies and docs

- **pyproject.toml**: Add optional extra `sql = ["sqlalchemy", "pandas"]` for **SqlAlchemySource**. **JdbcSource** requires no new Python deps (JDBC driver JAR on Spark classpath). For integration tests that use PostgreSQL, add **testing.postgresql** to the `test` extra or a dedicated `integration-test` extra (e.g. `testing.postgresql`); note that testing.postgresql requires a PostgreSQL server (initdb/postgres) in PATH.
- **Docs**: Document both source types: **JdbcSource** uses `spark.read.jdbc()` (driver JAR on classpath); **SqlAlchemySource** uses SQLAlchemy + pandas (install `pipeline_builder[sql]`). Same builder API; pass either type as `sql_source`.
- **README / docstrings**: Document `with_*_sql_source` and the two options (JdbcSource, SqlAlchemySource). Include JdbcSource example and a short SqlAlchemySource example.

## 6. Robust testing

**Layout**: Add `tests/unit/sql_source/` (or `tests/unit/test_sql_source_*.py`) for unit tests; `tests/integration/sql_source/` (or under existing `tests/integration/`) for integration tests. Use pytest markers: `sql_source`, `jdbc`, `sqlalchemy`, `requires_postgres`, `requires_jdbc_driver`; mark integration tests that require `[sql]`, JDBC driver, or PostgreSQL in PATH so CI can run/skip. For PostgreSQL-backed tests, use [testing.postgresql](https://pypi.org/project/testing.postgresql/) to spin up a temporary instance per test or per module. Reuse existing Spark fixtures (mock Spark or real Spark as in current tests).

### 6.1 Unit tests (no real DB; mock reader or Spark)

- **Source model validation**
  - **JdbcSource**: (1) Valid: either `table` or `query` set, not both; `url` and `properties` present. (2) Invalid: both `table` and `query` → raise `ValidationError` with message "exactly one of 'table' or 'query'". (3) Invalid: neither `table` nor `query` → same. (4) Invalid: missing `url` → clear error. Assert error messages include actionable hint where applicable.
  - **SqlAlchemySource**: Same table/query mutual exclusivity. Valid: `url` or `engine` (one required). Invalid: both `url` and `engine` or neither → clear error. Optional `schema` stored and passed through.
- **Builder methods**
  - **with_bronze_sql_source**: (1) Creates BronzeStep with `sql_source`, `rules`, `incremental_col`, `schema` as passed; step in `builder.bronze_steps`. (2) Rejects empty `rules` with same error as `with_bronze_rules`. (3) Rejects duplicate step name. (4) Accepts both JdbcSource and SqlAlchemySource. (5) String rules converted (e.g. `["not_null"]`) via existing `_convert_rules_to_expressions`. (6) Returns self for chaining.
  - **with_silver_sql_source**: (1) Creates SilverStep with `sql_source`, `table_name`, `rules`, `source_bronze=""`, `transform=None`, `existing=False`, `optional` as passed. (2) Rejects duplicate name; rejects empty rules. (3) Accepts both source types.
  - **with_gold_sql_source**: (1) Creates GoldStep with `sql_source`, `table_name`, `rules`, `source_silvers=None` (or []), `transform=None`, `existing=False`, `optional` as passed. (2) Rejects duplicate name; rejects empty rules. (3) Accepts both source types.
- **Unified reader (mocked)**
  - Mock `spark.read.jdbc` and (for SqlAlchemySource) pandas + `spark.createDataFrame`. (1) When source is JdbcSource, assert `spark.read.jdbc` called with correct `url`, `table` or query, `properties`. (2) When source is SqlAlchemySource, assert engine created from url (or engine used), pandas `read_sql`/`read_sql_table` called, then `spark.createDataFrame` with pandas DataFrame. (3) Return value is a Spark DataFrame (mock). (4) When SqlAlchemySource is used and sqlalchemy or pandas is not installed, assert error message includes `pip install pipeline_builder[sql]` (use import skip or mock to simulate missing deps).
- **Step models**
  - BronzeStep/SilverStep/GoldStep with `sql_source` set: `step_type`, `name`, `rules`, and (for silver/gold) `table_name` and `source_bronze`/`source_silvers` as specified. No transform for silver/gold sql_source steps.
- **Runner context resolution**
  - With a bronze step that has `sql_source` and is not in `bronze_sources`, assert unified reader is called with that step’s `sql_source` and spark, and result is placed in `context[step.name]`. Mock the reader to return a known DataFrame; assert it appears in context and pipeline run proceeds (or assert reader call count and args).

### 6.2 Integration tests (real Spark; real or in-memory DB where applicable)

- **SqlAlchemySource (in-memory SQLite)**
  - **Prereq**: Skip if sqlalchemy or pandas not installed (`pytest.importorskip("sqlalchemy")`, `pytest.importorskip("pandas")`).
  - **Setup**: Create SQLite in-memory DB (e.g. `sqlite:///:memory:`), create a table with known schema and rows (e.g. `id`, `name`, `amount`); create SparkSession and configure engine.
  - **Bronze only**: Build pipeline with `with_bronze_sql_source` (SqlAlchemySource pointing at SQLite table), rules that pass. `run_initial_load()`. Assert `result.status` success; assert context or downstream has DataFrame with expected row count and columns.
  - **Bronze + Silver + Gold**: Add `with_silver_sql_source` and `with_gold_sql_source` (e.g. silver reads another table or same table, gold reads query). Run full pipeline. Assert Delta tables exist in configured schema with expected row counts and schema (e.g. `spark.table("schema.silver_table")` count and columns).
  - **Query-based**: SqlAlchemySource with `query="SELECT * FROM ... WHERE ..."` (no table). Assert read succeeds and DataFrame content matches query.
  - **Empty result**: Query that returns 0 rows. Assert pipeline completes successfully, 0 rows written to Delta (or bronze context has 0 rows), no failure.
  - **optional=True**: Silver or gold step with SqlAlchemySource pointing at a non-existent table (or invalid query). With `optional=True`, assert step does not fail pipeline; downstream receives empty DataFrame or step is skipped as per existing optional behavior.
  - **String rules**: Use string rules (e.g. `{"id": ["not_null"], "amount": ["gt", 0]}`) and assert validation runs and invalid rows are handled per existing validation behavior.
- **JdbcSource and SqlAlchemySource with PostgreSQL (testing.postgresql)**
  - **Dependency**: Add [testing.postgresql](https://pypi.org/project/testing.postgresql/) to test optional deps (e.g. `test` or a new `integration-test` extra). Requires a PostgreSQL server in PATH (e.g. local `initdb`/`postgres`); CI or dev machines must have PostgreSQL installed.
  - **Setup**: Use `testing.postgresql.Postgresql()` as a context manager or pytest fixture. It starts a PostgreSQL instance in a temporary directory and tears it down after the test. Build JDBC URL from the instance: e.g. `postgresql.url()` returns `postgresql://user:pass@host:port/db` → convert to `jdbc:postgresql://host:port/db` and pass connection props (user, password) from `postgresql.dsn()` for **JdbcSource**. For **SqlAlchemySource** use `create_engine(postgresql.url())` directly. Create tables and insert fixture rows inside the context (e.g. with SQLAlchemy or psycopg2) so each test has a clean, isolated DB.
  - **JdbcSource tests**: Skip if PostgreSQL not in PATH or JDBC driver JAR not on classpath (`@pytest.mark.requires_postgres`, `@pytest.mark.requires_jdbc_driver`). With testing.postgresql running: Bronze-only pipeline with `with_bronze_sql_source` (JdbcSource); table and query reads; empty result; optional=True. Assert success and expected row counts.
  - **SqlAlchemySource tests (PostgreSQL)**: Same fixture; use `SqlAlchemySource(url=postgresql.url(), table=..., schema="public")` (or query). Covers schema parameter and PostgreSQL-specific behavior in addition to SQLite.
  - **Optional**: Use `testing.postgresql.PostgresqlFactory(cache_initialized_db=True)` with an `on_initialized` handler to create tables once and reuse the DB across tests for speed; clear cache in `tearDownModule`. Prefer per-test `Postgresql()` for isolation when tests modify schema or data.
- **Mixed pipelines**
  - **Bronze**: One bronze step with `with_bronze_sql_source` (SqlAlchemySource + SQLite) and one with `with_bronze_rules`; provide `bronze_sources` only for the second. Run initial load. Assert both steps get data (sql_source from reader, other from bronze_sources) and pipeline succeeds.
  - **Silver**: One silver via `with_silver_sql_source` (SQLite), one via `add_silver_transform` (source_bronze = the sql_source bronze). Run pipeline. Assert both silver outputs in context/Delta and correct execution order.
  - **Gold**: Similarly mix `with_gold_sql_source` and `add_gold_transform` (source_silvers including the sql_source silver). Assert both gold outputs and success.

### 6.3 Error and edge-case tests

- **Validation**: (1) `with_bronze_sql_source(..., rules={})` → ValidationError or StepError, message like "rules must be non-empty". (2) JdbcSource(table="t", query="SELECT 1") → ValidationError "exactly one of 'table' or 'query'". (3) SqlAlchemySource() with neither url nor engine → clear error.
- **Missing dependencies**: Test that uses SqlAlchemySource with sqlalchemy (or pandas) unimportable (e.g. mock or separate env). Assert raised error message includes `pip install pipeline_builder[sql]`.
- **Runner**: Pipeline with only sql_source bronze steps and no bronze_sources → reader called for each bronze step; no KeyError or "missing data" for bronze. Pipeline with mix of sql_source and in-memory bronze → only sql_source steps trigger reader.
- **Executor**: Silver step with sql_source: assert reader called once per step execution with correct (source, spark); result used as input (mock reader and assert output DataFrame is the one returned by reader). Same for gold.

### 6.4 Test data and isolation

- Use distinct schema names per test (or per test file) to avoid Delta table collisions when running in parallel (consistent with existing project practice). For **SQLite** (SqlAlchemySource): create tables and data in fixture; use `:memory:` for isolation. For **PostgreSQL**: use [testing.postgresql](https://pypi.org/project/testing.postgresql/) so each test (or test class) gets a temporary PostgreSQL instance that is destroyed after the test; create tables and insert fixture data inside the context. Prefer small, deterministic datasets (e.g. 3–5 rows) for readability; add one test with larger or edge-case data if needed for validation behavior.

## User experience and improvements

- **Credentials**: Do not require or encourage hardcoded passwords in examples. Document that users should pass credentials via environment variables (e.g. `os.environ["DB_PASSWORD"]`) or a secrets manager. Support `properties={"user": os.environ["DB_USER"], "password": os.environ["DB_PASSWORD"]}` for JdbcSource and equivalent for SqlAlchemySource (url with placeholders or engine created with env vars). Docstrings and README should mention this.
- **Validation and errors**: (1) **Table vs query**: At construction or first read, validate "exactly one of table or query"; raise `ValidationError` with message like `"JdbcSource requires exactly one of 'table' or 'query', got both/neither"`. (2) **SqlAlchemySource missing deps**: If sqlalchemy or pandas is missing when a step uses SqlAlchemySource, raise with message that includes `pip install pipeline_builder[sql]`. (3) **JDBC driver missing**: Where possible, catch Spark's JDBC error and re-raise with a hint: e.g. "Ensure the JDBC driver JAR is on the classpath (e.g. spark-submit --jars path/to/postgresql.jar)".
- **Mixed pipelines**: Explicitly document that pipelines can mix SQL-source steps and in-memory/transform steps: e.g. one bronze from `with_bronze_sql_source`, another from `with_bronze_rules` + `bronze_sources`; silver can mix `with_silver_sql_source` and `add_silver_transform`. Same for gold.
- **SqlAlchemySource schema**: For databases that use schemas (e.g. PostgreSQL), add an optional **schema** parameter to SqlAlchemySource and pass it to `pandas.read_sql_table(..., schema=schema)` when reading a table. Omit for query-based reads.
- **Empty results**: If the SQL table or query returns 0 rows, treat as a valid DataFrame (0 rows); validation and write still run. Document that this is supported (no special failure).
- **optional=True for SQL steps**: For silver/gold sql_source steps, **optional** already exists. Specify: when `optional=True`, if the SQL read fails (e.g. table does not exist, connection error) or returns empty, do not fail the step; produce an empty DataFrame and continue (consistent with existing optional behavior for Delta-backed steps).
- **Recommended import**: Document a single public import path, e.g. `from pipeline_builder.sql_source import JdbcSource, SqlAlchemySource` (re-export from `pipeline_builder.sql_source`), so users do not need to guess between `sql_source` and `sql_source.models`.
- **Large reads (optional enhancement)**: For JdbcSource, document or expose common options (e.g. `partitionColumn`, `numPartitions`, `lowerBound`, `upperBound`, or fetch size) in the plan or reader for large tables. For SqlAlchemySource, consider supporting chunked read (e.g. `read_sql(..., chunksize=...)`) and creating Spark DataFrame from chunks to avoid loading the full result into memory at once. Can be a follow-up if not in scope for v1.

## Design notes

- **Two source types, one API**: **JdbcSource** (built-in PySpark JDBC; no extra deps) and **SqlAlchemySource** (SQLAlchemy + pandas; optional `[sql]` extra) are interchangeable: pass either to `with_bronze_sql_source` / `with_silver_sql_source` / `with_gold_sql_source`. Unified reader dispatches on type; runner and executors stay agnostic.
- **No change to abstracts/source protocol**: Bronze sources remain DataFrame-like in context; SQL resolution is an implementation detail that populates context before the engine runs.
- **Consistency with with_silver_rules / with_gold_rules**: SQL variants mirror the "rules + table" API; the only difference is the source of the DataFrame (SQL table instead of Delta table or in-memory).
- **with_bronze_sql_source and rules**: Validation rules are required and must be non-empty, identical to `with_bronze_rules`. Use the same rule conversion (`_convert_rules_to_expressions`), string-rule support, and validation/error handling so behavior is consistent.
- **Reuse sources**: Users can reuse the same `JdbcSource` or `SqlAlchemySource` instance (same url/table or query) for multiple steps; no need to create a new object per step.

