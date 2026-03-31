# SQL Pipeline Builder (Moltres + SQLAlchemy)

`sql_pipeline_builder` implements the Medallion Architecture (Bronze → Silver → Gold) for SQL databases.

You **author rules and transformation functions with Moltres** (a lazy DataFrame API that compiles to SQL), and the pipeline uses **SQLAlchemy** sessions + ORM models for execution and table creation/materialization.

See Moltres docs:
https://moltres.readthedocs.io/en/latest/

For a full walkthrough, see the dedicated user guide:
[`docs/SQL_PIPELINE_BUILDER_USER_GUIDE.md`](./SQL_PIPELINE_BUILDER_USER_GUIDE.md)

## Features

- **Moltres authoring**: Rules (`moltres.col(...)`) and transforms (`DataFrame.where(...)`, `group_by(...)`, etc.) compile to SQL
- **SQLAlchemy persistence**: Use SQLAlchemy sessions and ORM models to create/write Silver/Gold tables
- **Async Support**: Automatic detection and use of async SQLAlchemy when available (where supported)
- **Multi-Database**: Supports all SQLAlchemy-compatible databases (PostgreSQL, MySQL, SQLite, etc.)
- **Validation**: Moltres expression-based validation
- **Parallel Execution**: Async parallel execution when driver supports it

## Installation

```bash
# From this repository
pip install -e .
```

Moltres can also be installed directly (if you are using `sql_pipeline_builder` standalone outside this repo):
https://moltres.readthedocs.io/en/latest/guides/getting-started.html

## Quick Start

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import func, literal, select

from moltres import Database, col
from sql_pipeline_builder import SqlPipelineBuilder

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)


class CleanUser(Base):
    __tablename__ = "clean_users"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)


class UserMetric(Base):
    __tablename__ = "user_metrics"
    metric = Column(String, primary_key=True)
    total_users = Column(Integer)

# Create session
engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)
session = Session(engine)
db = Database.from_engine(engine)

# Build pipeline
builder = SqlPipelineBuilder(session=session, schema="analytics")

# Bronze: Validate raw data
builder.with_bronze_rules(
    name="users",
    rules={
        "email": [col("email").is_not_null()],
        "age": [col("age").between(18, 65)],
    },
    model_class=User
)

# Silver: Transform data
def clean_users(session, bronze_df, silvers):
    return bronze_df.select().where(col("email").is_not_null())

builder.add_silver_transform(
    name="clean_users",
    source_bronze="users",
    transform=clean_users,
    rules={"email": [col("email").is_not_null()]},
    table_name="clean_users",
    model_class=CleanUser  # Required so the pipeline can create/drop the table
)

# Gold: Business analytics
def user_metrics(session, silvers):
    clean_users_df = silvers["clean_users"]
    count_stmt = clean_users_df.to_sqlalchemy().subquery()
    return select(
        literal("total_users").label("metric"),
        select(func.count()).select_from(count_stmt).scalar_subquery().label("total_users"),
    )

builder.add_gold_transform(
    name="user_metrics",
    transform=user_metrics,
    rules={"metric": []},
    table_name="user_metrics",
    source_silvers=["clean_users"],
    model_class=UserMetric  # Required so the pipeline can create/drop the table
)

# Table management
# - Provide SQLAlchemy models for every Silver/Gold step so the pipeline can build the schema.
# - Silver tables are dropped and recreated on initial (full-refresh) runs before data is written.
# - Gold tables are dropped and recreated on every run to guarantee schema parity with the model.

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"users": db.table("users").select()})
```

## Validation Rules

SQL validation uses Moltres expressions:

```python
rules = {
    "email": [col("email").is_not_null()],
    "age": [col("age").between(18, 65)],
    "status": [col("status").isin(["active", "inactive"])],
}
```

## Dependencies

- `pipeline-builder-base>=2.1.2` - Shared base package
- `sqlalchemy>=2.0.0` - SQLAlchemy ORM
- `moltres` - Moltres DataFrame + expressions for authoring rules/transforms

Optional:
- `sqlalchemy[asyncio]` - For async support
- Database-specific drivers (psycopg2, pymysql, aiosqlite, etc.)

