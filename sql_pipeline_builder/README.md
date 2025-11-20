# SQL Pipeline Builder

SQLAlchemy-based pipeline builder implementing the Medallion Architecture pattern.

This package provides a SQLAlchemy-based implementation of the pipeline builder framework,
allowing you to build Bronze → Silver → Gold pipelines using SQL databases instead of Spark.

## Features

- **SQLAlchemy ORM**: Use SQLAlchemy ORM objects and queries
- **Async Support**: Automatic detection and use of async SQLAlchemy when available
- **Multi-Database**: Supports all SQLAlchemy-compatible databases (PostgreSQL, MySQL, SQLite, etc.)
- **Validation**: SQLAlchemy expression-based validation (e.g., `User.email.is_not(None)`)
- **Parallel Execution**: Async parallel execution when driver supports it

## Installation

```bash
pip install sql-pipeline-builder
```

For async support:
```bash
pip install sql-pipeline-builder[async]
```

For specific database drivers:
```bash
pip install sql-pipeline-builder[postgresql]
pip install sql-pipeline-builder[mysql]
pip install sql-pipeline-builder[sqlite]
```

## Quick Start

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import column

from sql_pipeline_builder import SqlPipelineBuilder

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)

# Create session
engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)
session = Session(engine)

# Build pipeline
builder = SqlPipelineBuilder(session=session, schema="analytics")

# Bronze: Validate raw data
builder.with_bronze_rules(
    name="users",
    rules={
        "email": [User.email.is_not(None)],
        "age": [column('age').between(18, 65)],
    },
    model_class=User
)

# Silver: Transform data
def clean_users(session, bronze_query, silvers):
    return bronze_query.filter(User.email.is_not(None))

builder.add_silver_transform(
    name="clean_users",
    source_bronze="users",
    transform=clean_users,
    rules={"email": [User.email.is_not(None)]},
    table_name="clean_users",
    model_class=User
)

# Gold: Business analytics
def user_metrics(session, silvers):
    from sqlalchemy import func
    clean_users = silvers["clean_users"]
    return clean_users.with_entities(
        func.count(User.id).label("total_users")
    )

builder.add_gold_transform(
    name="user_metrics",
    transform=user_metrics,
    rules={},
    table_name="user_metrics",
    source_silvers=["clean_users"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"users": session.query(User)})
```

## Validation Rules

SQL validation uses SQLAlchemy expressions:

```python
rules = {
    "email": [User.email.is_not(None)],  # IS NOT NULL
    "age": [column('age').between(18, 65)],  # BETWEEN
    "status": [User.status.in_(['active', 'inactive'])],  # IN
}
```

## Dependencies

- `pipeline-builder-base>=2.1.2` - Shared base package
- `sqlalchemy>=2.0.0` - SQLAlchemy ORM

Optional:
- `sqlalchemy[asyncio]` - For async support
- Database-specific drivers (psycopg2, pymysql, aiosqlite, etc.)

