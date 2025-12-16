"""
Basic tests for sql_pipeline_builder.

These tests verify that the SQL pipeline builder can be imported and
basic functionality works with SQLite.
"""

from typing import Any

import pytest
from pipeline_builder_base.errors import ValidationError
from sqlalchemy import Column, Integer, String, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from sql_pipeline_builder import SqlPipelineBuilder
from sql_pipeline_builder.compat import is_async_engine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep

Base: Any = declarative_base()


class User(Base):
    __tablename__ = "users"
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
    id = Column(Integer, primary_key=True)
    total_users = Column(Integer)


# Tables/classes without primary keys (simulate user-provided models)
silver_no_pk_table = Table(
    "silver_no_pk",
    Base.metadata,
    Column("id", Integer),
    Column("email", String),
)

gold_no_pk_table = Table(
    "gold_no_pk",
    Base.metadata,
    Column("event_date", String),
    Column("metric", Integer),
)


class SilverNoPK:
    __table__ = silver_no_pk_table


class GoldNoPK:
    __table__ = gold_no_pk_table


@pytest.fixture
def sqlite_session():
    """Create a SQLite in-memory session for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield session
    session.close()


def test_sql_pipeline_builder_import():
    """Test that SqlPipelineBuilder can be imported."""
    from sql_pipeline_builder import SqlPipelineBuilder

    assert SqlPipelineBuilder is not None


def test_sql_pipeline_builder_initialization(sqlite_session):
    """Test that SqlPipelineBuilder can be initialized."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")
    assert builder is not None
    assert builder.schema == "test_schema"


def test_bronze_step_creation(sqlite_session):
    """Test creating a bronze step with SQLAlchemy validation rules."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    assert "users" in builder.bronze_steps
    assert builder.bronze_steps["users"].name == "users"


def test_silver_step_creation(sqlite_session):
    """Test creating a silver step with SQLAlchemy transform."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    def clean_users(session, bronze_query, silvers):
        return bronze_query.filter(User.email.is_not(None))

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    builder.add_silver_transform(
        name="clean_users",
        source_bronze="users",
        transform=clean_users,
        rules={"email": [User.email.is_not(None)]},
        table_name="clean_users",
        model_class=CleanUser,
    )

    assert "clean_users" in builder.silver_steps
    assert builder.silver_steps["clean_users"].source_bronze == "users"


def test_gold_step_creation(sqlite_session):
    """Test creating a gold step with SQLAlchemy transform."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    def clean_users(session, bronze_query, silvers):
        return bronze_query.filter(User.email.is_not(None))

    def user_metrics(session, silvers):
        from sqlalchemy import func

        clean_users_query = silvers["clean_users"]
        return clean_users_query.with_entities(func.count(User.id).label("total_users"))

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    builder.add_silver_transform(
        name="clean_users",
        source_bronze="users",
        transform=clean_users,
        rules={"email": [User.email.is_not(None)]},
        table_name="clean_users",
        model_class=User,
    )

    builder.add_gold_transform(
        name="user_metrics",
        transform=user_metrics,
        rules={
            "total_users": []
        },  # Empty list means no validation, but dict must be non-empty
        table_name="user_metrics",
        source_silvers=["clean_users"],
        model_class=UserMetric,
    )

    assert "user_metrics" in builder.gold_steps
    assert builder.gold_steps["user_metrics"].source_silvers == ["clean_users"]


def test_add_silver_transform_requires_callable(sqlite_session):
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    with pytest.raises(ValidationError):
        builder.add_silver_transform(
            name="bad_silver",
            source_bronze="users",
            transform=None,  # type: ignore[arg-type]
            rules={"email": [User.email.is_not(None)]},
            table_name="bad_silver",
            model_class=CleanUser,
        )


def test_add_silver_transform_duplicate_name(sqlite_session):
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    def transform(session, bronze_query, silvers):
        return bronze_query

    builder.add_silver_transform(
        name="clean_users",
        source_bronze="users",
        transform=transform,
        rules={"email": [User.email.is_not(None)]},
        table_name="clean_users",
        model_class=CleanUser,
    )

    with pytest.raises(ValidationError, match="already exists"):
        builder.add_silver_transform(
            name="clean_users",
            source_bronze="users",
            transform=transform,
            rules={"email": [User.email.is_not(None)]},
            table_name="clean_users_v2",
            model_class=CleanUser,
        )


def test_add_gold_transform_duplicate_name(sqlite_session):
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    def transform(session, bronze_query, silvers):
        return bronze_query

    builder.add_silver_transform(
        name="clean_users",
        source_bronze="users",
        transform=transform,
        rules={"email": [User.email.is_not(None)]},
        table_name="clean_users",
        model_class=CleanUser,
    )

    def gold_transform(session, silvers):
        return silvers["clean_users"]

    builder.add_gold_transform(
        name="user_metrics",
        transform=gold_transform,
        rules={"metric": []},
        table_name="user_metrics",
        source_silvers=["clean_users"],
        model_class=UserMetric,
    )

    with pytest.raises(ValidationError, match="already exists"):
        builder.add_gold_transform(
            name="user_metrics",
            transform=gold_transform,
            rules={"metric": []},
            table_name="user_metrics_v2",
            source_silvers=["clean_users"],
            model_class=UserMetric,
        )


def test_async_engine_detection():
    """Test async engine detection."""
    # Sync engine
    sync_engine = create_engine("sqlite:///:memory:")
    assert not is_async_engine(sync_engine)

    # Try async engine if available
    try:
        from sqlalchemy.ext.asyncio import create_async_engine

        async_engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        assert is_async_engine(async_engine)
    except ImportError:
        pytest.skip("Async SQLAlchemy not available")


def test_sql_step_models():
    """Test that SQL step models can be created."""
    from sqlalchemy.sql import column

    bronze_step = SqlBronzeStep(
        name="test_bronze",
        rules={"age": [column("age").between(18, 65)]},
    )
    assert bronze_step.name == "test_bronze"

    def silver_transform(session, bronze_query, silvers):
        return bronze_query

    silver_step = SqlSilverStep(
        name="test_silver",
        source_bronze="test_bronze",
        transform=silver_transform,
        rules={"age": [column("age").between(18, 65)]},
        table_name="test_silver",
        model_class=CleanUser,
    )
    assert silver_step.name == "test_silver"

    def gold_transform(session, silvers):
        return list(silvers.values())[0]

    gold_step = SqlGoldStep(
        name="test_gold",
        transform=gold_transform,
        rules={"col": []},  # Empty list means no validation, but dict must be non-empty
        table_name="test_gold",
        model_class=UserMetric,
    )
    assert gold_step.name == "test_gold"


def test_to_pipeline_requires_primary_keys(sqlite_session):
    """Pipeline builder raises if silver/gold models are missing PKs."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="test_schema")

    builder.with_bronze_rules(
        name="users",
        rules={"email": [User.email.is_not(None)]},
        model_class=User,
    )

    def passthrough(session, bronze_query, silvers):
        return bronze_query

    builder.add_silver_transform(
        name="clean_users",
        source_bronze="users",
        transform=passthrough,
        rules={"email": [User.email.is_not(None)]},
        table_name="silver_no_pk",
        model_class=SilverNoPK,
    )

    def gold_transform(session, silvers):
        return silvers["clean_users"]

    builder.add_gold_transform(
        name="metrics",
        transform=gold_transform,
        rules={"metric": []},
        table_name="gold_no_pk",
        source_silvers=["clean_users"],
        model_class=GoldNoPK,
    )

    with pytest.raises(ValidationError, match="primary key"):
        builder.to_pipeline()
