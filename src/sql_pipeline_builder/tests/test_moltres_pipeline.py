"""
Integration tests for Moltres-authored rules and transforms flowing through sql_pipeline_builder.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from abstracts.reports.run import Report
from pipeline_builder_base.models import ExecutionResult
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from moltres import col

from sql_pipeline_builder import SqlPipelineBuilder
from sql_pipeline_builder.moltres_integration import moltres_database_from_session

Base: Any = declarative_base()


class BronzeUser(Base):
    __tablename__ = "bronze_users"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)


class SilverUser(Base):
    __tablename__ = "silver_users"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)


class GoldMetric(Base):
    __tablename__ = "gold_metrics"
    metric = Column(String, primary_key=True)
    value = Column(Integer)


@pytest.fixture
def sqlite_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session = Session(engine)
    session.add_all(
        [
            BronzeUser(id=1, email="a@example.com", age=30),
            BronzeUser(id=2, email=None, age=25),  # invalid: null email
            BronzeUser(id=3, email="c@example.com", age=17),  # invalid: age < 18
            BronzeUser(id=4, email="d@example.com", age=40),
        ]
    )
    session.commit()
    yield session
    session.close()


def test_moltres_rules_and_transforms_end_to_end(sqlite_session: Session):
    db = moltres_database_from_session(sqlite_session)

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    # Bronze rules authored in Moltres (Column expressions)
    builder.with_bronze_rules(
        name="users",
        rules={
            "email": [col("email").is_not_null()],
            "age": [col("age") >= 18],
        },
        model_class=BronzeUser,
    )

    # Silver transform authored in Moltres (DataFrame chaining)
    def silver_users(session: Session, bronze_df, silvers):
        # bronze_df is expected to be a Moltres DataFrame
        return bronze_df.select().where(col("age") >= 18)

    builder.add_silver_transform(
        name="silver_users",
        source_bronze="users",
        transform=silver_users,
        rules={"id": [col("id").is_not_null()]},
        table_name="silver_users",
        model_class=SilverUser,
    )

    # Gold transform authored in Moltres; aggregate count
    def gold_metrics(session: Session, silvers):
        silver_df = silvers["silver_users"]
        # simple metric: count rows (use SQLAlchemy conversion for select literal)
        count_stmt = silver_df.to_sqlalchemy().subquery()
        from sqlalchemy import func, literal, select

        return select(
            literal("silver_user_rows").label("metric"),
            select(func.count()).select_from(count_stmt).scalar_subquery().label("value"),
        )

    builder.add_gold_transform(
        name="gold_metrics",
        transform=gold_metrics,
        rules={"metric": []},
        table_name="gold_metrics",
        source_silvers=["silver_users"],
        model_class=GoldMetric,
    )

    pipeline = builder.to_pipeline()

    # Bronze source is Moltres DataFrame
    bronze_source = db.table("bronze_users").select()
    result: Report = pipeline.run_initial_load(bronze_sources={"users": bronze_source})
    exec_result = cast(ExecutionResult, result)

    assert exec_result.success
    assert sqlite_session.query(SilverUser).count() == 2
    assert sqlite_session.query(GoldMetric).count() == 1

