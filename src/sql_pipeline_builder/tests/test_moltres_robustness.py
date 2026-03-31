"""
More robust coverage for Moltres-authored rules/transforms in sql_pipeline_builder.

SQLite-only to keep CI fast and deterministic.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from abstracts.reports.run import Report
from pipeline_builder_base.models import ExecutionResult
from sqlalchemy import Column, Integer, String, create_engine, literal, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from moltres import col

from sql_pipeline_builder import SqlPipelineBuilder
from sql_pipeline_builder.moltres_integration import moltres_database_from_session
from sql_pipeline_builder.validation.sql_validation import apply_sql_validation_rules

Base: Any = declarative_base()


class BronzeUser(Base):
    __tablename__ = "bronze_users2"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    age = Column(Integer)


class BronzeOrder(Base):
    __tablename__ = "bronze_orders2"
    order_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    amount = Column(Integer)


class SilverUserOrder(Base):
    __tablename__ = "silver_user_orders2"
    order_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    email = Column(String)
    amount = Column(Integer)


class GoldMetric(Base):
    __tablename__ = "gold_metrics2"
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
            BronzeUser(id=2, email="b@example.com", age=20),
            BronzeUser(id=3, email=None, age=99),
        ]
    )
    session.add_all(
        [
            BronzeOrder(order_id=10, user_id=1, amount=100),
            BronzeOrder(order_id=11, user_id=1, amount=50),
            BronzeOrder(order_id=12, user_id=2, amount=25),
        ]
    )
    session.commit()
    yield session
    session.close()


def test_apply_sql_validation_rules_accepts_core_select(sqlite_session: Session):
    stmt = select(BronzeUser.id, BronzeUser.email, BronzeUser.age).select_from(BronzeUser)
    rules = {"age": [BronzeUser.age >= 21]}
    valid, invalid, stats = apply_sql_validation_rules(stmt, rules, "core_select", sqlite_session)
    # core Select path uses .count() only when available; here we just verify it doesn't crash
    # and returns a SQLAlchemy object we can execute.
    rows = sqlite_session.execute(valid).all()
    assert len(rows) == 2
    assert stats.total_rows >= stats.valid_rows


def test_moltres_join_transform_writes_silver(sqlite_session: Session):
    db = moltres_database_from_session(sqlite_session)

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")
    builder.with_bronze_rules(
        name="users",
        rules={"id": [col("id").is_not_null()]},
        model_class=BronzeUser,
    )

    def user_orders(session: Session, bronze_users_df, silvers):
        orders_df = db.table("bronze_orders2").select()
        # Join users->orders using Moltres expressions
        return bronze_users_df.select().join(
            orders_df,
            on=[col("bronze_users2.id") == col("bronze_orders2.user_id")],
            how="inner",
        )

    builder.add_silver_transform(
        name="user_orders",
        source_bronze="users",
        transform=user_orders,
        rules={"id": [col("bronze_users2.id").is_not_null()]},
        table_name="silver_user_orders2",
        model_class=SilverUserOrder,
    )

    def metrics(session: Session, silvers):
        df = silvers["user_orders"]
        stmt = df.to_sqlalchemy().subquery()
        from sqlalchemy import func

        return select(
            literal("rows").label("metric"),
            select(func.count()).select_from(stmt).scalar_subquery().label("value"),
        )

    builder.add_gold_transform(
        name="metrics",
        transform=metrics,
        rules={"metric": []},
        table_name="gold_metrics2",
        source_silvers=["user_orders"],
        model_class=GoldMetric,
    )

    pipeline = builder.to_pipeline()
    result: Report = pipeline.run_initial_load(
        bronze_sources={"users": db.table("bronze_users2").select()}
    )
    exec_result = cast(ExecutionResult, result)
    assert exec_result.success
    assert sqlite_session.query(SilverUserOrder).count() == 3
    assert sqlite_session.query(GoldMetric).count() == 1


def test_moltres_bad_rule_type_marks_step_failed(sqlite_session: Session):
    db = moltres_database_from_session(sqlite_session)

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")
    # Intentionally incorrect rule object (string instead of expression)
    builder.with_bronze_rules(
        name="users",
        rules={"id": ["not an expression"]},  # type: ignore[list-item]
        model_class=BronzeUser,
    )

    pipeline = builder.to_pipeline()
    result: Report = pipeline.run_initial_load(
        bronze_sources={"users": db.table("bronze_users2").select()}
    )
    exec_result = cast(ExecutionResult, result)

    assert not exec_result.success
    bronze_result = next(r for r in exec_result.step_results if r.step_name == "users")
    assert not bronze_result.success
    assert bronze_result.error_message


def test_transform_returning_invalid_type_marks_silver_failed(sqlite_session: Session):
    db = moltres_database_from_session(sqlite_session)
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    builder.with_bronze_rules(
        name="users",
        rules={"id": [col("id").is_not_null()]},
        model_class=BronzeUser,
    )

    def bad_transform(session: Session, bronze_df, silvers):
        return 123  # invalid

    builder.add_silver_transform(
        name="bad_silver",
        source_bronze="users",
        transform=bad_transform,
        rules={"id": [col("id").is_not_null()]},
        table_name="silver_user_orders2",
        model_class=SilverUserOrder,
    )

    pipeline = builder.to_pipeline()
    result: Report = pipeline.run_initial_load(
        bronze_sources={"users": db.table("bronze_users2").select()}
    )
    exec_result = cast(ExecutionResult, result)
    assert not exec_result.success
    silver_result = next(r for r in exec_result.step_results if r.step_name == "bad_silver")
    assert not silver_result.success
    assert silver_result.error_message

