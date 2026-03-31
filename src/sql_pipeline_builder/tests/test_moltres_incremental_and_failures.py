"""
Additional Moltres-focused SQL pipeline tests:

- incremental append behavior
- failures in Moltres->SQLAlchemy conversion
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from abstracts.reports.run import Report
from pipeline_builder_base.models import ExecutionMode, ExecutionResult
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from moltres import col

from sql_pipeline_builder import SqlPipelineBuilder
from sql_pipeline_builder.moltres_integration import moltres_database_from_session

Base: Any = declarative_base()


class BronzeUser(Base):
    __tablename__ = "bronze_users_inc"
    id = Column(Integer, primary_key=True)
    email = Column(String)


class SilverUser(Base):
    __tablename__ = "silver_users_inc"
    id = Column(Integer, primary_key=True)
    email = Column(String)


@pytest.fixture
def sqlite_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session = Session(engine)
    session.add_all(
        [
            BronzeUser(id=1, email="a@example.com"),
            BronzeUser(id=2, email="b@example.com"),
        ]
    )
    session.commit()
    yield session
    session.close()


def _builder(sqlite_session: Session) -> SqlPipelineBuilder:
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")
    builder.with_bronze_rules(
        name="users",
        rules={"email": [col("email").is_not_null()]},
        incremental_col=None,
        model_class=BronzeUser,
    )

    def to_silver(session: Session, bronze_df, silvers):
        return bronze_df.select().where(col("email").is_not_null())

    builder.add_silver_transform(
        name="silver_users",
        source_bronze="users",
        transform=to_silver,
        rules={"id": [col("id").is_not_null()]},
        table_name="silver_users_inc",
        model_class=SilverUser,
    )
    return builder


def test_moltres_incremental_appends_new_rows(sqlite_session: Session):
    db = moltres_database_from_session(sqlite_session)
    builder = _builder(sqlite_session)
    pipeline = builder.to_pipeline()

    # Initial load writes 2 rows
    res1: Report = pipeline.run_initial_load(
        bronze_sources={"users": db.table("bronze_users_inc").select()}
    )
    exec1 = cast(ExecutionResult, res1)
    assert exec1.success
    assert sqlite_session.query(SilverUser).count() == 2

    # Add one new bronze row
    sqlite_session.add(BronzeUser(id=3, email="c@example.com"))
    sqlite_session.commit()

    # Incremental run: only new row
    res2: Report = pipeline.run_incremental(
        bronze_sources={"users": db.table("bronze_users_inc").select().where(col("id") == 3)}
    )
    exec2 = cast(ExecutionResult, res2)
    assert exec2.success
    assert exec2.context.mode == ExecutionMode.INCREMENTAL
    assert sqlite_session.query(SilverUser).count() == 3

    silver_step = next(r for r in exec2.step_results if r.step_name == "silver_users")
    assert silver_step.write_mode == "append"


def test_moltres_to_sqlalchemy_failure_marks_step_failed(sqlite_session: Session):
    """
    If a Moltres-like object cannot convert to SQLAlchemy, writing should fail cleanly.
    """

    class FakeMoltresDF:
        def where(self, *_args, **_kwargs):
            return self

        def collect(self, *_args, **_kwargs):
            return []

        def to_sqlalchemy(self, *_args, **_kwargs):
            raise RuntimeError("boom to_sqlalchemy")

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")
    builder.with_bronze_rules(
        name="users",
        rules={"email": [col("email").is_not_null()]},
        model_class=BronzeUser,
    )

    def bad_silver(session: Session, bronze_df, silvers):
        return FakeMoltresDF()

    builder.add_silver_transform(
        name="silver_users",
        source_bronze="users",
        transform=bad_silver,
        rules={"email": [col("email").is_not_null()]},
        table_name="silver_users_inc",
        model_class=SilverUser,
    )

    pipeline = builder.to_pipeline()
    db = moltres_database_from_session(sqlite_session)
    res: Report = pipeline.run_initial_load(
        bronze_sources={"users": db.table("bronze_users_inc").select()}
    )
    exec_res = cast(ExecutionResult, res)
    assert not exec_res.success
    silver_result = next(r for r in exec_res.step_results if r.step_name == "silver_users")
    assert not silver_result.success
    assert "boom to_sqlalchemy" in (silver_result.error_message or "")

