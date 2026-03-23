"""
Contract tests for the SqlEngine adapter.
"""

from __future__ import annotations

from typing import Any

import pytest
from pipeline_builder_base.models import PipelineConfig, ValidationThresholds
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from sql_pipeline_builder.engine.sql_engine import SqlEngine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep

Base: Any = declarative_base()


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    category = Column(String)
    value = Column(Integer)


class TargetItem(Base):
    __tablename__ = "target_items"
    id = Column(Integer, primary_key=True)
    category = Column(String)
    value = Column(Integer)


def _config() -> PipelineConfig:
    return PipelineConfig(
        schema="main",
        thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
        verbose=False,
    )


@pytest.fixture
def seeded_session(sqlite_session):
    Base.metadata.create_all(sqlite_session.bind)
    sqlite_session.add_all(
        [
            Item(id=1, category="valid", value=10),
            Item(id=2, category="valid", value=20),
        ]
    )
    sqlite_session.commit()
    return sqlite_session


def test_validate_source_rejects_non_query_source(seeded_session):
    engine = SqlEngine(seeded_session, _config())
    bronze_step = SqlBronzeStep(
        name="items",
        rules={"value": [Item.value > 0]},
        model_class=Item,
    )

    with pytest.raises(TypeError, match="Source must be a SQLAlchemy Query"):
        engine.validate_source(bronze_step, source=123)


def test_transform_source_gold_requires_dict_source(seeded_session):
    engine = SqlEngine(seeded_session, _config())

    def gold_transform(session, silvers):
        return list(silvers.values())[0]

    gold_step = SqlGoldStep(
        name="gold_items",
        transform=gold_transform,
        rules={"value": []},
        table_name="gold_items",
        model_class=TargetItem,
    )

    query = seeded_session.query(Item)
    report = engine.transform_source(gold_step, query)
    assert report.error is not None
    assert "requires a dict of silvers" in str(report.error)


def test_write_target_bronze_returns_counts_without_table_write(seeded_session):
    engine = SqlEngine(seeded_session, _config())
    bronze_step = SqlBronzeStep(
        name="items",
        rules={"value": [Item.value > 0]},
        model_class=Item,
    )
    query = seeded_session.query(Item)

    report = engine.write_target(bronze_step, query)

    assert report.error is None
    assert report.written_rows == 2
    assert report.failed_rows == 0


def test_write_target_wraps_schema_creation_failure(seeded_session, monkeypatch):
    engine = SqlEngine(seeded_session, _config())

    def silver_transform(session, bronze_query, silvers):
        return bronze_query

    silver_step = SqlSilverStep(
        name="silver_items",
        source_bronze="items",
        transform=silver_transform,
        rules={"value": [Item.value > 0]},
        table_name="target_items",
        model_class=TargetItem,
    )

    def fail_schema_creation(session, schema):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "sql_pipeline_builder.table_operations.create_schema_if_not_exists",
        fail_schema_creation,
    )

    query = seeded_session.query(Item)
    with pytest.raises(RuntimeError, match="Failed to create schema 'main': boom"):
        engine.write_target(silver_step, query)
