"""
Error-path tests for SQL execution engine behavior.
"""

from __future__ import annotations

from typing import Any

from pipeline_builder_base.models import ExecutionMode, PipelineConfig, ValidationThresholds
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from sql_pipeline_builder.execution import SqlExecutionEngine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep

Base: Any = declarative_base()


class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    value = Column(Integer)


class CleanEvent(Base):
    __tablename__ = "clean_events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    value = Column(Integer)


class GoldMetric(Base):
    __tablename__ = "gold_metrics"
    id = Column(Integer, primary_key=True)
    metric = Column(Integer)


def _config() -> PipelineConfig:
    return PipelineConfig(
        schema="analytics",
        thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
        verbose=False,
    )


def _bronze_step() -> SqlBronzeStep:
    return SqlBronzeStep(
        name="events",
        rules={"user_id": [Event.user_id.is_not(None)]},
        model_class=Event,
    )


def test_execute_pipeline_marks_missing_bronze_source_as_failed(sqlite_session):
    Base.metadata.create_all(sqlite_session.bind)
    engine = SqlExecutionEngine(sqlite_session, _config())
    bronze = _bronze_step()

    result = engine.execute_pipeline(
        bronze_steps={"events": bronze},
        silver_steps={},
        gold_steps={},
        bronze_sources={},
        mode=ExecutionMode.INITIAL,
    )

    assert not result.success
    assert len(result.step_results) == 1
    assert result.step_results[0].step_name == "events"
    assert not result.step_results[0].success
    assert "Bronze source 'events' not found in context" in (
        result.step_results[0].error_message or ""
    )


def test_execute_pipeline_marks_missing_validated_bronze_for_silver_failed(sqlite_session):
    Base.metadata.create_all(sqlite_session.bind)
    engine = SqlExecutionEngine(sqlite_session, _config())

    def silver_transform(session, bronze_query, silvers):
        return bronze_query

    silver = SqlSilverStep(
        name="clean_events",
        source_bronze="events",
        transform=silver_transform,
        rules={"value": [Event.value > 0]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    result = engine.execute_pipeline(
        bronze_steps={},
        silver_steps={"clean_events": silver},
        gold_steps={},
        bronze_sources={},
        mode=ExecutionMode.INITIAL,
    )

    assert not result.success
    assert len(result.step_results) == 1
    assert result.step_results[0].step_name == "clean_events"
    assert not result.step_results[0].success
    assert "Bronze source 'events' not found in context" in (
        result.step_results[0].error_message or ""
    )


def test_execute_pipeline_marks_gold_without_silvers_failed(sqlite_session):
    Base.metadata.create_all(sqlite_session.bind)
    engine = SqlExecutionEngine(sqlite_session, _config())

    def gold_transform(session, silvers):
        return list(silvers.values())[0]

    gold = SqlGoldStep(
        name="gold_metrics",
        transform=gold_transform,
        rules={"metric": []},
        table_name="gold_metrics",
        source_silvers=["clean_events"],
        model_class=GoldMetric,
    )

    result = engine.execute_pipeline(
        bronze_steps={},
        silver_steps={},
        gold_steps={"gold_metrics": gold},
        bronze_sources={},
        mode=ExecutionMode.INITIAL,
    )

    assert not result.success
    assert len(result.step_results) == 1
    assert result.step_results[0].step_name == "gold_metrics"
    assert not result.step_results[0].success
    assert "No silver sources available for gold step 'gold_metrics'" in (
        result.step_results[0].error_message or ""
    )
