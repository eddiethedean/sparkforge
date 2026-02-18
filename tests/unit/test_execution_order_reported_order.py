"""
Tests that pipeline steps run in the order initially reported by the builder.

Ensures:
- Runner._get_all_steps(None) returns steps in execution_order when set, so users
  can plan based on the reported order.
- When the engine falls back to dependency analysis (no execution_order), it uses
  creation_order from the steps list so execution order is deterministic and
  matches the order the caller passed.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.models import BronzeStep, GoldStep, SilverStep
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder_base.models import PipelineConfig


class TestGetAllStepsOrder:
    """Test that _get_all_steps returns steps in execution_order when set."""

    def test_get_all_steps_returns_steps_in_execution_order_when_set(
        self, spark_session
    ):
        """When execution_order is set, _get_all_steps(None) returns steps in that order."""
        config = PipelineConfig.create_default(schema="test_schema")
        bronze_a = BronzeStep(name="bronze_a", rules={"x": ["not_null"]}, schema="test_schema")
        bronze_b = BronzeStep(name="bronze_b", rules={"x": ["not_null"]}, schema="test_schema")
        silver_a = SilverStep(
            name="silver_a",
            source_bronze="bronze_a",
            transform=lambda spark, df, silvers: df,
            rules={"x": ["not_null"]},
            table_name="silver_a",
            schema="test_schema",
        )
        # Reported order: bronze_b first, then bronze_a, then silver_a (e.g. user added in that order)
        execution_order = ["bronze_b", "bronze_a", "silver_a"]

        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            bronze_steps={"bronze_a": bronze_a, "bronze_b": bronze_b},
            silver_steps={"silver_a": silver_a},
            gold_steps={},
            execution_order=execution_order,
        )

        steps = runner._get_all_steps(None)

        assert [s.name for s in steps] == execution_order

    def test_get_all_steps_fallback_when_execution_order_none(self, spark_session):
        """When execution_order is None, _get_all_steps returns bronze then silver then gold."""
        config = PipelineConfig.create_default(schema="test_schema")
        bronze_a = BronzeStep(name="bronze_a", rules={"x": ["not_null"]}, schema="test_schema")
        silver_a = SilverStep(
            name="silver_a",
            source_bronze="bronze_a",
            transform=lambda spark, df, silvers: df,
            rules={"x": ["not_null"]},
            table_name="silver_a",
            schema="test_schema",
        )

        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            bronze_steps={"bronze_a": bronze_a},
            silver_steps={"silver_a": silver_a},
            gold_steps={},
            execution_order=None,
        )

        steps = runner._get_all_steps(None)

        assert [s.name for s in steps] == ["bronze_a", "silver_a"]

    def test_get_all_steps_fallback_when_execution_order_mismatch(self, spark_session):
        """When execution_order does not cover all steps, fall back to dict order."""
        config = PipelineConfig.create_default(schema="test_schema")
        bronze_a = BronzeStep(name="bronze_a", rules={"x": ["not_null"]}, schema="test_schema")
        silver_a = SilverStep(
            name="silver_a",
            source_bronze="bronze_a",
            transform=lambda spark, df, silvers: df,
            rules={"x": ["not_null"]},
            table_name="silver_a",
            schema="test_schema",
        )
        # execution_order missing silver_a (e.g. stale), so we fall back
        execution_order = ["bronze_a"]

        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            bronze_steps={"bronze_a": bronze_a},
            silver_steps={"silver_a": silver_a},
            gold_steps={},
            execution_order=execution_order,
        )

        steps = runner._get_all_steps(None)

        # Fallback: bronze then silver (dict order)
        assert [s.name for s in steps] == ["bronze_a", "silver_a"]

    def test_get_all_steps_with_provided_steps_returns_unchanged(self, spark_session):
        """When steps is provided, _get_all_steps returns it unchanged."""
        config = PipelineConfig.create_default(schema="test_schema")
        runner = SimplePipelineRunner(spark=spark_session, config=config)
        provided = [
            BronzeStep(name="b", rules={"x": ["not_null"]}, schema="s"),
            SilverStep(
                name="s",
                source_bronze="b",
                transform=lambda spark, df, silvers: df,
                rules={"x": ["not_null"]},
                table_name="s",
                schema="s",
            ),
        ]

        result = runner._get_all_steps(provided)

        assert result is provided
        assert [s.name for s in result] == ["b", "s"]


class TestRunInitialLoadPassesStepsInExecutionOrder:
    """Test that run_initial_load passes steps in execution_order to the engine."""

    def test_run_initial_load_passes_steps_in_execution_order(self, spark_session):
        """run_initial_load(None) should call execute_pipeline with steps in execution_order."""
        config = PipelineConfig.create_default(schema="test_schema")
        bronze_a = BronzeStep(name="bronze_a", rules={"x": ["not_null"]}, schema="test_schema")
        bronze_b = BronzeStep(name="bronze_b", rules={"x": ["not_null"]}, schema="test_schema")
        silver_a = SilverStep(
            name="silver_a",
            source_bronze="bronze_a",
            transform=lambda spark, df, silvers: df,
            rules={"x": ["not_null"]},
            table_name="silver_a",
            schema="test_schema",
        )
        execution_order = ["bronze_b", "bronze_a", "silver_a"]

        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            bronze_steps={"bronze_a": bronze_a, "bronze_b": bronze_b},
            silver_steps={"silver_a": silver_a},
            gold_steps={},
            execution_order=execution_order,
        )

        mock_engine = Mock()
        from datetime import datetime

        from pipeline_builder.execution import ExecutionMode, ExecutionResult

        mock_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
        )
        runner.execution_engine = mock_engine

        runner.run_initial_load()

        mock_engine.execute_pipeline.assert_called_once()
        steps_passed = mock_engine.execute_pipeline.call_args[0][0]
        execution_order_passed = mock_engine.execute_pipeline.call_args[1].get(
            "execution_order"
        )
        assert [s.name for s in steps_passed] == execution_order
        assert execution_order_passed == execution_order
