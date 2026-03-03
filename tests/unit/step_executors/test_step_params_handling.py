"""Tests for step_params handling in silver and gold executors."""

from __future__ import annotations

import pytest

from pipeline_builder.models import GoldStep, SilverStep
from pipeline_builder.step_executors.gold import GoldStepExecutor
from pipeline_builder.step_executors.silver import SilverStepExecutor
from pipeline_builder_base.models import ExecutionMode


class TestSilverStepParamsHandling:
    def test_silver_falls_back_on_typeerror_and_succeeds(self, mock_spark_session):
        executor = SilverStepExecutor(spark=mock_spark_session)

        calls = {"with_params": 0, "without_params": 0}

        def transform(spark, bronze_df, prior_silvers, **kwargs):
            if kwargs:
                calls["with_params"] += 1
                raise TypeError("unexpected keyword argument")
            calls["without_params"] += 1
            return "ok"

        step = SilverStep(
            name="s1",
            source_bronze="bronze_src",
            rules={"x": []},
            table_name="t_silver",
            transform=transform,
        )

        context = {"bronze_src": object()}

        result = executor.execute(
            step=step,
            context=context,
            mode=ExecutionMode.INITIAL,
            step_params={"foo": "bar"},
            step_types=None,
        )

        assert result == "ok"
        assert calls["with_params"] == 1
        assert calls["without_params"] == 1

    def test_silver_does_not_swallow_non_typeerror(self, mock_spark_session):
        executor = SilverStepExecutor(spark=mock_spark_session)

        def transform(spark, bronze_df, prior_silvers, **kwargs):
            raise ValueError("boom")

        step = SilverStep(
            name="s1",
            source_bronze="bronze_src",
            rules={"x": []},
            table_name="t_silver",
            transform=transform,
        )

        context = {"bronze_src": object()}

        with pytest.raises(ValueError) as exc_info:
            executor.execute(
                step=step,
                context=context,
                mode=ExecutionMode.INITIAL,
                step_params={"foo": "bar"},
                step_types=None,
            )

        assert "boom" in str(exc_info.value)


class TestGoldStepParamsHandling:
    def test_gold_falls_back_on_typeerror_and_succeeds(self, mock_spark_session):
        executor = GoldStepExecutor(spark=mock_spark_session)

        calls = {"with_params": 0, "without_params": 0}

        def transform(spark, silvers, **kwargs):
            if kwargs:
                calls["with_params"] += 1
                raise TypeError("unexpected keyword")
            calls["without_params"] += 1
            return "ok"

        step = GoldStep(
            name="g1",
            rules={"x": []},
            table_name="t_gold",
            transform=transform,
            source_silvers=["s1"],
        )

        context = {"s1": object()}

        result = executor.execute(
            step=step,
            context=context,
            mode=None,
            step_params={"foo": "bar"},
            step_types={"s1": "silver"},
        )

        assert result == "ok"
        assert calls["with_params"] == 1
        assert calls["without_params"] == 1

    def test_gold_does_not_swallow_non_typeerror(self, mock_spark_session):
        executor = GoldStepExecutor(spark=mock_spark_session)

        def transform(spark, silvers, **kwargs):
            raise ValueError("boom")

        step = GoldStep(
            name="g1",
            rules={"x": []},
            table_name="t_gold",
            transform=transform,
            source_silvers=["s1"],
        )

        context = {"s1": object()}

        with pytest.raises(ValueError) as exc_info:
            executor.execute(
                step=step,
                context=context,
                mode=None,
                step_params={"foo": "bar"},
                step_types={"s1": "silver"},
            )

        assert "boom" in str(exc_info.value)

