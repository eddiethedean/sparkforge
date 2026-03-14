"""Tests for optional SQL-source behavior in silver and gold executors."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipeline_builder.models import GoldStep, SilverStep
from pipeline_builder.sql_source import JdbcSource
from pipeline_builder.step_executors.gold import GoldStepExecutor
from pipeline_builder.step_executors.silver import SilverStepExecutor
from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.models import ExecutionMode


class TestSilverSqlOptionalBehavior:
    def test_silver_sql_optional_true_returns_empty_dataframe(
        self, spark, monkeypatch
    ):
        executor = SilverStepExecutor(spark=spark)

        sql_source = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="inventory",
            properties={"user": "u", "password": "p"},
        )
        step = SilverStep(
            name="inventory_snapshot",
            source_bronze="",
            rules={"sku": []},
            table_name="inventory_snapshot",
            existing=False,
            optional=True,
            sql_source=sql_source,
        )

        sentinel_df = MagicMock(name="empty_df")

        monkeypatch.setattr(
            "pipeline_builder.step_executors.base.BaseStepExecutor._empty_dataframe",
            lambda self: sentinel_df,
        )

        def failing_read_sql_source(source, spark):
            raise RuntimeError("DB not reachable")

        monkeypatch.setattr(
            "pipeline_builder.sql_source.read_sql_source",
            failing_read_sql_source,
        )

        result = executor.execute(
            step=step, context={}, mode=ExecutionMode.INITIAL, step_params=None
        )

        assert result is sentinel_df

    def test_silver_sql_optional_false_raises_execution_error(
        self, spark, monkeypatch
    ):
        executor = SilverStepExecutor(spark=spark)

        sql_source = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="inventory",
            properties={"user": "u", "password": "p"},
        )
        step = SilverStep(
            name="inventory_snapshot",
            source_bronze="",
            rules={"sku": []},
            table_name="inventory_snapshot",
            existing=False,
            optional=False,
            sql_source=sql_source,
        )

        def failing_read_sql_source(source, spark):
            raise RuntimeError("DB not reachable")

        monkeypatch.setattr(
            "pipeline_builder.sql_source.read_sql_source",
            failing_read_sql_source,
        )

        with pytest.raises(ExecutionError) as exc_info:
            executor.execute(
                step=step, context={}, mode=ExecutionMode.INITIAL, step_params=None
            )

        message = str(exc_info.value)
        assert "Silver SQL-source step 'inventory_snapshot'" in message
        assert "DB not reachable" in message


class TestGoldSqlOptionalBehavior:
    def test_gold_sql_optional_true_returns_empty_dataframe(
        self, spark, monkeypatch
    ):
        executor = GoldStepExecutor(spark=spark)

        sql_source = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="revenue",
            properties={"user": "u", "password": "p"},
        )
        step = GoldStep(
            name="revenue_by_region",
            rules={"region": []},
            table_name="revenue_by_region",
            existing=False,
            optional=True,
            sql_source=sql_source,
        )

        sentinel_df = MagicMock(name="empty_df")

        monkeypatch.setattr(
            "pipeline_builder.step_executors.base.BaseStepExecutor._empty_dataframe",
            lambda self: sentinel_df,
        )

        def failing_read_sql_source(source, spark):
            raise RuntimeError("DB not reachable")

        monkeypatch.setattr(
            "pipeline_builder.sql_source.read_sql_source",
            failing_read_sql_source,
        )

        result = executor.execute(
            step=step, context={}, mode=None, step_params=None, step_types=None
        )

        assert result is sentinel_df

    def test_gold_sql_optional_false_raises_execution_error(
        self, spark, monkeypatch
    ):
        executor = GoldStepExecutor(spark=spark)

        sql_source = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="revenue",
            properties={"user": "u", "password": "p"},
        )
        step = GoldStep(
            name="revenue_by_region",
            rules={"region": []},
            table_name="revenue_by_region",
            existing=False,
            optional=False,
            sql_source=sql_source,
        )

        def failing_read_sql_source(source, spark):
            raise RuntimeError("DB not reachable")

        monkeypatch.setattr(
            "pipeline_builder.sql_source.read_sql_source",
            failing_read_sql_source,
        )

        with pytest.raises(ExecutionError) as exc_info:
            executor.execute(
                step=step, context={}, mode=None, step_params=None, step_types=None
            )

        message = str(exc_info.value)
        assert "Gold SQL-source step 'revenue_by_region'" in message
        assert "DB not reachable" in message

