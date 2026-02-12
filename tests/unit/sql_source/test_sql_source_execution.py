"""Execution tests for SQL source steps: runner bronze resolution, silver/gold executors."""

import os
from unittest.mock import patch

import pytest

from pipeline_builder_base.models import ExecutionMode

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]

pytestmark = pytest.mark.skipif(
    spark_mode == "real",
    reason="SQL source execution tests use mock Spark",
)


class TestRunnerBronzeSqlSourceResolution:
    """Runner resolves SQL-source bronze steps into context before execution."""

    def test_run_initial_load_calls_read_sql_source_for_bronze_sql_step(
        self, mock_spark_session
    ):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.sql_source import JdbcSource

        read_calls = []

        def capture_read(source, spark):
            read_calls.append((source, spark))
            return mock_spark_session.createDataFrame([(1, "a")], ["id", "name"])

        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics")
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="orders",
            properties={"user": "u"},
        )
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=source,
            rules={"id": [F.col("id").isNotNull()]},
        )
        pipeline = builder.to_pipeline()

        with patch(
            "pipeline_builder.sql_source.read_sql_source",
            side_effect=capture_read,
        ):
            result = pipeline.run_initial_load(bronze_sources={})

        assert len(read_calls) == 1
        assert read_calls[0][0] is source
        assert read_calls[0][1] is mock_spark_session
        assert result is not None

    def test_bronze_sql_step_not_in_bronze_sources_still_resolved(
        self, mock_spark_session
    ):
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.sql_source import JdbcSource

        sql_source_called = []

        def mock_read(source, spark):
            sql_source_called.append(True)
            return mock_spark_session.createDataFrame([(1,)], ["id"])

        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics")
        builder.with_bronze_sql_source(
            name="from_db",
            sql_source=JdbcSource(
                url="jdbc:postgresql://h/db",
                table="t",
                properties={},
            ),
            rules={"id": [F.col("id").isNotNull()]},
        )
        pipeline = builder.to_pipeline()

        with patch(
            "pipeline_builder.sql_source.read_sql_source",
            side_effect=mock_read,
        ):
            pipeline.run_initial_load(bronze_sources={})

        assert len(sql_source_called) == 1


class TestSilverStepExecutorSqlSource:
    """SilverStepExecutor uses read_sql_source when step has sql_source."""

    def test_silver_executor_returns_read_sql_source_result(self, mock_spark_session):
        from pipeline_builder.models import SilverStep
        from pipeline_builder.sql_source import JdbcSource
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        expected_df = mock_spark_session.createDataFrame([(1, "x")], ["a", "b"])

        def mock_read(source, spark):
            return expected_df

        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="inv",
            properties={},
        )
        step = SilverStep(
            name="inventory",
            source_bronze="",
            transform=None,
            rules={"a": [F.col("a").isNotNull()]},
            table_name="inventory",
            sql_source=source,
        )
        executor = SilverStepExecutor(mock_spark_session)
        context = {}

        with patch(
            "pipeline_builder.sql_source.read_sql_source",
            side_effect=mock_read,
        ):
            result = executor.execute(
                step,
                context=context,
                mode=ExecutionMode.INITIAL,
            )

        assert result is expected_df


class TestGoldStepExecutorSqlSource:
    """GoldStepExecutor uses read_sql_source when step has sql_source."""

    def test_gold_executor_returns_read_sql_source_result(self, mock_spark_session):
        from pipeline_builder.models import GoldStep
        from pipeline_builder.sql_source import JdbcSource
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        expected_df = mock_spark_session.createDataFrame([("r1", 100.0)], ["region", "rev"])

        def mock_read(source, spark):
            return expected_df

        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            query="(SELECT region, SUM(amt) AS rev FROM t GROUP BY region) AS q",
            properties={},
        )
        step = GoldStep(
            name="revenue",
            transform=None,
            rules={"region": [F.col("region").isNotNull()]},
            table_name="revenue",
            sql_source=source,
        )
        executor = GoldStepExecutor(mock_spark_session)
        context = {}

        with patch(
            "pipeline_builder.sql_source.read_sql_source",
            side_effect=mock_read,
        ):
            result = executor.execute(
                step,
                context=context,
                mode=ExecutionMode.INITIAL,
            )

        assert result is expected_df
