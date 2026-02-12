"""Unit tests for with_bronze_sql_source, with_silver_sql_source, with_gold_sql_source."""

import os
from unittest.mock import patch

import pytest

from pipeline_builder.pipeline.builder import PipelineBuilder
from pipeline_builder.sql_source import JdbcSource, SqlAlchemySource

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]

pytestmark = pytest.mark.skipif(
    spark_mode == "real",
    reason="SQL source builder tests use mock Spark",
)


class TestWithBronzeSqlSource:
    def test_adds_bronze_step_with_sql_source(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="orders",
            properties={"user": "u", "password": "p"},
        )
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=source,
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="updated_at",
        )
        assert "orders" in builder.bronze_steps
        step = builder.bronze_steps["orders"]
        assert step.sql_source is source
        assert step.name == "orders"
        assert step.incremental_col == "updated_at"

    def test_rejects_empty_rules(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(url="jdbc:postgresql://h/db", table="t", properties={})
        with pytest.raises(Exception):
            builder.with_bronze_sql_source(name="x", sql_source=source, rules={})

    def test_accepts_sqlalchemy_source(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = SqlAlchemySource(url="sqlite:///x.db", table="t")
        builder.with_bronze_sql_source(
            name="tbl",
            sql_source=source,
            rules={"a": [F.col("a").isNotNull()]},
        )
        assert builder.bronze_steps["tbl"].sql_source is source


class TestWithSilverSqlSource:
    def test_adds_silver_step_with_sql_source(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(url="jdbc:postgresql://h/db", table="inv", properties={})
        builder.with_silver_sql_source(
            name="inventory",
            sql_source=source,
            table_name="inventory_snapshot",
            rules={"sku": [F.col("sku").isNotNull()]},
        )
        assert "inventory" in builder.silver_steps
        step = builder.silver_steps["inventory"]
        assert step.sql_source is source
        assert step.table_name == "inventory_snapshot"
        assert step.source_bronze == ""
        assert step.transform is None


class TestWithGoldSqlSource:
    def test_adds_gold_step_with_sql_source(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(url="jdbc:postgresql://h/db", query="(SELECT 1) AS q", properties={})
        builder.with_gold_sql_source(
            name="metrics",
            sql_source=source,
            table_name="metrics",
            rules={"x": [F.col("x").isNotNull()]},
        )
        assert "metrics" in builder.gold_steps
        step = builder.gold_steps["metrics"]
        assert step.sql_source is source
        assert step.table_name == "metrics"
        assert step.transform is None
        assert step.source_silvers is None


class TestSqlSourceBuilderValidation:
    """Validation and error paths for SQL source builder methods."""

    def test_with_bronze_sql_source_rejects_invalid_source_type(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        with pytest.raises(Exception) as exc_info:
            builder.with_bronze_sql_source(
                name="x",
                sql_source="not a source",  # type: ignore[arg-type]
                rules={"id": [F.col("id").isNotNull()]},
            )
        assert "JdbcSource" in str(exc_info.value) or "SqlAlchemySource" in str(
            exc_info.value
        )

    def test_with_bronze_sql_source_duplicate_name_raises(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="t",
            properties={},
        )
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=source,
            rules={"id": [F.col("id").isNotNull()]},
        )
        with pytest.raises(Exception) as exc_info:
            builder.with_bronze_sql_source(
                name="orders",
                sql_source=source,
                rules={"id": [F.col("id").isNotNull()]},
            )
        assert "orders" in str(exc_info.value) or "already" in str(exc_info.value).lower()

    def test_with_bronze_sql_source_accepts_schema_parameter(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="t",
            properties={},
        )
        with patch.object(builder, "_validate_schema"):
            builder.with_bronze_sql_source(
                name="orders",
                sql_source=source,
                rules={"id": [F.col("id").isNotNull()]},
                schema="custom_bronze_schema",
            )
        assert builder.bronze_steps["orders"].schema == "custom_bronze_schema"

    def test_with_silver_sql_source_rejects_invalid_source_type(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        with pytest.raises(Exception) as exc_info:
            builder.with_silver_sql_source(
                name="inv",
                sql_source=None,  # type: ignore[arg-type]
                table_name="inv",
                rules={"sku": [F.col("sku").isNotNull()]},
            )
        assert "JdbcSource" in str(exc_info.value) or "SqlAlchemySource" in str(
            exc_info.value
        )

    def test_with_gold_sql_source_rejects_invalid_source_type(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        with pytest.raises(Exception) as exc_info:
            builder.with_gold_sql_source(
                name="m",
                sql_source=123,  # type: ignore[arg-type]
                table_name="m",
                rules={"x": [F.col("x").isNotNull()]},
            )
        assert "JdbcSource" in str(exc_info.value) or "SqlAlchemySource" in str(
            exc_info.value
        )

    def test_with_silver_sql_source_accepts_schema_parameter(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="inv",
            properties={},
        )
        with patch.object(builder, "_validate_schema"):
            builder.with_silver_sql_source(
                name="inventory",
                sql_source=source,
                table_name="inventory_snapshot",
                rules={"sku": [F.col("sku").isNotNull()]},
                schema="custom_schema",
            )
        assert builder.silver_steps["inventory"].schema == "custom_schema"

    def test_with_gold_sql_source_accepts_schema_parameter(self, mock_spark_session):
        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics", functions=F)
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            query="(SELECT 1 AS x) AS q",
            properties={},
        )
        with patch.object(builder, "_validate_schema"):
            builder.with_gold_sql_source(
                name="metrics",
                sql_source=source,
                table_name="metrics",
                rules={"x": [F.col("x").isNotNull()]},
                schema="gold_schema",
            )
        assert builder.gold_steps["metrics"].schema == "gold_schema"
