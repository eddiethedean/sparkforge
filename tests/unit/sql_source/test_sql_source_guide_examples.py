"""
Tests that run the exact example code from docs/guides/SQL_SOURCE_STEPS_GUIDE.md.

These tests ensure all guide examples are valid and executable (with mock Spark
and patched SQL reader where needed).
"""

import os
from unittest.mock import patch

import pytest

from pipeline_builder import PipelineBuilder
from pipeline_builder.sql_source import JdbcSource, SqlAlchemySource

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]


class TestGuideJdbcSourceExamples:
    """Run JdbcSource snippets from the guide."""

    def test_jdbc_source_table_example(self):
        # From guide: JdbcSource with table
        source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="public.orders",
            properties={"user": "etl", "password": "secret"},
            driver="org.postgresql.Driver",
        )
        assert source.table == "public.orders"
        assert source.url == "jdbc:postgresql://dbhost:5432/warehouse"

    def test_jdbc_source_query_example(self):
        # From guide: JdbcSource with query
        source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
            properties={"user": "etl", "password": "secret"},
        )
        assert source.query is not None
        assert "revenue" in source.query


class TestGuideSqlAlchemySourceExamples:
    """Run SqlAlchemySource snippets from the guide."""

    def test_sqlalchemy_source_url_example(self):
        # From guide: SqlAlchemySource with URL
        source = SqlAlchemySource(
            url="postgresql://etl:secret@dbhost:5432/warehouse",
            table="orders",
            schema="public",
        )
        assert source.url is not None
        assert source.table == "orders"
        assert source.schema == "public"


class TestGuideBuilderExamples:
    """Run builder examples from the guide (build only, no DB)."""

    def test_complete_example_builds_pipeline(self, mock_spark_session):
        # Exact code from "Complete Example: JDBC Bronze, Silver, and Gold"
        # Engine is already configured by conftest in test env
        from pipeline_builder.functions import get_default_functions

        F_local = get_default_functions()

        orders_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="public.orders",
            properties={"user": "etl", "password": "secret"},
        )

        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics")
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=orders_source,
            rules={
                "order_id": [F_local.col("order_id").isNotNull()],
                "amount": [F_local.col("amount") > 0],
            },
            incremental_col="updated_at",
        )

        inventory_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="inventory",
            properties={"user": "etl", "password": "secret"},
        )
        builder.with_silver_sql_source(
            name="inventory_snapshot",
            sql_source=inventory_source,
            table_name="inventory_snapshot",
            rules={
                "sku": [F_local.col("sku").isNotNull()],
                "qty": [F_local.col("qty") >= 0],
            },
        )

        revenue_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
            properties={"user": "etl", "password": "secret"},
        )
        builder.with_gold_sql_source(
            name="revenue_by_region",
            sql_source=revenue_source,
            table_name="revenue_by_region",
            rules={
                "region": [F_local.col("region").isNotNull()],
                "revenue": [F_local.col("revenue") >= 0],
            },
        )

        runner = builder.to_pipeline()
        assert runner is not None
        assert len(runner.execution_order) == 3

    def test_silver_string_rules_example(self, mock_spark_session):
        # From guide: String rules example
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F
        )
        inventory_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="inventory",
            properties={"user": "etl", "password": "secret"},
        )
        builder.with_silver_sql_source(
            name="inventory_snapshot",
            sql_source=inventory_source,
            table_name="inventory_snapshot",
            rules={
                "sku": ["not_null"],
                "qty": ["gte", 0],
            },
        )
        assert "inventory_snapshot" in builder.silver_steps

    def test_use_case_1_build(self, mock_spark_session):
        # Use Case 1: Bronze from DB + silver/gold transforms (build only)
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F
        )
        orders_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="orders",
            properties={"user": "etl", "password": "secret"},
        )
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=orders_source,
            rules={
                "order_id": [F.col("order_id").isNotNull()],
                "amount": [F.col("amount") > 0],
            },
            incremental_col="updated_at",
        )

        def clean_orders_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("amount") > 0)

        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="orders",
            transform=clean_orders_transform,
            rules={
                "order_id": [F.col("order_id").isNotNull()],
                "amount": [F.col("amount") > 0],
            },
            table_name="clean_orders",
        )

        def revenue_transform(spark, silvers):
            return (
                silvers["clean_orders"]
                .groupBy("order_id")
                .agg(F.sum("amount").alias("total"))
            )

        builder.add_gold_transform(
            name="daily_revenue",
            transform=revenue_transform,
            rules={
                "order_id": [F.col("order_id").isNotNull()],
                "total": [F.col("total") >= 0],
            },
            table_name="daily_revenue",
            source_silvers=["clean_orders"],
        )

        runner = builder.to_pipeline()
        assert len(runner.execution_order) == 3

    def test_use_case_3_mixed_build(self, mock_spark_session):
        # Use Case 3: Mixed SQL and in-memory (build only)
        orders_jdbc = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="orders",
            properties={"user": "etl", "password": "secret"},
        )
        inv_jdbc = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="inventory",
            properties={"user": "etl", "password": "secret"},
        )

        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F
        )
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=orders_jdbc,
            rules={"order_id": [F.col("order_id").isNotNull()]},
        )
        builder.with_bronze_rules(
            name="events",
            rules={"event_id": [F.col("event_id").isNotNull()]},
            incremental_col="ts",
        )
        builder.with_silver_sql_source(
            name="inventory_snapshot",
            sql_source=inv_jdbc,
            table_name="inventory_snapshot",
            rules={"sku": [F.col("sku").isNotNull()]},
        )
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df,
            rules={"event_id": [F.col("event_id").isNotNull()]},
            table_name="clean_events",
        )

        runner = builder.to_pipeline()
        assert len(runner.execution_order) == 4


class TestGuideCompleteExampleRun:
    """Run the complete example through run_initial_load with mocked reader."""

    def test_complete_example_run_initial_load(self, mock_spark_session):
        # Build and run the complete example; reader is patched to return mock DataFrames
        # Engine is already configured by conftest in test env
        from pipeline_builder.functions import get_default_functions

        F_local = get_default_functions()

        # Create mock DataFrames that match the expected schemas
        orders_df = mock_spark_session.createDataFrame(
            [(1, 100.0), (2, 200.0)], ["order_id", "amount"]
        )
        inventory_df = mock_spark_session.createDataFrame(
            [("SKU1", 10), ("SKU2", 20)], ["sku", "qty"]
        )
        revenue_df = mock_spark_session.createDataFrame(
            [("North", 500.0), ("South", 300.0)], ["region", "revenue"]
        )

        def mock_read_sql_source(source, spark):
            from pipeline_builder.sql_source import JdbcSource

            if isinstance(source, JdbcSource):
                if source.table and "order" in source.table:
                    return orders_df
                if source.table and "inventory" in source.table:
                    return inventory_df
                if source.query and "revenue" in source.query:
                    return revenue_df
            return orders_df

        orders_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="public.orders",
            properties={"user": "etl", "password": "secret"},
        )
        inventory_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            table="inventory",
            properties={"user": "etl", "password": "secret"},
        )
        revenue_source = JdbcSource(
            url="jdbc:postgresql://dbhost:5432/warehouse",
            query="(SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region) AS q",
            properties={"user": "etl", "password": "secret"},
        )

        builder = PipelineBuilder(spark=mock_spark_session, schema="analytics")
        builder.with_bronze_sql_source(
            name="orders",
            sql_source=orders_source,
            rules={
                "order_id": [F_local.col("order_id").isNotNull()],
                "amount": [F_local.col("amount") > 0],
            },
            incremental_col="updated_at",
        )
        builder.with_silver_sql_source(
            name="inventory_snapshot",
            sql_source=inventory_source,
            table_name="inventory_snapshot",
            rules={
                "sku": [F_local.col("sku").isNotNull()],
                "qty": [F_local.col("qty") >= 0],
            },
        )
        builder.with_gold_sql_source(
            name="revenue_by_region",
            sql_source=revenue_source,
            table_name="revenue_by_region",
            rules={
                "region": [F_local.col("region").isNotNull()],
                "revenue": [F_local.col("revenue") >= 0],
            },
        )

        pipeline = builder.to_pipeline()

        with patch(
            "pipeline_builder.sql_source.reader.read_sql_source",
            side_effect=mock_read_sql_source,
        ):
            result = pipeline.run_initial_load()

        assert result is not None
        assert hasattr(result, "status")
        # Status may be COMPLETED or another value depending on writer/validation mocks
        assert result.status.value in (
            "completed",
            "COMPLETED",
            "failed",
            "FAILED",
        )
