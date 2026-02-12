"""Integration tests for SQL source steps against a real PostgreSQL instance.

These tests use testing.postgresql + SQLAlchemy to start a temporary PostgreSQL
database and exercise SqlAlchemySource, JdbcSource, and SQL-source steps
end-to-end.

Notes:
    - Requires SPARK_MODE=real (PySpark). Tests are skipped when running in mock mode.
    - Requires the optional 'sql' extras (sqlalchemy, pandas) and a working
      PostgreSQL toolchain for testing.postgresql (initdb/postgres in PATH).
    - JdbcSource tests require the PostgreSQL JDBC driver on the Spark classpath;
      use the spark_session_with_pg_jdbc fixture (driver is added via
      spark.jars.packages in conftest).
"""

from __future__ import annotations

import os
from typing import Iterator, Tuple

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    text,
)
from sqlalchemy.engine import Engine

from pipeline_builder.sql_source import JdbcSource, SqlAlchemySource, read_sql_source

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

# Ensure first Spark session in this process has PostgreSQL JDBC driver (spark.jars.packages
# on the builder is often ignored; PYSPARK_SUBMIT_ARGS is used at JVM startup).
if spark_mode == "real":
    _args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "org.postgresql" not in _args:
        if "io.delta" in _args:
            _args = _args.replace(
                "io.delta:delta-spark_2.12:3.0.0",
                "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3",
            )
        else:
            _args = (
                "--packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3 pyspark-shell"
            )
        os.environ["PYSPARK_SUBMIT_ARGS"] = _args

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        spark_mode != "real",
        reason="PostgreSQL SQL-source integration tests require SPARK_MODE=real (PySpark).",
    ),
]


@pytest.fixture(scope="module")
def postgres_engine() -> Iterator[Engine]:
    """Start a temporary PostgreSQL instance and return a SQLAlchemy engine.

    Uses testing.postgresql to spin up a throwaway Postgres server.
    """
    testing_pg = pytest.importorskip("testing.postgresql")
    try:
        pg = testing_pg.Postgresql()
    except Exception as exc:  # pragma: no cover - environment specific
        pytest.skip(f"Unable to start temporary PostgreSQL instance: {exc}")
        raise

    try:
        engine = create_engine(pg.url())
        yield engine
    finally:
        # Dispose engine and stop Postgres instance
        try:
            engine.dispose()
        except Exception:
            pass
        pg.stop()


def _jdbc_url_and_properties(engine: Engine) -> Tuple[str, dict]:
    """Build JDBC URL and connection properties from a SQLAlchemy engine (PostgreSQL)."""
    url = engine.url
    # url.host, url.port, url.database, url.username, url.password
    host = url.host or "127.0.0.1"
    port = url.port or 5432
    database = url.database or "test"
    user = url.username or "postgres"
    password = url.password or ""
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    properties = {"user": user}
    if password:
        properties["password"] = password
    return jdbc_url, properties


def _create_orders_table(engine: Engine) -> None:
    """Create a simple orders table and insert a few rows."""
    metadata = MetaData()
    orders = Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("customer", String(50), nullable=False),
        Column("amount", Integer, nullable=False),
        schema="public",
    )

    with engine.begin() as conn:
        # Ensure a clean table for each test invocation
        conn.execute(text("DROP TABLE IF EXISTS public.orders"))
        metadata.create_all(conn)
        conn.execute(
            insert(orders),
            [
                {"id": 1, "customer": "alice", "amount": 100},
                {"id": 2, "customer": "bob", "amount": 200},
                {"id": 3, "customer": "carol", "amount": 50},
            ],
        )


class TestSqlAlchemySourcePostgres:
    """Exercise SqlAlchemySource + read_sql_source against real PostgreSQL."""

    def test_read_sqlalchemy_table_from_postgres(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """SqlAlchemySource(table=...) reads all rows from PostgreSQL."""
        _create_orders_table(postgres_engine)

        # Use the same URL the engine was created from
        url = str(postgres_engine.url)
        source = SqlAlchemySource(url=url, table="orders", schema="public")

        df = read_sql_source(source, spark_session)

        # Expect 3 rows and required columns
        assert df.count() == 3
        cols = set(df.columns)
        assert {"id", "customer", "amount"}.issubset(cols)

    def test_read_sqlalchemy_query_from_postgres(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """SqlAlchemySource(query=...) executes a SELECT against PostgreSQL."""
        _create_orders_table(postgres_engine)

        url = str(postgres_engine.url)
        source = SqlAlchemySource(
            url=url,
            query="SELECT customer, SUM(amount) AS total FROM public.orders GROUP BY customer",
        )

        df = read_sql_source(source, spark_session)

        # Should aggregate to one row per customer
        rows = df.collect()
        totals_by_customer = {r["customer"]: r["total"] for r in rows}

        assert totals_by_customer["alice"] == 100
        assert totals_by_customer["bob"] == 200
        assert totals_by_customer["carol"] == 50


class TestSqlSourceStepsWithPostgres:
    """End-to-end pipeline using with_bronze_sql_source + PostgreSQL."""

    def test_bronze_sql_source_pipeline_reads_from_postgres(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """with_bronze_sql_source uses PostgreSQL as the backing source."""
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        _create_orders_table(postgres_engine)
        url = str(postgres_engine.url)

        # Ensure engine is configured for this Spark session
        configure_engine(spark=spark_session)
        F = get_default_functions()

        source = SqlAlchemySource(url=url, table="orders", schema="public")

        builder = PipelineBuilder(spark=spark_session, schema="pg_sql_source")
        builder.with_bronze_sql_source(
            name="orders_bronze",
            sql_source=source,
            rules={"id": [F.col("id").isNotNull()], "amount": [F.col("amount") > 0]},
        )

        pipeline = builder.to_pipeline()

        # No bronze_sources: runner must resolve SQL-source bronze step via PostgreSQL
        result = pipeline.run_initial_load(bronze_sources={})

        # We expect the pipeline to complete and to have written 3 rows
        assert result.status.value in ("completed", "COMPLETED")
        assert "orders_bronze" in result.bronze_results
        assert result.bronze_results["orders_bronze"]["rows_processed"] == 3


class TestJdbcSourcePostgres:
    """Exercise JdbcSource + read_sql_source against real PostgreSQL via JDBC.

    The PostgreSQL JDBC driver is loaded by setting PYSPARK_SUBMIT_ARGS (with
    org.postgresql:postgresql) when this module loads, so the first Spark session
    in the process has the driver on the classpath.
    """

    def test_read_jdbc_table_from_postgres(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """JdbcSource(table=...) reads all rows from PostgreSQL via spark.read.jdbc."""
        _create_orders_table(postgres_engine)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)
        source = JdbcSource(
            url=jdbc_url,
            table="public.orders",
            properties=properties,
            driver="org.postgresql.Driver",
        )
        df = read_sql_source(source, spark_session)
        assert df.count() == 3
        assert {"id", "customer", "amount"}.issubset(set(df.columns))

    def test_read_jdbc_query_from_postgres(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """JdbcSource(query=...) runs a subquery against PostgreSQL via JDBC."""
        _create_orders_table(postgres_engine)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)
        source = JdbcSource(
            url=jdbc_url,
            query="(SELECT customer, SUM(amount) AS total FROM public.orders GROUP BY customer) AS q",
            properties=properties,
            driver="org.postgresql.Driver",
        )
        df = read_sql_source(source, spark_session)
        rows = df.collect()
        totals_by_customer = {r["customer"]: r["total"] for r in rows}
        assert totals_by_customer["alice"] == 100
        assert totals_by_customer["bob"] == 200
        assert totals_by_customer["carol"] == 50

    def test_bronze_sql_source_pipeline_with_jdbc(
        self, postgres_engine: Engine, spark_session
    ) -> None:
        """with_bronze_sql_source with JdbcSource reads from PostgreSQL and runs pipeline."""
        from pipeline_builder import PipelineBuilder
        from pipeline_builder.engine_config import configure_engine
        from pipeline_builder.functions import get_default_functions

        _create_orders_table(postgres_engine)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)
        configure_engine(spark=spark_session)
        F = get_default_functions()
        source = JdbcSource(
            url=jdbc_url,
            table="public.orders",
            properties=properties,
            driver="org.postgresql.Driver",
        )
        builder = PipelineBuilder(spark=spark_session, schema="pg_sql_source_jdbc")
        builder.with_bronze_sql_source(
            name="orders_bronze",
            sql_source=source,
            rules={"id": [F.col("id").isNotNull()], "amount": [F.col("amount") > 0]},
        )
        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={})
        assert result.status.value in ("completed", "COMPLETED")
        assert "orders_bronze" in result.bronze_results
        assert result.bronze_results["orders_bronze"]["rows_processed"] == 3

