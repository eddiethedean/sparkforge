"""Integration tests for JDBC read and write against a real PostgreSQL instance.

These tests use testing.postgresql + SQLAlchemy to start a temporary PostgreSQL
database and exercise JdbcSource reads and JdbcWriteConfig writes end-to-end.

Notes:
    - Requires SPARK_MODE=real (PySpark). Tests are skipped when running in mock mode.
    - Requires the PostgreSQL JDBC driver on the Spark classpath.
    - Uses testing.postgresql for a throwaway database instance.
"""

from __future__ import annotations

import os
import uuid
from typing import Iterator, Tuple

import pytest
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
    text,
)
from sqlalchemy.engine import Engine

from pipeline_builder.sql_source import (
    JdbcSource,
    JdbcWriteConfig,
    read_sql_source,
    write_jdbc,
)

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

if spark_mode == "real":
    _args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "org.postgresql" not in _args:
        if "io.delta" in _args:
            _args = _args.replace(
                "io.delta:delta-spark_2.12:3.0.0",
                "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3",
            )
        else:
            _args = "--packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3 pyspark-shell"
        os.environ["PYSPARK_SUBMIT_ARGS"] = _args

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.jdbc,
    pytest.mark.skipif(
        spark_mode != "real",
        reason="JDBC integration tests require SPARK_MODE=real (PySpark).",
    ),
]


@pytest.fixture(scope="module")
def postgres_engine() -> Iterator[Engine]:
    """Start a temporary PostgreSQL instance and return a SQLAlchemy engine."""
    testing_pg = pytest.importorskip("testing.postgresql")
    try:
        pg = testing_pg.Postgresql()
    except Exception as exc:
        pytest.skip(f"Unable to start temporary PostgreSQL instance: {exc}")
        raise

    try:
        engine = create_engine(pg.url())
        yield engine
    finally:
        try:
            engine.dispose()
        except Exception:
            pass
        pg.stop()


def _jdbc_url_and_properties(engine: Engine) -> Tuple[str, dict]:
    """Build JDBC URL and connection properties from a SQLAlchemy engine."""
    url = engine.url
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


def _create_test_table(engine: Engine, table_name: str) -> Table:
    """Create a test table with sample data."""
    metadata = MetaData()
    test_table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100), nullable=False),
        Column("value", Float, nullable=False),
        schema="public",
    )

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name}"))
        metadata.create_all(conn)
        conn.execute(
            insert(test_table),
            [
                {"id": 1, "name": "alpha", "value": 10.5},
                {"id": 2, "name": "beta", "value": 20.0},
                {"id": 3, "name": "gamma", "value": 30.75},
            ],
        )

    return test_table


def _get_unique_table_name() -> str:
    """Generate a unique table name for test isolation."""
    return f"test_{uuid.uuid4().hex[:8]}"


class TestJdbcRead:
    """Test JDBC read operations against PostgreSQL."""

    def test_read_jdbc_full_table(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading a full table via JDBC."""
        table_name = _get_unique_table_name()
        _create_test_table(postgres_engine, table_name)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        source = JdbcSource(
            url=jdbc_url,
            table=f"public.{table_name}",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)

        assert df.count() == 3
        cols = set(df.columns)
        assert {"id", "name", "value"}.issubset(cols)

        rows = {r["id"]: r for r in df.collect()}
        assert rows[1]["name"] == "alpha"
        assert rows[2]["value"] == 20.0

    def test_read_jdbc_with_query(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading with a custom query via JDBC."""
        table_name = _get_unique_table_name()
        _create_test_table(postgres_engine, table_name)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        source = JdbcSource(
            url=jdbc_url,
            query=f"(SELECT name, value FROM public.{table_name} WHERE value > 15) AS q",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)

        assert df.count() == 2
        names = {r["name"] for r in df.collect()}
        assert names == {"beta", "gamma"}

    def test_read_jdbc_with_aggregation_query(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading with an aggregation query."""
        table_name = _get_unique_table_name()
        _create_test_table(postgres_engine, table_name)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        source = JdbcSource(
            url=jdbc_url,
            query=f"(SELECT COUNT(*) AS cnt, SUM(value) AS total FROM public.{table_name}) AS agg",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)

        row = df.collect()[0]
        assert row["cnt"] == 3
        assert abs(row["total"] - 61.25) < 0.01


class TestJdbcWrite:
    """Test JDBC write operations against PostgreSQL."""

    def test_write_jdbc_new_table(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test writing to a new table via JDBC."""
        from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

        table_name = _get_unique_table_name()
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name}"))

        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("product", StringType(), False),
            StructField("price", FloatType(), False),
        ])

        data = [
            (1, "Widget A", 19.99),
            (2, "Widget B", 29.99),
            (3, "Widget C", 39.99),
        ]

        df = spark.createDataFrame(data, schema)

        config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{table_name}",
            properties=properties,
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(df, config)

        assert rows_written == 3

        with postgres_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table_name}")
            ).scalar()
            assert result == 3

    def test_write_jdbc_append_mode(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test appending to an existing table via JDBC."""
        from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

        table_name = _get_unique_table_name()
        _create_test_table(postgres_engine, table_name)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", FloatType(), False),
        ])

        new_data = [
            (4, "delta", 40.0),
            (5, "epsilon", 50.5),
        ]

        df = spark.createDataFrame(new_data, schema)

        config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{table_name}",
            properties=properties,
            mode="append",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(df, config)

        assert rows_written == 2

        with postgres_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table_name}")
            ).scalar()
            assert result == 5

    def test_write_jdbc_overwrite_mode(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test overwriting an existing table via JDBC."""
        from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

        table_name = _get_unique_table_name()
        _create_test_table(postgres_engine, table_name)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        with postgres_engine.connect() as conn:
            initial_count = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table_name}")
            ).scalar()
            assert initial_count == 3

        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", FloatType(), False),
        ])

        new_data = [
            (100, "new_alpha", 100.0),
            (200, "new_beta", 200.0),
        ]

        df = spark.createDataFrame(new_data, schema)

        config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{table_name}",
            properties=properties,
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(df, config)

        assert rows_written == 2

        with postgres_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table_name}")
            ).scalar()
            assert result == 2


class TestJdbcReadWriteRoundTrip:
    """Test complete read-write round-trip scenarios."""

    def test_read_transform_write_round_trip(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading data, transforming it, and writing to a new table."""
        source_table = _get_unique_table_name()
        target_table = _get_unique_table_name()
        _create_test_table(postgres_engine, source_table)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{target_table}"))

        source = JdbcSource(
            url=jdbc_url,
            table=f"public.{source_table}",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)
        assert df.count() == 3

        from pipeline_builder.compat import F

        transformed_df = df.withColumn("doubled_value", F.col("value") * 2)

        target_config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{target_table}",
            properties=properties,
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(transformed_df, target_config)
        assert rows_written == 3

        verify_source = JdbcSource(
            url=jdbc_url,
            table=f"public.{target_table}",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        result_df = read_sql_source(verify_source, spark)
        assert result_df.count() == 3
        assert "doubled_value" in result_df.columns

        rows = {r["id"]: r for r in result_df.collect()}
        assert rows[1]["doubled_value"] == 21.0
        assert rows[2]["doubled_value"] == 40.0

    def test_filter_and_write_subset(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading, filtering, and writing a subset of data."""
        source_table = _get_unique_table_name()
        target_table = _get_unique_table_name()
        _create_test_table(postgres_engine, source_table)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{target_table}"))

        source = JdbcSource(
            url=jdbc_url,
            table=f"public.{source_table}",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)

        from pipeline_builder.compat import F

        filtered_df = df.filter(F.col("value") > 15)
        assert filtered_df.count() == 2

        target_config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{target_table}",
            properties=properties,
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(filtered_df, target_config)
        assert rows_written == 2

        with postgres_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT name FROM public.{target_table} ORDER BY name")
            ).fetchall()
            names = [r[0] for r in result]
            assert names == ["beta", "gamma"]

    def test_aggregate_and_write(
        self, postgres_engine: Engine, spark
    ) -> None:
        """Test reading, aggregating, and writing aggregated results."""
        source_table = _get_unique_table_name()
        target_table = _get_unique_table_name()
        _create_test_table(postgres_engine, source_table)
        jdbc_url, properties = _jdbc_url_and_properties(postgres_engine)

        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{target_table}"))

        source = JdbcSource(
            url=jdbc_url,
            table=f"public.{source_table}",
            properties=properties,
            driver="org.postgresql.Driver",
        )

        df = read_sql_source(source, spark)

        from pipeline_builder.compat import F

        agg_df = df.agg(
            F.count("*").alias("total_count"),
            F.sum("value").alias("total_value"),
            F.avg("value").alias("avg_value"),
        )

        target_config = JdbcWriteConfig(
            url=jdbc_url,
            table=f"public.{target_table}",
            properties=properties,
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        rows_written = write_jdbc(agg_df, target_config)
        assert rows_written == 1

        with postgres_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT total_count, total_value FROM public.{target_table}")
            ).fetchone()
            assert result[0] == 3
            assert abs(result[1] - 61.25) < 0.01
