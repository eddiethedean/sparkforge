"""Unit tests for read_sql_source (mocked)."""

import builtins
from unittest.mock import MagicMock, patch

import pytest

from pipeline_builder.sql_source import read_sql_source
from pipeline_builder.sql_source.models import JdbcSource, SqlAlchemySource


class TestReadSqlSourceDispatch:
    def test_jdbc_source_calls_spark_read_jdbc(self):
        spark = type("Spark", (), {})()
        read_calls = []
        def mock_jdbc(self, **kwargs):
            read_calls.append(kwargs)
            return "mock_df"
        spark.read = type("Reader", (), {"jdbc": mock_jdbc})()
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="t",
            properties={"user": "u", "password": "p"},
        )
        result = read_sql_source(source, spark)
        assert result == "mock_df"
        assert len(read_calls) == 1
        assert read_calls[0]["url"] == "jdbc:postgresql://h/db"
        assert read_calls[0]["table"] == "t"
        assert read_calls[0]["properties"] == {"user": "u", "password": "p"}

    def test_jdbc_source_with_query(self):
        spark = type("Spark", (), {})()
        read_calls = []
        def mock_jdbc(self, **kwargs):
            read_calls.append(kwargs)
            return "mock_df"
        spark.read = type("Reader", (), {"jdbc": mock_jdbc})()
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            query="(SELECT 1) AS q",
            properties={},
        )
        read_sql_source(source, spark)
        assert read_calls[0]["table"] == "(SELECT 1) AS q"

    def test_jdbc_source_with_driver_adds_driver_to_properties(self):
        spark = type("Spark", (), {})()
        read_calls = []
        def mock_jdbc(self, **kwargs):
            read_calls.append(kwargs)
            return "mock_df"
        spark.read = type("Reader", (), {"jdbc": mock_jdbc})()
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="t",
            properties={"user": "u"},
            driver="org.postgresql.Driver",
        )
        read_sql_source(source, spark)
        assert read_calls[0]["properties"].get("driver") == "org.postgresql.Driver"
        assert read_calls[0]["properties"].get("user") == "u"

    def test_jdbc_source_driver_does_not_mutate_original_properties(self):
        spark = type("Spark", (), {})()
        def mock_jdbc(self, **kwargs):
            return "mock_df"
        spark.read = type("Reader", (), {"jdbc": mock_jdbc})()
        props = {"user": "u"}
        source = JdbcSource(
            url="jdbc:postgresql://h/db",
            table="t",
            properties=dict(props),
            driver="org.postgresql.Driver",
        )
        read_sql_source(source, spark)
        assert "driver" not in source.properties

    def test_invalid_source_type_raises(self):
        with pytest.raises(TypeError) as exc_info:
            read_sql_source("not a source", None)
        assert "JdbcSource" in str(exc_info.value) or "SqlAlchemySource" in str(
            exc_info.value
        )


class TestReadSqlSourceSqlAlchemy:
    """SqlAlchemySource reader paths (mocked pandas/sqlalchemy)."""

    def test_sqlalchemy_source_with_url_and_table_creates_engine_and_reads_table(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        spark = MagicMock()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df
        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            with patch.object(pd, "read_sql_table") as mock_read_sql_table:
                mock_read_sql_table.return_value = pd.DataFrame({"a": [1]})
                source = SqlAlchemySource(
                    url="postgresql://u:p@host/db",
                    table="orders",
                )
                result = read_sql_source(source, spark)
                mock_create_engine.assert_called_once_with("postgresql://u:p@host/db")
                mock_read_sql_table.assert_called_once_with("orders", mock_engine)
                assert result is mock_df

    def test_sqlalchemy_source_with_schema_passes_schema_to_read_sql_table(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        spark = MagicMock()
        spark.createDataFrame.return_value = "mock_spark_df"
        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            with patch.object(pd, "read_sql_table") as mock_read_sql_table:
                mock_read_sql_table.return_value = pd.DataFrame({"x": [1]})
                source = SqlAlchemySource(
                    url="postgresql://localhost/db",
                    table="t",
                    schema="public",
                )
                read_sql_source(source, spark)
                mock_read_sql_table.assert_called_once_with(
                    "t", mock_engine, schema="public"
                )

    def test_sqlalchemy_source_with_query_calls_read_sql(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        spark = MagicMock()
        spark.createDataFrame.return_value = "mock_spark_df"
        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            with patch.object(pd, "read_sql") as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({"id": [1]})
                source = SqlAlchemySource(
                    url="sqlite:///file.db",
                    query="SELECT * FROM t",
                )
                read_sql_source(source, spark)
                mock_read_sql.assert_called_once_with("SELECT * FROM t", mock_engine)

    def test_sqlalchemy_source_with_engine_uses_engine_not_create_engine(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        spark = MagicMock()
        spark.createDataFrame.return_value = "mock_spark_df"
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine") as mock_create_engine:
            with patch.object(pd, "read_sql_table") as mock_read_sql_table:
                mock_read_sql_table.return_value = pd.DataFrame({"a": [1]})
                source = SqlAlchemySource(engine=mock_engine, table="t")
                read_sql_source(source, spark)
                mock_create_engine.assert_not_called()
                mock_read_sql_table.assert_called_once_with("t", mock_engine)

    def test_sqlalchemy_source_missing_pandas_raises_runtime_error(self):
        spark = MagicMock()
        source = SqlAlchemySource(url="sqlite:///x.db", table="t")
        real_import = getattr(builtins, "__import__", __import__)

        def block_pandas(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("No module named 'pandas'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=block_pandas):
            with pytest.raises(RuntimeError) as exc_info:
                read_sql_source(source, spark)
        assert "pandas" in str(exc_info.value).lower()

    def test_sqlalchemy_source_missing_sqlalchemy_raises_runtime_error(self):
        spark = MagicMock()
        source = SqlAlchemySource(url="sqlite:///x.db", table="t")
        real_import = getattr(builtins, "__import__", __import__)

        def block_sqlalchemy(name, *args, **kwargs):
            if name == "sqlalchemy":
                raise ImportError("No module named 'sqlalchemy'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=block_sqlalchemy):
            with pytest.raises(RuntimeError) as exc_info:
                read_sql_source(source, spark)
        assert "sqlalchemy" in str(exc_info.value).lower()
