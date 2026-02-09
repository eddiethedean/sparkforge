"""Unit tests for read_sql_source (mocked)."""

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

    def test_invalid_source_type_raises(self):
        with pytest.raises(TypeError) as exc_info:
            read_sql_source("not a source", None)
        assert "JdbcSource" in str(exc_info.value) or "SqlAlchemySource" in str(exc_info.value)
