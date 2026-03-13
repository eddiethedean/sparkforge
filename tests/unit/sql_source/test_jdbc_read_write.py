"""Unit tests for JDBC read and write functionality (mocked).

Tests cover:
- JdbcSource reading via spark.read.jdbc
- JdbcWriteConfig validation
- write_jdbc via spark.write.jdbc
- Error handling and edge cases
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from pipeline_builder.sql_source import (
    JdbcSource,
    JdbcWriteConfig,
    read_sql_source,
    write_jdbc,
    write_sql_target,
)
from pipeline_builder_base.errors import ValidationError


class TestJdbcSourceRead:
    """Test JdbcSource reading functionality."""

    def _create_mock_spark(self):
        """Create a mock Spark session with read.jdbc method."""
        spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 10
        spark.read.jdbc.return_value = mock_df
        return spark, mock_df

    def test_read_jdbc_with_table(self):
        """Test reading from a table via JDBC."""
        spark, mock_df = self._create_mock_spark()

        source = JdbcSource(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="public.users",
            properties={"user": "testuser", "password": "testpass"},
        )

        result = read_sql_source(source, spark)

        assert result == mock_df
        spark.read.jdbc.assert_called_once()
        call_kwargs = spark.read.jdbc.call_args[1]
        assert call_kwargs["url"] == "jdbc:postgresql://localhost:5432/testdb"
        assert call_kwargs["table"] == "public.users"
        assert call_kwargs["properties"]["user"] == "testuser"
        assert call_kwargs["properties"]["password"] == "testpass"

    def test_read_jdbc_with_query(self):
        """Test reading from a query (subquery) via JDBC."""
        spark, mock_df = self._create_mock_spark()

        source = JdbcSource(
            url="jdbc:postgresql://localhost:5432/testdb",
            query="(SELECT id, name FROM users WHERE active = true) AS active_users",
            properties={"user": "testuser"},
        )

        result = read_sql_source(source, spark)

        assert result == mock_df
        call_kwargs = spark.read.jdbc.call_args[1]
        assert "(SELECT id, name FROM users WHERE active = true)" in call_kwargs["table"]

    def test_read_jdbc_with_driver(self):
        """Test that driver is added to properties."""
        spark, _ = self._create_mock_spark()

        source = JdbcSource(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="orders",
            properties={"user": "testuser"},
            driver="org.postgresql.Driver",
        )

        read_sql_source(source, spark)

        call_kwargs = spark.read.jdbc.call_args[1]
        assert call_kwargs["properties"]["driver"] == "org.postgresql.Driver"
        assert call_kwargs["properties"]["user"] == "testuser"

    def test_read_jdbc_does_not_mutate_original_properties(self):
        """Test that original properties dict is not mutated."""
        spark, _ = self._create_mock_spark()

        original_props = {"user": "testuser"}
        source = JdbcSource(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="orders",
            properties=dict(original_props),
            driver="org.postgresql.Driver",
        )

        read_sql_source(source, spark)

        assert "driver" not in source.properties

    def test_read_jdbc_with_empty_properties(self):
        """Test reading with empty properties dict."""
        spark, mock_df = self._create_mock_spark()

        source = JdbcSource(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="orders",
            properties={},
        )

        result = read_sql_source(source, spark)

        assert result == mock_df
        call_kwargs = spark.read.jdbc.call_args[1]
        assert call_kwargs["properties"] == {}

    def test_jdbc_source_requires_url(self):
        """Test that JdbcSource requires a URL."""
        with pytest.raises(ValidationError, match="non-empty 'url'"):
            JdbcSource(url="", table="orders", properties={})

    def test_jdbc_source_requires_table_or_query(self):
        """Test that JdbcSource requires table or query."""
        with pytest.raises(ValidationError, match="table.*query"):
            JdbcSource(
                url="jdbc:postgresql://localhost/db",
                properties={},
            )

    def test_jdbc_source_rejects_both_table_and_query(self):
        """Test that JdbcSource rejects both table and query."""
        with pytest.raises(ValidationError, match="table.*query"):
            JdbcSource(
                url="jdbc:postgresql://localhost/db",
                table="orders",
                query="(SELECT * FROM orders) AS q",
                properties={},
            )


class TestJdbcWriteConfigValidation:
    """Test JdbcWriteConfig validation."""

    def test_valid_config_creation(self):
        """Test creating a valid JdbcWriteConfig."""
        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="public.orders",
            properties={"user": "testuser", "password": "testpass"},
            mode="append",
            driver="org.postgresql.Driver",
        )

        assert config.url == "jdbc:postgresql://localhost:5432/testdb"
        assert config.table == "public.orders"
        assert config.mode == "append"
        assert config.driver == "org.postgresql.Driver"

    def test_config_requires_url(self):
        """Test that JdbcWriteConfig requires a URL."""
        with pytest.raises(ValidationError, match="non-empty 'url'"):
            JdbcWriteConfig(
                url="",
                table="orders",
                properties={},
            )

    def test_config_requires_table(self):
        """Test that JdbcWriteConfig requires a table."""
        with pytest.raises(ValidationError, match="non-empty 'table'"):
            JdbcWriteConfig(
                url="jdbc:postgresql://localhost/db",
                table="",
                properties={},
            )

    def test_config_requires_valid_mode(self):
        """Test that JdbcWriteConfig requires a valid mode."""
        with pytest.raises(ValidationError, match="mode"):
            JdbcWriteConfig(
                url="jdbc:postgresql://localhost/db",
                table="orders",
                properties={},
                mode="invalid_mode",  # type: ignore
            )

    def test_config_valid_modes(self):
        """Test all valid write modes."""
        for mode in ["overwrite", "append", "ignore", "error"]:
            config = JdbcWriteConfig(
                url="jdbc:postgresql://localhost/db",
                table="orders",
                properties={},
                mode=mode,  # type: ignore
            )
            assert config.mode == mode

    def test_config_default_mode_is_append(self):
        """Test that default mode is append."""
        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
        )
        assert config.mode == "append"


class TestWriteJdbc:
    """Test write_jdbc functionality."""

    def _create_mock_df(self, row_count: int = 10):
        """Create a mock DataFrame with write.jdbc method."""
        mock_df = Mock()
        mock_df.count.return_value = row_count
        mock_df.write.jdbc = Mock()
        return mock_df

    def test_write_jdbc_basic(self):
        """Test basic JDBC write."""
        mock_df = self._create_mock_df(row_count=100)

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="public.orders",
            properties={"user": "testuser", "password": "testpass"},
            mode="append",
        )

        rows_written = write_jdbc(mock_df, config)

        assert rows_written == 100
        mock_df.write.jdbc.assert_called_once()
        call_kwargs = mock_df.write.jdbc.call_args[1]
        assert call_kwargs["url"] == "jdbc:postgresql://localhost:5432/testdb"
        assert call_kwargs["table"] == "public.orders"
        assert call_kwargs["mode"] == "append"
        assert call_kwargs["properties"]["user"] == "testuser"

    def test_write_jdbc_with_driver(self):
        """Test JDBC write with driver specified."""
        mock_df = self._create_mock_df()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost:5432/testdb",
            table="orders",
            properties={"user": "testuser"},
            mode="overwrite",
            driver="org.postgresql.Driver",
        )

        write_jdbc(mock_df, config)

        call_kwargs = mock_df.write.jdbc.call_args[1]
        assert call_kwargs["properties"]["driver"] == "org.postgresql.Driver"
        assert call_kwargs["mode"] == "overwrite"

    def test_write_jdbc_overwrite_mode(self):
        """Test JDBC write in overwrite mode."""
        mock_df = self._create_mock_df()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
            mode="overwrite",
        )

        write_jdbc(mock_df, config)

        call_kwargs = mock_df.write.jdbc.call_args[1]
        assert call_kwargs["mode"] == "overwrite"

    def test_write_jdbc_ignore_mode(self):
        """Test JDBC write in ignore mode."""
        mock_df = self._create_mock_df()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
            mode="ignore",
        )

        write_jdbc(mock_df, config)

        call_kwargs = mock_df.write.jdbc.call_args[1]
        assert call_kwargs["mode"] == "ignore"

    def test_write_jdbc_error_mode(self):
        """Test JDBC write in error mode."""
        mock_df = self._create_mock_df()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
            mode="error",
        )

        write_jdbc(mock_df, config)

        call_kwargs = mock_df.write.jdbc.call_args[1]
        assert call_kwargs["mode"] == "error"

    def test_write_jdbc_does_not_mutate_properties(self):
        """Test that write_jdbc doesn't mutate original properties."""
        mock_df = self._create_mock_df()

        original_props = {"user": "testuser"}
        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties=dict(original_props),
            driver="org.postgresql.Driver",
        )

        write_jdbc(mock_df, config)

        assert "driver" not in config.properties

    def test_write_jdbc_returns_row_count(self):
        """Test that write_jdbc returns the correct row count."""
        mock_df = self._create_mock_df(row_count=42)

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
        )

        rows_written = write_jdbc(mock_df, config)

        assert rows_written == 42


class TestWriteSqlTarget:
    """Test write_sql_target dispatch functionality."""

    def test_dispatch_to_jdbc(self):
        """Test that JdbcWriteConfig dispatches to write_jdbc."""
        mock_df = Mock()
        mock_df.count.return_value = 10
        mock_df.write.jdbc = Mock()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={},
        )

        rows = write_sql_target(mock_df, config)

        assert rows == 10
        mock_df.write.jdbc.assert_called_once()

    def test_invalid_config_type_raises(self):
        """Test that invalid config type raises TypeError."""
        mock_df = Mock()

        with pytest.raises(TypeError, match="JdbcWriteConfig or SqlAlchemyWriteConfig"):
            write_sql_target(mock_df, "invalid_config")  # type: ignore


class TestJdbcReadWriteRoundTrip:
    """Test read/write round-trip scenarios (mocked)."""

    def test_read_modify_write_flow(self):
        """Test a typical read-modify-write workflow."""
        read_spark = Mock()
        read_df = Mock()
        read_df.count.return_value = 5
        read_spark.read.jdbc.return_value = read_df

        source = JdbcSource(
            url="jdbc:postgresql://localhost/sourcedb",
            table="source_table",
            properties={"user": "reader"},
        )

        df = read_sql_source(source, read_spark)
        assert df.count() == 5

        transformed_df = Mock()
        transformed_df.count.return_value = 5
        transformed_df.write.jdbc = Mock()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/targetdb",
            table="target_table",
            properties={"user": "writer"},
            mode="append",
        )

        rows = write_jdbc(transformed_df, config)

        assert rows == 5
        transformed_df.write.jdbc.assert_called_once()

    def test_read_aggregate_write_flow(self):
        """Test reading, aggregating, and writing to JDBC."""
        spark = Mock()
        source_df = Mock()
        source_df.count.return_value = 1000
        spark.read.jdbc.return_value = source_df

        source = JdbcSource(
            url="jdbc:postgresql://localhost/analytics",
            table="raw_events",
            properties={"user": "analyst"},
        )

        df = read_sql_source(source, spark)
        assert df.count() == 1000

        aggregated_df = Mock()
        aggregated_df.count.return_value = 50
        aggregated_df.write.jdbc = Mock()

        config = JdbcWriteConfig(
            url="jdbc:postgresql://localhost/analytics",
            table="daily_summary",
            properties={"user": "analyst"},
            mode="overwrite",
        )

        rows = write_jdbc(aggregated_df, config)

        assert rows == 50
        call_kwargs = aggregated_df.write.jdbc.call_args[1]
        assert call_kwargs["mode"] == "overwrite"
