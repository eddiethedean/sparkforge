# test_table_operations.py
"""
Unit tests for the table_operations module.

This module tests all table read/write/management operations.
"""

from unittest.mock import patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge.errors import TableOperationError
from sparkforge.table_operations import (
    drop_table,
    fqn,
    read_table,
    table_exists,
    write_append_table,
    write_overwrite_table,
)

# Using shared spark_session fixture from conftest.py


@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    return spark_session.createDataFrame(data, schema)


class TestFqn:
    """Test fqn function."""

    def test_basic_fqn(self):
        """Test basic FQN creation."""
        result = fqn("my_schema", "my_table")
        assert result == "my_schema.my_table"

    def test_empty_schema_raises_error(self):
        """Test that empty schema raises ValueError."""
        with pytest.raises(ValueError):
            fqn("", "my_table")

    def test_empty_table_raises_error(self):
        """Test that empty table raises ValueError."""
        with pytest.raises(ValueError):
            fqn("my_schema", "")

    def test_none_schema_raises_error(self):
        """Test that None schema raises ValueError."""
        with pytest.raises(ValueError):
            fqn(None, "my_table")

    def test_none_table_raises_error(self):
        """Test that None table raises ValueError."""
        with pytest.raises(ValueError):
            fqn("my_schema", None)

    def test_special_characters(self):
        """Test FQN with special characters."""
        result = fqn("schema_123", "table-name")
        assert result == "schema_123.table-name"


class TestWriteOverwriteTable:
    """Test write_overwrite_table function."""

    def test_write_overwrite_success(self, spark_session, sample_dataframe):
        """Test successful overwrite write."""
        # Create test database
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_table")
            rows_written = write_overwrite_table(sample_dataframe, table_name)

            assert rows_written == 3
            # Verify table was created
            assert table_exists(spark_session, table_name)

        finally:
            # Cleanup
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_write_overwrite_with_options(self, spark_session, sample_dataframe):
        """Test overwrite write with additional options."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_table_options")
            rows_written = write_overwrite_table(
                sample_dataframe, table_name, compression="snappy"
            )

            assert rows_written == 3
            assert table_exists(spark_session, table_name)

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_write_overwrite_nonexistent_database(self, sample_dataframe):
        """Test overwrite write to nonexistent database."""
        table_name = fqn("nonexistent_schema", "test_table")

        with pytest.raises(TableOperationError):
            write_overwrite_table(sample_dataframe, table_name)


class TestWriteAppendTable:
    """Test write_append_table function."""

    def test_write_append_success(self, spark_session, sample_dataframe):
        """Test successful append write."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_append_table")

            # First write
            rows_written1 = write_append_table(sample_dataframe, table_name)
            assert rows_written1 == 3

            # Second append write
            rows_written2 = write_append_table(sample_dataframe, table_name)
            assert rows_written2 == 3

            # Verify total rows
            df = read_table(spark_session, table_name)
            assert df.count() == 6

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_write_append_with_options(self, spark_session, sample_dataframe):
        """Test append write with additional options."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_append_options")
            rows_written = write_append_table(
                sample_dataframe, table_name, compression="snappy"
            )

            assert rows_written == 3
            assert table_exists(spark_session, table_name)

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_write_append_nonexistent_database(self, sample_dataframe):
        """Test append write to nonexistent database."""
        table_name = fqn("nonexistent_schema", "test_table")

        with pytest.raises(TableOperationError):
            write_append_table(sample_dataframe, table_name)


class TestReadTable:
    """Test read_table function."""

    def test_read_existing_table(self, spark_session, sample_dataframe):
        """Test reading existing table."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_read_table")
            write_overwrite_table(sample_dataframe, table_name)

            df = read_table(spark_session, table_name)
            assert df.count() == 3
            assert df.columns == ["id", "name", "value"]

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_read_nonexistent_table(self, spark_session):
        """Test reading nonexistent table raises error."""
        table_name = fqn("test_schema", "nonexistent_table")

        with pytest.raises(TableOperationError):
            read_table(spark_session, table_name)

    def test_read_table_general_exception(self, spark_session):
        """Test read_table handles general exceptions."""
        # Mock spark.table to raise a general exception
        with patch.object(spark_session, "table") as mock_table:
            mock_table.side_effect = Exception("General error")

            with pytest.raises(TableOperationError, match="Failed to read table"):
                read_table(spark_session, "test_schema.test_table")

    def test_read_table_data_integrity(self, spark_session, sample_dataframe):
        """Test that read table preserves data integrity."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_data_integrity")
            write_overwrite_table(sample_dataframe, table_name)

            df = read_table(spark_session, table_name)

            # Check specific values
            rows = df.collect()
            assert len(rows) == 3

            # Check that all expected data is present (order may vary)
            ids = [row["id"] for row in rows]
            names = [row["name"] for row in rows]
            values = [row["value"] for row in rows]

            assert set(ids) == {1, 2, 3}
            assert set(names) == {"Alice", "Bob", "Charlie"}
            assert set(values) == {100, 200, 300}

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")


class TestTableExists:
    """Test table_exists function."""

    def test_existing_table(self, spark_session, sample_dataframe):
        """Test table_exists with existing table."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_exists_table")
            write_overwrite_table(sample_dataframe, table_name)

            assert table_exists(spark_session, table_name) is True

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_nonexistent_table(self, spark_session):
        """Test table_exists with nonexistent table."""
        table_name = fqn("test_schema", "nonexistent_table")
        assert table_exists(spark_session, table_name) is False

    def test_nonexistent_database(self, spark_session):
        """Test table_exists with nonexistent database."""
        table_name = fqn("nonexistent_schema", "test_table")
        assert table_exists(spark_session, table_name) is False

    def test_table_exists_general_exception(self, spark_session):
        """Test table_exists handles general exceptions."""
        # Mock spark.table to raise a general exception
        with patch.object(spark_session, "table") as mock_table:
            mock_table.side_effect = Exception("General error")

            with patch("sparkforge.table_operations.logger") as mock_logger:
                result = table_exists(spark_session, "test_schema.test_table")
                assert result is False

                # Verify that warning was logged
                mock_logger.warning.assert_called_once()
                warning_call = mock_logger.warning.call_args[0][0]
                assert (
                    "Error checking if table test_schema.test_table exists"
                    in warning_call
                )


class TestDropTable:
    """Test drop_table function."""

    def test_drop_existing_table(self, spark_session, sample_dataframe):
        """Test dropping existing table."""
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")

        try:
            table_name = fqn("test_schema", "test_drop_table")
            write_overwrite_table(sample_dataframe, table_name)

            assert table_exists(spark_session, table_name) is True

            result = drop_table(spark_session, table_name)
            assert result is True
            assert table_exists(spark_session, table_name) is False

        finally:
            spark_session.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

    def test_drop_nonexistent_table(self, spark_session):
        """Test dropping nonexistent table."""
        table_name = fqn("test_schema", "nonexistent_table")

        result = drop_table(spark_session, table_name)
        assert result is False

    def test_drop_table_error_handling(self, spark_session):
        """Test drop_table error handling."""
        # This should not raise an exception even if there's an error
        table_name = fqn("invalid_schema", "invalid_table")
        result = drop_table(spark_session, table_name)
        assert result is False

    def test_drop_table_general_exception(self, spark_session):
        """Test drop_table handles general exceptions."""
        # Mock table_exists to return True, then mock spark.sql to raise exception
        with patch("sparkforge.table_operations.table_exists", return_value=True):
            with patch.object(spark_session, "sql") as mock_sql:
                mock_sql.side_effect = Exception("General error")

                result = drop_table(spark_session, "test_schema.test_table")
                assert result is False
