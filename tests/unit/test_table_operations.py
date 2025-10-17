"""
Tests for sparkforge.table_operations module.

This module tests all table operation utilities and functions.
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.utils import AnalysisException

from sparkforge.errors import TableOperationError
from sparkforge.table_operations import (
    drop_table,
    fqn,
    read_table,
    table_exists,
    write_append_table,
    write_overwrite_table,
)


class TestFqn:
    """Test fqn function."""

    def test_fqn_basic(self):
        """Test basic FQN creation."""
        result = fqn("test_schema", "test_table")
        assert result == "test_schema.test_table"

    def test_fqn_different_names(self):
        """Test FQN with different schema and table names."""
        result = fqn("production", "users")
        assert result == "production.users"

        result = fqn("analytics", "events")
        assert result == "analytics.events"

    def test_fqn_empty_schema(self):
        """Test FQN with empty schema raises ValueError."""
        with pytest.raises(ValueError, match="Schema and table names cannot be empty"):
            fqn("", "test_table")

    def test_fqn_empty_table(self):
        """Test FQN with empty table raises ValueError."""
        with pytest.raises(ValueError, match="Schema and table names cannot be empty"):
            fqn("test_schema", "")

    def test_fqn_both_empty(self):
        """Test FQN with both empty raises ValueError."""
        with pytest.raises(ValueError, match="Schema and table names cannot be empty"):
            fqn("", "")

    def test_fqn_none_schema(self):
        """Test FQN with None schema raises ValueError."""
        with pytest.raises(ValueError, match="Schema and table names cannot be empty"):
            fqn(None, "test_table")

    def test_fqn_none_table(self):
        """Test FQN with None table raises ValueError."""
        with pytest.raises(ValueError, match="Schema and table names cannot be empty"):
            fqn("test_schema", None)


class TestWriteOverwriteTable:
    """Test write_overwrite_table function."""

    def test_write_overwrite_table_success(self):
        """Test successful overwrite table write."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_overwrite_table(mock_df, "test_schema.test_table")

            assert result == 100
            mock_df.cache.assert_called_once()
            mock_df.count.assert_called_once()
            mock_df.write.format.assert_called_once_with("parquet")
            mock_writer.mode.assert_called_once_with("overwrite")
            mock_writer.option.assert_called_once_with("overwriteSchema", "true")
            mock_writer.saveAsTable.assert_called_once_with("test_schema.test_table")

    def test_write_overwrite_table_with_options(self):
        """Test overwrite table write with additional options."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_overwrite_table(
                mock_df,
                "test_schema.test_table",
                compression="snappy",
                partitionBy="date",
            )

            assert result == 50
            # Should call option for each additional option
            assert mock_writer.option.call_count == 3  # overwriteSchema + 2 additional
            mock_writer.option.assert_any_call("overwriteSchema", "true")
            mock_writer.option.assert_any_call("compression", "snappy")
            mock_writer.option.assert_any_call("partitionBy", "date")

    def test_write_overwrite_table_failure(self):
        """Test overwrite table write failure."""
        mock_df = MagicMock()
        mock_df.cache.side_effect = Exception("Cache failed")

        with pytest.raises(
            TableOperationError,
            match="Failed to write table test_schema.test_table: Cache failed",
        ):
            write_overwrite_table(mock_df, "test_schema.test_table")

    def test_write_overwrite_table_zero_rows(self):
        """Test overwrite table write with zero rows."""
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_overwrite_table(mock_df, "test_schema.test_table")

            assert result == 0


class TestWriteAppendTable:
    """Test write_append_table function."""

    def test_write_append_table_success(self):
        """Test successful append table write."""
        mock_df = MagicMock()
        mock_df.count.return_value = 75
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_append_table(mock_df, "test_schema.test_table")

            assert result == 75
            mock_df.cache.assert_called_once()
            mock_df.count.assert_called_once()
            mock_df.write.format.assert_called_once_with("parquet")
            mock_writer.mode.assert_called_once_with("append")
            mock_writer.saveAsTable.assert_called_once_with("test_schema.test_table")

    def test_write_append_table_with_options(self):
        """Test append table write with additional options."""
        mock_df = MagicMock()
        mock_df.count.return_value = 25
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_append_table(
                mock_df,
                "test_schema.test_table",
                compression="gzip",
                maxRecordsPerFile=1000,
            )

            assert result == 25
            # Should call option for each additional option
            assert mock_writer.option.call_count == 2
            mock_writer.option.assert_any_call("compression", "gzip")
            mock_writer.option.assert_any_call("maxRecordsPerFile", 1000)

    def test_write_append_table_failure(self):
        """Test append table write failure."""
        mock_df = MagicMock()
        mock_df.cache.side_effect = Exception("Cache failed")

        with pytest.raises(
            TableOperationError,
            match="Failed to write table test_schema.test_table: Cache failed",
        ):
            write_append_table(mock_df, "test_schema.test_table")

    def test_write_append_table_zero_rows(self):
        """Test append table write with zero rows."""
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        with patch("sparkforge.table_operations.logger"):
            result = write_append_table(mock_df, "test_schema.test_table")

            assert result == 0


class TestReadTable:
    """Test read_table function."""

    def test_read_table_success(self):
        """Test successful table read."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df

        with patch("sparkforge.table_operations.logger"):
            result = read_table(mock_spark, "test_schema.test_table")

            assert result == mock_df
            mock_spark.table.assert_called_once_with("test_schema.test_table")

    def test_read_table_analysis_exception(self):
        """Test table read with AnalysisException (table doesn't exist)."""
        mock_spark = MagicMock()
        # Use a simple exception instead of AnalysisException to avoid JVM issues
        mock_spark.table.side_effect = Exception("Table not found")

        with pytest.raises(
            TableOperationError,
            match="Failed to read table test_schema.test_table: Table not found",
        ):
            read_table(mock_spark, "test_schema.test_table")

    def test_read_table_other_exception(self):
        """Test table read with other exception."""
        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Connection failed")

        with pytest.raises(
            TableOperationError,
            match="Failed to read table test_schema.test_table: Connection failed",
        ):
            read_table(mock_spark, "test_schema.test_table")


class TestTableExists:
    """Test table_exists function."""

    def test_table_exists_true(self):
        """Test table_exists returns True when table exists."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df

        result = table_exists(mock_spark, "test_schema.test_table")

        assert result is True
        mock_spark.table.assert_called_once_with("test_schema.test_table")
        mock_df.count.assert_called_once()

    def test_table_exists_false_analysis_exception(self):
        """Test table_exists returns False with AnalysisException."""
        mock_spark = MagicMock()
        # Create a proper AnalysisException - newer PySpark versions have different constructors
        try:
            analysis_exception = AnalysisException("Table not found")
            is_analysis_exception = True
        except (TypeError, AssertionError):
            # Fallback for compatibility - mock AnalysisException to trigger debug logging
            # We patch the exception check in table_operations to recognize our mock
            analysis_exception = AnalysisException.__new__(AnalysisException)
            is_analysis_exception = True
        
        mock_spark.table.side_effect = analysis_exception

        with patch("sparkforge.table_operations.logger") as mock_logger:
            result = table_exists(mock_spark, "test_schema.test_table")

            assert result is False
            # Should log debug message for AnalysisException
            mock_logger.debug.assert_called_once_with(
                "Table test_schema.test_table does not exist (AnalysisException)"
            )

    def test_table_exists_false_other_exception(self):
        """Test table_exists returns False with other exception."""
        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Connection failed")

        with patch("sparkforge.table_operations.logger") as mock_logger:
            result = table_exists(mock_spark, "test_schema.test_table")

            assert result is False
            mock_logger.warning.assert_called_once_with(
                "Error checking if table test_schema.test_table exists: Connection failed"
            )


class TestDropTable:
    """Test drop_table function."""

    def test_drop_table_success(self):
        """Test successful table drop."""
        mock_spark = MagicMock()
        mock_jspark_session = MagicMock()
        mock_external_catalog = MagicMock()
        mock_spark._jsparkSession = mock_jspark_session
        mock_jspark_session.sharedState.return_value.externalCatalog.return_value = (
            mock_external_catalog
        )

        with patch("sparkforge.table_operations.table_exists", return_value=True):
            with patch("sparkforge.table_operations.logger"):
                result = drop_table(mock_spark, "test_schema.test_table")

                assert result is True
                mock_external_catalog.dropTable.assert_called_once_with(
                    "test_schema", "test_table", True, True
                )

    def test_drop_table_with_default_schema(self):
        """Test table drop with default schema (no dot in FQN)."""
        mock_spark = MagicMock()
        mock_jspark_session = MagicMock()
        mock_external_catalog = MagicMock()
        mock_spark._jsparkSession = mock_jspark_session
        mock_jspark_session.sharedState.return_value.externalCatalog.return_value = (
            mock_external_catalog
        )

        with patch("sparkforge.table_operations.table_exists", return_value=True):
            with patch("sparkforge.table_operations.logger"):
                result = drop_table(mock_spark, "test_table")

                assert result is True
                mock_external_catalog.dropTable.assert_called_once_with(
                    "default", "test_table", True, True
                )

    def test_drop_table_not_exists(self):
        """Test table drop when table doesn't exist."""
        mock_spark = MagicMock()

        with patch("sparkforge.table_operations.table_exists", return_value=False):
            result = drop_table(mock_spark, "test_schema.test_table")

            assert result is False

    def test_drop_table_failure(self):
        """Test table drop failure."""
        mock_spark = MagicMock()
        mock_jspark_session = MagicMock()
        mock_external_catalog = MagicMock()
        mock_spark._jsparkSession = mock_jspark_session
        mock_jspark_session.sharedState.return_value.externalCatalog.return_value = (
            mock_external_catalog
        )
        mock_external_catalog.dropTable.side_effect = Exception("Drop failed")

        with patch("sparkforge.table_operations.table_exists", return_value=True):
            with patch("sparkforge.table_operations.logger") as mock_logger:
                result = drop_table(mock_spark, "test_schema.test_table")

                assert result is False
                mock_logger.warning.assert_called_once_with(
                    "Failed to drop table test_schema.test_table: Drop failed"
                )

    def test_drop_table_exception_during_check(self):
        """Test table drop when table_exists check fails."""
        mock_spark = MagicMock()

        with patch(
            "sparkforge.table_operations.table_exists",
            side_effect=Exception("Check failed"),
        ):
            with patch("sparkforge.table_operations.logger") as mock_logger:
                result = drop_table(mock_spark, "test_schema.test_table")

                assert result is False
                mock_logger.warning.assert_called_once_with(
                    "Failed to drop table test_schema.test_table: Check failed"
                )


class TestTableOperationsIntegration:
    """Test table operations integration."""

    def test_fqn_with_table_operations(self):
        """Test FQN function with other table operations."""
        schema = "test_schema"
        table = "test_table"
        fqn_name = fqn(schema, table)

        assert fqn_name == "test_schema.test_table"

        # Test that FQN can be used with other functions
        mock_spark = MagicMock()
        with patch("sparkforge.table_operations.table_exists", return_value=True):
            result = table_exists(mock_spark, fqn_name)
            assert result is True

    def test_write_and_read_workflow(self):
        """Test complete write and read workflow."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        mock_spark = MagicMock()
        mock_spark.table.return_value = mock_df

        with patch("sparkforge.table_operations.logger"):
            # Write table
            rows_written = write_overwrite_table(mock_df, "test_schema.test_table")
            assert rows_written == 100

            # Read table
            result_df = read_table(mock_spark, "test_schema.test_table")
            assert result_df == mock_df

    def test_table_lifecycle(self):
        """Test complete table lifecycle (create, check, drop)."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        mock_spark = MagicMock()
        mock_jspark_session = MagicMock()
        mock_external_catalog = MagicMock()
        mock_spark._jsparkSession = mock_jspark_session
        mock_jspark_session.sharedState.return_value.externalCatalog.return_value = (
            mock_external_catalog
        )

        with patch("sparkforge.table_operations.logger"):
            # Create table
            rows_written = write_append_table(mock_df, "test_schema.test_table")
            assert rows_written == 50

            # Check table exists
            with patch("sparkforge.table_operations.table_exists", return_value=True):
                exists = table_exists(mock_spark, "test_schema.test_table")
                assert exists is True

            # Drop table
            with patch("sparkforge.table_operations.table_exists", return_value=True):
                dropped = drop_table(mock_spark, "test_schema.test_table")
                assert dropped is True
