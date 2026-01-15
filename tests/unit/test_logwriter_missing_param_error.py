"""
Test to reproduce the error when schema or table_name is missing.

This test reproduces the issue where providing only one of schema or table_name
should raise an error but might not be caught properly.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.writer import LogWriter
from pipeline_builder.writer.exceptions import WriterConfigurationError


class TestLogWriterMissingParameterError:
    """Test error handling for missing parameters."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock()
        spark.table.return_value.count.return_value = 0
        return spark

    def test_init_with_only_schema_raises_error(self, mock_spark):
        """Test that providing only schema (without table_name) raises error."""
        # This should raise WriterConfigurationError because table_name is None
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="analytics")

        # Verify the error message mentions missing parameters
        error_msg = str(exc_info.value).lower()
        assert "schema and table_name" in error_msg or "missing" in error_msg

    def test_init_with_only_table_name_raises_error(self, mock_spark):
        """Test that providing only table_name (without schema) raises error."""
        # This should raise WriterConfigurationError because schema is None
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, table_name="logs")

        # Verify the error message mentions missing parameters
        error_msg = str(exc_info.value).lower()
        assert "schema and table_name" in error_msg or "missing" in error_msg

    def test_init_with_both_none_raises_error(self, mock_spark):
        """Test that providing both as None raises error."""
        # Explicitly passing None should raise error
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema=None, table_name=None)

        # Verify the error message mentions missing parameters
        error_msg = str(exc_info.value).lower()
        assert "schema and table_name" in error_msg or "missing" in error_msg

    def test_init_with_schema_none_table_name_provided_raises_error(self, mock_spark):
        """Test that schema=None with table_name provided raises error."""
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema=None, table_name="logs")

        error_msg = str(exc_info.value).lower()
        assert "schema and table_name" in error_msg or "missing" in error_msg

    def test_init_with_table_name_none_schema_provided_raises_error(self, mock_spark):
        """Test that table_name=None with schema provided raises error."""
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="analytics", table_name=None)

        error_msg = str(exc_info.value).lower()
        assert "schema and table_name" in error_msg or "missing" in error_msg
