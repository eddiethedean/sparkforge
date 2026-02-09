"""
Test to reproduce the error when empty strings are passed to LogWriter.

This test reproduces the issue where empty strings for schema or table_name
pass the `is not None` check but fail during WriterConfig validation.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.writer import LogWriter
from pipeline_builder.writer.exceptions import WriterConfigurationError


class TestLogWriterEmptyStringError:
    """Test error handling for empty string parameters."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock()
        spark.table.return_value.count.return_value = 0
        return spark

    def test_init_with_empty_schema_raises_error(self, mock_spark):
        """Test that initialization with empty schema string raises error."""
        # Empty string passes 'is not None' check but fails validation
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="", table_name="logs")

        # Should have a meaningful error message about empty schema
        error_msg = str(exc_info.value).lower()
        assert "schema" in error_msg or "empty" in error_msg or "invalid" in error_msg

    def test_init_with_empty_table_name_raises_error(self, mock_spark):
        """Test that initialization with empty table_name string raises error."""
        # Empty string passes 'is not None' check but fails validation
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="analytics", table_name="")

        # Should have a meaningful error message about empty table name
        error_msg = str(exc_info.value).lower()
        assert "table" in error_msg or "empty" in error_msg or "invalid" in error_msg

    def test_init_with_both_empty_strings_raises_error(self, mock_spark):
        """Test that initialization with both empty strings raises error."""
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="", table_name="")

        # Should have a meaningful error message
        error_msg = str(exc_info.value).lower()
        assert "empty" in error_msg or "invalid" in error_msg

    def test_init_with_whitespace_only_schema_raises_error(self, mock_spark):
        """Test that initialization with whitespace-only schema raises error."""
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="   ", table_name="logs")
        error_msg = str(exc_info.value).lower()
        assert "schema" in error_msg or "empty" in error_msg or "invalid" in error_msg

    def test_init_with_whitespace_only_table_name_raises_error(self, mock_spark):
        """Test that initialization with whitespace-only table_name raises error."""
        with pytest.raises(WriterConfigurationError) as exc_info:
            LogWriter(mock_spark, schema="analytics", table_name="   ")
        error_msg = str(exc_info.value).lower()
        assert "table" in error_msg or "empty" in error_msg or "invalid" in error_msg
