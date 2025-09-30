"""
Simple unit tests for the refactored writer module.

This module tests the refactored writer components without requiring
a full Spark environment.
"""

from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from sparkforge.writer import (
    AnalyticsEngine,
    DataProcessor,
    DataQualityAnalyzer,
    LogRow,
    LogWriter,
    PerformanceMonitor,
    QueryBuilder,
    StorageManager,
    TrendAnalyzer,
    WriteMode,
    WriterConfig,
    WriterConfigurationError,
    WriterDataQualityError,
    WriterError,
    WriterMetrics,
    WriterPerformanceError,
    WriterTableError,
    WriterValidationError,
    __author__,
    __description__,
    __version__,
)


class TestWriterModels:
    """Test cases for writer models."""

    def test_writer_config_creation(self):
        """Test WriterConfig creation."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        assert config.table_schema == "test_schema"
        assert config.table_name == "test_table"
        assert config.write_mode == WriteMode.APPEND

    def test_writer_config_validation_success(self):
        """Test successful WriterConfig validation."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        # Should not raise exception
        config.validate()

    def test_writer_config_validation_failure(self):
        """Test WriterConfig validation failure."""
        with pytest.raises(ValueError):
            WriterConfig(
                table_schema="",  # Empty schema should fail
                table_name="test_table",
                write_mode=WriteMode.APPEND,
            )

    def test_log_row_creation(self):
        """Test LogRow creation."""
        log_row = LogRow(
            run_id="test_run",
            run_mode="initial",
            run_started_at=datetime.now(),
            run_ended_at=datetime.now(),
            phase="bronze",
            step="test_step",
            start_time=datetime.now(),
            end_time=datetime.now(),
            table_fqn="test.table",
            write_mode="append",
            input_rows=100,
            output_rows=95,
            rows_written=95,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            execution_time=60.0,
            success=True,
            error_message=None,
            metadata={},
        )

        assert log_row.run_id == "test_run"
        assert log_row.phase == "bronze"
        assert log_row.validation_rate == 95.0
        assert log_row.success is True

    def test_writer_metrics_creation(self):
        """Test WriterMetrics creation."""
        metrics = WriterMetrics(
            total_writes=10,
            successful_writes=9,
            failed_writes=1,
            total_duration_secs=600.0,
            avg_write_duration_secs=60.0,
            total_rows_written=1000,
            memory_usage_peak_mb=512.0,
        )

        assert metrics["total_writes"] == 10
        assert metrics["successful_writes"] == 9
        assert metrics["failed_writes"] == 1
        assert metrics["total_duration_secs"] == 600.0
        assert metrics["avg_write_duration_secs"] == 60.0
        assert metrics["total_rows_written"] == 1000
        assert metrics["memory_usage_peak_mb"] == 512.0


class TestWriterExceptions:
    """Test cases for writer exceptions."""

    def test_writer_error_basic(self):
        """Test basic WriterError creation."""
        error = WriterError("Test error message")

        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.context == {}
        assert error.suggestions == []
        assert error.cause is None

    def test_writer_error_with_context(self):
        """Test WriterError with context and suggestions."""
        context = {"key": "value", "operation": "test"}
        suggestions = ["suggestion1", "suggestion2"]
        cause = Exception("root cause")

        error = WriterError(
            "Test error message", context=context, suggestions=suggestions, cause=cause
        )

        assert error.message == "Test error message"
        assert error.context == context
        assert error.suggestions == suggestions
        assert error.cause == cause

    def test_writer_configuration_error(self):
        """Test WriterConfigurationError."""
        config_errors = ["error1", "error2"]
        context = {"config": {"table_schema": ""}}
        suggestions = ["fix schema", "check config"]

        error = WriterConfigurationError(
            "Configuration error",
            config_errors=config_errors,
            context=context,
            suggestions=suggestions,
        )

        assert isinstance(error, WriterError)
        assert error.config_errors == config_errors
        assert error.context == context
        assert error.suggestions == suggestions

    def test_writer_validation_error(self):
        """Test WriterValidationError."""
        validation_errors = ["validation1", "validation2"]
        context = {"data": "invalid"}
        suggestions = ["fix data", "check format"]

        error = WriterValidationError(
            "Validation error",
            validation_errors=validation_errors,
            context=context,
            suggestions=suggestions,
        )

        assert isinstance(error, WriterError)
        assert error.validation_errors == validation_errors
        assert error.context == context
        assert error.suggestions == suggestions

    def test_writer_table_error(self):
        """Test WriterTableError."""
        table_name = "test.table"
        operation = "write"
        context = {"mode": "append"}
        suggestions = ["check permissions", "verify table exists"]

        error = WriterTableError(
            "Table error",
            table_name=table_name,
            operation=operation,
            context=context,
            suggestions=suggestions,
        )

        assert isinstance(error, WriterError)
        assert error.table_name == table_name
        assert error.operation == operation
        assert error.context == context
        assert error.suggestions == suggestions

    def test_writer_data_quality_error(self):
        """Test WriterDataQualityError."""
        quality_issues = ["issue1", "issue2"]
        context = {"validation_rate": 85.0}
        suggestions = ["improve data quality", "check sources"]

        error = WriterDataQualityError(
            "Data quality error",
            quality_issues=quality_issues,
            context=context,
            suggestions=suggestions,
        )

        assert isinstance(error, WriterError)
        assert error.quality_issues == quality_issues
        assert error.context == context
        assert error.suggestions == suggestions

    def test_writer_performance_error(self):
        """Test WriterPerformanceError."""
        performance_issues = ["slow", "memory_high"]
        context = {"execution_time": 300.0, "memory_usage": 8192.0}
        suggestions = ["optimize code", "increase resources"]

        error = WriterPerformanceError(
            "Performance error",
            performance_issues=performance_issues,
            context=context,
            suggestions=suggestions,
        )

        assert isinstance(error, WriterError)
        assert error.performance_issues == performance_issues
        assert error.context == context
        assert error.suggestions == suggestions


class TestWriterComponents:
    """Test cases for writer components without Spark dependencies."""

    def test_write_mode_enum(self):
        """Test WriteMode enum values."""
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.APPEND.value == "append"
        assert WriteMode.MERGE.value == "merge"
        assert WriteMode.IGNORE.value == "ignore"

    def test_log_row_attributes(self):
        """Test LogRow has all required attributes."""
        log_row = LogRow(
            run_id="test",
            run_mode="initial",
            run_started_at=datetime.now(),
            run_ended_at=datetime.now(),
            phase="bronze",
            step="test_step",
            start_time=datetime.now(),
            end_time=datetime.now(),
            table_fqn="test.table",
            write_mode="append",
            input_rows=100,
            output_rows=95,
            rows_written=95,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            execution_time=60.0,
            success=True,
            error_message=None,
            metadata={},
        )

        # Test all required attributes exist
        required_attrs = [
            "run_id",
            "run_mode",
            "run_started_at",
            "run_ended_at",
            "phase",
            "step",
            "start_time",
            "end_time",
            "table_fqn",
            "write_mode",
            "input_rows",
            "output_rows",
            "rows_written",
            "valid_rows",
            "invalid_rows",
            "validation_rate",
            "execution_time",
            "success",
            "error_message",
            "metadata",
        ]

        for attr in required_attrs:
            assert hasattr(log_row, attr), f"LogRow missing attribute: {attr}"

    def test_writer_metrics_structure(self):
        """Test WriterMetrics has all required fields."""
        metrics = WriterMetrics(
            total_writes=0,
            successful_writes=0,
            failed_writes=0,
            total_duration_secs=0.0,
            avg_write_duration_secs=0.0,
            total_rows_written=0,
            memory_usage_peak_mb=0.0,
        )

        # Test all required fields exist
        required_fields = [
            "total_writes",
            "successful_writes",
            "failed_writes",
            "total_duration_secs",
            "avg_write_duration_secs",
            "total_rows_written",
            "memory_usage_peak_mb",
        ]

        for field in required_fields:
            assert field in metrics, f"WriterMetrics missing field: {field}"

    def test_writer_config_defaults(self):
        """Test WriterConfig default values."""
        config = WriterConfig(table_schema="test_schema", table_name="test_table")

        # Test default values
        assert config.table_schema == "test_schema"
        assert config.table_name == "test_table"
        assert config.write_mode == WriteMode.APPEND  # Default value

    def test_writer_config_equality(self):
        """Test WriterConfig equality."""
        config1 = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        config2 = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        assert config1 == config2

    def test_writer_config_inequality(self):
        """Test WriterConfig inequality."""
        config1 = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        config2 = WriterConfig(
            table_schema="different_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        assert config1 != config2


class TestWriterModuleStructure:
    """Test cases for writer module structure."""

    def test_writer_module_imports(self):
        """Test that all writer module components can be imported."""
        # Test that all imports are successful
        assert LogWriter is not None
        assert DataProcessor is not None
        assert StorageManager is not None
        assert PerformanceMonitor is not None
        assert AnalyticsEngine is not None
        assert DataQualityAnalyzer is not None
        assert TrendAnalyzer is not None
        assert WriterConfig is not None
        assert LogRow is not None
        assert WriteMode is not None
        assert WriterMetrics is not None
        assert WriterError is not None
        assert WriterConfigurationError is not None
        assert WriterValidationError is not None
        assert WriterTableError is not None
        assert WriterDataQualityError is not None
        assert WriterPerformanceError is not None

    def test_writer_module_version(self):
        """Test writer module version information."""
        assert __version__ is not None
        assert __author__ is not None
        assert __description__ is not None
        assert isinstance(__version__, str)
        assert isinstance(__author__, str)
        assert isinstance(__description__, str)


if __name__ == "__main__":
    pytest.main([__file__])
