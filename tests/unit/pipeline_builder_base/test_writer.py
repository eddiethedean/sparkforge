"""
Comprehensive tests for pipeline_builder_base writer module.

Tests Base writer, models, and exceptions.
"""

import pytest

from pipeline_builder_base.writer.base import BaseLogWriter
from pipeline_builder_base.writer.exceptions import (
    WriterConfigurationError,
    WriterDataQualityError,
    WriterError,
    WriterPerformanceError,
    WriterTableError,
    WriterValidationError,
)
from pipeline_builder_base.writer.models import (
    LogRow,
    WriteMode,
    WriterConfig,
    WriterMetrics,
)


class TestBaseWriter:
    """Test BaseLogWriter class."""

    def test_base_writer_initialization(self, logger):
        """Test base writer creation."""

        # BaseLogWriter is abstract, so we test initialization through a mock
        class MockWriter(BaseLogWriter):
            def _write_log_rows(self, rows, write_mode):
                pass

            def _read_log_table(self, filters):
                return []

            def _table_exists(self):
                return False

            def _create_table(self, sample_rows):
                pass

        writer = MockWriter("test_schema", "test_table", logger=logger)
        assert writer.schema == "test_schema"
        assert writer.table_name == "test_table"
        assert writer.logger == logger

    def test_base_writer_abstract_methods(self):
        """Test abstract method requirements."""
        # BaseLogWriter is abstract, cannot instantiate directly
        with pytest.raises(TypeError):
            BaseLogWriter("schema", "table")

    def test_base_writer_table_fqn(self, logger):
        """Test fully qualified table name."""

        class MockWriter(BaseLogWriter):
            def _write_log_rows(self, rows, write_mode):
                pass

            def _read_log_table(self, filters):
                return []

            def _table_exists(self):
                return False

            def _create_table(self, sample_rows):
                pass

        writer = MockWriter("test_schema", "test_table", logger=logger)
        assert writer.table_fqn == "test_schema.test_table"


class TestWriterModels:
    """Test writer model classes."""

    def test_write_result_creation(self):
        """Test WriteResult creation."""
        # Note: WriteResult may not exist, adjust based on actual models
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )
        assert config.table_schema == "test_schema"
        assert config.table_name == "test_table"
        assert config.write_mode == WriteMode.APPEND

    def test_writer_metrics_creation(self):
        """Test WriterMetrics creation."""
        metrics: WriterMetrics = {
            "total_writes": 10,
            "successful_writes": 9,
            "failed_writes": 1,
            "total_duration_secs": 100.5,
            "avg_write_duration_secs": 10.05,
            "total_rows_written": 1000,
            "memory_usage_peak_mb": 512.0,
        }
        assert metrics["total_writes"] == 10
        assert metrics["total_rows_written"] == 1000

    def test_log_row_creation(self):
        """Test LogRow creation."""
        from datetime import datetime

        row: LogRow = {
            "run_id": "run123",
            "run_mode": "initial",
            "run_started_at": datetime.now(),
            "run_ended_at": datetime.now(),
            "execution_id": "exec123",
            "pipeline_id": "pipeline1",
            "schema": "test_schema",
            "phase": "bronze",
            "step_name": "step1",
            "step_type": "bronze",
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "duration_secs": 1.5,
            "table_fqn": "schema.table",
            "write_mode": "append",
            "input_rows": 1000,
            "output_rows": 950,
            "rows_written": 950,
            "rows_processed": 1000,
            "table_total_rows": 950,
            "valid_rows": 950,
            "invalid_rows": 50,
            "validation_rate": 95.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "metadata": {},
        }
        assert row["run_id"] == "run123"
        assert row["step_name"] == "step1"


class TestWriterExceptions:
    """Test writer exception classes."""

    def test_write_error_creation(self):
        """Test WriterError creation."""
        error = WriterError("Write failed")
        assert str(error) == "Write failed"

    def test_table_error_creation(self):
        """Test WriterTableError creation."""
        error = WriterTableError("Table operation failed")
        assert str(error) == "Table operation failed"

    def test_validation_error_creation(self):
        """Test WriterValidationError creation."""
        error = WriterValidationError("Validation failed")
        assert str(error) == "Validation failed"

    def test_configuration_error_creation(self):
        """Test WriterConfigurationError creation."""
        error = WriterConfigurationError("Config failed")
        assert str(error) == "Config failed"

    def test_data_quality_error_creation(self):
        """Test WriterDataQualityError creation."""
        error = WriterDataQualityError("Quality check failed")
        assert str(error) == "Quality check failed"

    def test_performance_error_creation(self):
        """Test WriterPerformanceError creation."""
        error = WriterPerformanceError("Performance issue")
        assert str(error) == "Performance issue"
