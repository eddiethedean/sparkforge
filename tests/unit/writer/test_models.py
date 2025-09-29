"""
Unit tests for writer models.
"""

from datetime import datetime

import pytest

from sparkforge.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    PipelinePhase,
    StepResult,
)
from sparkforge.writer.models import (
    LogRow,
    WriteMode,
    WriterConfig,
    create_log_row_from_step_result,
    create_log_rows_from_execution_result,
    create_log_schema,
    validate_log_data,
    validate_log_row,
)


class TestWriterConfig:
    """Test WriterConfig functionality."""

    def test_valid_config(self):
        """Test valid configuration creation."""
        config = WriterConfig(
            table_schema="analytics",
            table_name="pipeline_logs",
            write_mode=WriteMode.APPEND,
            batch_size=1000,
            enable_validation=True,
        )

        assert config.table_schema == "analytics"
        assert config.table_name == "pipeline_logs"
        assert config.write_mode == WriteMode.APPEND
        assert config.batch_size == 1000
        assert config.enable_validation is True

    def test_config_validation_success(self):
        """Test successful configuration validation."""
        config = WriterConfig(table_schema="analytics", table_name="pipeline_logs")
        config.validate()  # Should not raise

    def test_config_validation_empty_schema(self):
        """Test configuration validation with empty schema."""
        config = WriterConfig(table_schema="", table_name="pipeline_logs")
        with pytest.raises(ValueError, match="Table schema cannot be empty"):
            config.validate()

    def test_config_validation_empty_table_name(self):
        """Test configuration validation with empty table name."""
        config = WriterConfig(table_schema="analytics", table_name="")
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            config.validate()

    def test_config_validation_invalid_batch_size(self):
        """Test configuration validation with invalid batch size."""
        config = WriterConfig(
            table_schema="analytics", table_name="pipeline_logs", batch_size=0
        )
        with pytest.raises(ValueError, match="Batch size must be positive"):
            config.validate()


class TestLogSchema:
    """Test log schema creation."""

    def test_create_log_schema(self):
        """Test log schema creation."""
        schema = create_log_schema()

        assert schema is not None
        assert len(schema.fields) > 0

        # Check for key fields
        field_names = [field.name for field in schema.fields]
        assert "run_id" in field_names
        assert "execution_id" in field_names
        assert "step_name" in field_names
        assert "phase" in field_names
        assert "success" in field_names


class TestLogRowCreation:
    """Test log row creation from models."""

    def test_create_log_row_from_step_result(self):
        """Test creating log row from step result."""
        # Create execution context
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            execution_id="test-exec-123",
            pipeline_id="test-pipeline",
            schema="analytics",
            run_mode="initial",
        )

        # Create step result
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.SILVER,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.5,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
        )

        # Create log row
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=context,
            run_id="test-run-123",
            run_mode="initial",
        )

        # Verify log row
        assert log_row["run_id"] == "test-run-123"
        assert log_row["execution_id"] == "test-exec-123"
        assert log_row["pipeline_id"] == "test-pipeline"
        assert log_row["step_name"] == "test_step"
        assert log_row["phase"] == "silver"
        assert log_row["success"] is True
        assert log_row["rows_processed"] == 1000
        assert log_row["rows_written"] == 950
        assert log_row["validation_rate"] == 95.0

    def test_create_log_rows_from_execution_result(self):
        """Test creating log rows from execution result."""
        # Create execution context
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            execution_id="test-exec-123",
            pipeline_id="test-pipeline",
            schema="analytics",
            run_mode="initial",
        )

        # Create step results
        step_results = [
            StepResult(
                step_name="step1",
                phase=PipelinePhase.BRONZE,
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=5.0,
                rows_processed=500,
                rows_written=500,
                validation_rate=100.0,
            ),
            StepResult(
                step_name="step2",
                phase=PipelinePhase.SILVER,
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=8.0,
                rows_processed=500,
                rows_written=480,
                validation_rate=96.0,
            ),
        ]

        # Create execution result
        execution_result = ExecutionResult.from_context_and_results(
            context, step_results
        )

        # Create log rows
        log_rows = create_log_rows_from_execution_result(
            execution_result=execution_result, run_id="test-run-123", run_mode="initial"
        )

        # Verify log rows
        assert len(log_rows) == 2

        # Check first row
        row1 = log_rows[0]
        assert row1["step_name"] == "step1"
        assert row1["phase"] == "bronze"
        assert row1["rows_processed"] == 500

        # Check second row
        row2 = log_rows[1]
        assert row2["step_name"] == "step2"
        assert row2["phase"] == "silver"
        assert row2["rows_processed"] == 500


class TestLogRowValidation:
    """Test log row validation."""

    def test_validate_valid_log_row(self):
        """Test validation of valid log row."""
        log_row: LogRow = {
            "run_id": "test-run-123",
            "run_mode": "initial",
            "run_started_at": datetime.now(),
            "run_ended_at": None,
            "execution_id": "test-exec-123",
            "pipeline_id": "test-pipeline",
            "schema": "analytics",
            "phase": "bronze",
            "step_name": "test_step",
            "step_type": "transform",
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "duration_secs": 10.0,
            "table_fqn": None,
            "write_mode": None,
            "input_rows": None,
            "output_rows": None,
            "rows_written": None,
            "rows_processed": 1000,
            "valid_rows": 950,
            "invalid_rows": 50,
            "validation_rate": 95.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "metadata": {},
        }

        validate_log_row(log_row)  # Should not raise

    def test_validate_log_row_empty_run_id(self):
        """Test validation with empty run ID."""
        log_row: LogRow = {
            "run_id": "",
            "run_mode": "initial",
            "run_started_at": None,
            "run_ended_at": None,
            "execution_id": "test-exec-123",
            "pipeline_id": "test-pipeline",
            "schema": "analytics",
            "phase": "bronze",
            "step_name": "test_step",
            "step_type": "transform",
            "start_time": None,
            "end_time": None,
            "duration_secs": 0.0,
            "table_fqn": None,
            "write_mode": None,
            "input_rows": None,
            "output_rows": None,
            "rows_written": None,
            "rows_processed": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "metadata": {},
        }

        with pytest.raises(ValueError, match="Run ID cannot be empty"):
            validate_log_row(log_row)

    def test_validate_log_row_negative_duration(self):
        """Test validation with negative duration."""
        log_row: LogRow = {
            "run_id": "test-run-123",
            "run_mode": "initial",
            "run_started_at": None,
            "run_ended_at": None,
            "execution_id": "test-exec-123",
            "pipeline_id": "test-pipeline",
            "schema": "analytics",
            "phase": "bronze",
            "step_name": "test_step",
            "step_type": "transform",
            "start_time": None,
            "end_time": None,
            "duration_secs": -1.0,  # Negative duration
            "table_fqn": None,
            "write_mode": None,
            "input_rows": None,
            "output_rows": None,
            "rows_written": None,
            "rows_processed": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "metadata": {},
        }

        with pytest.raises(ValueError, match="Duration cannot be negative"):
            validate_log_row(log_row)

    def test_validate_log_data_valid(self):
        """Test validation of valid log data."""
        log_rows = [
            {
                "run_id": "test-run-123",
                "run_mode": "initial",
                "run_started_at": None,
                "run_ended_at": None,
                "execution_id": "test-exec-123",
                "pipeline_id": "test-pipeline",
                "schema": "analytics",
                "phase": "bronze",
                "step_name": "step1",
                "step_type": "transform",
                "start_time": None,
                "end_time": None,
                "duration_secs": 10.0,
                "table_fqn": None,
                "write_mode": None,
                "input_rows": None,
                "output_rows": None,
                "rows_written": None,
                "rows_processed": 1000,
                "valid_rows": 950,
                "invalid_rows": 50,
                "validation_rate": 95.0,
                "success": True,
                "error_message": None,
                "memory_usage_mb": None,
                "cpu_usage_percent": None,
                "metadata": {},
            }
        ]

        validate_log_data(log_rows)  # Should not raise

    def test_validate_log_data_invalid_row(self):
        """Test validation of invalid log data."""
        log_rows = [
            {
                "run_id": "",  # Invalid empty run ID
                "run_mode": "initial",
                "run_started_at": None,
                "run_ended_at": None,
                "execution_id": "test-exec-123",
                "pipeline_id": "test-pipeline",
                "schema": "analytics",
                "phase": "bronze",
                "step_name": "step1",
                "step_type": "transform",
                "start_time": None,
                "end_time": None,
                "duration_secs": 10.0,
                "table_fqn": None,
                "write_mode": None,
                "input_rows": None,
                "output_rows": None,
                "rows_written": None,
                "rows_processed": 1000,
                "valid_rows": 950,
                "invalid_rows": 50,
                "validation_rate": 95.0,
                "success": True,
                "error_message": None,
                "memory_usage_mb": None,
                "cpu_usage_percent": None,
                "metadata": {},
            }
        ]

        with pytest.raises(ValueError, match="Invalid log row at index 0"):
            validate_log_data(log_rows)
