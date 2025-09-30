#!/usr/bin/env python3
"""
Test for Trap 3: Hardcoded Fallback Values in LogRow Creation fix.

This test verifies that LogRow creation uses actual data instead of
hardcoded fallback values like "unknown" and None.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock

from sparkforge.models.execution import StepResult, ExecutionContext, PipelinePhase, ExecutionMode
from sparkforge.writer.models import create_log_row_from_step_result


class TestTrap3HardcodedFallbackValues:
    """Test that LogRow creation uses actual data instead of hardcoded fallbacks."""

    def test_log_row_uses_actual_step_type(self):
        """Test that step_type is extracted from StepResult instead of hardcoded 'unknown'."""
        # Create StepResult with actual step_type
        step_result = StepResult(
            step_name="test_bronze",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.0,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
            step_type="bronze_validation",
            table_fqn="test_schema.bronze_table",
            write_mode="append",
            input_rows=100,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            pipeline_id="test_pipeline",
            schema="test_schema",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="test_run",
            run_mode="initial",
        )
        
        # Verify actual data is used instead of hardcoded fallbacks
        assert log_row["step_type"] == "bronze_validation"
        assert log_row["table_fqn"] == "test_schema.bronze_table"
        assert log_row["write_mode"] == "append"
        assert log_row["input_rows"] == 100

    def test_log_row_uses_actual_table_info(self):
        """Test that table_fqn is extracted from StepResult instead of hardcoded None."""
        step_result = StepResult(
            step_name="test_silver",
            phase=PipelinePhase.SILVER,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=15.0,
            rows_processed=200,
            rows_written=200,
            validation_rate=98.0,
            step_type="silver_transformation",
            table_fqn="test_schema.silver_table",
            write_mode="overwrite",
            input_rows=200,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            pipeline_id="test_pipeline",
            schema="test_schema",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="test_run",
            run_mode="initial",
        )
        
        # Verify actual table info is used
        assert log_row["table_fqn"] == "test_schema.silver_table"
        assert log_row["write_mode"] == "overwrite"

    def test_log_row_uses_actual_input_rows(self):
        """Test that input_rows is extracted from StepResult instead of hardcoded None."""
        step_result = StepResult(
            step_name="test_gold",
            phase=PipelinePhase.GOLD,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=20.0,
            rows_processed=150,
            rows_written=150,
            validation_rate=99.0,
            step_type="gold_aggregation",
            table_fqn="test_schema.gold_table",
            write_mode="overwrite",
            input_rows=150,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            pipeline_id="test_pipeline",
            schema="test_schema",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="test_run",
            run_mode="initial",
        )
        
        # Verify actual input rows are used
        assert log_row["input_rows"] == 150

    def test_log_row_fallback_for_missing_data(self):
        """Test that appropriate fallbacks are used when data is missing."""
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=5.0,
            rows_processed=50,
            rows_written=50,
            validation_rate=100.0,
            # Missing optional fields
            step_type=None,
            table_fqn=None,
            write_mode=None,
            input_rows=None,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            pipeline_id="test_pipeline",
            schema="test_schema",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="test_run",
            run_mode="initial",
        )
        
        # Verify appropriate fallbacks are used
        assert log_row["step_type"] == "unknown"  # Only fallback for step_type
        assert log_row["table_fqn"] is None
        assert log_row["write_mode"] is None
        assert log_row["input_rows"] is None

    def test_log_row_validation_metrics_are_calculated_correctly(self):
        """Test that validation metrics are calculated from actual data."""
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.0,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
            step_type="bronze_validation",
            table_fqn="test_schema.bronze_table",
            write_mode="append",
            input_rows=100,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            pipeline_id="test_pipeline",
            schema="test_schema",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="test_run",
            run_mode="initial",
        )
        
        # Verify validation metrics are calculated correctly
        expected_valid_rows = int(100 * 95.0 / 100)  # 95
        expected_invalid_rows = int(100 * (100 - 95.0) / 100)  # 5
        
        assert log_row["valid_rows"] == expected_valid_rows
        assert log_row["invalid_rows"] == expected_invalid_rows
        assert log_row["validation_rate"] == 95.0

    def test_log_row_uses_actual_execution_context_data(self):
        """Test that execution context data is used instead of hardcoded values."""
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.0,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
        )
        
        execution_context = ExecutionContext(
            mode=ExecutionMode.INCREMENTAL,
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            end_time=datetime(2024, 1, 1, 10, 30, 0),
            pipeline_id="my_pipeline",
            schema="my_schema",
            execution_id="exec_123",
        )
        
        log_row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_context,
            run_id="run_456",
            run_mode="incremental",
        )
        
        # Verify execution context data is used
        assert log_row["execution_id"] == "exec_123"
        assert log_row["pipeline_id"] == "my_pipeline"
        assert log_row["schema"] == "my_schema"
        assert log_row["run_id"] == "run_456"
        assert log_row["run_mode"] == "incremental"
