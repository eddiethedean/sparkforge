"""
Tests for log row field completeness and correctness.

This module tests that all necessary fields from StepExecutionResult
are properly propagated through the pipeline to the final log rows.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from sparkforge.execution import ExecutionMode, StepExecutionResult, StepStatus, StepType
from sparkforge.models import PipelineMetrics
from sparkforge.pipeline.models import PipelineMode, PipelineReport, PipelineStatus
from sparkforge.writer import LogWriter


class TestStepExecutionResultFields:
    """Test that StepExecutionResult has all required fields."""

    def test_step_execution_result_has_write_mode_field(self):
        """Test that StepExecutionResult includes write_mode field."""
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            write_mode="append"
        )
        
        assert hasattr(result, 'write_mode')
        assert result.write_mode == "append"

    def test_step_execution_result_has_validation_rate_field(self):
        """Test that StepExecutionResult includes validation_rate field."""
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.SILVER,
            status=StepStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            validation_rate=98.5
        )
        
        assert hasattr(result, 'validation_rate')
        assert result.validation_rate == 98.5

    def test_step_execution_result_has_rows_written_field(self):
        """Test that StepExecutionResult includes rows_written field."""
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.GOLD,
            status=StepStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            rows_written=1000
        )
        
        assert hasattr(result, 'rows_written')
        assert result.rows_written == 1000

    def test_step_execution_result_has_input_rows_field(self):
        """Test that StepExecutionResult includes input_rows field."""
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.SILVER,
            status=StepStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            input_rows=1200
        )
        
        assert hasattr(result, 'input_rows')
        assert result.input_rows == 1200

    def test_step_execution_result_default_values(self):
        """Test that StepExecutionResult has sensible default values."""
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
        )
        
        # Check default values
        assert result.write_mode is None
        assert result.validation_rate == 100.0
        assert result.rows_written is None
        assert result.input_rows is None


class TestPipelineReportFieldPropagation:
    """Test that fields are properly propagated to PipelineReport."""

    def test_pipeline_report_includes_write_mode(self):
        """Test that step_info in PipelineReport includes write_mode."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_123",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 5, 0),
            duration_seconds=300.0,
            metrics=PipelineMetrics(total_steps=1),
            silver_results={
                "test_step": {
                    "status": "completed",
                    "duration": 60.0,
                    "rows_processed": 1000,
                    "write_mode": "overwrite",
                    "validation_rate": 97.5,
                    "rows_written": 1000,
                    "input_rows": 1050,
                }
            }
        )
        
        assert "test_step" in report.silver_results
        step_info = report.silver_results["test_step"]
        assert step_info["write_mode"] == "overwrite"
        assert step_info["validation_rate"] == 97.5
        assert step_info["rows_written"] == 1000
        assert step_info["input_rows"] == 1050


class TestLogRowFieldCalculations:
    """Test that LogWriter correctly calculates and populates log row fields."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    @pytest.fixture
    def writer(self, mock_spark):
        """Create a LogWriter instance."""
        with patch('sparkforge.writer.core.StorageManager'):
            with patch('sparkforge.writer.core.PerformanceMonitor'):
                with patch('sparkforge.writer.core.DataProcessor'):
                    with patch('sparkforge.writer.core.AnalyticsEngine'):
                        with patch('sparkforge.writer.core.DataQualityAnalyzer'):
                            with patch('sparkforge.writer.core.TrendAnalyzer'):
                                return LogWriter(mock_spark, schema="test", table_name="logs")

    def test_log_row_write_mode_populated_from_step_info(self, writer):
        """Test that write_mode is populated from step_info."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_001",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(total_steps=1),
            silver_results={
                "test_step": {
                    "status": "completed",
                    "duration": 30.0,
                    "rows_processed": 500,
                    "output_table": "silver.test",
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:30",
                    "write_mode": "append",
                    "validation_rate": 100.0,
                    "rows_written": 500,
                    "input_rows": 500,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        assert log_rows[0]["write_mode"] == "append"

    def test_log_row_validation_fields_calculated_correctly(self, writer):
        """Test that valid_rows and invalid_rows are calculated correctly."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_002",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 2, 0),
            duration_seconds=120.0,
            metrics=PipelineMetrics(total_steps=1),
            bronze_results={
                "test_bronze": {
                    "status": "completed",
                    "duration": 60.0,
                    "rows_processed": 1000,
                    "output_table": "bronze.test",
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:01:00",
                    "write_mode": None,
                    "validation_rate": 95.0,  # 95% valid
                    "rows_written": 1000,
                    "input_rows": 1000,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        # Verify validation fields
        assert log_row["validation_rate"] == 95.0
        assert log_row["valid_rows"] == 950  # 1000 * 95.0 / 100
        assert log_row["invalid_rows"] == 50  # 1000 - 950
        assert log_row["rows_processed"] == 1000

    def test_log_row_validation_perfect_rate(self, writer):
        """Test validation calculation with 100% validation rate."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_003",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(total_steps=1),
            gold_results={
                "perfect_step": {
                    "status": "completed",
                    "duration": 45.0,
                    "rows_processed": 500,
                    "output_table": "gold.test",
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:45",
                    "write_mode": "overwrite",
                    "validation_rate": 100.0,
                    "rows_written": 500,
                    "input_rows": 500,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        assert log_row["validation_rate"] == 100.0
        assert log_row["valid_rows"] == 500
        assert log_row["invalid_rows"] == 0

    def test_log_row_validation_low_rate(self, writer):
        """Test validation calculation with low validation rate."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_004",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(total_steps=1),
            bronze_results={
                "low_quality_step": {
                    "status": "completed",
                    "duration": 30.0,
                    "rows_processed": 2000,
                    "output_table": "bronze.test",
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:30",
                    "write_mode": None,
                    "validation_rate": 75.0,  # Only 75% valid
                    "rows_written": 2000,
                    "input_rows": 2000,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        assert log_row["validation_rate"] == 75.0
        assert log_row["valid_rows"] == 1500  # 2000 * 75.0 / 100
        assert log_row["invalid_rows"] == 500  # 2000 - 1500

    def test_log_row_rows_written_vs_rows_processed(self, writer):
        """Test that rows_written and rows_processed can differ."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_005",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(total_steps=1),
            silver_results={
                "filter_step": {
                    "status": "completed",
                    "duration": 40.0,
                    "rows_processed": 1000,  # Input
                    "output_table": "silver.test",
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:40",
                    "write_mode": "overwrite",
                    "validation_rate": 100.0,
                    "rows_written": 800,  # After filtering
                    "input_rows": 1200,  # Original input before processing
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        assert log_row["rows_processed"] == 1000
        assert log_row["rows_written"] == 800
        assert log_row["input_rows"] == 1200
        assert log_row["output_rows"] == 800  # Should equal rows_written

    def test_log_row_all_phases_have_correct_fields(self, writer):
        """Test that all phases (bronze, silver, gold) have correct fields."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_006",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 5, 0),
            duration_seconds=300.0,
            metrics=PipelineMetrics(total_steps=3),
            bronze_results={
                "bronze_step": {
                    "status": "completed",
                    "duration": 50.0,
                    "rows_processed": 5000,
                    "output_table": None,
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:50",
                    "write_mode": None,  # Bronze doesn't write
                    "validation_rate": 98.0,
                    "rows_written": None,
                    "input_rows": 5000,
                }
            },
            silver_results={
                "silver_step": {
                    "status": "completed",
                    "duration": 100.0,
                    "rows_processed": 4900,
                    "output_table": "silver.test",
                    "start_time": "2024-01-15T10:00:50",
                    "end_time": "2024-01-15T10:02:30",
                    "write_mode": "append",
                    "validation_rate": 99.5,
                    "rows_written": 4900,
                    "input_rows": 4900,
                }
            },
            gold_results={
                "gold_step": {
                    "status": "completed",
                    "duration": 150.0,
                    "rows_processed": 1000,
                    "output_table": "gold.test",
                    "start_time": "2024-01-15T10:02:30",
                    "end_time": "2024-01-15T10:05:00",
                    "write_mode": "overwrite",
                    "validation_rate": 100.0,
                    "rows_written": 1000,
                    "input_rows": 4900,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 3
        
        # Check bronze
        bronze_row = log_rows[0]
        assert bronze_row["phase"] == "bronze"
        assert bronze_row["write_mode"] is None
        assert bronze_row["validation_rate"] == 98.0
        assert bronze_row["valid_rows"] == 4900  # 5000 * 98.0 / 100
        assert bronze_row["invalid_rows"] == 100
        
        # Check silver
        silver_row = log_rows[1]
        assert silver_row["phase"] == "silver"
        assert silver_row["write_mode"] == "append"
        assert silver_row["validation_rate"] == 99.5
        assert silver_row["valid_rows"] == 4875  # 4900 * 99.5 / 100
        assert silver_row["invalid_rows"] == 25
        assert silver_row["rows_written"] == 4900
        
        # Check gold
        gold_row = log_rows[2]
        assert gold_row["phase"] == "gold"
        assert gold_row["write_mode"] == "overwrite"
        assert gold_row["validation_rate"] == 100.0
        assert gold_row["valid_rows"] == 1000
        assert gold_row["invalid_rows"] == 0
        assert gold_row["input_rows"] == 4900

    def test_log_row_failed_step_has_zero_validation(self, writer):
        """Test that failed steps have appropriate validation values."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_007",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.FAILED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(total_steps=1, failed_steps=1),
            silver_results={
                "failed_step": {
                    "status": "failed",
                    "duration": 20.0,
                    "rows_processed": 0,
                    "output_table": None,
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:00:20",
                    "error": "Processing failed",
                    "write_mode": None,
                    "validation_rate": 0.0,
                    "rows_written": 0,
                    "input_rows": 0,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        assert log_row["success"] is False
        assert log_row["validation_rate"] == 0.0
        assert log_row["valid_rows"] == 0
        assert log_row["invalid_rows"] == 0
        assert log_row["write_mode"] is None
        assert log_row["error_message"] == "Processing failed"

    def test_log_row_validation_calculation_edge_cases(self, writer):
        """Test validation calculations with edge case values."""
        test_cases = [
            # (rows_processed, validation_rate, expected_valid, expected_invalid)
            (100, 100.0, 100, 0),
            (100, 0.0, 0, 100),
            (1000, 99.9, 999, 1),
            (1000, 50.0, 500, 500),
            (333, 33.33, 110, 223),  # int(333 * 33.33 / 100) = 110
            (0, 100.0, 0, 0),
        ]
        
        for rows_processed, validation_rate, expected_valid, expected_invalid in test_cases:
            report = PipelineReport(
                pipeline_id="test",
                execution_id=f"exec_{rows_processed}_{validation_rate}",
                mode=PipelineMode.INITIAL,
                status=PipelineStatus.COMPLETED,
                start_time=datetime(2024, 1, 15, 10, 0, 0),
                end_time=datetime(2024, 1, 15, 10, 1, 0),
                duration_seconds=60.0,
                metrics=PipelineMetrics(total_steps=1),
                bronze_results={
                    "test": {
                        "status": "completed",
                        "duration": 30.0,
                        "rows_processed": rows_processed,
                        "output_table": "bronze.test",
                        "start_time": "2024-01-15T10:00:00",
                        "end_time": "2024-01-15T10:00:30",
                        "write_mode": None,
                        "validation_rate": validation_rate,
                        "rows_written": rows_processed,
                        "input_rows": rows_processed,
                    }
                }
            )
            
            log_rows = writer._convert_report_to_log_rows(report)
            log_row = log_rows[0]
            
            assert log_row["valid_rows"] == expected_valid, \
                f"Failed for rows={rows_processed}, rate={validation_rate}: " \
                f"expected {expected_valid}, got {log_row['valid_rows']}"
            assert log_row["invalid_rows"] == expected_invalid, \
                f"Failed for rows={rows_processed}, rate={validation_rate}: " \
                f"expected {expected_invalid}, got {log_row['invalid_rows']}"

    def test_log_row_different_write_modes(self, writer):
        """Test that different write modes are correctly recorded."""
        write_modes = ["append", "overwrite", None]
        
        for mode_value in write_modes:
            report = PipelineReport(
                pipeline_id="test",
                execution_id=f"exec_{mode_value}",
                mode=PipelineMode.INITIAL,
                status=PipelineStatus.COMPLETED,
                start_time=datetime(2024, 1, 15, 10, 0, 0),
                end_time=datetime(2024, 1, 15, 10, 1, 0),
                duration_seconds=60.0,
                metrics=PipelineMetrics(total_steps=1),
                silver_results={
                    "test": {
                        "status": "completed",
                        "duration": 30.0,
                        "rows_processed": 100,
                        "output_table": "silver.test" if mode_value else None,
                        "start_time": "2024-01-15T10:00:00",
                        "end_time": "2024-01-15T10:00:30",
                        "write_mode": mode_value,
                        "validation_rate": 100.0,
                        "rows_written": 100 if mode_value else None,
                        "input_rows": 100,
                    }
                }
            )
            
            log_rows = writer._convert_report_to_log_rows(report)
            log_row = log_rows[0]
            
            assert log_row["write_mode"] == mode_value, \
                f"Expected write_mode={mode_value}, got {log_row['write_mode']}"


class TestMultiStepLogRowFields:
    """Test field correctness across multiple steps."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    @pytest.fixture
    def writer(self, mock_spark):
        """Create a LogWriter instance."""
        with patch('sparkforge.writer.core.StorageManager'):
            with patch('sparkforge.writer.core.PerformanceMonitor'):
                with patch('sparkforge.writer.core.DataProcessor'):
                    with patch('sparkforge.writer.core.AnalyticsEngine'):
                        with patch('sparkforge.writer.core.DataQualityAnalyzer'):
                            with patch('sparkforge.writer.core.TrendAnalyzer'):
                                return LogWriter(mock_spark, schema="test", table_name="logs")

    def test_multiple_steps_each_have_correct_fields(self, writer):
        """Test that each step in a multi-step pipeline has correct fields."""
        report = PipelineReport(
            pipeline_id="multi_test",
            execution_id="exec_multi",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 10, 0),
            duration_seconds=600.0,
            metrics=PipelineMetrics(total_steps=4),
            bronze_results={
                "b1": {
                    "status": "completed",
                    "duration": 100.0,
                    "rows_processed": 10000,
                    "output_table": None,
                    "start_time": "2024-01-15T10:00:00",
                    "end_time": "2024-01-15T10:01:40",
                    "write_mode": None,
                    "validation_rate": 96.5,
                    "rows_written": None,
                    "input_rows": 10000,
                },
                "b2": {
                    "status": "completed",
                    "duration": 120.0,
                    "rows_processed": 8000,
                    "output_table": None,
                    "start_time": "2024-01-15T10:01:40",
                    "end_time": "2024-01-15T10:03:40",
                    "write_mode": None,
                    "validation_rate": 97.5,
                    "rows_written": None,
                    "input_rows": 8000,
                }
            },
            silver_results={
                "s1": {
                    "status": "completed",
                    "duration": 200.0,
                    "rows_processed": 9650,
                    "output_table": "silver.s1",
                    "start_time": "2024-01-15T10:03:40",
                    "end_time": "2024-01-15T10:07:00",
                    "write_mode": "overwrite",
                    "validation_rate": 99.0,
                    "rows_written": 9650,
                    "input_rows": 9650,
                }
            },
            gold_results={
                "g1": {
                    "status": "completed",
                    "duration": 180.0,
                    "rows_processed": 100,
                    "output_table": "gold.g1",
                    "start_time": "2024-01-15T10:07:00",
                    "end_time": "2024-01-15T10:10:00",
                    "write_mode": "overwrite",
                    "validation_rate": 100.0,
                    "rows_written": 100,
                    "input_rows": 9650,
                }
            }
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        
        assert len(log_rows) == 4
        
        # Verify each step has all required fields populated
        for log_row in log_rows:
            # All rows must have these fields
            assert "write_mode" in log_row
            assert "validation_rate" in log_row
            assert "valid_rows" in log_row
            assert "invalid_rows" in log_row
            assert "rows_written" in log_row
            assert "input_rows" in log_row
            assert "rows_processed" in log_row
            
            # Validation math must be correct
            rows_processed = log_row["rows_processed"]
            validation_rate = log_row["validation_rate"]
            valid_rows = log_row["valid_rows"]
            invalid_rows = log_row["invalid_rows"]
            
            expected_valid = int(rows_processed * validation_rate / 100.0)
            assert valid_rows == expected_valid
            assert invalid_rows == rows_processed - valid_rows

