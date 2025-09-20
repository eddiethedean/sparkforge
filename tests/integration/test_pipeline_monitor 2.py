#!/usr/bin/env python3
"""
Tests for pipeline monitoring functionality.

This module tests the SimplePipelineMonitor class and its methods.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from sparkforge.pipeline.models import PipelineMode, PipelineStatus
from sparkforge.pipeline.monitor import PipelineMonitor, SimplePipelineMonitor


class TestSimplePipelineMonitor:
    """Test cases for SimplePipelineMonitor."""

    def test_monitor_initialization_with_logger(self):
        """Test monitor initialization with custom logger."""
        mock_logger = Mock()
        monitor = SimplePipelineMonitor(logger=mock_logger)

        assert monitor.logger == mock_logger
        assert monitor._current_report is None

    def test_monitor_initialization_without_logger(self):
        """Test monitor initialization without logger."""
        monitor = SimplePipelineMonitor()

        assert monitor.logger is not None
        assert monitor._current_report is None

    def test_start_execution(self):
        """Test starting pipeline execution monitoring."""
        monitor = SimplePipelineMonitor()

        bronze_steps = {"step1": "data1", "step2": "data2"}
        silver_steps = {"step3": "data3"}
        gold_steps = {"step4": "data4"}

        report = monitor.start_execution(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            bronze_steps=bronze_steps,
            silver_steps=silver_steps,
            gold_steps=gold_steps,
        )

        assert report.pipeline_id == "test_pipeline"
        assert report.status == PipelineStatus.RUNNING
        assert report.mode == PipelineMode.INITIAL
        assert report.start_time is not None
        assert report.end_time is None
        assert report.duration_seconds == 0.0
        assert report.metrics.total_steps == 4  # 2 bronze + 1 silver + 1 gold
        assert report.metrics.successful_steps == 0
        assert report.metrics.failed_steps == 0
        assert report.errors == []
        assert report.warnings == []
        assert report.metrics.total_duration == 0.0

    def test_start_execution_with_empty_steps(self):
        """Test starting execution with empty step dictionaries."""
        monitor = SimplePipelineMonitor()

        report = monitor.start_execution(
            pipeline_id="empty_pipeline",
            mode=PipelineMode.INCREMENTAL,
            bronze_steps={},
            silver_steps={},
            gold_steps={},
        )

        assert report.metrics.total_steps == 0

    @patch("sparkforge.pipeline.monitor.datetime")
    def test_start_execution_with_mocked_time(self, mock_datetime):
        """Test start execution with mocked datetime."""
        fixed_time = datetime(2024, 1, 15, 10, 30, 0)
        mock_datetime.now.return_value = fixed_time

        monitor = SimplePipelineMonitor()
        report = monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        assert report.start_time == fixed_time

    def test_update_step_execution_success(self):
        """Test updating step execution with success."""
        monitor = SimplePipelineMonitor()

        # Start execution first
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Update with successful step
        monitor.update_step_execution(
            step_name="test_step",
            step_type="bronze",
            success=True,
            duration=1.5,
            rows_processed=100,
            rows_written=95,
        )

        assert monitor._current_report.metrics.successful_steps == 1
        assert monitor._current_report.metrics.failed_steps == 0
        assert len(monitor._current_report.errors) == 0

    def test_update_step_execution_failure(self):
        """Test updating step execution with failure."""
        monitor = SimplePipelineMonitor()

        # Start execution first
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Update with failed step
        monitor.update_step_execution(
            step_name="test_step",
            step_type="bronze",
            success=False,
            duration=0.5,
            error_message="Test error occurred",
            rows_processed=50,
            rows_written=0,
        )

        assert monitor._current_report.metrics.successful_steps == 0
        assert monitor._current_report.metrics.failed_steps == 1
        assert len(monitor._current_report.errors) == 1
        assert "test_step: Test error occurred" in monitor._current_report.errors

    def test_update_step_execution_without_active_report(self):
        """Test updating step execution without active report."""
        monitor = SimplePipelineMonitor()

        # Try to update without starting execution
        monitor.update_step_execution(
            step_name="test_step", step_type="bronze", success=True, duration=1.0
        )

        # Should not raise an error, just return
        assert monitor._current_report is None

    def test_update_step_execution_failure_without_error_message(self):
        """Test updating step execution failure without error message."""
        monitor = SimplePipelineMonitor()

        # Start execution first
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Update with failed step but no error message
        monitor.update_step_execution(
            step_name="test_step", step_type="bronze", success=False, duration=0.5
        )

        assert monitor._current_report.metrics.failed_steps == 1
        assert len(monitor._current_report.errors) == 0

    def test_finish_execution_success(self):
        """Test finishing execution with success."""
        monitor = SimplePipelineMonitor()

        # Start execution
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Add some successful steps
        monitor.update_step_execution("step1", "bronze", True, 1.0)

        # Finish execution
        report = monitor.finish_execution(success=True)

        assert report.status == PipelineStatus.COMPLETED
        assert report.end_time is not None
        assert report.duration_seconds > 0
        assert report.metrics.total_duration > 0

    def test_finish_execution_failure(self):
        """Test finishing execution with failure."""
        monitor = SimplePipelineMonitor()

        # Start execution
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Add some failed steps
        monitor.update_step_execution("step1", "bronze", False, 0.5, "Test error")

        # Finish execution
        report = monitor.finish_execution(success=False)

        assert report.status == PipelineStatus.FAILED
        assert report.end_time is not None
        assert report.duration_seconds > 0

    def test_finish_execution_without_active_report(self):
        """Test finishing execution without active report."""
        monitor = SimplePipelineMonitor()

        with pytest.raises(RuntimeError, match="No active execution to finish"):
            monitor.finish_execution(success=True)

    def test_finish_execution_with_zero_steps(self):
        """Test finishing execution with zero total steps."""
        monitor = SimplePipelineMonitor()

        # Start execution with empty steps
        monitor.start_execution(
            pipeline_id="empty",
            mode=PipelineMode.INITIAL,
            bronze_steps={},
            silver_steps={},
            gold_steps={},
        )

        # Finish execution
        monitor.finish_execution(success=True)

    @patch("sparkforge.pipeline.monitor.datetime")
    def test_finish_execution_with_mocked_time(self, mock_datetime):
        """Test finish execution with mocked datetime."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        mock_datetime.now.side_effect = [start_time, end_time]

        monitor = SimplePipelineMonitor()
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        report = monitor.finish_execution(success=True)

        assert report.start_time == start_time
        assert report.end_time == end_time
        assert report.duration_seconds == 300.0  # 5 minutes

    def test_finish_execution_mixed_results(self):
        """Test finishing execution with mixed success/failure results."""
        monitor = SimplePipelineMonitor()

        # Start execution
        monitor.start_execution(
            pipeline_id="mixed",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={"step2": "data"},
            gold_steps={"step3": "data"},
        )

        # Add mixed results
        monitor.update_step_execution("step1", "bronze", True, 1.0)
        monitor.update_step_execution("step2", "silver", False, 0.5, "Silver error")
        monitor.update_step_execution("step3", "gold", True, 2.0)

        # Finish execution
        report = monitor.finish_execution(success=True)

        assert report.metrics.successful_steps == 2
        assert report.metrics.failed_steps == 1
        assert len(report.errors) == 1

    def test_pipeline_monitor_alias(self):
        """Test that PipelineMonitor alias works correctly."""
        # Test that the alias is properly defined
        assert PipelineMonitor == SimplePipelineMonitor

        # Test instantiation through alias
        monitor = PipelineMonitor()
        assert isinstance(monitor, SimplePipelineMonitor)

    def test_monitor_logging_calls(self):
        """Test that monitor makes appropriate logging calls."""
        mock_logger = Mock()
        monitor = SimplePipelineMonitor(logger=mock_logger)

        # Start execution
        monitor.start_execution(
            pipeline_id="test",
            mode=PipelineMode.INITIAL,
            bronze_steps={"step1": "data"},
            silver_steps={},
            gold_steps={},
        )

        # Update step
        monitor.update_step_execution("step1", "bronze", True, 1.0)

        # Finish execution
        monitor.finish_execution(success=True)

        # Verify logging calls were made
        assert mock_logger.info.call_count >= 2  # Start and finish
        assert mock_logger.debug.call_count >= 1  # Step update
