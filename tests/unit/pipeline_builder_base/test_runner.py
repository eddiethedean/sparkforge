"""
Comprehensive tests for pipeline_builder_base runner module.

Tests BaseRunner and execution_helpers.
"""

from datetime import datetime
from unittest.mock import Mock

import pytest

from pipeline_builder_base.models import (
    ExecutionMode,
    StepResult,
)
from pipeline_builder_base.runner.base_runner import BaseRunner
from pipeline_builder_base.runner.execution_helpers import (
    determine_execution_mode,
    prepare_sources_for_execution,
    should_run_incremental,
    validate_bronze_sources,
)


class TestBaseRunner:
    """Test BaseRunner class."""

    def test_base_runner_initialization(self, pipeline_config, logger):
        """Test constructor with config and logger."""
        runner = BaseRunner(config=pipeline_config, logger=logger)

        assert runner.config == pipeline_config
        assert runner.logger == logger

    def test_base_runner_default_logger(self, pipeline_config):
        """Test default logger creation."""
        runner = BaseRunner(config=pipeline_config)

        assert runner.logger is not None
        assert runner.logger.name == "PipelineRunner"

    def test_handle_step_error(self, pipeline_config, logger):
        """Test error handling with logging."""
        runner = BaseRunner(config=pipeline_config, logger=logger)

        class MockStep:
            def __init__(self):
                self.name = "test_step"

        step = MockStep()
        error = ValueError("Test error")

        # Should not raise, just log
        runner._handle_step_error(step, error, "bronze")

    def test_handle_step_error_with_context(self, pipeline_config, logger):
        """Test error handling with context."""
        runner = BaseRunner(config=pipeline_config, logger=logger)

        class MockStep:
            def __init__(self):
                self.name = "test_step"
                self.table_name = "test_table"

        step = MockStep()
        error = ValueError("Test error")

        # Should not raise
        runner._handle_step_error(step, error, "silver")

    def test_collect_step_results(self, pipeline_config):
        """Test result collection."""
        from datetime import datetime

        from pipeline_builder_base.models import PipelinePhase

        runner = BaseRunner(config=pipeline_config)

        start_time = datetime.now()
        end_time = datetime.now()

        step_results = [
            StepResult(
                step_name="step1",
                phase=PipelinePhase.BRONZE,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_secs=1.0,
                rows_processed=100,
                rows_written=100,
                validation_rate=95.0,
            ),
            StepResult(
                step_name="step2",
                phase=PipelinePhase.SILVER,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_secs=2.0,
                rows_processed=200,
                rows_written=200,
                validation_rate=98.0,
            ),
        ]

        result = runner._collect_step_results(step_results)

        assert result.success is True
        assert len(result.step_results) == 2
        assert result.metrics is not None
        assert result.context is not None

    def test_collect_step_results_empty(self, pipeline_config):
        """Test empty result collection."""
        runner = BaseRunner(config=pipeline_config)

        result = runner._collect_step_results([])

        assert result.success is True
        assert len(result.step_results) == 0
        assert result.metrics is not None
        assert result.context is not None

    def test_collect_step_results_with_errors(self, pipeline_config):
        """Test collection with errors."""
        from datetime import datetime

        from pipeline_builder_base.models import PipelinePhase

        runner = BaseRunner(config=pipeline_config)

        start_time = datetime.now()
        end_time = datetime.now()

        step_results = [
            StepResult(
                step_name="step1",
                phase=PipelinePhase.BRONZE,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_secs=1.0,
                rows_processed=100,
                rows_written=100,
                validation_rate=95.0,
            ),
            StepResult(
                step_name="step2",
                phase=PipelinePhase.SILVER,
                success=False,
                start_time=start_time,
                end_time=end_time,
                duration_secs=0.0,
                rows_processed=0,
                rows_written=0,
                validation_rate=0.0,
            ),
        ]

        result = runner._collect_step_results(step_results)

        assert result.success is False
        assert len(result.step_results) == 2
        assert result.metrics.failed_steps == 1
        assert result.metrics.successful_steps == 1

    def test_create_pipeline_report(self, pipeline_config):
        """Test pipeline report creation."""
        runner = BaseRunner(config=pipeline_config)

        start_time = datetime.now()
        end_time = datetime.now()

        report = runner._create_pipeline_report(
            status="success",
            start_time=start_time,
            end_time=end_time,
        )

        assert report["status"] == "success"
        assert report["start_time"] == start_time
        assert report["end_time"] == end_time
        assert "duration_seconds" in report

    def test_aggregate_step_reports(self, pipeline_config):
        """Test step report aggregation."""
        runner = BaseRunner(config=pipeline_config)

        reports = [
            {"status": "success", "duration_seconds": 10.0},
            {"status": "success", "duration_seconds": 20.0},
        ]

        result = runner._aggregate_step_reports(reports)

        assert result["status"] == "success"
        assert result["step_count"] == 2
        assert result["success_count"] == 2
        assert result["total_duration_seconds"] == 30.0

    def test_aggregate_step_reports_empty(self, pipeline_config):
        """Test empty step report aggregation."""
        runner = BaseRunner(config=pipeline_config)

        result = runner._aggregate_step_reports([])

        assert result["status"] == "unknown"
        assert result["step_count"] == 0


class TestExecutionHelpers:
    """Test execution helper functions."""

    def test_determine_execution_mode_initial(self, pipeline_config):
        """Test initial mode detection."""
        mode = determine_execution_mode(config=pipeline_config, bronze_sources=None)
        assert mode == ExecutionMode.INITIAL

    def test_determine_execution_mode_initial_empty_sources(self, pipeline_config):
        """Test initial mode with empty sources."""
        mode = determine_execution_mode(config=pipeline_config, bronze_sources={})
        assert mode == ExecutionMode.INITIAL

    def test_determine_execution_mode_incremental(self, pipeline_config):
        """Test incremental mode detection."""
        bronze_sources = {"step1": Mock()}
        last_run = datetime.now()

        mode = determine_execution_mode(
            config=pipeline_config,
            bronze_sources=bronze_sources,
            last_run=last_run,
        )
        assert mode == ExecutionMode.INCREMENTAL

    def test_should_run_incremental_with_last_run(self, pipeline_config):
        """Test incremental run decision with last run time."""
        last_run = datetime.now()
        result = should_run_incremental(config=pipeline_config, last_run=last_run)
        assert result is True

    def test_should_run_incremental_no_last_run(self, pipeline_config):
        """Test incremental run decision without last run time."""
        result = should_run_incremental(config=pipeline_config, last_run=None)
        assert result is False

    def test_prepare_sources_for_execution_bronze(self):
        """Test source preparation for bronze steps."""
        sources = {"source1": Mock(), "source2": Mock()}

        result = prepare_sources_for_execution(sources, "bronze")
        assert result == sources

    def test_prepare_sources_for_execution_silver(self):
        """Test source preparation for silver steps."""
        sources = {"source1": Mock(), "source2": Mock()}

        result = prepare_sources_for_execution(sources, "silver")
        # Should return same sources (filtering logic may vary)
        assert isinstance(result, dict)

    def test_prepare_sources_for_execution_gold(self):
        """Test source preparation for gold steps."""
        sources = {"source1": Mock(), "source2": Mock()}

        result = prepare_sources_for_execution(sources, "gold")
        # Should return same sources (filtering logic may vary)
        assert isinstance(result, dict)

    def test_validate_bronze_sources_valid(self):
        """Test bronze source validation with valid sources."""
        sources = {"step1": Mock(), "step2": Mock()}
        expected_steps = {"step1": Mock(), "step2": Mock()}

        # Should not raise
        validate_bronze_sources(sources, expected_steps)

    def test_validate_bronze_sources_missing(self):
        """Test bronze source validation with missing sources."""
        sources = {"step1": Mock()}
        expected_steps = {"step1": Mock(), "step2": Mock()}

        with pytest.raises(ValueError) as exc_info:
            validate_bronze_sources(sources, expected_steps)

        assert "Missing bronze sources" in str(exc_info.value)

    def test_validate_bronze_sources_unexpected(self):
        """Test bronze source validation with unexpected sources."""
        sources = {"step1": Mock(), "step2": Mock(), "step3": Mock()}
        expected_steps = {"step1": Mock(), "step2": Mock()}

        with pytest.raises(ValueError) as exc_info:
            validate_bronze_sources(sources, expected_steps)

        assert "Unexpected bronze sources" in str(exc_info.value)

    def test_validate_bronze_sources_with_validator(self):
        """Test bronze source validation with custom validator."""
        sources = {"step1": Mock()}
        expected_steps = {"step1": Mock()}

        def validator(source):
            return source is not None

        # Should not raise
        validate_bronze_sources(sources, expected_steps, source_validator=validator)

    def test_validate_bronze_sources_with_invalid_validator(self):
        """Test bronze source validation with failing validator."""
        sources = {"step1": None}
        expected_steps = {"step1": Mock()}

        def validator(source):
            return source is not None

        with pytest.raises(ValueError) as exc_info:
            validate_bronze_sources(sources, expected_steps, source_validator=validator)

        assert "Invalid bronze source" in str(exc_info.value)
