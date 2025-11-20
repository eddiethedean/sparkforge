#!/usr/bin/env python3
"""
Comprehensive tests for logging module functionality.

This module tests all logging functions and classes with extensive coverage.
"""

import logging
import os
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest

from pipeline_builder.logging import PipelineLogger

# These functions were removed in the refactoring - tests updated to use PipelineLogger directly
# create_logger, get_global_logger, get_logger, reset_global_logger, set_global_logger, set_logger


class TestPipelineLoggerComprehensive:
    """Comprehensive test cases for PipelineLogger class."""

    @pytest.mark.skip(reason="pipeline_start method was removed in refactoring")
    def test_pipeline_start(self):
        """Test pipeline start logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_start("test_pipeline", "initial")
            mock_info.assert_called_once_with(
                "🚀 Starting pipeline: test_pipeline (mode: initial)"
            )

    @pytest.mark.skip(reason="pipeline_start method was removed in refactoring")
    def test_pipeline_start_custom_mode(self):
        """Test pipeline start logging with custom mode."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_start("test_pipeline", "incremental")
            mock_info.assert_called_once_with(
                "🚀 Starting pipeline: test_pipeline (mode: incremental)"
            )

    @pytest.mark.skip(reason="pipeline_end method was removed in refactoring")
    def test_pipeline_end_success(self):
        """Test pipeline end logging for successful pipeline."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_end("test_pipeline", 120.5, success=True)
            mock_info.assert_called_once_with(
                "✅ Success pipeline: test_pipeline (120.50s)"
            )

    @pytest.mark.skip(reason="pipeline_end method was removed in refactoring")
    def test_pipeline_end_failure(self):
        """Test pipeline end logging for failed pipeline."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_end("test_pipeline", 120.5, success=False)
            mock_info.assert_called_once_with(
                "❌ Failed pipeline: test_pipeline (120.50s)"
            )

    @pytest.mark.skip(reason="performance_metric method was removed in refactoring")
    def test_performance_metric(self):
        """Test performance metric logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.performance_metric("execution_time", 123.45, "s")
            mock_info.assert_called_once_with("📊 execution_time: 123.45s")

    @pytest.mark.skip(reason="performance_metric method was removed in refactoring")
    def test_performance_metric_custom_unit(self):
        """Test performance metric logging with custom unit."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.performance_metric("memory_usage", 1024.0, "MB")
            mock_info.assert_called_once_with("📊 memory_usage: 1024.00MB")

    def test_format_message_with_kwargs(self):
        """Test _format_message method with kwargs."""
        logger = PipelineLogger()

        # Test with kwargs
        result = logger._format_message(
            "Test message", {"key1": "value1", "key2": "value2"}
        )
        assert result == "Test message (key1=value1, key2=value2)"

    def test_format_message_without_kwargs(self):
        """Test _format_message method without kwargs."""
        logger = PipelineLogger()

        # Test without kwargs
        result = logger._format_message("Test message", {})
        assert result == "Test message"

    def test_context_manager(self):
        """Test context manager functionality."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            with logger.log_context("test_operation"):
                pass
            mock_info.assert_any_call("Starting: test_operation")
            mock_info.assert_any_call("Completed: test_operation")

    @pytest.mark.skip(reason="context method with kwargs was removed, use log_context instead")
    def test_context_manager_with_existing_extra(self):
        """Test context manager with existing extra data."""
        logger = PipelineLogger()

        # Set existing extra data
        logger.logger.extra = {"existing": "data"}

        with logger.context(operation="test"):
            # Check that context was merged
            assert logger.logger.extra == {"existing": "data", "operation": "test"}

        # Check that original context was restored
        assert logger.logger.extra == {"existing": "data"}

    def test_step_start(self):
        """Test step start logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_start("bronze", "user_events")
            mock_info.assert_called_once_with("▶️ Starting BRONZE step: user_events")

    def test_step_start_different_stage(self):
        """Test step start logging for different stages."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_start("silver", "enriched_events")
            mock_info.assert_called_once_with(
                "▶️ Starting SILVER step: enriched_events"
            )

    def test_step_complete(self):
        """Test step completion logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_complete("bronze", "user_events", 45.2, rows_processed=1000)
            mock_info.assert_called_once_with(
                "✅ Completed BRONZE step: user_events (45.20s) - 1000 rows processed, 0 rows written, 0 invalid, 100.0% valid"
            )

    def test_step_complete_no_rows(self):
        """Test step completion logging without row count."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_complete("silver", "enriched_events", 30.1)
            mock_info.assert_called_once_with(
                "✅ Completed SILVER step: enriched_events (30.10s) - 0 rows processed, 0 rows written, 0 invalid, 100.0% valid"
            )

    @pytest.mark.skip(reason="step_failed method was removed in refactoring, use error() instead")
    def test_step_failed(self):
        """Test step failure logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "error") as mock_error:
            logger.step_failed("bronze", "user_events", "Connection timeout", 45.2)
            mock_error.assert_called_once_with(
                "❌ Failed BRONZE step: user_events (45.20s) - Connection timeout"
            )

    @pytest.mark.skip(reason="step_failed method was removed in refactoring, use error() instead")
    def test_step_failed_no_duration(self):
        """Test step failure logging without duration."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "error") as mock_error:
            logger.step_failed("silver", "enriched_events", "Validation error")
            mock_error.assert_called_once_with(
                "❌ Failed SILVER step: enriched_events (0.00s) - Validation error"
            )

    @pytest.mark.skip(reason="validation_passed method was removed in refactoring")
    def test_validation_passed(self):
        """Test validation success logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.validation_passed("bronze", "user_events", 98.5, 95.0)
            mock_info.assert_called_once_with(
                "✅ Validation passed for bronze:user_events - 98.50% >= 95.00%"
            )

    @pytest.mark.skip(reason="validation_failed method was removed in refactoring")
    def test_validation_failed(self):
        """Test validation failure logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "warning") as mock_warning:
            logger.validation_failed("silver", "enriched_events", 92.3, 95.0)
            mock_warning.assert_called_once_with(
                "❌ Validation failed for silver:enriched_events - 92.30% < 95.00%"
            )

    def test_timer_start(self):
        """Test timer start functionality."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime:
            from datetime import timezone

            mock_now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            # Mock datetime.now to return mock_now when called with timezone.utc
            mock_datetime.now = lambda tz=None: mock_now

            logger.start_timer("test_timer")

            assert "test_timer" in logger._timers
            assert logger._timers["test_timer"] == mock_now

    def test_timer_end(self):
        """Test timer end functionality."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime:
            from datetime import timezone

            start_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 15, 10, 32, 30, tzinfo=timezone.utc)
            call_count = [0]

            def mock_now(tz=None):
                call_count[0] += 1
                return [start_time, end_time][call_count[0] - 1]

            mock_datetime.now = mock_now

            logger.start_timer("test_timer")
            duration = logger.stop_timer("test_timer")

            assert duration == 150.0  # 2.5 minutes = 150 seconds

    def test_timer_end_nonexistent(self):
        """Test timer end for nonexistent timer."""
        logger = PipelineLogger()

        duration = logger.stop_timer("nonexistent_timer")

        assert duration == 0.0

    def test_timer_context_manager(self):
        """Test timer context manager."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime, patch.object(
            logger.logger, "info"
        ) as mock_info:
            from datetime import timezone

            start_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 15, 10, 30, 5, tzinfo=timezone.utc)
            call_count = [0]

            def mock_now(tz=None):
                call_count[0] += 1
                return [start_time, end_time][call_count[0] - 1]

            mock_datetime.now = mock_now

            with logger.time_operation("context_timer"):
                pass

            # Check that info was called with timing message
            assert any("context_timer" in str(call) and "5.00" in str(call) for call in mock_info.call_args_list)

    def test_timer_context_manager_exception(self):
        """Test timer context manager with exception."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime, patch.object(
            logger.logger, "info"
        ) as mock_info:
            from datetime import timezone

            start_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 15, 10, 30, 5, tzinfo=timezone.utc)
            call_count = [0]

            def mock_now(tz=None):
                call_count[0] += 1
                return [start_time, end_time][call_count[0] - 1]

            mock_datetime.now = mock_now

            with pytest.raises(ValueError):
                with logger.time_operation("context_timer"):
                    raise ValueError("Test exception")

            # Timer should still be cleaned up even on exception
            assert "context_timer" not in logger._timers
            # Check that info was called with timing message
            assert any("context_timer" in str(call) and "5.00" in str(call) for call in mock_info.call_args_list)

    def test_setup_handlers_console_only(self):
        """Test handler setup with console only."""
        logger = PipelineLogger(log_file=None)

        assert len(logger.logger.handlers) == 1
        assert isinstance(logger.logger.handlers[0], logging.StreamHandler)

    def test_setup_handlers_with_file(self):
        """Test handler setup with file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            log_file = f.name

        try:
            logger = PipelineLogger(log_file=log_file)

            assert len(logger.logger.handlers) == 2
            assert any(
                isinstance(h, logging.StreamHandler) for h in logger.logger.handlers
            )
            assert any(
                isinstance(h, logging.FileHandler) for h in logger.logger.handlers
            )
        finally:
            os.unlink(log_file)

    def test_setup_handlers_verbose_false(self):
        """Test handler setup with verbose=False."""
        logger = PipelineLogger(verbose=False)

        # When verbose=False, no handlers are added
        assert len(logger.logger.handlers) == 0

    def test_logger_creation_with_custom_name(self):
        """Test logger creation with custom name."""
        logger = PipelineLogger(name="CustomLogger")

        assert logger.name == "CustomLogger"
        assert logger.logger.name == "CustomLogger"

    def test_logger_creation_with_custom_level(self):
        """Test logger creation with custom level."""
        logger = PipelineLogger(level=logging.DEBUG)

        assert logger.level == logging.DEBUG
        assert logger.logger.level == logging.DEBUG

    def test_logger_creation_with_file(self):
        """Test logger creation with log file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            log_file = f.name

        try:
            logger = PipelineLogger(log_file=log_file)

            assert logger.log_file == log_file
            assert len(logger.logger.handlers) == 2
        finally:
            os.unlink(log_file)

    def test_basic_logging_methods(self):
        """Test basic logging methods."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "debug") as mock_debug, patch.object(
            logger.logger, "info"
        ) as mock_info, patch.object(
            logger.logger, "warning"
        ) as mock_warning, patch.object(
            logger.logger, "error"
        ) as mock_error, patch.object(logger.logger, "critical") as mock_critical:
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            logger.critical("Critical message")

            mock_debug.assert_called_once_with("Debug message")
            mock_info.assert_called_once_with("Info message")
            mock_warning.assert_called_once_with("Warning message")
            mock_error.assert_called_once_with("Error message")
            mock_critical.assert_called_once_with("Critical message")

    def test_set_level(self):
        """Test setting log level."""
        logger = PipelineLogger()

        logger.set_level(logging.DEBUG)
        assert logger.level == logging.DEBUG
        assert logger.logger.level == logging.DEBUG


class TestGlobalLoggerFunctions:
    """Test cases for global logger functions."""

    @pytest.mark.skip(reason="get_logger function was removed in refactoring")
    def test_get_logger_default(self):
        """Test getting default logger."""
        logger = get_logger()

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "PipelineRunner"

    @pytest.mark.skip(reason="set_logger function was removed in refactoring")
    def test_set_logger(self):
        """Test setting custom logger."""
        custom_logger = PipelineLogger(name="CustomLogger")
        set_logger(custom_logger)

        logger = get_logger()
        assert logger == custom_logger
        assert logger.name == "CustomLogger"

    @pytest.mark.skip(reason="create_logger function was removed in refactoring")
    def test_create_logger_default(self):
        """Test creating logger with default parameters."""
        logger = create_logger()

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "PipelineRunner"

    @pytest.mark.skip(reason="create_logger function was removed in refactoring")
    def test_create_logger_custom(self):
        """Test creating logger with custom parameters."""
        logger = create_logger(name="CustomLogger", level=logging.DEBUG, verbose=False)

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "CustomLogger"
        assert logger.level == logging.DEBUG
        assert logger.verbose is False

    @pytest.mark.skip(reason="create_logger function was removed in refactoring")
    def test_create_logger_with_file(self):
        """Test creating logger with file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            log_file = f.name

        try:
            logger = create_logger(log_file=log_file)

            assert isinstance(logger, PipelineLogger)
            assert logger.log_file == log_file
        finally:
            os.unlink(log_file)

    @pytest.mark.skip(reason="get_global_logger function was removed in refactoring")
    def test_get_global_logger(self):
        """Test getting global logger."""
        logger = get_global_logger()

        assert isinstance(logger, PipelineLogger)

    @pytest.mark.skip(reason="set_global_logger function was removed in refactoring")
    def test_set_global_logger(self):
        """Test setting global logger."""
        custom_logger = PipelineLogger(name="GlobalLogger")
        set_global_logger(custom_logger)

        logger = get_global_logger()
        assert logger == custom_logger

    @pytest.mark.skip(reason="reset_global_logger function was removed in refactoring")
    def test_reset_global_logger(self):
        """Test resetting global logger."""
        custom_logger = PipelineLogger(name="GlobalLogger")
        set_global_logger(custom_logger)

        reset_global_logger()

        logger = get_global_logger()
        assert logger != custom_logger
        assert isinstance(logger, PipelineLogger)


class TestTimerContextManager:
    """Test cases for timer context manager."""

    def test_timer_context_manager_success(self):
        """Test timer context manager on success."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime, patch.object(
            logger.logger, "info"
        ) as mock_info:
            from datetime import timezone

            start_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 15, 10, 30, 3, tzinfo=timezone.utc)
            call_count = [0]

            def mock_now(tz=None):
                call_count[0] += 1
                return [start_time, end_time][call_count[0] - 1]

            mock_datetime.now = mock_now

            with logger.time_operation("test_timer"):
                pass

            # Check that info was called with timing message
            assert any("test_timer" in str(call) and "3.00" in str(call) for call in mock_info.call_args_list)
            assert "test_timer" not in logger._timers

    def test_timer_context_manager_exception(self):
        """Test timer context manager with exception."""
        logger = PipelineLogger()

        with patch("pipeline_builder_base.logging.datetime") as mock_datetime, patch.object(
            logger.logger, "info"
        ) as mock_info:
            from datetime import timezone

            start_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 15, 10, 30, 3, tzinfo=timezone.utc)
            call_count = [0]

            def mock_now(tz=None):
                call_count[0] += 1
                return [start_time, end_time][call_count[0] - 1]

            mock_datetime.now = mock_now

            with pytest.raises(ValueError):
                with logger.time_operation("test_timer"):
                    raise ValueError("Test exception")

            # Timer should be cleaned up even on exception
            assert "test_timer" not in logger._timers
