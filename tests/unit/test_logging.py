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

from sparkforge.logging import (
    PipelineLogger,
    create_logger,
    get_global_logger,
    get_logger,
    reset_global_logger,
    set_global_logger,
    set_logger,
)


class TestPipelineLoggerComprehensive:
    """Comprehensive test cases for PipelineLogger class."""

    def test_pipeline_start(self):
        """Test pipeline start logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_start("test_pipeline", "initial")
            mock_info.assert_called_once_with(
                "🚀 Starting pipeline: test_pipeline (mode: initial)"
            )

    def test_pipeline_start_custom_mode(self):
        """Test pipeline start logging with custom mode."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_start("test_pipeline", "incremental")
            mock_info.assert_called_once_with(
                "🚀 Starting pipeline: test_pipeline (mode: incremental)"
            )

    def test_pipeline_end_success(self):
        """Test pipeline end logging for successful pipeline."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_end("test_pipeline", 120.5, success=True)
            mock_info.assert_called_once_with(
                "✅ Success pipeline: test_pipeline (120.50s)"
            )

    def test_pipeline_end_failure(self):
        """Test pipeline end logging for failed pipeline."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.pipeline_end("test_pipeline", 120.5, success=False)
            mock_info.assert_called_once_with(
                "❌ Failed pipeline: test_pipeline (120.50s)"
            )

    def test_performance_metric(self):
        """Test performance metric logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.performance_metric("execution_time", 123.45, "s")
            mock_info.assert_called_once_with("📊 execution_time: 123.45s")

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
        assert result == "Test message | key1=value1, key2=value2"

    def test_format_message_without_kwargs(self):
        """Test _format_message method without kwargs."""
        logger = PipelineLogger()

        # Test without kwargs
        result = logger._format_message("Test message", {})
        assert result == "Test message"

    def test_context_manager(self):
        """Test context manager functionality."""
        logger = PipelineLogger()

        # Mock the logger.extra attribute
        logger.logger.extra = {}

        with logger.context(operation="test", step="validation"):
            # Check that context was set
            assert logger.logger.extra == {"operation": "test", "step": "validation"}

        # Check that context was restored
        assert logger.logger.extra == {}

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
            mock_info.assert_called_once_with("🚀 Starting BRONZE step: user_events")

    def test_step_start_different_stage(self):
        """Test step start logging for different stages."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_start("silver", "enriched_events")
            mock_info.assert_called_once_with(
                "🚀 Starting SILVER step: enriched_events"
            )

    def test_step_complete(self):
        """Test step completion logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_complete("bronze", "user_events", 45.2, 1000)
            mock_info.assert_called_once_with(
                "✅ Completed BRONZE step: user_events (45.20s, 1,000 rows)"
            )

    def test_step_complete_no_rows(self):
        """Test step completion logging without row count."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.step_complete("silver", "enriched_events", 30.1)
            mock_info.assert_called_once_with(
                "✅ Completed SILVER step: enriched_events (30.10s, 0 rows)"
            )

    def test_step_failed(self):
        """Test step failure logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "error") as mock_error:
            logger.step_failed("bronze", "user_events", "Connection timeout", 45.2)
            mock_error.assert_called_once_with(
                "❌ Failed BRONZE step: user_events (45.20s) - Connection timeout"
            )

    def test_step_failed_no_duration(self):
        """Test step failure logging without duration."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "error") as mock_error:
            logger.step_failed("silver", "enriched_events", "Validation error")
            mock_error.assert_called_once_with(
                "❌ Failed SILVER step: enriched_events (0.00s) - Validation error"
            )

    def test_validation_passed(self):
        """Test validation success logging."""
        logger = PipelineLogger()

        with patch.object(logger.logger, "info") as mock_info:
            logger.validation_passed("bronze", "user_events", 98.5, 95.0)
            mock_info.assert_called_once_with(
                "✅ Validation passed for bronze:user_events - 98.50% >= 95.00%"
            )

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

        with patch("sparkforge.logging.datetime") as mock_datetime:
            mock_now = datetime(2024, 1, 15, 10, 30, 0)
            mock_datetime.utcnow.return_value = mock_now

            logger.start_timer("test_timer")

            assert "test_timer" in logger._timers
            assert logger._timers["test_timer"] == mock_now

    def test_timer_end(self):
        """Test timer end functionality."""
        logger = PipelineLogger()

        with patch("sparkforge.logging.datetime") as mock_datetime, patch.object(
            logger, "performance_metric"
        ) as mock_perf:
            start_time = datetime(2024, 1, 15, 10, 30, 0)
            end_time = datetime(2024, 1, 15, 10, 32, 30)
            mock_datetime.utcnow.side_effect = [start_time, end_time]

            logger.start_timer("test_timer")
            duration = logger.end_timer("test_timer")

            assert duration == 150.0  # 2.5 minutes = 150 seconds
            mock_perf.assert_called_once_with("test_timer", 150.0)

    def test_timer_end_nonexistent(self):
        """Test timer end for nonexistent timer."""
        logger = PipelineLogger()

        duration = logger.end_timer("nonexistent_timer")

        assert duration == 0.0

    def test_timer_context_manager(self):
        """Test timer context manager."""
        logger = PipelineLogger()

        with patch("sparkforge.logging.datetime") as mock_datetime, patch.object(
            logger, "performance_metric"
        ) as mock_perf:
            start_time = datetime(2024, 1, 15, 10, 30, 0)
            end_time = datetime(2024, 1, 15, 10, 30, 5)
            mock_datetime.utcnow.side_effect = [start_time, end_time]

            with logger.timer("context_timer"):
                pass

            mock_perf.assert_called_once_with("context_timer", 5.0)

    def test_timer_context_manager_exception(self):
        """Test timer context manager with exception."""
        logger = PipelineLogger()

        with patch("sparkforge.logging.datetime") as mock_datetime, patch.object(
            logger, "performance_metric"
        ) as mock_perf:
            start_time = datetime(2024, 1, 15, 10, 30, 0)
            end_time = datetime(2024, 1, 15, 10, 30, 5)
            mock_datetime.utcnow.side_effect = [start_time, end_time]

            with pytest.raises(ValueError):
                with logger.timer("context_timer"):
                    raise ValueError("Test exception")

            # Timer should still be cleaned up
            mock_perf.assert_called_once_with("context_timer", 5.0)
            assert "context_timer" not in logger._timers

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
        ) as mock_error, patch.object(
            logger.logger, "critical"
        ) as mock_critical:
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

    def test_get_logger_default(self):
        """Test getting default logger."""
        logger = get_logger()

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "SparkForge"

    def test_set_logger(self):
        """Test setting custom logger."""
        custom_logger = PipelineLogger(name="CustomLogger")
        set_logger(custom_logger)

        logger = get_logger()
        assert logger == custom_logger
        assert logger.name == "CustomLogger"

    def test_create_logger_default(self):
        """Test creating logger with default parameters."""
        logger = create_logger()

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "SparkForge"

    def test_create_logger_custom(self):
        """Test creating logger with custom parameters."""
        logger = create_logger(name="CustomLogger", level=logging.DEBUG, verbose=False)

        assert isinstance(logger, PipelineLogger)
        assert logger.name == "CustomLogger"
        assert logger.level == logging.DEBUG
        assert logger.verbose is False

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

    def test_get_global_logger(self):
        """Test getting global logger."""
        logger = get_global_logger()

        assert isinstance(logger, PipelineLogger)

    def test_set_global_logger(self):
        """Test setting global logger."""
        custom_logger = PipelineLogger(name="GlobalLogger")
        set_global_logger(custom_logger)

        logger = get_global_logger()
        assert logger == custom_logger

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

        with patch("sparkforge.logging.datetime") as mock_datetime, patch.object(
            logger, "performance_metric"
        ) as mock_perf:
            start_time = datetime(2024, 1, 15, 10, 30, 0)
            end_time = datetime(2024, 1, 15, 10, 30, 3)
            mock_datetime.utcnow.side_effect = [start_time, end_time]

            with logger.timer("test_timer"):
                pass

            mock_perf.assert_called_once_with("test_timer", 3.0)
            assert "test_timer" not in logger._timers

    def test_timer_context_manager_exception(self):
        """Test timer context manager with exception."""
        logger = PipelineLogger()

        with patch("sparkforge.logging.datetime") as mock_datetime, patch.object(
            logger, "performance_metric"
        ) as mock_perf:
            start_time = datetime(2024, 1, 15, 10, 30, 0)
            end_time = datetime(2024, 1, 15, 10, 30, 3)
            mock_datetime.utcnow.side_effect = [start_time, end_time]

            with pytest.raises(ValueError):
                with logger.timer("test_timer"):
                    raise ValueError("Test exception")

            # Timer should be cleaned up even on exception
            mock_perf.assert_called_once_with("test_timer", 3.0)
            assert "test_timer" not in logger._timers
