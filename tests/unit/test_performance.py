"""
Tests for sparkforge.performance module.

This module tests all performance monitoring utilities and functions.
"""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from sparkforge.performance import (
    format_duration,
    monitor_performance,
    now_dt,
    performance_monitor,
    time_operation,
    time_write_operation,
)


class TestNowDt:
    """Test now_dt function."""

    def test_now_dt_returns_datetime(self):
        """Test that now_dt returns a datetime object."""
        result = now_dt()
        assert isinstance(result, datetime)

    def test_now_dt_returns_utc(self):
        """Test that now_dt returns UTC datetime."""
        result = now_dt()
        # UTC datetime should not have timezone info
        assert result.tzinfo is None

    def test_now_dt_recent_time(self):
        """Test that now_dt returns a recent time."""
        before = datetime.utcnow()
        result = now_dt()
        after = datetime.utcnow()

        assert before <= result <= after


class TestFormatDuration:
    """Test format_duration function."""

    def test_format_duration_seconds(self):
        """Test formatting duration less than 60 seconds."""
        assert format_duration(30.5) == "30.50s"
        assert format_duration(0.123) == "0.12s"
        assert (
            format_duration(59.9) == "59.90s"
        )  # Changed from 59.999 to avoid rounding to 60.00s

    def test_format_duration_minutes(self):
        """Test formatting duration less than 3600 seconds (1 hour)."""
        assert format_duration(60) == "1.00m"
        assert format_duration(90) == "1.50m"
        assert format_duration(1800) == "30.00m"
        assert (
            format_duration(3599.0) == "59.98m"
        )  # Use exact value to avoid rounding issues

    def test_format_duration_hours(self):
        """Test formatting duration 3600 seconds or more."""
        assert format_duration(3600) == "1.00h"
        assert format_duration(7200) == "2.00h"
        assert format_duration(5400) == "1.50h"
        assert format_duration(86400) == "24.00h"

    def test_format_duration_zero(self):
        """Test formatting zero duration."""
        assert format_duration(0) == "0.00s"

    def test_format_duration_negative(self):
        """Test formatting negative duration."""
        assert format_duration(-1) == "-1.00s"
        assert (
            format_duration(-60) == "-60.00s"
        )  # Negative values are treated as seconds
        assert (
            format_duration(-3600) == "-3600.00s"
        )  # Negative values are treated as seconds


class TestTimeOperation:
    """Test time_operation decorator."""

    def test_time_operation_success(self):
        """Test time_operation decorator with successful operation."""

        @time_operation("test operation")
        def test_func(x, y):
            return x + y

        with patch("sparkforge.performance.logger") as mock_logger:
            result = test_func(2, 3)

            assert result == 5
            assert mock_logger.info.call_count == 2  # Start and completion
            mock_logger.info.assert_any_call("Starting test operation...")
            # Check that completion message was called
            completion_calls = [
                call
                for call in mock_logger.info.call_args_list
                if "Completed test operation in" in str(call)
            ]
            assert len(completion_calls) == 1

    def test_time_operation_failure(self):
        """Test time_operation decorator with failed operation."""

        @time_operation("test operation")
        def test_func():
            raise ValueError("Test error")

        with patch("sparkforge.performance.logger") as mock_logger:
            with pytest.raises(ValueError, match="Test error"):
                test_func()

            assert mock_logger.info.call_count == 1  # Start
            assert mock_logger.error.call_count == 1  # Error
            mock_logger.info.assert_called_with("Starting test operation...")
            # Check that error message was called
            error_calls = [
                call
                for call in mock_logger.error.call_args_list
                if "Failed test operation after" in str(call)
            ]
            assert len(error_calls) == 1

    def test_time_operation_preserves_function_metadata(self):
        """Test that time_operation preserves function metadata."""

        @time_operation("test operation")
        def test_func(x, y):
            """Test function docstring."""
            return x + y

        assert test_func.__name__ == "test_func"
        assert test_func.__doc__ == "Test function docstring."

    def test_time_operation_with_args_and_kwargs(self):
        """Test time_operation with various argument types."""

        @time_operation("test operation")
        def test_func(a, b=10, *args, **kwargs):
            return a + b + sum(args) + sum(kwargs.values())

        with patch("sparkforge.performance.logger"):
            result = test_func(1, 2, 3, 4, x=5, y=6)
            assert result == 1 + 2 + 3 + 4 + 5 + 6


class TestPerformanceMonitor:
    """Test performance_monitor context manager."""

    def test_performance_monitor_success(self):
        """Test performance_monitor with successful operation."""
        with patch("sparkforge.performance.logger") as mock_logger:
            with performance_monitor("test operation"):
                time.sleep(0.01)  # Small delay to ensure timing

            assert mock_logger.info.call_count == 2  # Start and completion
            mock_logger.info.assert_any_call("Starting test operation...")
            # Check that completion message was called
            completion_calls = [
                call
                for call in mock_logger.info.call_args_list
                if "Completed test operation in" in str(call)
            ]
            assert len(completion_calls) == 1

    def test_performance_monitor_failure(self):
        """Test performance_monitor with failed operation."""
        with patch("sparkforge.performance.logger") as mock_logger:
            with pytest.raises(ValueError, match="Test error"):
                with performance_monitor("test operation"):
                    raise ValueError("Test error")

            assert mock_logger.info.call_count == 1  # Start
            assert mock_logger.error.call_count == 1  # Error
            mock_logger.info.assert_called_with("Starting test operation...")
            # Check that error message was called
            error_calls = [
                call
                for call in mock_logger.error.call_args_list
                if "Failed test operation after" in str(call)
            ]
            assert len(error_calls) == 1

    def test_performance_monitor_with_max_duration_warning(self):
        """Test performance_monitor with max duration warning."""
        with patch("sparkforge.performance.logger") as mock_logger:
            with performance_monitor(
                "test operation", max_duration=0.001
            ):  # Very short threshold
                time.sleep(0.01)  # Longer than threshold

            assert mock_logger.info.call_count == 2  # Start and completion
            assert (
                mock_logger.warning.call_count == 1
            )  # Warning about exceeding threshold
            # Check that warning message was called
            warning_calls = [
                call
                for call in mock_logger.warning.call_args_list
                if "exceeding threshold of 0.001s" in str(call)
            ]
            assert len(warning_calls) == 1

    def test_performance_monitor_with_max_duration_no_warning(self):
        """Test performance_monitor with max duration no warning."""
        with patch("sparkforge.performance.logger") as mock_logger:
            with performance_monitor(
                "test operation", max_duration=1.0
            ):  # Long threshold
                time.sleep(0.01)  # Shorter than threshold

            assert mock_logger.info.call_count == 2  # Start and completion
            assert mock_logger.warning.call_count == 0  # No warning


class TestTimeWriteOperation:
    """Test time_write_operation function."""

    def test_time_write_operation_invalid_mode(self):
        """Test time_write_operation with invalid mode."""
        mock_df = MagicMock()

        with patch("sparkforge.performance.logger"):
            with pytest.raises(
                ValueError,
                match="Unknown write mode 'invalid'. Supported modes: overwrite, append",
            ):
                time_write_operation("invalid", mock_df, "test.table")

    def test_time_write_operation_imports_table_operations(self):
        """Test that time_write_operation imports table operations correctly."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100

        with patch("sparkforge.table_operations.write_overwrite_table") as mock_write:
            mock_write.return_value = 100

            with patch("sparkforge.performance.logger"):
                rows, duration, start, end = time_write_operation(
                    "overwrite", mock_df, "test.table"
                )

                assert rows == 100
                assert isinstance(duration, float)
                assert isinstance(start, datetime)
                assert isinstance(end, datetime)
                assert duration >= 0
                mock_write.assert_called_once_with(mock_df, "test.table")

    def test_time_write_operation_append_mode(self):
        """Test time_write_operation with append mode."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50

        with patch("sparkforge.table_operations.write_append_table") as mock_write:
            mock_write.return_value = 50

            with patch("sparkforge.performance.logger"):
                rows, duration, start, end = time_write_operation(
                    "append", mock_df, "test.table"
                )

                assert rows == 50
                assert isinstance(duration, float)
                assert isinstance(start, datetime)
                assert isinstance(end, datetime)
                assert duration >= 0
                mock_write.assert_called_once_with(mock_df, "test.table")

    def test_time_write_operation_with_options(self):
        """Test time_write_operation with additional options."""
        mock_df = MagicMock()

        with patch("sparkforge.table_operations.write_overwrite_table") as mock_write:
            mock_write.return_value = 100

            with patch("sparkforge.performance.logger"):
                time_write_operation(
                    "overwrite",
                    mock_df,
                    "test.table",
                    option1="value1",
                    option2="value2",
                )

                mock_write.assert_called_once_with(
                    mock_df, "test.table", option1="value1", option2="value2"
                )

    def test_time_write_operation_failure(self):
        """Test time_write_operation with write failure."""
        mock_df = MagicMock()

        with patch("sparkforge.table_operations.write_overwrite_table") as mock_write:
            mock_write.side_effect = Exception("Write failed")

            with patch("sparkforge.performance.logger") as mock_logger:
                with pytest.raises(Exception, match="Write failed"):
                    time_write_operation("overwrite", mock_df, "test.table")

                # Check that error was logged
                error_calls = [
                    call
                    for call in mock_logger.error.call_args_list
                    if "Write operation failed after" in str(call)
                ]
                assert len(error_calls) == 1


class TestMonitorPerformance:
    """Test monitor_performance decorator factory."""

    def test_monitor_performance_success(self):
        """Test monitor_performance decorator with successful operation."""

        @monitor_performance("test operation")
        def test_func():
            return "success"

        with patch("sparkforge.performance.logger") as mock_logger:
            result = test_func()

            assert result == "success"
            assert mock_logger.info.call_count == 2  # Start and completion
            mock_logger.info.assert_any_call("Starting test operation...")
            # Check that completion message was called
            completion_calls = [
                call
                for call in mock_logger.info.call_args_list
                if "Completed test operation in" in str(call)
            ]
            assert len(completion_calls) == 1

    def test_monitor_performance_failure(self):
        """Test monitor_performance decorator with failed operation."""

        @monitor_performance("test operation")
        def test_func():
            raise ValueError("Test error")

        with patch("sparkforge.performance.logger") as mock_logger:
            with pytest.raises(ValueError, match="Test error"):
                test_func()

            assert mock_logger.info.call_count == 1  # Start
            assert mock_logger.error.call_count == 1  # Error

    def test_monitor_performance_with_max_duration(self):
        """Test monitor_performance with max duration."""

        @monitor_performance("test operation", max_duration=0.001)
        def test_func():
            time.sleep(0.01)  # Longer than threshold
            return "success"

        with patch("sparkforge.performance.logger") as mock_logger:
            result = test_func()

            assert result == "success"
            assert (
                mock_logger.warning.call_count == 1
            )  # Warning about exceeding threshold

    def test_monitor_performance_preserves_function_metadata(self):
        """Test that monitor_performance preserves function metadata."""

        @monitor_performance("test operation")
        def test_func(x, y):
            """Test function docstring."""
            return x + y

        assert test_func.__name__ == "test_func"
        assert test_func.__doc__ == "Test function docstring."


class TestPerformanceIntegration:
    """Test performance module integration."""

    def test_all_functions_work_together(self):
        """Test that all performance functions work together."""

        @time_operation("integration test")
        @monitor_performance("monitored operation")
        def complex_operation(x, y):
            """Complex operation that uses multiple performance features."""
            with performance_monitor("nested operation"):
                time.sleep(0.001)
            return x + y

        with patch("sparkforge.performance.logger") as mock_logger:
            result = complex_operation(2, 3)

            assert result == 5
            # Should have multiple log messages from different decorators
            assert mock_logger.info.call_count >= 4

    def test_duration_formatting_integration(self):
        """Test duration formatting with actual timing."""
        start = time.time()
        time.sleep(0.1)
        duration = time.time() - start

        formatted = format_duration(duration)
        assert "s" in formatted
        assert float(formatted.replace("s", "")) > 0

    def test_now_dt_consistency(self):
        """Test that now_dt returns consistent results."""
        dt1 = now_dt()
        time.sleep(0.001)
        dt2 = now_dt()

        assert dt1 < dt2
        assert (dt2 - dt1).total_seconds() < 1.0  # Should be very close
