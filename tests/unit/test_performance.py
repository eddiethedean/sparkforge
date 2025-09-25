# test_performance.py
"""
Unit tests for the performance module.

This module tests all performance monitoring and timing utilities.
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
        # UTC datetime should have no timezone info
        assert result.tzinfo is None

    def test_now_dt_returns_recent_time(self):
        """Test that now_dt returns recent time."""
        before = datetime.utcnow()
        result = now_dt()
        after = datetime.utcnow()

        assert before <= result <= after


class TestFormatDuration:
    """Test format_duration function."""

    def test_format_seconds(self):
        """Test formatting seconds."""
        result = format_duration(45.5)
        assert result == "45.50s"

    def test_format_minutes(self):
        """Test formatting minutes."""
        result = format_duration(125.0)  # 2 minutes 5 seconds
        assert result == "2.08m"

    def test_format_hours(self):
        """Test formatting hours."""
        result = format_duration(7200.0)  # 2 hours
        assert result == "2.00h"

    def test_format_zero(self):
        """Test formatting zero duration."""
        result = format_duration(0.0)
        assert result == "0.00s"

    def test_format_very_small(self):
        """Test formatting very small duration."""
        result = format_duration(0.001)
        assert result == "0.00s"

    def test_format_large_hours(self):
        """Test formatting large hours."""
        result = format_duration(14400.0)  # 4 hours
        assert result == "4.00h"


class TestTimeOperation:
    """Test time_operation decorator."""

    def test_time_operation_success(self):
        """Test time_operation with successful function."""

        @time_operation("test operation")
        def test_func():
            time.sleep(0.01)  # Small delay
            return "success"

        result = test_func()
        assert result == "success"

    def test_time_operation_exception(self):
        """Test time_operation with function that raises exception."""

        @time_operation("test operation")
        def test_func():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            test_func()

    def test_time_operation_logging(self, caplog):
        """Test that time_operation logs start and completion."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        @time_operation("test operation")
        def test_func():
            return "success"

        test_func()

        # Check that logging occurred
        assert len(caplog.records) >= 2
        assert "Starting test operation" in caplog.text
        assert "Completed test operation" in caplog.text


class TestPerformanceMonitor:
    """Test performance_monitor context manager."""

    def test_performance_monitor_success(self, caplog):
        """Test performance_monitor with successful operation."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        with performance_monitor("test operation"):
            time.sleep(0.01)

        assert "Starting test operation" in caplog.text
        assert "Completed test operation" in caplog.text

    def test_performance_monitor_exception(self, caplog):
        """Test performance_monitor with exception."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        with pytest.raises(ValueError):
            with performance_monitor("test operation"):
                raise ValueError("Test error")

        assert "Starting test operation" in caplog.text
        assert "Failed test operation" in caplog.text

    def test_performance_monitor_max_duration_warning(self, caplog):
        """Test performance_monitor with max duration warning."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        with performance_monitor("test operation", max_duration=0.001):
            time.sleep(0.01)  # Exceed max duration

        assert "Starting test operation" in caplog.text
        assert "Completed test operation" in caplog.text
        assert "exceeding threshold" in caplog.text


class TestTimeWriteOperation:
    """Test time_write_operation function."""

    @patch("sparkforge.table_operations.write_overwrite_table")
    def test_time_write_operation_overwrite(self, mock_write):
        """Test time_write_operation with overwrite mode."""
        mock_write.return_value = 100

        # Mock DataFrame
        mock_df = MagicMock()

        result = time_write_operation("overwrite", mock_df, "test.table")

        assert result[0] == 100  # rows_written
        assert isinstance(result[1], float)  # duration_secs
        assert isinstance(result[2], datetime)  # start_time
        assert isinstance(result[3], datetime)  # end_time
        mock_write.assert_called_once_with(mock_df, "test.table")

    @patch("sparkforge.table_operations.write_append_table")
    def test_time_write_operation_append(self, mock_write):
        """Test time_write_operation with append mode."""
        mock_write.return_value = 50

        mock_df = MagicMock()

        result = time_write_operation("append", mock_df, "test.table")

        assert result[0] == 50
        assert isinstance(result[1], float)
        assert isinstance(result[2], datetime)
        assert isinstance(result[3], datetime)
        mock_write.assert_called_once_with(mock_df, "test.table")

    def test_time_write_operation_invalid_mode(self):
        """Test time_write_operation with invalid mode."""
        mock_df = MagicMock()

        with pytest.raises(ValueError):
            time_write_operation("invalid", mock_df, "test.table")

    @patch("sparkforge.table_operations.write_overwrite_table")
    def test_time_write_operation_with_options(self, mock_write):
        """Test time_write_operation with additional options."""
        mock_write.return_value = 100
        mock_df = MagicMock()

        result = time_write_operation(
            "overwrite", mock_df, "test.table", compression="snappy"
        )

        assert result[0] == 100
        mock_write.assert_called_once_with(mock_df, "test.table", compression="snappy")

    @patch("sparkforge.table_operations.write_overwrite_table")
    def test_time_write_operation_exception(self, mock_write):
        """Test time_write_operation with exception."""
        mock_write.side_effect = Exception("Write failed")
        mock_df = MagicMock()

        with pytest.raises(Exception, match="Write failed"):
            time_write_operation("overwrite", mock_df, "test.table")


class TestMonitorPerformance:
    """Test monitor_performance decorator factory."""

    def test_monitor_performance_success(self, caplog):
        """Test monitor_performance with successful function."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        @monitor_performance("test operation")
        def test_func():
            return "success"

        result = test_func()
        assert result == "success"
        assert "Starting test operation" in caplog.text
        assert "Completed test operation" in caplog.text

    def test_monitor_performance_with_max_duration(self, caplog):
        """Test monitor_performance with max duration."""

        @monitor_performance("test operation", max_duration=0.001)
        def test_func():
            time.sleep(0.01)
            return "success"

        result = test_func()
        assert result == "success"
        assert "exceeding threshold" in caplog.text

    def test_monitor_performance_exception(self, caplog):
        """Test monitor_performance with exception."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        @monitor_performance("test operation")
        def test_func():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            test_func()

        assert "Starting test operation" in caplog.text
        assert "Failed test operation" in caplog.text


class TestPerformanceIntegration:
    """Integration tests for performance monitoring."""

    def test_multiple_operations_timing(self):
        """Test timing multiple operations."""

        @time_operation("operation 1")
        def op1():
            time.sleep(0.01)
            return 1

        @time_operation("operation 2")
        def op2():
            time.sleep(0.01)
            return 2

        result1 = op1()
        result2 = op2()

        assert result1 == 1
        assert result2 == 2

    def test_nested_performance_monitors(self, caplog):
        """Test nested performance monitors."""
        import logging

        logging.getLogger("sparkforge.performance").setLevel(logging.INFO)

        with performance_monitor("outer operation"):
            with performance_monitor("inner operation"):
                time.sleep(0.01)

        assert "Starting outer operation" in caplog.text
        assert "Starting inner operation" in caplog.text
        assert "Completed inner operation" in caplog.text
        assert "Completed outer operation" in caplog.text
