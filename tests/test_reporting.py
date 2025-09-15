# test_reporting.py
"""
Unit tests for the reporting module.

This module tests all report generation and statistics functions.
"""

from datetime import datetime

from sparkforge.models import StageStats
from sparkforge.reporting import (
    create_summary_report,
    create_transform_dict,
    create_validation_dict,
    create_write_dict,
)


class TestCreateValidationDict:
    """Test create_validation_dict function."""

    def test_with_valid_stats(self):
        """Test with valid StageStats object."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=300.0,
        )

        result = create_validation_dict(stats, start_at=start_at, end_at=end_at)

        assert result["stage"] == "bronze"
        assert result["step"] == "test_step"
        assert result["total_rows"] == 1000
        assert result["valid_rows"] == 950
        assert result["invalid_rows"] == 50
        assert result["validation_rate"] == 95.0
        assert result["duration_secs"] == 300.0
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_with_none_stats(self):
        """Test with None stats object."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_validation_dict(None, start_at=start_at, end_at=end_at)

        assert result["stage"] is None
        assert result["step"] is None
        assert result["total_rows"] == 0
        assert result["valid_rows"] == 0
        assert result["invalid_rows"] == 0
        assert result["validation_rate"] == 100.0
        assert result["duration_secs"] == 0.0
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_rounding_precision(self):
        """Test that values are rounded to correct precision."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.123456789,  # Should be rounded to 2 decimal places
            duration_secs=300.123456789,  # Should be rounded to 3 decimal places
        )

        result = create_validation_dict(stats, start_at=start_at, end_at=end_at)

        assert result["validation_rate"] == 95.12
        assert result["duration_secs"] == 300.123


class TestCreateTransformDict:
    """Test create_transform_dict function."""

    def test_basic_transform_dict(self):
        """Test basic transform dictionary creation."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000,
            output_rows=950,
            duration_secs=300.123456789,
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["input_rows"] == 1000
        assert result["output_rows"] == 950
        assert result["duration_secs"] == 300.123  # Rounded to 3 decimal places
        assert result["skipped"] is False
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_skipped_transform(self):
        """Test transform dictionary with skipped operation."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000,
            output_rows=0,
            duration_secs=0.0,
            skipped=True,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["input_rows"] == 1000
        assert result["output_rows"] == 0
        assert result["duration_secs"] == 0.0
        assert result["skipped"] is True

    def test_type_conversion(self):
        """Test that values are converted to correct types."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000.7,  # Should be converted to int
            output_rows=950.3,  # Should be converted to int
            duration_secs=300.123456789,
            skipped=1,  # Should be converted to bool
            start_at=start_at,
            end_at=end_at,
        )

        assert isinstance(result["input_rows"], int)
        assert isinstance(result["output_rows"], int)
        assert isinstance(result["skipped"], bool)
        assert result["input_rows"] == 1000
        assert result["output_rows"] == 950
        assert result["skipped"] is True


class TestCreateWriteDict:
    """Test create_write_dict function."""

    def test_basic_write_dict(self):
        """Test basic write dictionary creation."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="overwrite",
            rows=1000,
            duration_secs=300.123456789,
            table_fqn="test_schema.test_table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["mode"] == "overwrite"
        assert result["rows_written"] == 1000
        assert result["duration_secs"] == 300.123  # Rounded to 3 decimal places
        assert result["table_fqn"] == "test_schema.test_table"
        assert result["skipped"] is False
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_append_mode(self):
        """Test write dictionary with append mode."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="append",
            rows=500,
            duration_secs=150.0,
            table_fqn="test_schema.test_table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["mode"] == "append"
        assert result["rows_written"] == 500
        assert result["duration_secs"] == 150.0

    def test_skipped_write(self):
        """Test write dictionary with skipped operation."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="overwrite",
            rows=0,
            duration_secs=0.0,
            table_fqn="test_schema.test_table",
            skipped=True,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["rows_written"] == 0
        assert result["duration_secs"] == 0.0
        assert result["skipped"] is True

    def test_type_conversion(self):
        """Test that values are converted to correct types."""
        start_at = datetime(2023, 1, 1, 10, 0, 0)
        end_at = datetime(2023, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="overwrite",
            rows=1000.7,  # Should be converted to int
            duration_secs=300.123456789,
            table_fqn="test_schema.test_table",
            skipped=1,  # Should be converted to bool
            start_at=start_at,
            end_at=end_at,
        )

        assert isinstance(result["rows_written"], int)
        assert isinstance(result["skipped"], bool)
        assert result["rows_written"] == 1000
        assert result["skipped"] is True


class TestCreateSummaryReport:
    """Test create_summary_report function."""

    def test_basic_summary_report(self):
        """Test basic summary report creation."""
        result = create_summary_report(
            total_steps=10,
            successful_steps=8,
            failed_steps=2,
            total_duration=3600.0,
            total_rows_processed=100000,
            total_rows_written=95000,
            avg_validation_rate=95.5,
        )

        # Check execution summary
        exec_summary = result["execution_summary"]
        assert exec_summary["total_steps"] == 10
        assert exec_summary["successful_steps"] == 8
        assert exec_summary["failed_steps"] == 2
        assert exec_summary["success_rate"] == 80.0
        assert exec_summary["failure_rate"] == 20.0

        # Check performance metrics
        perf_metrics = result["performance_metrics"]
        assert perf_metrics["total_duration_secs"] == 3600.0
        assert perf_metrics["formatted_duration"] == "1.00h"
        assert perf_metrics["avg_validation_rate"] == 95.5

        # Check data metrics
        data_metrics = result["data_metrics"]
        assert data_metrics["total_rows_processed"] == 100000
        assert data_metrics["total_rows_written"] == 95000
        assert data_metrics["processing_efficiency"] == 95.0

    def test_perfect_success_rate(self):
        """Test summary report with 100% success rate."""
        result = create_summary_report(
            total_steps=5,
            successful_steps=5,
            failed_steps=0,
            total_duration=1800.0,
            total_rows_processed=50000,
            total_rows_written=50000,
            avg_validation_rate=100.0,
        )

        exec_summary = result["execution_summary"]
        assert exec_summary["success_rate"] == 100.0
        assert exec_summary["failure_rate"] == 0.0

        data_metrics = result["data_metrics"]
        assert data_metrics["processing_efficiency"] == 100.0

    def test_complete_failure(self):
        """Test summary report with complete failure."""
        result = create_summary_report(
            total_steps=5,
            successful_steps=0,
            failed_steps=5,
            total_duration=300.0,
            total_rows_processed=10000,
            total_rows_written=0,
            avg_validation_rate=0.0,
        )

        exec_summary = result["execution_summary"]
        assert exec_summary["success_rate"] == 0.0
        assert exec_summary["failure_rate"] == 100.0

        data_metrics = result["data_metrics"]
        assert data_metrics["processing_efficiency"] == 0.0

    def test_rounding_precision(self):
        """Test that values are rounded to correct precision."""
        result = create_summary_report(
            total_steps=3,
            successful_steps=2,
            failed_steps=1,
            total_duration=123.456789,
            total_rows_processed=1000,
            total_rows_written=950,
            avg_validation_rate=95.123456789,
        )

        exec_summary = result["execution_summary"]
        assert exec_summary["success_rate"] == 66.67  # Rounded to 2 decimal places
        assert exec_summary["failure_rate"] == 33.33

        perf_metrics = result["performance_metrics"]
        assert (
            perf_metrics["total_duration_secs"] == 123.457
        )  # Rounded to 3 decimal places
        assert (
            perf_metrics["avg_validation_rate"] == 95.12
        )  # Rounded to 2 decimal places

        data_metrics = result["data_metrics"]
        assert (
            data_metrics["processing_efficiency"] == 95.0
        )  # Rounded to 2 decimal places

    def test_zero_division_handling(self):
        """Test handling of zero division scenarios."""
        result = create_summary_report(
            total_steps=0,
            successful_steps=0,
            failed_steps=0,
            total_duration=0.0,
            total_rows_processed=0,
            total_rows_written=0,
            avg_validation_rate=0.0,
        )

        exec_summary = result["execution_summary"]
        assert exec_summary["success_rate"] == 0.0
        assert exec_summary["failure_rate"] == 0.0

        data_metrics = result["data_metrics"]
        assert data_metrics["processing_efficiency"] == 0.0

    def test_duration_formatting(self):
        """Test duration formatting in summary report."""
        # Test seconds
        result1 = create_summary_report(
            total_steps=1,
            successful_steps=1,
            failed_steps=0,
            total_duration=45.5,
            total_rows_processed=1000,
            total_rows_written=1000,
            avg_validation_rate=100.0,
        )
        assert result1["performance_metrics"]["formatted_duration"] == "45.50s"

        # Test minutes
        result2 = create_summary_report(
            total_steps=1,
            successful_steps=1,
            failed_steps=0,
            total_duration=125.0,
            total_rows_processed=1000,
            total_rows_written=1000,
            avg_validation_rate=100.0,
        )
        assert result2["performance_metrics"]["formatted_duration"] == "2.08m"

        # Test hours
        result3 = create_summary_report(
            total_steps=1,
            successful_steps=1,
            failed_steps=0,
            total_duration=7200.0,
            total_rows_processed=1000,
            total_rows_written=1000,
            avg_validation_rate=100.0,
        )
        assert result3["performance_metrics"]["formatted_duration"] == "2.00h"
