"""
Tests for sparkforge.reporting module.

This module tests all reporting utilities and functions.
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

    def test_create_validation_dict_with_stats(self):
        """Test creating validation dict with stats."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        stats = StageStats(
            stage="bronze",
            step="validation",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=300.0,
            start_time=start_at,
            end_time=end_at,
        )

        result = create_validation_dict(stats, start_at=start_at, end_at=end_at)

        assert result["stage"] == "bronze"
        assert result["step"] == "validation"
        assert result["total_rows"] == 1000
        assert result["valid_rows"] == 950
        assert result["invalid_rows"] == 50
        assert result["validation_rate"] == 95.0
        assert result["duration_secs"] == 300.0
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_create_validation_dict_without_stats(self):
        """Test creating validation dict without stats."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

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

    def test_create_validation_dict_rounding(self):
        """Test that validation dict properly rounds values."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        stats = StageStats(
            stage="bronze",
            step="validation",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.123456789,  # Should be rounded to 2 decimal places
            duration_secs=300.123456789,  # Should be rounded to 3 decimal places
            start_time=start_at,
            end_time=end_at,
        )

        result = create_validation_dict(stats, start_at=start_at, end_at=end_at)

        assert result["validation_rate"] == 95.12
        assert result["duration_secs"] == 300.123


class TestCreateTransformDict:
    """Test create_transform_dict function."""

    def test_create_transform_dict_basic(self):
        """Test creating transform dict with basic values."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000,
            output_rows=950,
            duration_secs=300.0,
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["input_rows"] == 1000
        assert result["output_rows"] == 950
        assert result["duration_secs"] == 300.0
        assert result["skipped"] is False
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_create_transform_dict_skipped(self):
        """Test creating transform dict with skipped operation."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000,
            output_rows=1000,
            duration_secs=0.0,
            skipped=True,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["input_rows"] == 1000
        assert result["output_rows"] == 1000
        assert result["duration_secs"] == 0.0
        assert result["skipped"] is True
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_create_transform_dict_rounding(self):
        """Test that transform dict properly rounds duration."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000,
            output_rows=950,
            duration_secs=300.123456789,  # Should be rounded to 3 decimal places
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["duration_secs"] == 300.123

    def test_create_transform_dict_type_conversion(self):
        """Test that transform dict converts types properly."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_transform_dict(
            input_rows=1000.5,  # Should be converted to int
            output_rows=950.7,  # Should be converted to int
            duration_secs=300.0,
            skipped=1,  # Should be converted to bool
            start_at=start_at,
            end_at=end_at,
        )

        assert result["input_rows"] == 1000
        assert result["output_rows"] == 950
        assert result["skipped"] is True


class TestCreateWriteDict:
    """Test create_write_dict function."""

    def test_create_write_dict_basic(self):
        """Test creating write dict with basic values."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="append",
            rows=1000,
            duration_secs=300.0,
            table_fqn="test_schema.test_table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["mode"] == "append"
        assert result["rows_written"] == 1000
        assert result["duration_secs"] == 300.0
        assert result["table_fqn"] == "test_schema.test_table"
        assert result["skipped"] is False
        assert result["start_at"] == start_at
        assert result["end_at"] == end_at

    def test_create_write_dict_skipped(self):
        """Test creating write dict with skipped operation."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="overwrite",
            rows=0,
            duration_secs=0.0,
            table_fqn="test_schema.test_table",
            skipped=True,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["mode"] == "overwrite"
        assert result["rows_written"] == 0
        assert result["duration_secs"] == 0.0
        assert result["skipped"] is True

    def test_create_write_dict_rounding(self):
        """Test that write dict properly rounds duration."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="append",
            rows=1000,
            duration_secs=300.123456789,  # Should be rounded to 3 decimal places
            table_fqn="test_schema.test_table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )

        assert result["duration_secs"] == 300.123

    def test_create_write_dict_type_conversion(self):
        """Test that write dict converts types properly."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="append",
            rows=1000.5,  # Should be converted to int
            duration_secs=300.0,
            table_fqn="test_schema.test_table",
            skipped=1,  # Should be converted to bool
            start_at=start_at,
            end_at=end_at,
        )

        assert result["rows_written"] == 1000
        assert result["skipped"] is True


class TestCreateSummaryReport:
    """Test create_summary_report function."""

    def test_create_summary_report_basic(self):
        """Test creating summary report with basic values."""
        result = create_summary_report(
            total_steps=10,
            successful_steps=8,
            failed_steps=2,
            total_duration=600.0,
            total_rows_processed=10000,
            total_rows_written=9500,
            avg_validation_rate=95.0,
        )

        # Check execution summary
        assert result["execution_summary"]["total_steps"] == 10
        assert result["execution_summary"]["successful_steps"] == 8
        assert result["execution_summary"]["failed_steps"] == 2
        assert result["execution_summary"]["success_rate"] == 80.0
        assert result["execution_summary"]["failure_rate"] == 20.0

        # Check performance metrics
        assert result["performance_metrics"]["total_duration_secs"] == 600.0
        assert result["performance_metrics"]["avg_validation_rate"] == 95.0

        # Check data metrics
        assert result["data_metrics"]["total_rows_processed"] == 10000
        assert result["data_metrics"]["total_rows_written"] == 9500
        assert result["data_metrics"]["processing_efficiency"] == 95.0

    def test_create_summary_report_zero_steps(self):
        """Test creating summary report with zero steps."""
        result = create_summary_report(
            total_steps=0,
            successful_steps=0,
            failed_steps=0,
            total_duration=0.0,
            total_rows_processed=0,
            total_rows_written=0,
            avg_validation_rate=0.0,
        )

        # Check execution summary
        assert result["execution_summary"]["total_steps"] == 0
        assert result["execution_summary"]["successful_steps"] == 0
        assert result["execution_summary"]["failed_steps"] == 0
        assert result["execution_summary"]["success_rate"] == 0.0
        assert result["execution_summary"]["failure_rate"] == 0.0

        # Check performance metrics
        assert result["performance_metrics"]["total_duration_secs"] == 0.0
        assert result["performance_metrics"]["avg_validation_rate"] == 0.0

        # Check data metrics
        assert result["data_metrics"]["total_rows_processed"] == 0
        assert result["data_metrics"]["total_rows_written"] == 0
        assert result["data_metrics"]["processing_efficiency"] == 0.0

    def test_create_summary_report_perfect_success(self):
        """Test creating summary report with perfect success."""
        result = create_summary_report(
            total_steps=5,
            successful_steps=5,
            failed_steps=0,
            total_duration=300.0,
            total_rows_processed=5000,
            total_rows_written=5000,
            avg_validation_rate=100.0,
        )

        # Check execution summary
        assert result["execution_summary"]["success_rate"] == 100.0
        assert result["execution_summary"]["failure_rate"] == 0.0

        # Check data metrics
        assert result["data_metrics"]["processing_efficiency"] == 100.0

    def test_create_summary_report_rounding(self):
        """Test that summary report properly rounds values."""
        result = create_summary_report(
            total_steps=3,
            successful_steps=2,
            failed_steps=1,
            total_duration=300.123456789,  # Should be rounded to 3 decimal places
            total_rows_processed=1000,
            total_rows_written=950,
            avg_validation_rate=95.123456789,  # Should be rounded to 2 decimal places
        )

        # Check execution summary rounding
        assert result["execution_summary"]["success_rate"] == 66.67
        assert result["execution_summary"]["failure_rate"] == 33.33

        # Check performance metrics rounding
        assert result["performance_metrics"]["total_duration_secs"] == 300.123
        assert result["performance_metrics"]["avg_validation_rate"] == 95.12

        # Check data metrics rounding
        assert result["data_metrics"]["processing_efficiency"] == 95.0

    def test_create_summary_report_zero_division_handling(self):
        """Test that summary report handles zero division properly."""
        result = create_summary_report(
            total_steps=5,
            successful_steps=3,
            failed_steps=2,
            total_duration=300.0,
            total_rows_processed=0,  # This will cause zero division in processing_efficiency
            total_rows_written=1000,
            avg_validation_rate=95.0,
        )

        # Check that processing efficiency defaults to 0.0 when total_rows_processed is 0
        assert result["data_metrics"]["processing_efficiency"] == 0.0


class TestReportingIntegration:
    """Test reporting functions integration."""

    def test_all_functions_return_dicts(self):
        """Test that all reporting functions return dictionaries."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        # Test create_validation_dict
        validation_dict = create_validation_dict(None, start_at=start_at, end_at=end_at)
        assert isinstance(validation_dict, dict)

        # Test create_transform_dict
        transform_dict = create_transform_dict(
            input_rows=1000,
            output_rows=950,
            duration_secs=300.0,
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )
        assert isinstance(transform_dict, dict)

        # Test create_write_dict
        write_dict = create_write_dict(
            mode="append",
            rows=1000,
            duration_secs=300.0,
            table_fqn="test.table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )
        assert isinstance(write_dict, dict)

        # Test create_summary_report
        summary_dict = create_summary_report(
            total_steps=5,
            successful_steps=4,
            failed_steps=1,
            total_duration=300.0,
            total_rows_processed=1000,
            total_rows_written=950,
            avg_validation_rate=95.0,
        )
        assert isinstance(summary_dict, dict)

    def test_consistent_datetime_handling(self):
        """Test that all functions handle datetime objects consistently."""
        start_at = datetime(2024, 1, 1, 10, 0, 0)
        end_at = datetime(2024, 1, 1, 10, 5, 0)

        # All functions should preserve datetime objects
        validation_dict = create_validation_dict(None, start_at=start_at, end_at=end_at)
        assert validation_dict["start_at"] == start_at
        assert validation_dict["end_at"] == end_at

        transform_dict = create_transform_dict(
            input_rows=1000,
            output_rows=950,
            duration_secs=300.0,
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )
        assert transform_dict["start_at"] == start_at
        assert transform_dict["end_at"] == end_at

        write_dict = create_write_dict(
            mode="append",
            rows=1000,
            duration_secs=300.0,
            table_fqn="test.table",
            skipped=False,
            start_at=start_at,
            end_at=end_at,
        )
        assert write_dict["start_at"] == start_at
        assert write_dict["end_at"] == end_at
