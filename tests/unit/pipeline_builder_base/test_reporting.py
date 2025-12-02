"""
Comprehensive tests for pipeline_builder_base reporting module.

Tests report classes and utilities.
"""

from datetime import datetime


from pipeline_builder_base.models import StageStats
from pipeline_builder_base.reporting import (
    DataMetrics,
    ExecutionSummary,
    PerformanceMetrics,
    SummaryReport,
    TransformReport,
    ValidationReport,
    WriteReport,
    create_validation_dict,
)


class TestReportClasses:
    """Test report TypedDict classes."""

    def test_validation_report_creation(self):
        """Test ValidationReport creation."""
        report: ValidationReport = {
            "stage": "bronze",
            "step": "step1",
            "total_rows": 1000,
            "valid_rows": 950,
            "invalid_rows": 50,
            "validation_rate": 95.0,
            "duration_secs": 10.5,
            "start_at": datetime.now(),
            "end_at": datetime.now(),
        }

        assert report["stage"] == "bronze"
        assert report["total_rows"] == 1000
        assert report["validation_rate"] == 95.0

    def test_validation_report_to_dict(self):
        """Test report serialization."""
        report: ValidationReport = {
            "stage": "silver",
            "step": "step2",
            "total_rows": 500,
            "valid_rows": 490,
            "invalid_rows": 10,
            "validation_rate": 98.0,
            "duration_secs": 5.0,
            "start_at": datetime.now(),
            "end_at": datetime.now(),
        }

        assert isinstance(report, dict)
        assert report["validation_rate"] == 98.0

    def test_transform_report_creation(self):
        """Test TransformReport creation."""
        report: TransformReport = {
            "input_rows": 1000,
            "output_rows": 950,
            "duration_secs": 5.0,
            "skipped": False,
            "start_at": datetime.now(),
            "end_at": datetime.now(),
        }

        assert report["input_rows"] == 1000
        assert report["output_rows"] == 950

    def test_write_report_creation(self):
        """Test WriteReport creation."""
        report: WriteReport = {
            "mode": "append",
            "rows_written": 950,
            "duration_secs": 2.0,
            "table_fqn": "schema.table",
            "skipped": False,
            "start_at": datetime.now(),
            "end_at": datetime.now(),
        }

        assert report["mode"] == "append"
        assert report["rows_written"] == 950

    def test_execution_summary_creation(self):
        """Test ExecutionSummary creation."""
        summary: ExecutionSummary = {
            "total_steps": 10,
            "successful_steps": 9,
            "failed_steps": 1,
            "success_rate": 90.0,
            "failure_rate": 10.0,
        }

        assert summary["total_steps"] == 10
        assert summary["success_rate"] == 90.0

    def test_performance_metrics_creation(self):
        """Test PerformanceMetrics creation."""
        metrics: PerformanceMetrics = {
            "total_duration_secs": 100.5,
            "formatted_duration": "1m 40s",
            "avg_validation_rate": 95.0,
        }

        assert metrics["total_duration_secs"] == 100.5
        assert metrics["avg_validation_rate"] == 95.0

    def test_data_metrics_creation(self):
        """Test DataMetrics creation."""
        metrics: DataMetrics = {
            "total_rows_processed": 10000,
            "total_rows_written": 9500,
            "processing_efficiency": 95.0,
        }

        assert metrics["total_rows_processed"] == 10000
        assert metrics["processing_efficiency"] == 95.0

    def test_summary_report_creation(self):
        """Test SummaryReport creation."""
        summary: SummaryReport = {
            "execution_summary": {
                "total_steps": 10,
                "successful_steps": 9,
                "failed_steps": 1,
                "success_rate": 90.0,
                "failure_rate": 10.0,
            },
            "performance_metrics": {
                "total_duration_secs": 100.5,
                "formatted_duration": "1m 40s",
                "avg_validation_rate": 95.0,
            },
            "data_metrics": {
                "total_rows_processed": 10000,
                "total_rows_written": 9500,
                "processing_efficiency": 95.0,
            },
        }

        assert summary["execution_summary"]["total_steps"] == 10
        assert summary["performance_metrics"]["total_duration_secs"] == 100.5


class TestReportUtilities:
    """Test report utility functions."""

    def test_create_validation_dict(self):
        """Test validation dict creation."""

        stats = StageStats(
            stage="bronze",
            step="step1",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=10.0,
        )

        start_at = datetime.now()
        end_at = datetime.now()

        report = create_validation_dict(stats, start_at=start_at, end_at=end_at)

        assert report["total_rows"] == 1000
        assert report["valid_rows"] == 950
        assert report["validation_rate"] == 95.0

    def test_create_validation_dict_none_stats(self):
        """Test validation dict with None stats."""
        start_at = datetime.now()
        end_at = datetime.now()

        report = create_validation_dict(None, start_at=start_at, end_at=end_at)

        assert report["total_rows"] == 0
        assert report["valid_rows"] == 0
