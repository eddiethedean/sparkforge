"""
Tests for the new simplified LogWriter API.

This module tests the new LogWriter API that works with PipelineReport
objects and simplified initialization.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from sparkforge.writer import LogWriter, WriterConfig, WriteMode
from sparkforge.pipeline.models import PipelineReport, PipelineMode, PipelineStatus
from sparkforge.models import PipelineMetrics


class TestLogWriterSimplifiedInit:
    """Test simplified LogWriter initialization."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    def test_init_with_schema_and_table_name(self, mock_spark):
        """Test initialization with schema and table_name (new API)."""
        writer = LogWriter(mock_spark, schema="analytics", table_name="pipeline_logs")
        
        assert writer.spark == mock_spark
        assert writer.config.table_schema == "analytics"
        assert writer.config.table_name == "pipeline_logs"
        assert writer.config.write_mode == WriteMode.APPEND
        assert writer.table_fqn == "analytics.pipeline_logs"

    def test_init_with_config_shows_deprecation_warning(self, mock_spark):
        """Test that using WriterConfig shows deprecation warning."""
        config = WriterConfig(table_schema="analytics", table_name="logs")
        
        with pytest.warns(DeprecationWarning, match="WriterConfig is deprecated"):
            writer = LogWriter(mock_spark, config=config)
        
        assert writer.config == config

    def test_init_without_required_params_raises_error(self, mock_spark):
        """Test that initialization without required params raises error."""
        with pytest.raises(Exception) as exc_info:
            LogWriter(mock_spark)
        
        assert "schema and table_name" in str(exc_info.value).lower()

    def test_init_with_only_schema_raises_error(self, mock_spark):
        """Test that providing only schema raises error."""
        with pytest.raises(Exception):
            LogWriter(mock_spark, schema="analytics")

    def test_init_with_only_table_name_raises_error(self, mock_spark):
        """Test that providing only table_name raises error."""
        with pytest.raises(Exception):
            LogWriter(mock_spark, table_name="logs")


class TestConvertReportToLogRows:
    """Test _convert_report_to_log_rows method."""

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

    @pytest.fixture
    def sample_report(self):
        """Create a sample PipelineReport."""
        return PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="exec_123",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 5, 0),
            duration_seconds=300.0,
            metrics=PipelineMetrics(
                total_steps=5,
                successful_steps=5,
                failed_steps=0,
                total_duration=300.0,
                bronze_duration=100.0,
                silver_duration=120.0,
                gold_duration=80.0,
                total_rows_processed=10000,
                total_rows_written=9500,
                avg_validation_rate=98.5,
                parallel_efficiency=85.0,
            ),
            errors=[],
            warnings=["Minor warning"],
            recommendations=["Consider optimization"],
        )

    def test_convert_report_creates_log_row(self, writer, sample_report):
        """Test that converting a report creates a valid log row."""
        log_rows = writer._convert_report_to_log_rows(sample_report, run_id="run_123")
        
        assert len(log_rows) == 1
        log_row = log_rows[0]
        
        # Verify run-level information
        assert log_row["run_id"] == "run_123"
        assert log_row["run_mode"] == "initial"
        assert log_row["run_started_at"] == sample_report.start_time
        assert log_row["run_ended_at"] == sample_report.end_time
        
        # Verify execution context
        assert log_row["execution_id"] == "exec_123"
        assert log_row["pipeline_id"] == "test_pipeline"
        assert log_row["schema"] == "test"
        
        # Verify step-level (summary)
        assert log_row["phase"] == "pipeline"
        assert log_row["step_name"] == "pipeline_summary"
        
        # Verify timing
        assert log_row["duration_secs"] == 300.0
        
        # Verify metrics
        assert log_row["rows_processed"] == 10000
        assert log_row["rows_written"] == 9500
        assert log_row["validation_rate"] == 98.5
        
        # Verify status
        assert log_row["success"] is True
        assert log_row["error_message"] is None

    def test_convert_report_with_errors(self, writer):
        """Test converting a report with errors."""
        report = PipelineReport(
            pipeline_id="test",
            execution_id="exec_456",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.FAILED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 1, 0),
            duration_seconds=60.0,
            metrics=PipelineMetrics(
                total_steps=3,
                successful_steps=2,
                failed_steps=1,
                total_duration=60.0,
            ),
            errors=["Step failed", "Connection timeout"],
        )
        
        log_rows = writer._convert_report_to_log_rows(report)
        log_row = log_rows[0]
        
        assert log_row["success"] is False
        assert log_row["error_message"] == "Step failed, Connection timeout"

    def test_convert_report_generates_run_id_if_not_provided(self, writer, sample_report):
        """Test that run_id is generated if not provided."""
        log_rows = writer._convert_report_to_log_rows(sample_report)
        log_row = log_rows[0]
        
        assert "run_id" in log_row
        assert len(log_row["run_id"]) > 0  # UUID should be generated

    def test_convert_report_includes_metadata(self, writer, sample_report):
        """Test that metadata includes all necessary fields."""
        log_rows = writer._convert_report_to_log_rows(sample_report)
        log_row = log_rows[0]
        metadata = log_row["metadata"]
        
        assert metadata["total_steps"] == 5
        assert metadata["successful_steps"] == 5
        assert metadata["failed_steps"] == 0
        assert metadata["bronze_duration"] == 100.0
        assert metadata["silver_duration"] == 120.0
        assert metadata["gold_duration"] == 80.0
        assert metadata["parallel_efficiency"] == 85.0
        assert metadata["warnings"] == ["Minor warning"]
        assert metadata["recommendations"] == ["Consider optimization"]


class TestCreateTableMethod:
    """Test create_table method."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    @pytest.fixture
    def writer(self, mock_spark):
        """Create a LogWriter with mocked dependencies."""
        with patch('sparkforge.writer.core.StorageManager') as MockStorage:
            with patch('sparkforge.writer.core.PerformanceMonitor') as MockPerf:
                with patch('sparkforge.writer.core.DataProcessor'):
                    with patch('sparkforge.writer.core.AnalyticsEngine'):
                        with patch('sparkforge.writer.core.DataQualityAnalyzer'):
                            with patch('sparkforge.writer.core.TrendAnalyzer'):
                                writer = LogWriter(mock_spark, schema="test", table_name="logs")
                                
                                # Setup mock storage manager
                                mock_storage = MockStorage.return_value
                                mock_storage.write_batch.return_value = {
                                    "rows_written": 1,
                                    "success": True
                                }
                                writer.storage_manager = mock_storage
                                
                                # Setup mock performance monitor
                                mock_perf = MockPerf.return_value
                                mock_perf.end_operation.return_value = {
                                    "duration_secs": 1.5
                                }
                                writer.performance_monitor = mock_perf
                                
                                return writer

    @pytest.fixture
    def sample_report(self):
        """Create a sample report."""
        return PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="exec_789",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 10, 0, 0),
            end_time=datetime(2024, 1, 15, 10, 5, 0),
            duration_seconds=300.0,
            metrics=PipelineMetrics(
                total_steps=3,
                successful_steps=3,
                failed_steps=0,
                total_duration=300.0,
                total_rows_processed=5000,
                total_rows_written=4800,
            ),
        )

    def test_create_table_calls_storage_with_overwrite(self, writer, sample_report):
        """Test that create_table uses OVERWRITE mode."""
        result = writer.create_table(sample_report, run_id="run_456")
        
        # Verify write_batch was called with OVERWRITE mode
        writer.storage_manager.write_batch.assert_called_once()
        call_args = writer.storage_manager.write_batch.call_args
        assert call_args[0][1] == WriteMode.OVERWRITE  # Second argument is write mode

    def test_create_table_returns_success_result(self, writer, sample_report):
        """Test that create_table returns successful result."""
        result = writer.create_table(sample_report, run_id="run_789")
        
        assert result["success"] is True
        assert result["run_id"] == "run_789"
        assert result["rows_written"] == 1
        assert result["table_fqn"] == "test.logs"

    def test_create_table_generates_run_id_if_not_provided(self, writer, sample_report):
        """Test that run_id is generated if not provided."""
        result = writer.create_table(sample_report)
        
        assert "run_id" in result
        assert len(result["run_id"]) > 0


class TestAppendMethod:
    """Test append method."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    @pytest.fixture
    def writer(self, mock_spark):
        """Create a LogWriter with mocked dependencies."""
        with patch('sparkforge.writer.core.StorageManager') as MockStorage:
            with patch('sparkforge.writer.core.PerformanceMonitor') as MockPerf:
                with patch('sparkforge.writer.core.DataProcessor'):
                    with patch('sparkforge.writer.core.AnalyticsEngine'):
                        with patch('sparkforge.writer.core.DataQualityAnalyzer'):
                            with patch('sparkforge.writer.core.TrendAnalyzer'):
                                writer = LogWriter(mock_spark, schema="test", table_name="logs")
                                
                                # Setup mock storage manager
                                mock_storage = MockStorage.return_value
                                mock_storage.write_batch.return_value = {
                                    "rows_written": 1,
                                    "success": True
                                }
                                writer.storage_manager = mock_storage
                                
                                # Setup mock performance monitor
                                mock_perf = MockPerf.return_value
                                mock_perf.end_operation.return_value = {
                                    "duration_secs": 1.0
                                }
                                writer.performance_monitor = mock_perf
                                
                                return writer

    @pytest.fixture
    def sample_report(self):
        """Create a sample report."""
        return PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="exec_999",
            mode=PipelineMode.INCREMENTAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime(2024, 1, 15, 11, 0, 0),
            end_time=datetime(2024, 1, 15, 11, 2, 0),
            duration_seconds=120.0,
            metrics=PipelineMetrics(
                total_steps=2,
                successful_steps=2,
                failed_steps=0,
                total_duration=120.0,
                total_rows_processed=1000,
                total_rows_written=950,
            ),
        )

    def test_append_calls_storage_with_append_mode(self, writer, sample_report):
        """Test that append uses APPEND mode."""
        result = writer.append(sample_report, run_id="run_append_1")
        
        # Verify write_batch was called with APPEND mode
        writer.storage_manager.write_batch.assert_called_once()
        call_args = writer.storage_manager.write_batch.call_args
        assert call_args[0][1] == WriteMode.APPEND  # Second argument is write mode

    def test_append_returns_success_result(self, writer, sample_report):
        """Test that append returns successful result."""
        result = writer.append(sample_report, run_id="run_append_2")
        
        assert result["success"] is True
        assert result["run_id"] == "run_append_2"
        assert result["rows_written"] == 1
        assert result["table_fqn"] == "test.logs"

    def test_append_multiple_times(self, writer, sample_report):
        """Test appending multiple reports."""
        result1 = writer.append(sample_report, run_id="run_1")
        result2 = writer.append(sample_report, run_id="run_2")
        result3 = writer.append(sample_report, run_id="run_3")
        
        assert result1["success"] is True
        assert result2["success"] is True
        assert result3["success"] is True
        
        # Verify write_batch was called 3 times
        assert writer.storage_manager.write_batch.call_count == 3


class TestLogWriterNewAPIIntegration:
    """Integration tests for the new API."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    def test_complete_workflow_create_and_append(self, mock_spark):
        """Test complete workflow: create table, then append."""
        with patch('sparkforge.writer.core.StorageManager') as MockStorage:
            with patch('sparkforge.writer.core.PerformanceMonitor') as MockPerf:
                with patch('sparkforge.writer.core.DataProcessor'):
                    with patch('sparkforge.writer.core.AnalyticsEngine'):
                        with patch('sparkforge.writer.core.DataQualityAnalyzer'):
                            with patch('sparkforge.writer.core.TrendAnalyzer'):
                                # Initialize writer with simple API
                                writer = LogWriter(mock_spark, schema="analytics", table_name="pipeline_logs")
                                
                                # Setup mocks
                                mock_storage = MockStorage.return_value
                                mock_storage.write_batch.return_value = {"rows_written": 1}
                                writer.storage_manager = mock_storage
                                
                                mock_perf = MockPerf.return_value
                                mock_perf.end_operation.return_value = {"duration_secs": 1.0}
                                writer.performance_monitor = mock_perf
                                
                                # Create initial report
                                report1 = PipelineReport(
                                    pipeline_id="pipeline1",
                                    execution_id="exec1",
                                    mode=PipelineMode.INITIAL,
                                    status=PipelineStatus.COMPLETED,
                                    start_time=datetime.now(),
                                    metrics=PipelineMetrics(total_steps=3),
                                )
                                
                                # Create table with first report
                                result1 = writer.create_table(report1)
                                assert result1["success"] is True
                                
                                # Append second report
                                report2 = PipelineReport(
                                    pipeline_id="pipeline1",
                                    execution_id="exec2",
                                    mode=PipelineMode.INCREMENTAL,
                                    status=PipelineStatus.COMPLETED,
                                    start_time=datetime.now(),
                                    metrics=PipelineMetrics(total_steps=2),
                                )
                                
                                result2 = writer.append(report2)
                                assert result2["success"] is True
                                
                                # Verify write_batch was called twice
                                assert writer.storage_manager.write_batch.call_count == 2

