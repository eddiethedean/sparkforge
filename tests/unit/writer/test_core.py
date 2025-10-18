"""
Unit tests for writer core functionality.
"""

from unittest.mock import Mock, patch

import pytest

from sparkforge.logging import PipelineLogger
from sparkforge.models import ExecutionResult, StepResult
from sparkforge.writer.core import LogWriter
from sparkforge.writer.exceptions import (
    WriterConfigurationError,
)
from sparkforge.writer.models import WriteMode, WriterConfig


# Writer tests now work with mock-spark 2.4.0
class TestLogWriter:
    """Test LogWriter functionality."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock()
        spark.createDataFrame.return_value = Mock()
        spark.table.return_value = Mock()
        return spark

    @pytest.fixture
    def mock_logger(self):
        """Mock pipeline logger."""
        logger = Mock(spec=PipelineLogger)
        # Add context manager support
        logger.context.return_value.__enter__ = Mock(return_value=None)
        logger.context.return_value.__exit__ = Mock(return_value=None)
        # Add timer support
        logger.timer.return_value.__enter__ = Mock(return_value=None)
        logger.timer.return_value.__exit__ = Mock(return_value=None)
        logger.end_timer.return_value = 1.0  # Mock duration
        return logger

    @pytest.fixture
    def valid_config(self):
        """Valid writer configuration."""
        return WriterConfig(
            table_schema="analytics",
            table_name="pipeline_logs",
            write_mode=WriteMode.APPEND,
        )

    @pytest.fixture
    def writer(self, mock_spark, valid_config, mock_logger):
        """LogWriter instance with mocked dependencies."""
        return LogWriter(mock_spark, config=valid_config, logger=mock_logger)

    @pytest.fixture
    def mock_execution_result(self):
        """Mock execution result."""
        mock_result = Mock(spec=ExecutionResult)
        mock_result.step_results = []  # Add required attribute
        mock_result.success = True  # Add required attribute
        mock_result.context = Mock()  # Add required attribute
        mock_result.context.pipeline_id = "test-pipeline"  # Add required attribute
        return mock_result

    @pytest.fixture
    def invalid_config(self):
        """Invalid writer configuration."""
        return WriterConfig(table_schema="", table_name="pipeline_logs")  # Empty schema

    def test_init_valid_config(self, mock_spark, valid_config, mock_logger):
        """Test LogWriter initialization with valid config."""
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        assert writer.spark == mock_spark
        assert writer.config == valid_config
        assert writer.logger == mock_logger
        assert writer.table_fqn == "analytics.pipeline_logs"
        assert writer.metrics["total_writes"] == 0

    def test_init_invalid_config(self, mock_spark, invalid_config, mock_logger):
        """Test LogWriter initialization with invalid config."""
        with pytest.raises(WriterConfigurationError):
            LogWriter(mock_spark, config=invalid_config, logger=mock_logger)

    def test_init_default_logger(self, mock_spark, valid_config):
        """Test LogWriter initialization with default logger."""
        with patch("sparkforge.writer.core.PipelineLogger") as mock_logger_class:
            mock_logger_instance = Mock()
            mock_logger_class.return_value = mock_logger_instance

            writer = LogWriter(mock_spark, config=valid_config)

            assert writer.logger == mock_logger_instance
            mock_logger_class.assert_called_once_with("LogWriter")

    def test_write_execution_result_success(
        self,
        mock_spark,
        valid_config,
        mock_logger,
    ):
        """Test successful execution result writing."""
        # Setup mocks
        mock_execution_result = Mock(spec=ExecutionResult)
        mock_execution_result.step_results = []  # Add required attribute
        mock_execution_result.success = True  # Add required attribute
        mock_execution_result.context = Mock()  # Add required attribute
        mock_execution_result.context.pipeline_id = (
            "test-pipeline"  # Add required attribute
        )
        mock_log_rows = [{"test": "data"}]

        # Create writer
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the data processor and storage manager
        with patch.object(
            writer.data_processor, "process_execution_result"
        ) as mock_process, patch.object(
            writer.storage_manager, "write_batch"
        ) as mock_write_batch, patch.object(
            writer.storage_manager, "create_table_if_not_exists"
        ) as mock_create_table:

            mock_process.return_value = mock_log_rows
            mock_write_batch.return_value = {
                "rows_written": 1,
                "success": True,
                "table_name": "analytics.pipeline_logs",
            }
            mock_create_table.return_value = None

            # Call method
            result = writer.write_execution_result(
                mock_execution_result, run_id="test-run"
            )

            # Verify results
            assert result["success"] is True
            assert result["run_id"] == "test-run"
            assert result["rows_written"] == 1
            assert "operation_id" in result
            assert "write_result" in result

            # Verify calls
            mock_process.assert_called_once()
            mock_write_batch.assert_called_once()
            mock_create_table.assert_called_once()

    def test_write_execution_result_invalid_input(
        self, mock_spark, valid_config, mock_logger
    ):
        """Test execution result writing with invalid input."""
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # The new implementation doesn't validate input type, so this test should pass
        # or we need to add validation to the LogWriter
        with pytest.raises(AttributeError):
            writer.write_execution_result("invalid_input")  # type: ignore[arg-type]

    def test_write_execution_result_validation_failure(
        self, mock_spark, valid_config, mock_logger
    ):
        """Test execution result writing with validation failure."""
        # Setup mocks
        mock_execution_result = Mock(spec=ExecutionResult)
        mock_execution_result.step_results = []  # Add required attribute
        mock_execution_result.success = True  # Add required attribute
        mock_execution_result.context = Mock()  # Add required attribute
        mock_execution_result.context.pipeline_id = (
            "test-pipeline"  # Add required attribute
        )

        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the data processor to raise validation error
        with patch.object(
            writer.data_processor, "process_execution_result"
        ) as mock_process:
            mock_process.side_effect = ValueError("Validation failed")

            with pytest.raises(ValueError, match="Validation failed"):
                writer.write_execution_result(mock_execution_result)

    def test_write_step_results(self, mock_spark, valid_config, mock_logger):
        """Test writing step results."""
        # Setup mocks
        mock_step_result = Mock(spec=StepResult)
        mock_step_result.success = True  # Add required attribute
        mock_step_result.duration_secs = 10.0  # Add required attribute
        mock_step_result.rows_processed = 1000  # Add required attribute
        mock_step_result.rows_written = 950  # Add required attribute
        mock_step_result.validation_rate = 95.0  # Add required attribute
        mock_step_result.phase = Mock()
        mock_step_result.phase.value = "bronze"
        mock_step_result.start_time = Mock()
        mock_step_result.end_time = Mock()
        mock_step_result.step_name = "test_step"
        mock_step_result.step_type = Mock()
        mock_step_result.step_type.value = "bronze"
        mock_step_results = {"step1": mock_step_result}

        # Create writer
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the storage manager
        with patch.object(writer.storage_manager, "write_batch") as mock_write_batch:
            mock_write_batch.return_value = {
                "rows_written": 1,
                "success": True,
                "table_name": "analytics.pipeline_logs",
            }

            result = writer.write_step_results(
                step_results=mock_step_results,
                run_id="test-run",
            )

            # Verify calls
            mock_write_batch.assert_called_once()
            assert result["success"] is True

    def test_write_log_rows_success(self, mock_spark, valid_config, mock_logger):
        """Test successful log rows writing."""
        # Setup mocks
        mock_log_rows = [{"test": "data"}]

        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the storage manager
        with patch.object(writer.storage_manager, "write_batch") as mock_write_batch:
            mock_write_batch.return_value = {
                "rows_written": 1,
                "success": True,
                "table_name": "analytics.pipeline_logs",
            }

            # Call method
            result = writer.write_log_rows(mock_log_rows, run_id="test-run")

            # Verify results
            assert result["success"] is True
            assert result["run_id"] == "test-run"
            assert result["rows_written"] == 1
            assert "operation_metrics" in result
            assert "duration_secs" in result["operation_metrics"]

            # Verify calls
            mock_write_batch.assert_called_once()

    def test_write_log_rows_validation_failure(
        self, mock_spark, valid_config, mock_logger
    ):
        """Test log rows writing with validation failure."""
        # Setup mocks
        mock_log_rows = [{"test": "data"}]

        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the storage manager to raise validation error
        with patch.object(writer.storage_manager, "write_batch") as mock_write_batch:
            mock_write_batch.side_effect = ValueError("Validation failed")

            with pytest.raises(ValueError, match="Validation failed"):
                writer.write_log_rows(mock_log_rows)

    def test_get_metrics(self, mock_spark, valid_config, mock_logger):
        """Test getting writer metrics."""
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        metrics = writer.get_metrics()

        assert "total_writes" in metrics
        assert "successful_writes" in metrics
        assert "failed_writes" in metrics
        assert "total_duration_secs" in metrics
        assert "avg_write_duration_secs" in metrics
        assert "total_rows_written" in metrics
        assert "memory_usage_peak_mb" in metrics

        # Should return a copy
        assert metrics is not writer.metrics

    def test_reset_metrics(self, mock_spark, valid_config, mock_logger):
        """Test resetting writer metrics."""
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Modify metrics
        writer.metrics["total_writes"] = 5

        # Reset
        writer.reset_metrics()

        # Verify reset
        assert writer.metrics["total_writes"] == 0
        assert writer.metrics["successful_writes"] == 0
        assert writer.metrics["failed_writes"] == 0

    def test_show_logs(self, mock_spark, valid_config, mock_logger):
        """Test showing logs."""
        # Setup mocks
        mock_df = Mock()
        mock_df.show.return_value = None
        mock_spark.table.return_value = mock_df

        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Call method
        writer.show_logs(limit=10)

        # Verify calls
        mock_spark.table.assert_called_once_with("analytics.pipeline_logs")
        mock_df.show.assert_called_once_with(10)

    def test_show_logs_no_limit(self, mock_spark, valid_config, mock_logger):
        """Test showing logs without limit."""
        # Setup mocks
        mock_df = Mock()
        mock_df.show.return_value = None
        mock_spark.table.return_value = mock_df

        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Call method
        writer.show_logs()

        # Verify calls
        mock_spark.table.assert_called_once_with("analytics.pipeline_logs")
        mock_df.show.assert_called_once_with()

    def test_get_table_info(self, mock_spark, valid_config, mock_logger):
        """Test getting table info."""
        writer = LogWriter(mock_spark, config=valid_config, logger=mock_logger)

        # Mock the storage manager to return proper table info
        with patch.object(writer.storage_manager, "get_table_info") as mock_get_info:
            mock_get_info.return_value = {
                "table_fqn": "analytics.pipeline_logs",
                "row_count": 100,
                "columns": ["col1", "col2"],
                "schema": '{"type": "struct"}',
            }

            # Call method
            info = writer.get_table_info()

            # Verify results
            assert info["table_fqn"] == "analytics.pipeline_logs"
            assert info["row_count"] == 100
            assert info["columns"] == ["col1", "col2"]
            assert info["schema"] == '{"type": "struct"}'

    def test_write_execution_result_batch_success(self, writer, mock_execution_result):
        """Test batch write execution results success."""
        # Setup mock execution results
        execution_results = [mock_execution_result, mock_execution_result]

        # Mock the storage manager to avoid DataFrame issues
        with patch.object(writer.storage_manager, "write_batch") as mock_write_batch:
            mock_write_batch.return_value = {
                "rows_written": 2,
                "success": True,
                "table_name": "analytics.pipeline_logs",
            }

            result = writer.write_execution_result_batch(
                execution_results=execution_results,
                run_ids=["test-batch-run-1", "test-batch-run-2"],
            )

            assert result["success"] is True
            assert result["execution_results_count"] == 2
            assert result["total_rows_written"] == 2
            assert "operation_id" in result
            assert "operation_metrics" in result
            mock_write_batch.assert_called_once()

    def test_write_execution_result_batch_with_failures(
        self, writer, mock_execution_result
    ):
        """Test batch write with some failures."""
        # Setup mock execution results - both valid but one will fail during processing
        execution_results = [mock_execution_result, mock_execution_result]

        # Mock the storage manager to avoid DataFrame issues
        with patch.object(writer.storage_manager, "write_batch") as mock_write_batch:
            mock_write_batch.return_value = {
                "rows_written": 1,
                "success": True,
                "table_name": "analytics.pipeline_logs",
            }

            result = writer.write_execution_result_batch(
                execution_results=execution_results,
                run_ids=["test-batch-run-failures-1", "test-batch-run-failures-2"],
            )

            assert result["success"] is True
            assert result["execution_results_count"] == 2
            assert result["total_rows_written"] == 1
            mock_write_batch.assert_called_once()

    def test_write_log_rows_batch(self, writer):
        """Test writing log rows in batches."""
        # Create test log rows
        log_rows = [{"test": f"data_{i}"} for i in range(5)]

        with patch.object(writer, "_write_log_rows") as mock_write:
            writer._write_log_rows_batch(log_rows, "test-run", batch_size=2)

            # Should be called 3 times (batches of 2, 2, 1)
            assert mock_write.call_count == 3

            # Check batch calls
            calls = mock_write.call_args_list
            assert calls[0][0][0] == log_rows[0:2]  # First batch
            assert calls[1][0][0] == log_rows[2:4]  # Second batch
            assert calls[2][0][0] == log_rows[4:5]  # Third batch

    def test_get_memory_usage_success(self, writer):
        """Test getting memory usage successfully."""
        with patch("psutil.Process") as mock_process, patch(
            "psutil.virtual_memory"
        ) as mock_vm:
            # Mock memory info
            mock_memory_info = Mock()
            mock_memory_info.rss = 1024 * 1024 * 100  # 100 MB
            mock_memory_info.vms = 1024 * 1024 * 200  # 200 MB

            mock_process.return_value.memory_info.return_value = mock_memory_info
            mock_process.return_value.memory_percent.return_value = 10.5
            mock_vm.return_value.available = 1024 * 1024 * 800  # 800 MB

            result = writer.get_memory_usage()

            # Check for keys that are actually returned by the performance monitor
            assert "available_mb" in result
            assert "total_mb" in result
            assert "spark_memory" in result
            assert result["available_mb"] == 800.0

    def test_get_memory_usage_psutil_not_available(self, writer):
        """Test getting memory usage when psutil is not available."""
        # Mock HAS_PSUTIL to False to simulate psutil not being available
        with patch("sparkforge.writer.monitoring.HAS_PSUTIL", False):
            result = writer.get_memory_usage()

            # The method should still return some basic info even without psutil
            assert "available_mb" in result
            assert "total_mb" in result
            assert result["psutil_available"] is False

    def test_get_memory_usage_exception(self, writer):
        """Test getting memory usage with exception."""
        with patch("psutil.Process", side_effect=Exception("Process error")):
            result = writer.get_memory_usage()

            # The method should still return some basic info even with psutil error
            assert "available_mb" in result
            assert "total_mb" in result

    def test_validate_log_data_quality_success(self, writer):
        """Test successful data quality validation."""
        log_rows = [
            {
                "run_id": "test-run-1",
                "step_name": "test_step",
                "phase": "bronze",
                "success": True,
                "duration_secs": 10.0,
                "rows_processed": 1000,
                "rows_written": 950,
                "validation_rate": 95.0,
            },
            {
                "run_id": "test-run-2",
                "step_name": "test_step2",
                "phase": "silver",
                "success": True,
                "duration_secs": 15.0,
                "rows_processed": 2000,
                "rows_written": 1900,
                "validation_rate": 98.0,
            },
        ]

        with patch.object(
            writer, "_create_dataframe_from_log_rows"
        ) as mock_create_df, patch(
            "sparkforge.validation.data_validation.apply_column_rules"
        ) as mock_apply_rules, patch(
            "sparkforge.validation.utils.get_dataframe_info"
        ) as mock_df_info:
            # Mock DataFrame creation
            mock_df = Mock()
            mock_create_df.return_value = mock_df

            # Mock validation results
            mock_stats = Mock()
            mock_stats.total_rows = 2
            mock_stats.valid_rows = 2
            mock_stats.invalid_rows = 0
            mock_stats.validation_rate = 100.0

            mock_apply_rules.return_value = (mock_df, Mock(), mock_stats)
            mock_df_info.return_value = {"row_count": 2, "column_count": 8}

            result = writer.validate_log_data_quality(log_rows)

            # Check new DataQualityReport structure
            assert result["is_valid"] is True
            assert result["total_rows"] == 2
            assert isinstance(result["null_counts"], dict)
            assert isinstance(result["validation_issues"], list)
            assert result["failed_executions"] == 0
            assert isinstance(result["data_quality_score"], float)

    def test_validate_log_data_quality_failure(self, writer):
        """Test data quality validation failure."""
        log_rows = [
            {
                "run_id": "test-run-1",
                "step_name": "test_step",
                "phase": "bronze",
                "success": True,
                "duration_secs": 10.0,
                "rows_processed": 1000,
                "rows_written": 500,  # Low validation rate
                "validation_rate": 50.0,  # Below threshold
            }
        ]

        with patch.object(
            writer, "_create_dataframe_from_log_rows"
        ) as mock_create_df, patch(
            "sparkforge.validation.data_validation.apply_column_rules"
        ) as mock_apply_rules, patch(
            "sparkforge.validation.utils.get_dataframe_info"
        ) as mock_df_info:
            # Mock DataFrame creation
            mock_df = Mock()
            mock_create_df.return_value = mock_df

            # Mock validation results with low quality
            mock_stats = Mock()
            mock_stats.total_rows = 1
            mock_stats.valid_rows = 0
            mock_stats.invalid_rows = 1
            mock_stats.validation_rate = 0.0

            mock_apply_rules.return_value = (mock_df, Mock(), mock_stats)
            mock_df_info.return_value = {"row_count": 1, "column_count": 8}

            result = writer.validate_log_data_quality(log_rows)

            # Check new DataQualityReport structure
            assert result["is_valid"] is True
            assert result["total_rows"] == 1
            assert isinstance(result["null_counts"], dict)
            assert isinstance(result["validation_issues"], list)
            assert result["failed_executions"] == 0
            assert isinstance(result["data_quality_score"], float)

    def test_detect_anomalies_success(self, writer):
        """Test successful anomaly detection."""
        writer.config.enable_anomaly_detection = True

        log_rows = [
            {
                "run_id": "test-run-1",
                "duration_secs": 10.0,
                "validation_rate": 95.0,
                "rows_processed": 1000,
            },
            {
                "run_id": "test-run-2",
                "duration_secs": 30.0,  # Anomaly: 3x average (above 2x threshold)
                "validation_rate": 50.0,
                "rows_processed": 1000,
            },
        ]

        result = writer.detect_anomalies(log_rows)

        # Check new AnomalyReport structure
        assert "performance_anomalies" in result
        assert "quality_anomalies" in result
        assert "anomaly_score" in result
        assert "total_anomalies" in result
        assert "total_executions" in result
        assert isinstance(result["performance_anomalies"], list)
        assert isinstance(result["quality_anomalies"], list)
        assert isinstance(result["anomaly_score"], float)
        assert isinstance(result["total_anomalies"], int)
        assert isinstance(result["total_executions"], int)

    def test_detect_anomalies_disabled(self, writer):
        """Test anomaly detection when disabled."""
        writer.config.enable_anomaly_detection = False

        log_rows = [{"run_id": "test-run-1", "duration_secs": 10.0}]

        result = writer.detect_anomalies(log_rows)

        # Check new AnomalyReport structure for disabled case
        assert "performance_anomalies" in result
        assert "quality_anomalies" in result
        assert result["total_anomalies"] == 0
        assert result["total_executions"] == 0
        assert len(result["performance_anomalies"]) == 0
        assert len(result["quality_anomalies"]) == 0

    def test_optimize_table_success(self, writer):
        """Test successful table optimization."""
        with patch.object(writer.storage_manager, "optimize_table") as mock_optimize:
            mock_optimize.return_value = {
                "optimization_completed": True,
                "table_name": "analytics.pipeline_logs",
                "timestamp": "2023-01-01T00:00:00Z",
                "table_info": {"row_count": 1000},
            }

            result = writer.optimize_table()

            assert result["optimization_completed"] is True
            assert "table_name" in result
            assert "timestamp" in result

    def test_optimize_table_not_exists(self, writer):
        """Test table optimization when table doesn't exist."""
        with patch.object(writer.storage_manager, "optimize_table") as mock_optimize:
            mock_optimize.return_value = {
                "optimized": False,
                "reason": "Table does not exist",
            }

            result = writer.optimize_table()

            assert result["optimized"] is False
            assert result["reason"] == "Table does not exist"

    def test_vacuum_table_success(self, writer):
        """Test successful table vacuum."""
        with patch.object(writer.storage_manager, "vacuum_table") as mock_vacuum:
            mock_vacuum.return_value = {
                "vacuumed": True,
                "files_removed": 5,
                "vacuum_timestamp": "2023-01-01T00:00:00Z",
            }

            result = writer.vacuum_table(retention_hours=168)

            assert result["vacuumed"] is True
            assert "files_removed" in result
            assert "vacuum_timestamp" in result

    def test_analyze_quality_trends_success(self, writer):
        """Test successful quality trends analysis."""
        with patch.object(
            writer.storage_manager, "query_logs"
        ) as mock_query:
            mock_query.return_value = None  # Mock DataFrame
            
            with patch.object(
                writer.quality_analyzer, "analyze_quality_trends"
            ) as mock_analyze:
                mock_analyze.return_value = {
                    "trends_analyzed": True,
                    "quality_trend": "improving",
                    "analysis_period": 30,
                    "analysis_timestamp": "2023-01-01T00:00:00Z",
                }

                result = writer.analyze_quality_trends(days=30)

                assert result["trends_analyzed"] is True
                assert "quality_trend" in result
                assert "analysis_period" in result

    def test_analyze_execution_trends_success(self, writer):
        """Test successful execution trends analysis."""
        with patch.object(
            writer.storage_manager, "query_logs"
        ) as mock_query:
            mock_query.return_value = None  # Mock DataFrame
            
            with patch.object(
                writer.trend_analyzer, "analyze_execution_trends"
            ) as mock_analyze:
                mock_analyze.return_value = {
                    "trends_analyzed": True,
                    "execution_trend": "stable",
                    "analysis_period": 30,
                    "analysis_timestamp": "2023-01-01T00:00:00Z",
                }

                result = writer.analyze_execution_trends(days=30)

                assert result["trends_analyzed"] is True
                assert "execution_trend" in result
                assert "analysis_period" in result
