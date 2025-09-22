"""
Unit tests for writer core functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any

from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode, LogLevel
from sparkforge.writer.exceptions import WriterConfigurationError, WriterValidationError, WriterError
from sparkforge.models import StepResult, PipelinePhase, ExecutionContext, ExecutionResult, PipelineMetrics
from sparkforge.logging import PipelineLogger


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
            write_mode=WriteMode.APPEND
        )
    
    @pytest.fixture
    def writer(self, mock_spark, valid_config, mock_logger):
        """LogWriter instance with mocked dependencies."""
        return LogWriter(mock_spark, valid_config, mock_logger)
    
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
        return WriterConfig(
            table_schema="",  # Empty schema
            table_name="pipeline_logs"
        )
    
    def test_init_valid_config(self, mock_spark, valid_config, mock_logger):
        """Test LogWriter initialization with valid config."""
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        assert writer.spark == mock_spark
        assert writer.config == valid_config
        assert writer.logger == mock_logger
        assert writer.table_fqn == "analytics.pipeline_logs"
        assert writer.metrics["total_writes"] == 0
    
    def test_init_invalid_config(self, mock_spark, invalid_config, mock_logger):
        """Test LogWriter initialization with invalid config."""
        with pytest.raises(WriterConfigurationError):
            LogWriter(mock_spark, invalid_config, mock_logger)
    
    def test_init_default_logger(self, mock_spark, valid_config):
        """Test LogWriter initialization with default logger."""
        with patch('sparkforge.writer.core.PipelineLogger') as mock_logger_class:
            mock_logger_instance = Mock()
            mock_logger_class.return_value = mock_logger_instance
            
            writer = LogWriter(mock_spark, valid_config)
            
            assert writer.logger == mock_logger_instance
            mock_logger_class.assert_called_once_with("LogWriter")
    
    @patch('sparkforge.writer.core.validate_log_data')
    @patch('sparkforge.writer.core.create_log_rows_from_execution_result')
    @patch('sparkforge.writer.core.time_write_operation')
    def test_write_execution_result_success(
        self, mock_time_write, mock_create_rows, mock_validate,
        mock_spark, valid_config, mock_logger
    ):
        """Test successful execution result writing."""
        # Setup mocks
        mock_execution_result = Mock(spec=ExecutionResult)
        mock_execution_result.step_results = []  # Add required attribute
        mock_execution_result.success = True  # Add required attribute
        mock_execution_result.context = Mock()  # Add required attribute
        mock_execution_result.context.pipeline_id = "test-pipeline"  # Add required attribute
        mock_log_rows = [{"test": "data"}]
        mock_create_rows.return_value = mock_log_rows
        mock_validate.return_value = None
        # Mock time_write_operation to return numeric values
        mock_time_write.return_value = (1, 0.5, datetime.now(), datetime.now())
        
        # Mock DataFrame count method
        mock_df = Mock()
        mock_df.count.return_value = 1
        mock_spark.createDataFrame.return_value = mock_df

        # Create writer
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        # Call method
        result = writer.write_execution_result(mock_execution_result, run_id="test-run")
        
        # Verify results
        assert result["success"] is True
        assert result["run_id"] == "test-run"
        assert result["rows_written"] == 1
        assert "duration_secs" in result
        assert "table_fqn" in result
        assert "metrics" in result
        
        # Verify calls
        mock_create_rows.assert_called_once()
        mock_validate.assert_called_once_with(mock_log_rows)
        mock_time_write.assert_called_once()
    
    @patch('sparkforge.writer.core.create_log_rows_from_execution_result')
    def test_write_execution_result_invalid_input(self, mock_create_rows, mock_spark, valid_config, mock_logger):
        """Test execution result writing with invalid input."""
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        with pytest.raises(WriterValidationError, match="execution_result must be an ExecutionResult"):
            writer.write_execution_result("invalid_input")  # type: ignore[arg-type]
    
    @patch('sparkforge.writer.core.validate_log_data')
    @patch('sparkforge.writer.core.create_log_rows_from_execution_result')
    def test_write_execution_result_validation_failure(
        self, mock_create_rows, mock_validate, mock_spark, valid_config, mock_logger
    ):
        """Test execution result writing with validation failure."""
        # Setup mocks
        mock_execution_result = Mock(spec=ExecutionResult)
        mock_execution_result.step_results = []  # Add required attribute
        mock_execution_result.success = True  # Add required attribute
        mock_execution_result.context = Mock()  # Add required attribute
        mock_execution_result.context.pipeline_id = "test-pipeline"  # Add required attribute
        mock_log_rows = [{"test": "data"}]
        mock_create_rows.return_value = mock_log_rows
        mock_validate.side_effect = ValueError("Validation failed")
        
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        with pytest.raises(WriterValidationError, match="Log data validation failed"):
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
        mock_step_results = [mock_step_result]
        mock_execution_context = Mock(spec=ExecutionContext)
        
        # Create writer
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        # Mock the write_execution_result method
        with patch.object(writer, 'write_execution_result') as mock_write_exec:
            mock_write_exec.return_value = {"success": True}
            
            result = writer.write_step_results(
                step_results=mock_step_results,
                execution_context=mock_execution_context,
                run_id="test-run"
            )
            
            # Verify calls
            mock_write_exec.assert_called_once()
            assert result == {"success": True}
    
    @patch('sparkforge.writer.core.validate_log_data')
    @patch('sparkforge.writer.core.time_write_operation')
    def test_write_log_rows_success(
        self, mock_time_write, mock_validate, mock_spark, valid_config, mock_logger
    ):
        """Test successful log rows writing."""
        # Setup mocks
        mock_log_rows = [{"test": "data"}]
        mock_validate.return_value = None
        # Mock time_write_operation to return numeric values
        mock_time_write.return_value = (1, 0.5, datetime.now(), datetime.now())
        
        # Mock DataFrame count method
        mock_df = Mock()
        mock_df.count.return_value = 1
        mock_spark.createDataFrame.return_value = mock_df

        # Create writer
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        # Call method
        result = writer.write_log_rows(mock_log_rows, run_id="test-run")
        
        # Verify results
        assert result["success"] is True
        assert result["run_id"] == "test-run"
        assert result["rows_written"] == 1
        assert "duration_secs" in result
        assert "table_fqn" in result
        
        # Verify calls
        mock_validate.assert_called_once_with(mock_log_rows)
        mock_time_write.assert_called_once()
    
    @patch('sparkforge.writer.core.validate_log_data')
    def test_write_log_rows_validation_failure(
        self, mock_validate, mock_spark, valid_config, mock_logger
    ):
        """Test log rows writing with validation failure."""
        # Setup mocks
        mock_log_rows = [{"test": "data"}]
        mock_validate.side_effect = ValueError("Validation failed")
        
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        with pytest.raises(WriterError, match="Failed to write log rows"):
            writer.write_log_rows(mock_log_rows)
    
    def test_get_metrics(self, mock_spark, valid_config, mock_logger):
        """Test getting writer metrics."""
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
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
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
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
        
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
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
        
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        # Call method
        writer.show_logs()
        
        # Verify calls
        mock_spark.table.assert_called_once_with("analytics.pipeline_logs")
        mock_df.show.assert_called_once_with()
    
    def test_get_table_info(self, mock_spark, valid_config, mock_logger):
        """Test getting table info."""
        # Setup mocks
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.columns = ["col1", "col2"]
        mock_df.schema.json.return_value = '{"type": "struct"}'
        mock_spark.table.return_value = mock_df
        
        writer = LogWriter(mock_spark, valid_config, mock_logger)
        
        # Call method
        info = writer.get_table_info()
        
        # Verify results
        assert info["table_fqn"] == "analytics.pipeline_logs"
        assert info["row_count"] == 100
        assert info["columns"] == ["col1", "col2"]
        assert info["schema"] == '{"type": "struct"}'
        
        # Verify calls
        mock_spark.table.assert_called_once_with("analytics.pipeline_logs")
        mock_df.count.assert_called_once()
        mock_df.schema.json.assert_called_once()
    
    def test_write_execution_result_batch_success(self, writer, mock_execution_result):
        """Test batch write execution results success."""
        # Setup mock execution results
        execution_results = [mock_execution_result, mock_execution_result]
        
        # Mock the batch write operation
        with patch.object(writer, '_write_log_rows') as mock_write:
            with patch('sparkforge.writer.core.create_log_rows_from_execution_result') as mock_create_rows:
                mock_create_rows.return_value = [{"test": "data"}]
                
                result = writer.write_execution_result_batch(
                    execution_results=execution_results,
                    run_id="test-batch-run",
                    batch_size=500
                )
                
                assert result["success"] is True
                assert result["total_executions"] == 2
                assert result["successful_writes"] == 2
                assert result["failed_writes"] == 0
                assert result["rows_written"] == 2
                assert mock_write.called
    
    def test_write_execution_result_batch_with_failures(self, writer, mock_execution_result):
        """Test batch write with some failures."""
        # Setup mock execution results - one will fail
        execution_results = [mock_execution_result, "invalid_result"]
        
        # Mock the batch write operation
        with patch.object(writer, '_write_log_rows') as mock_write:
            with patch('sparkforge.writer.core.create_log_rows_from_execution_result') as mock_create_rows:
                mock_create_rows.return_value = [{"test": "data"}]
                # Make the second call fail
                mock_create_rows.side_effect = [{"test": "data"}, Exception("Invalid result")]
                
                result = writer.write_execution_result_batch(
                    execution_results=execution_results,
                    run_id="test-batch-run-failures"
                )
                
                assert result["success"] is True  # Overall operation succeeds
                assert result["total_executions"] == 2
                assert result["successful_writes"] == 1
                assert result["failed_writes"] == 1
                assert result["rows_written"] == 1
    
    def test_write_log_rows_batch(self, writer):
        """Test writing log rows in batches."""
        # Create test log rows
        log_rows = [{"test": f"data_{i}"} for i in range(5)]
        
        with patch.object(writer, '_write_log_rows') as mock_write:
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
        with patch('psutil.Process') as mock_process, \
             patch('psutil.virtual_memory') as mock_vm:
            
            # Mock memory info
            mock_memory_info = Mock()
            mock_memory_info.rss = 1024 * 1024 * 100  # 100 MB
            mock_memory_info.vms = 1024 * 1024 * 200  # 200 MB
            
            mock_process.return_value.memory_info.return_value = mock_memory_info
            mock_process.return_value.memory_percent.return_value = 10.5
            mock_vm.return_value.available = 1024 * 1024 * 800  # 800 MB
            
            result = writer.get_memory_usage()
            
            assert "rss_mb" in result
            assert "vms_mb" in result
            assert "percent" in result
            assert "available_mb" in result
            assert result["rss_mb"] == 100.0
            assert result["vms_mb"] == 200.0
            assert result["percent"] == 10.5
            assert result["available_mb"] == 800.0
    
    def test_get_memory_usage_psutil_not_available(self, writer):
        """Test getting memory usage when psutil is not available."""
        def mock_import(name, *args, **kwargs):
            if name == 'psutil':
                raise ImportError("No module named 'psutil'")
            return __import__(name, *args, **kwargs)
        
        with patch('builtins.__import__', side_effect=mock_import):
            result = writer.get_memory_usage()
            
            assert "error" in result
            assert "psutil not installed" in result["error"]
    
    def test_get_memory_usage_exception(self, writer):
        """Test getting memory usage with exception."""
        with patch('psutil.Process', side_effect=Exception("Process error")):
            result = writer.get_memory_usage()
            
            assert "error" in result
            assert result["error"] == "Process error"
