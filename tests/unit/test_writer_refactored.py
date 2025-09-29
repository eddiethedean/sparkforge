"""
Unit tests for the refactored writer module.

This module tests all components of the refactored writer architecture
including operations, storage, monitoring, and analytics.
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkforge.models import ExecutionResult, StepResult
from sparkforge.writer import (
    AnalyticsEngine,
    DataProcessor,
    DataQualityAnalyzer,
    LogRow,
    LogWriter,
    PerformanceMonitor,
    StorageManager,
    TrendAnalyzer,
    WriteMode,
    WriterConfig,
    WriterConfigurationError,
    WriterDataQualityError,
    WriterError,
    WriterMetrics,
    WriterPerformanceError,
    WriterTableError,
    WriterValidationError,
)


class TestDataProcessor:
    """Test cases for DataProcessor class."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def data_processor(self, spark_session):
        """Create a DataProcessor instance."""
        return DataProcessor(spark_session)

    @pytest.fixture
    def sample_execution_result(self):
        """Create a sample execution result."""
        return ExecutionResult(
            success=True,
            execution_time=120.5,
            total_rows_processed=1000,
            validation_rate=95.5,
            error_message=None,
        )

    def test_data_processor_initialization(self, data_processor):
        """Test DataProcessor initialization."""
        assert data_processor.spark is not None
        assert data_processor.logger is not None

    def test_process_execution_result_success(
        self, data_processor, sample_execution_result
    ):
        """Test successful processing of execution result."""
        with patch(
            "sparkforge.writer.operations.create_log_rows_from_execution_result"
        ) as mock_create:
            with patch(
                "sparkforge.writer.operations.validate_log_data"
            ) as mock_validate:
                # Setup mocks
                mock_log_rows = [Mock(spec=LogRow)]
                mock_create.return_value = mock_log_rows
                mock_validate.return_value = {"is_valid": True}

                # Test processing
                result = data_processor.process_execution_result(
                    sample_execution_result, "test_run_id", "initial"
                )

                # Assertions
                assert result == mock_log_rows
                mock_create.assert_called_once()
                mock_validate.assert_called_once()

    def test_process_execution_result_validation_failure(
        self, data_processor, sample_execution_result
    ):
        """Test processing with validation failure."""
        with patch(
            "sparkforge.writer.operations.create_log_rows_from_execution_result"
        ) as mock_create:
            with patch(
                "sparkforge.writer.operations.validate_log_data"
            ) as mock_validate:
                # Setup mocks
                mock_log_rows = [Mock(spec=LogRow)]
                mock_create.return_value = mock_log_rows
                mock_validate.return_value = {
                    "is_valid": False,
                    "errors": ["validation error"],
                }

                # Test processing should raise exception
                with pytest.raises(WriterValidationError):
                    data_processor.process_execution_result(
                        sample_execution_result, "test_run_id", "initial"
                    )

    def test_create_dataframe_from_log_rows(self, data_processor):
        """Test DataFrame creation from log rows."""
        # Create mock log rows
        log_rows = [
            LogRow(
                run_id="test_run_1",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                phase="bronze",
                step="test_step",
                start_time=datetime.now(),
                end_time=datetime.now(),
                table_fqn="test.table",
                write_mode="append",
                input_rows=100,
                output_rows=95,
                rows_written=95,
                valid_rows=95,
                invalid_rows=5,
                validation_rate=95.0,
                execution_time=60.0,
                success=True,
                error_message=None,
                metadata={},
            )
        ]

        with patch.object(data_processor.spark, "createDataFrame") as mock_create_df:
            mock_df = Mock(spec=DataFrame)
            mock_create_df.return_value = mock_df

            result = data_processor.create_dataframe_from_log_rows(log_rows)

            assert result == mock_df
            mock_create_df.assert_called_once()

    def test_validate_data_quality(self, data_processor):
        """Test data quality validation."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["run_id", "phase", "step", "success", "validation_rate"]
        mock_df.count.return_value = 100

        with patch("sparkforge.writer.operations.get_dataframe_info") as mock_info:
            mock_info.return_value = {"row_count": 100}

            with patch.object(mock_df, "filter") as mock_filter:
                mock_filter.return_value.count.return_value = 0

                result = data_processor.validate_data_quality(mock_df)

                assert "is_valid" in result
                assert "total_rows" in result
                assert "data_quality_score" in result


class TestStorageManager:
    """Test cases for StorageManager class."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def writer_config(self):
        """Create a WriterConfig instance."""
        return WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

    @pytest.fixture
    def storage_manager(self, spark_session, writer_config):
        """Create a StorageManager instance."""
        return StorageManager(spark_session, writer_config)

    def test_storage_manager_initialization(self, storage_manager):
        """Test StorageManager initialization."""
        assert storage_manager.spark is not None
        assert storage_manager.config is not None
        assert storage_manager.logger is not None
        assert storage_manager.table_fqn == "test_schema.test_table"

    def test_create_table_if_not_exists_new_table(self, storage_manager):
        """Test creating a new table."""
        with patch("sparkforge.writer.storage.table_exists") as mock_exists:
            mock_exists.return_value = False

            with patch.object(
                storage_manager.spark, "createDataFrame"
            ) as mock_create_df:
                with patch.object(storage_manager.spark, "sql") as mock_sql:
                    mock_df = Mock(spec=DataFrame)
                    mock_create_df.return_value = mock_df

                    # Mock the write operation
                    mock_writer = Mock()
                    mock_df.write = mock_writer
                    mock_writer.format.return_value = mock_writer
                    mock_writer.mode.return_value = mock_writer
                    mock_writer.option.return_value = mock_writer
                    mock_writer.saveAsTable.return_value = None

                    storage_manager.create_table_if_not_exists(Mock())

                    mock_exists.assert_called_once()
                    mock_create_df.assert_called_once()

    def test_create_table_if_not_exists_existing_table(self, storage_manager):
        """Test when table already exists."""
        with patch("sparkforge.writer.storage.table_exists") as mock_exists:
            mock_exists.return_value = True

            storage_manager.create_table_if_not_exists(Mock())

            mock_exists.assert_called_once()

    def test_write_dataframe_success(self, storage_manager):
        """Test successful DataFrame writing."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100

        with patch.object(
            storage_manager, "_prepare_dataframe_for_write"
        ) as mock_prepare:
            mock_prepared_df = Mock(spec=DataFrame)
            mock_prepared_df.count.return_value = 100
            mock_prepare.return_value = mock_prepared_df

            with patch.object(mock_prepared_df, "write") as mock_writer:
                mock_writer.format.return_value = mock_writer
                mock_writer.mode.return_value = mock_writer
                mock_writer.saveAsTable.return_value = None

                result = storage_manager.write_dataframe(mock_df)

                assert result["success"] is True
                assert result["rows_written"] == 100
                assert result["table_name"] == "test_schema.test_table"

    def test_write_dataframe_failure(self, storage_manager):
        """Test DataFrame writing failure."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Write failed")

        with pytest.raises(WriterTableError):
            storage_manager.write_dataframe(mock_df)

    def test_get_table_info(self, storage_manager):
        """Test getting table information."""
        with patch.object(storage_manager.spark, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = [
                {"name": "test", "value": "test"}
            ]

            with patch.object(storage_manager.spark, "table") as mock_table:
                mock_df = Mock(spec=DataFrame)
                mock_df.count.return_value = 100
                mock_table.return_value = mock_df

                result = storage_manager.get_table_info()

                assert "table_name" in result
                assert "row_count" in result
                assert result["row_count"] == 100


class TestPerformanceMonitor:
    """Test cases for PerformanceMonitor class."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def performance_monitor(self, spark_session):
        """Create a PerformanceMonitor instance."""
        return PerformanceMonitor(spark_session)

    def test_performance_monitor_initialization(self, performance_monitor):
        """Test PerformanceMonitor initialization."""
        assert performance_monitor.spark is not None
        assert performance_monitor.logger is not None
        assert isinstance(performance_monitor.metrics, dict)
        assert performance_monitor.metrics["total_writes"] == 0

    def test_start_operation(self, performance_monitor):
        """Test starting operation monitoring."""
        operation_id = "test_operation"
        operation_type = "test_type"

        performance_monitor.start_operation(operation_id, operation_type)

        assert operation_id in performance_monitor.operation_start_times

    def test_end_operation_success(self, performance_monitor):
        """Test ending operation with success."""
        operation_id = "test_operation"
        performance_monitor.start_operation(operation_id, "test_type")

        result = performance_monitor.end_operation(operation_id, True, 100)

        assert result["success"] is True
        assert result["rows_written"] == 100
        assert result["operation_id"] == operation_id
        assert operation_id not in performance_monitor.operation_start_times

    def test_end_operation_failure(self, performance_monitor):
        """Test ending operation with failure."""
        operation_id = "test_operation"
        performance_monitor.start_operation(operation_id, "test_type")

        result = performance_monitor.end_operation(operation_id, False, 0, "Test error")

        assert result["success"] is False
        assert result["error_message"] == "Test error"
        assert operation_id not in performance_monitor.operation_start_times

    def test_get_metrics(self, performance_monitor):
        """Test getting performance metrics."""
        metrics = performance_monitor.get_metrics()

        assert isinstance(metrics, dict)
        assert "total_writes" in metrics
        assert "successful_writes" in metrics
        assert "failed_writes" in metrics

    def test_reset_metrics(self, performance_monitor):
        """Test resetting performance metrics."""
        # Modify metrics
        performance_monitor.metrics["total_writes"] = 10

        performance_monitor.reset_metrics()

        assert performance_monitor.metrics["total_writes"] == 0
        assert performance_monitor.metrics["successful_writes"] == 0

    def test_get_memory_usage(self, performance_monitor):
        """Test getting memory usage information."""
        with patch("sparkforge.writer.monitoring.psutil.virtual_memory") as mock_memory:
            mock_memory.return_value.total = 8 * 1024 * 1024 * 1024  # 8GB
            mock_memory.return_value.available = 4 * 1024 * 1024 * 1024  # 4GB
            mock_memory.return_value.used = 4 * 1024 * 1024 * 1024  # 4GB
            mock_memory.return_value.percent = 50.0

            result = performance_monitor.get_memory_usage()

            assert "total_mb" in result
            assert "available_mb" in result
            assert "used_mb" in result
            assert "percentage" in result

    def test_check_performance_thresholds(self, performance_monitor):
        """Test checking performance thresholds."""
        # Test with normal metrics
        normal_metrics = {"duration_secs": 60.0, "memory_usage_mb": 1024.0}

        violations = performance_monitor.check_performance_thresholds(normal_metrics)
        assert len(violations) == 0

        # Test with threshold violations
        violation_metrics = {
            "duration_secs": 400.0,  # Exceeds 5 minutes
            "memory_usage_mb": 10000.0,  # Exceeds 8GB
        }

        violations = performance_monitor.check_performance_thresholds(violation_metrics)
        assert len(violations) > 0


class TestLogWriter:
    """Test cases for the main LogWriter class."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def writer_config(self):
        """Create a WriterConfig instance."""
        return WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

    @pytest.fixture
    def log_writer(self, spark_session, writer_config):
        """Create a LogWriter instance."""
        return LogWriter(spark_session, writer_config)

    def test_log_writer_initialization(self, log_writer):
        """Test LogWriter initialization."""
        assert log_writer.spark is not None
        assert log_writer.config is not None
        assert log_writer.logger is not None
        assert log_writer.data_processor is not None
        assert log_writer.storage_manager is not None
        assert log_writer.performance_monitor is not None

    def test_write_execution_result_success(self, log_writer):
        """Test successful execution result writing."""
        execution_result = ExecutionResult(
            success=True,
            execution_time=120.5,
            total_rows_processed=1000,
            validation_rate=95.5,
            error_message=None,
        )

        with patch.object(
            log_writer.data_processor, "process_execution_result"
        ) as mock_process:
            with patch.object(
                log_writer.storage_manager, "create_table_if_not_exists"
            ) as mock_create:
                with patch.object(
                    log_writer.storage_manager, "write_batch"
                ) as mock_write:
                    with patch.object(
                        log_writer.performance_monitor, "start_operation"
                    ) as mock_start:
                        with patch.object(
                            log_writer.performance_monitor, "end_operation"
                        ) as mock_end:
                            # Setup mocks
                            mock_process.return_value = [Mock(spec=LogRow)]
                            mock_write.return_value = {
                                "rows_written": 100,
                                "success": True,
                            }
                            mock_end.return_value = {"duration_secs": 60.0}

                            result = log_writer.write_execution_result(execution_result)

                            assert result["success"] is True
                            assert "run_id" in result
                            assert "operation_id" in result
                            mock_process.assert_called_once()
                            mock_create.assert_called_once()
                            mock_write.assert_called_once()

    def test_write_execution_result_failure(self, log_writer):
        """Test execution result writing failure."""
        execution_result = ExecutionResult(
            success=True,
            execution_time=120.5,
            total_rows_processed=1000,
            validation_rate=95.5,
            error_message=None,
        )

        with patch.object(
            log_writer.data_processor, "process_execution_result"
        ) as mock_process:
            mock_process.side_effect = WriterValidationError("Validation failed")

            with pytest.raises(WriterValidationError):
                log_writer.write_execution_result(execution_result)

    def test_write_step_results_success(self, log_writer):
        """Test successful step results writing."""
        step_results = {
            "test_step": StepResult(
                step_name="test_step",
                success=True,
                execution_time=60.0,
                input_rows=100,
                output_rows=95,
                validation_rate=95.0,
                error_message=None,
            )
        }

        with patch.object(
            log_writer.data_processor, "process_step_results"
        ) as mock_process:
            with patch.object(
                log_writer.storage_manager, "create_table_if_not_exists"
            ) as mock_create:
                with patch.object(
                    log_writer.storage_manager, "write_batch"
                ) as mock_write:
                    with patch.object(
                        log_writer.performance_monitor, "start_operation"
                    ) as mock_start:
                        with patch.object(
                            log_writer.performance_monitor, "end_operation"
                        ) as mock_end:
                            # Setup mocks
                            mock_process.return_value = [Mock(spec=LogRow)]
                            mock_write.return_value = {
                                "rows_written": 1,
                                "success": True,
                            }
                            mock_end.return_value = {"duration_secs": 30.0}

                            result = log_writer.write_step_results(step_results)

                            assert result["success"] is True
                            assert "run_id" in result
                            assert "operation_id" in result
                            mock_process.assert_called_once()
                            mock_create.assert_called_once()
                            mock_write.assert_called_once()

    def test_show_logs(self, log_writer):
        """Test displaying logs."""
        with patch.object(log_writer.storage_manager, "query_logs") as mock_query:
            mock_df = Mock(spec=DataFrame)
            mock_df.show.return_value = None
            mock_query.return_value = mock_df

            log_writer.show_logs(limit=10)

            mock_query.assert_called_once_with(limit=10)
            mock_df.show.assert_called_once()

    def test_get_table_info(self, log_writer):
        """Test getting table information."""
        with patch.object(log_writer.storage_manager, "get_table_info") as mock_info:
            mock_info.return_value = {
                "table_name": "test_schema.test_table",
                "row_count": 100,
            }

            result = log_writer.get_table_info()

            assert result["table_name"] == "test_schema.test_table"
            assert result["row_count"] == 100
            mock_info.assert_called_once()

    def test_optimize_table(self, log_writer):
        """Test table optimization."""
        with patch.object(
            log_writer.storage_manager, "optimize_table"
        ) as mock_optimize:
            mock_optimize.return_value = {"optimization_completed": True}

            result = log_writer.optimize_table()

            assert result["optimization_completed"] is True
            mock_optimize.assert_called_once()

    def test_analyze_quality_trends(self, log_writer):
        """Test quality trends analysis."""
        with patch.object(log_writer.storage_manager, "query_logs") as mock_query:
            with patch.object(
                log_writer.quality_analyzer, "analyze_quality_trends"
            ) as mock_analyze:
                mock_df = Mock(spec=DataFrame)
                mock_query.return_value = mock_df
                mock_analyze.return_value = {"quality_grade": "A"}

                result = log_writer.analyze_quality_trends(days=30)

                assert result["quality_grade"] == "A"
                mock_query.assert_called_once()
                mock_analyze.assert_called_once_with(mock_df, 30)

    def test_get_metrics(self, log_writer):
        """Test getting writer metrics."""
        with patch.object(
            log_writer.performance_monitor, "get_metrics"
        ) as mock_metrics:
            mock_metrics.return_value = {"total_writes": 10, "successful_writes": 9}

            result = log_writer.get_metrics()

            assert result["total_writes"] == 10
            assert result["successful_writes"] == 9
            mock_metrics.assert_called_once()

    def test_reset_metrics(self, log_writer):
        """Test resetting writer metrics."""
        with patch.object(
            log_writer.performance_monitor, "reset_metrics"
        ) as mock_reset:
            log_writer.reset_metrics()
            mock_reset.assert_called_once()


class TestWriterExceptions:
    """Test cases for writer exceptions."""

    def test_writer_error(self):
        """Test WriterError exception."""
        error = WriterError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.context == {}
        assert error.suggestions == []
        assert error.cause is None

    def test_writer_error_with_context(self):
        """Test WriterError with context and suggestions."""
        error = WriterError(
            "Test error",
            context={"key": "value"},
            suggestions=["suggestion1", "suggestion2"],
            cause=Exception("cause"),
        )
        assert error.context == {"key": "value"}
        assert error.suggestions == ["suggestion1", "suggestion2"]
        assert error.cause is not None

    def test_writer_configuration_error(self):
        """Test WriterConfigurationError exception."""
        error = WriterConfigurationError("Config error", config_errors=["error1"])
        assert isinstance(error, WriterError)
        assert hasattr(error, "config_errors")

    def test_writer_validation_error(self):
        """Test WriterValidationError exception."""
        error = WriterValidationError("Validation error", validation_errors=["error1"])
        assert isinstance(error, WriterError)
        assert hasattr(error, "validation_errors")

    def test_writer_table_error(self):
        """Test WriterTableError exception."""
        error = WriterTableError(
            "Table error", table_name="test_table", operation="write"
        )
        assert isinstance(error, WriterError)
        assert error.table_name == "test_table"
        assert error.operation == "write"

    def test_writer_data_quality_error(self):
        """Test WriterDataQualityError exception."""
        error = WriterDataQualityError("Quality error", quality_issues=["issue1"])
        assert isinstance(error, WriterError)
        assert hasattr(error, "quality_issues")

    def test_writer_performance_error(self):
        """Test WriterPerformanceError exception."""
        error = WriterPerformanceError(
            "Performance error", performance_issues=["issue1"]
        )
        assert isinstance(error, WriterError)
        assert hasattr(error, "performance_issues")


class TestWriterModels:
    """Test cases for writer models."""

    def test_log_row_creation(self):
        """Test LogRow creation."""
        log_row = LogRow(
            run_id="test_run",
            run_mode="initial",
            run_started_at=datetime.now(),
            run_ended_at=datetime.now(),
            phase="bronze",
            step="test_step",
            start_time=datetime.now(),
            end_time=datetime.now(),
            table_fqn="test.table",
            write_mode="append",
            input_rows=100,
            output_rows=95,
            rows_written=95,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            execution_time=60.0,
            success=True,
            error_message=None,
            metadata={},
        )

        assert log_row.run_id == "test_run"
        assert log_row.phase == "bronze"
        assert log_row.validation_rate == 95.0

    def test_writer_config_validation(self):
        """Test WriterConfig validation."""
        # Valid config
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )

        # Should not raise exception
        config.validate()

        # Invalid config
        with pytest.raises(ValueError):
            WriterConfig(
                table_schema="",  # Empty schema
                table_name="test_table",
                write_mode=WriteMode.APPEND,
            )

    def test_writer_metrics(self):
        """Test WriterMetrics type."""
        metrics = WriterMetrics(
            total_writes=10,
            successful_writes=9,
            failed_writes=1,
            total_duration_secs=600.0,
            avg_write_duration_secs=60.0,
            total_rows_written=1000,
            memory_usage_peak_mb=512.0,
        )

        assert metrics["total_writes"] == 10
        assert metrics["successful_writes"] == 9
        assert metrics["failed_writes"] == 1


if __name__ == "__main__":
    pytest.main([__file__])
