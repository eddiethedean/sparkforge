"""
Comprehensive tests for sparkforge.writer.core module.
"""

from datetime import datetime

import pytest
from mock_spark import (
    IntegerType,
    MockStructField,
    MockStructType,
)

from sparkforge.logging import PipelineLogger
from sparkforge.models import ExecutionMode, ExecutionResult, PipelinePhase, StepResult
from sparkforge.writer.core import LogWriter, time_write_operation
from sparkforge.table_operations import table_exists
from sparkforge.writer.exceptions import WriterConfigurationError
from sparkforge.writer.models import (
    LogLevel,
    LogRow,
    WriteMode,
    WriterConfig,
)


def create_execution_result(
    execution_id: str = "test-execution-123",
    mode: ExecutionMode = ExecutionMode.INITIAL,
) -> ExecutionResult:
    """Helper function to create ExecutionResult objects for testing."""
    from sparkforge.models import ExecutionContext, PipelineMetrics

    context = ExecutionContext(
        execution_id=execution_id, mode=mode, start_time=datetime.now()
    )

    step_results = [
        StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.5,
            rows_processed=100,
            rows_written=100,
            validation_rate=100.0,
        )
    ]

    metrics = PipelineMetrics(
        total_steps=1,
        successful_steps=1,
        failed_steps=0,
        total_duration=1.5,
        total_rows_processed=100,
        total_rows_written=100,
        avg_validation_rate=100.0,
    )

    return ExecutionResult(
        context=context, step_results=step_results, metrics=metrics, success=True
    )


# Writer tests now work with mock-spark 2.4.0
class TestWriterComprehensive:
    """Comprehensive tests for writer module."""

    def test_table_exists_function(self, mock_spark_session):
        """Test table_exists utility function."""
        # Test with non-existent table
        assert not table_exists(mock_spark_session, "test_schema.non_existent_table")

        # Test with existing table
        mock_spark_session.catalog.createDatabase("test_schema")
        schema = MockStructType([MockStructField("id", IntegerType())])
        data = [{"id": 1}, {"id": 2}]
        df = mock_spark_session.createDataFrame(data, schema)
        df.write.mode("overwrite").saveAsTable("test_schema.existing_table")

        assert table_exists(mock_spark_session, "test_schema.existing_table")

    def test_time_write_operation_function(self, mock_spark_session):
        """Test time_write_operation utility function."""

        def test_operation():
            return "test_result"

        rows_written, duration, start_time, end_time = time_write_operation(
            test_operation
        )

        assert rows_written == 0  # Default for non-DataFrame operations
        assert duration >= 0
        assert isinstance(start_time, datetime)
        assert isinstance(end_time, datetime)
        assert end_time >= start_time

    def test_writer_config_creation(self, mock_spark_session):
        """Test WriterConfig creation and validation."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            batch_size=1000,
            compression="snappy",
            max_file_size_mb=128,
            partition_columns=["date"],
            partition_count=10,
            enable_schema_evolution=True,
            schema_validation_mode="strict",
            auto_optimize_schema=True,
        )

        assert config.table_schema == "test_schema"
        assert config.table_name == "test_table"
        assert config.write_mode == WriteMode.APPEND
        assert config.log_level == LogLevel.INFO
        assert config.batch_size == 1000
        assert config.compression == "snappy"
        assert config.max_file_size_mb == 128
        assert config.partition_columns == ["date"]
        assert config.partition_count == 10
        assert config.enable_schema_evolution is True
        assert config.schema_validation_mode == "strict"
        assert config.auto_optimize_schema is True

    def test_writer_config_defaults(self, mock_spark_session):
        """Test WriterConfig with default values."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        assert config.batch_size == 1000
        assert config.compression == "snappy"
        assert config.max_file_size_mb == 128
        assert config.partition_columns is None
        assert config.partition_count is None  # Default is None, not 10
        assert config.enable_schema_evolution is True
        assert config.schema_validation_mode == "strict"
        assert config.auto_optimize_schema is True

    def test_log_writer_initialization(self, mock_spark_session):
        """Test LogWriter initialization."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        writer = LogWriter(spark=mock_spark_session, config=config)

        assert writer.spark == mock_spark_session
        assert writer.config == config
        assert isinstance(writer.logger, PipelineLogger)
        assert writer.table_fqn == "test_schema.test_table"
        assert isinstance(writer.metrics, dict)
        assert writer.metrics["total_writes"] == 0
        assert writer.metrics["successful_writes"] == 0
        assert writer.metrics["failed_writes"] == 0

    def test_log_writer_with_custom_logger(self, mock_spark_session):
        """Test LogWriter with custom logger."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        custom_logger = PipelineLogger("CustomLogger")
        writer = LogWriter(
            spark=mock_spark_session, config=config, logger=custom_logger
        )

        assert writer.logger == custom_logger

    def test_log_writer_invalid_config(self, mock_spark_session):
        """Test LogWriter with invalid configuration."""
        # Create invalid config (missing required fields)
        config = WriterConfig(
            table_schema="",  # Empty schema name
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        with pytest.raises(WriterConfigurationError):
            LogWriter(spark=mock_spark_session, config=config)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_write_execution_result(self, mock_spark_session):
        """Test writing execution result."""
        # Create config and writer
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Create execution result
        from sparkforge.models import ExecutionContext, PipelineMetrics

        context = ExecutionContext(
            execution_id="test-execution-123",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
        )

        step_results = [
            StepResult(
                step_name="test_step",
                phase=PipelinePhase.BRONZE,
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=1.5,
                rows_processed=100,
                rows_written=100,
                validation_rate=100.0,
            )
        ]

        metrics = PipelineMetrics(
            total_steps=1,
            successful_steps=1,
            failed_steps=0,
            total_duration=1.5,
            total_rows_processed=100,
            total_rows_written=100,
            avg_validation_rate=100.0,
        )

        execution_result = ExecutionResult(
            context=context, step_results=step_results, metrics=metrics, success=True
        )

        # Write execution result
        result = writer.write_execution_result(execution_result)

        assert isinstance(result, dict)
        assert "operation_id" in result
        assert "operation_metrics" in result
        assert writer.metrics["total_writes"] == 1
        assert writer.metrics["successful_writes"] == 1

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_write_execution_result_with_metadata(self, mock_spark_session):
        """Test writing execution result with metadata."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        metadata = {"environment": "test", "version": "1.0.0"}
        result = writer.write_execution_result(
            execution_result,
            run_id="custom-run-123",
            run_mode="test",
            metadata=metadata,
        )

        assert isinstance(result, dict)
        assert writer.metrics["total_writes"] == 1

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_write_step_results(self, mock_spark_session):
        """Test writing step results."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Create step results
        step_results = {
            "bronze_step": StepResult(
                step_name="bronze_step",
                phase=PipelinePhase.BRONZE,
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=1.5,
                rows_processed=100,
                rows_written=100,
                validation_rate=100.0,
            ),
            "silver_step": StepResult(
                step_name="silver_step",
                phase=PipelinePhase.SILVER,
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=2.0,
                rows_processed=100,
                rows_written=100,
                validation_rate=98.0,
            ),
        }

        result = writer.write_step_results(step_results)

        assert isinstance(result, dict)
        assert writer.metrics["total_writes"] == 1

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_write_log_rows(self, mock_spark_session):
        """Test writing log rows directly."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Create log rows
        log_rows = [
            LogRow(
                run_id="test-run-123",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="test-execution-123",
                pipeline_id="test-pipeline",
                schema="test_schema",
                phase="bronze",
                step_name="test_step",
                step_type="bronze",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=1.5,
                table_fqn="test_schema.test_table",
                write_mode="append",
                input_rows=100,
                output_rows=100,
                rows_written=100,
                rows_processed=100,
                valid_rows=100,
                invalid_rows=0,
                validation_rate=100.0,
                success=True,
                error_message=None,
                memory_usage_mb=10.5,
                cpu_usage_percent=25.0,
                metadata=None,
            )
        ]

        result = writer.write_log_rows(log_rows)

        assert isinstance(result, dict)
        assert writer.metrics["total_writes"] == 1

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_write_execution_result_batch(self, mock_spark_session):
        """Test writing multiple execution results in batch."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Create multiple execution results
        execution_results = [
            create_execution_result("test-execution-1", ExecutionMode.INITIAL),
            create_execution_result("test-execution-2", ExecutionMode.INCREMENTAL),
        ]

        result = writer.write_execution_result_batch(execution_results)

        assert isinstance(result, dict)
        assert writer.metrics["total_writes"] == 1  # Batch write counts as one write

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_metrics_tracking(self, mock_spark_session):
        """Test that writer metrics are properly tracked."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Initial metrics
        assert writer.metrics["total_writes"] == 0
        assert writer.metrics["successful_writes"] == 0
        assert writer.metrics["failed_writes"] == 0
        assert writer.metrics["total_duration_secs"] == 0.0
        assert writer.metrics["avg_write_duration_secs"] == 0.0
        assert writer.metrics["total_rows_written"] == 0
        assert writer.metrics["memory_usage_peak_mb"] == 0.0

        # Write some data
        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        writer.write_execution_result(execution_result)

        # Check metrics updated
        assert writer.metrics["total_writes"] == 1
        assert writer.metrics["successful_writes"] == 1
        assert writer.metrics["failed_writes"] == 0
        assert (
            writer.metrics["total_duration_secs"] >= 0
        )  # Duration might be 0 for very fast operations
        assert (
            writer.metrics["avg_write_duration_secs"] >= 0
        )  # Duration might be 0 for very fast operations

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_with_different_write_modes(self, mock_spark_session):
        """Test writer with different write modes."""
        # Test APPEND mode
        config_append = WriterConfig(
            table_schema="test_schema",
            table_name="test_table_append",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer_append = LogWriter(spark=mock_spark_session, config=config_append)

        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        result_append = writer_append.write_execution_result(execution_result)
        assert isinstance(result_append, dict)

        # Test OVERWRITE mode
        config_overwrite = WriterConfig(
            table_schema="test_schema",
            table_name="test_table_overwrite",
            write_mode=WriteMode.OVERWRITE,
            log_level=LogLevel.INFO,
        )
        writer_overwrite = LogWriter(spark=mock_spark_session, config=config_overwrite)

        result_overwrite = writer_overwrite.write_execution_result(execution_result)
        assert isinstance(result_overwrite, dict)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_with_different_log_levels(self, mock_spark_session):
        """Test writer with different log levels."""
        log_levels = [
            LogLevel.DEBUG,
            LogLevel.INFO,
            LogLevel.WARNING,
            LogLevel.ERROR,
            LogLevel.CRITICAL,
        ]

        for log_level in log_levels:
            config = WriterConfig(
                table_schema="test_schema",
                table_name=f"test_table_{log_level.value}",
                write_mode=WriteMode.APPEND,
                log_level=log_level,
            )
            writer = LogWriter(spark=mock_spark_session, config=config)

            execution_result = create_execution_result(
                f"test-execution-{log_level.value}", ExecutionMode.INITIAL
            )

            result = writer.write_execution_result(execution_result)
            assert isinstance(result, dict)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_with_custom_batch_size(self, mock_spark_session):
        """Test writer with custom batch size."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            batch_size=500,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        assert writer.config.batch_size == 500

        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        result = writer.write_execution_result(execution_result)
        assert isinstance(result, dict)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_with_compression_settings(self, mock_spark_session):
        """Test writer with different compression settings."""
        compression_options = ["snappy", "gzip", "lz4", "zstd"]

        for compression in compression_options:
            config = WriterConfig(
                table_schema="test_schema",
                table_name=f"test_table_{compression}",
                write_mode=WriteMode.APPEND,
                log_level=LogLevel.INFO,
                compression=compression,
            )
            writer = LogWriter(spark=mock_spark_session, config=config)

            assert writer.config.compression == compression

            execution_result = create_execution_result(
                f"test-execution-{compression}", ExecutionMode.INITIAL
            )

            result = writer.write_execution_result(execution_result)
            assert isinstance(result, dict)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_with_partition_settings(self, mock_spark_session):
        """Test writer with partition settings."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            partition_columns=["date", "hour"],
            partition_count=20,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        assert writer.config.partition_columns == ["date", "hour"]
        assert writer.config.partition_count == 20

        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        result = writer.write_execution_result(execution_result)
        assert isinstance(result, dict)

    # No patch needed - F.current_timestamp works directly with mock-spark
    def test_writer_schema_evolution_settings(self, mock_spark_session):
        """Test writer with schema evolution settings."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            enable_schema_evolution=True,
            schema_validation_mode="lenient",  # Use valid value
            auto_optimize_schema=True,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        assert writer.config.enable_schema_evolution is True
        assert writer.config.schema_validation_mode == "lenient"
        assert writer.config.auto_optimize_schema is True

        execution_result = create_execution_result(
            "test-execution-123", ExecutionMode.INITIAL
        )

        result = writer.write_execution_result(execution_result)
        assert isinstance(result, dict)

    def test_writer_error_handling(self, mock_spark_session):
        """Test writer error handling."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Test with invalid execution result (missing required fields)
        with pytest.raises((ValueError, TypeError, AttributeError)):
            writer.write_execution_result(None)

    def test_writer_components_initialization(self, mock_spark_session):
        """Test that all writer components are properly initialized."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Check that all components are initialized
        assert hasattr(writer, "data_processor")
        assert hasattr(writer, "storage_manager")
        assert hasattr(writer, "performance_monitor")
        assert hasattr(writer, "analytics_engine")
        assert hasattr(writer, "quality_analyzer")
        assert hasattr(writer, "trend_analyzer")

        # Check that components have the expected types
        assert writer.data_processor is not None
        assert writer.storage_manager is not None
        assert writer.performance_monitor is not None
        assert writer.analytics_engine is not None
        assert writer.quality_analyzer is not None
        assert writer.trend_analyzer is not None

    def test_writer_schema_creation(self, mock_spark_session):
        """Test that writer schema is properly created."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        # Check that schema is created
        assert hasattr(writer, "schema")
        assert writer.schema is not None

        # Check that schema has the expected structure
        # The schema is a PySpark StructType, not MockStructType
        assert hasattr(writer.schema, "fields")
        assert len(writer.schema.fields) > 0

    def test_writer_table_fqn(self, mock_spark_session):
        """Test that table FQN is properly constructed."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer = LogWriter(spark=mock_spark_session, config=config)

        assert writer.table_fqn == "test_schema.test_table"

        # Test with different schema and table names
        config2 = WriterConfig(
            table_schema="another_schema",
            table_name="another_table",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )
        writer2 = LogWriter(spark=mock_spark_session, config=config2)

        assert writer2.table_fqn == "another_schema.another_table"
