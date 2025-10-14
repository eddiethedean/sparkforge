"""
Working unit tests that use actual SparkForge APIs.
"""

import pytest
from mock_spark.errors import AnalysisException

from sparkforge.execution import ExecutionEngine, ExecutionMode, StepStatus, StepType
from sparkforge.models import ParallelConfig, PipelineConfig, ValidationThresholds
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.validation.pipeline_validation import UnifiedValidator, ValidationResult
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import LogLevel, WriteMode, WriterConfig


class TestWorkingExamples:
    """Working tests that use actual SparkForge APIs."""

    def test_pipeline_builder_basic(self, mock_spark_session):
        """Test basic pipeline builder functionality."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema")
        assert builder.spark == mock_spark_session
        assert builder.schema == "test_schema"

    def test_pipeline_builder_with_quality_rates(self, mock_spark_session):
        """Test pipeline builder with custom quality rates."""
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="test_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=99.0,
        )
        # Check that the builder was created successfully
        assert builder.spark == mock_spark_session
        assert builder.schema == "test_schema"

    def test_execution_engine_with_config(self, mock_spark_session):
        """Test execution engine with proper config."""
        # Create proper config
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)
        assert engine.spark == mock_spark_session
        assert engine.config == config

    def test_unified_validator_basic(self, mock_spark_session):
        """Test unified validator basic functionality."""
        validator = UnifiedValidator()
        assert validator.logger is not None
        assert len(validator.custom_validators) == 0

    def test_validation_result_creation(self):
        """Test creating ValidationResult with correct parameters."""
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], recommendations=[]
        )
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        assert len(result.recommendations) == 0

    def test_validation_result_with_errors(self):
        """Test ValidationResult with errors."""
        errors = ["Error 1", "Error 2"]
        warnings = ["Warning 1"]
        recommendations = ["Fix this", "Fix that"]

        result = ValidationResult(
            is_valid=False,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations,
        )
        assert result.is_valid is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert len(result.recommendations) == 2
        assert "Error 1" in result.errors
        assert "Warning 1" in result.warnings

    def test_log_writer_with_config(self, mock_spark_session):
        """Test LogWriter with proper config."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        writer = LogWriter(spark=mock_spark_session, config=config)
        assert writer.spark == mock_spark_session
        assert writer.config == config

    def test_enum_values(self):
        """Test enum values match expected format."""
        # ExecutionMode
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

        # StepStatus
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

        # StepType
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

        # WriteMode
        assert WriteMode.APPEND.value == "append"
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.IGNORE.value == "ignore"
        assert WriteMode.MERGE.value == "merge"

        # LogLevel
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"

    def test_step_execution_result_creation(self):
        """Test creating StepExecutionResult."""
        from datetime import datetime

        from sparkforge.execution import StepExecutionResult

        start_time = datetime.now()
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            rows_processed=100,
        )

        assert result.step_name == "test_step"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.COMPLETED
        assert result.start_time == start_time
        assert result.rows_processed == 100

    def test_execution_result_creation(self):
        """Test creating ExecutionResult."""
        import uuid
        from datetime import datetime

        from sparkforge.execution import ExecutionResult, StepExecutionResult

        start_time = datetime.now()
        step_result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            rows_processed=100,
        )

        result = ExecutionResult(
            execution_id=str(uuid.uuid4()),
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            status="completed",
            steps=[step_result],
        )

        assert result.status == "completed"
        assert result.mode == ExecutionMode.INITIAL
        assert len(result.steps) == 1
        assert result.steps[0] == step_result

    def test_writer_config_creation(self):
        """Test creating WriterConfig with correct parameters."""
        config = WriterConfig(table_schema="test_schema", table_name="test_logs")

        assert config.table_schema == "test_schema"
        assert config.table_name == "test_logs"
        assert config.write_mode == WriteMode.APPEND  # Default value
        assert config.log_level == LogLevel.INFO  # Default value

    def test_writer_config_with_custom_values(self):
        """Test WriterConfig with custom values."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
            write_mode=WriteMode.OVERWRITE,
            log_level=LogLevel.DEBUG,
            batch_size=2000,
            compression="gzip",
        )

        assert config.table_schema == "test_schema"
        assert config.table_name == "test_logs"
        assert config.write_mode == WriteMode.OVERWRITE
        assert config.log_level == LogLevel.DEBUG
        assert config.batch_size == 2000
        assert config.compression == "gzip"

    def test_pipeline_config_creation(self):
        """Test creating PipelineConfig."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)

        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        assert config.schema == "test_schema"
        assert config.thresholds == thresholds
        assert config.parallel == parallel_config
        assert config.verbose is True

    def test_validation_thresholds_creation(self):
        """Test creating ValidationThresholds."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)

        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0

    def test_parallel_config_creation(self):
        """Test creating ParallelConfig."""
        config = ParallelConfig(enabled=True, max_workers=4)

        assert config.max_workers == 4
        assert config.enabled is True

    def test_mock_spark_integration(self, mock_spark_session, sample_dataframe):
        """Test integration with mock Spark session."""
        # Test basic DataFrame operations
        assert sample_dataframe.count() > 0
        assert len(sample_dataframe.columns) > 0

        # Test schema operations
        mock_spark_session.storage.create_schema("test_schema")
        assert mock_spark_session.storage.schema_exists("test_schema")

        # Test table operations
        mock_spark_session.storage.create_table(
            "test_schema", "test_table", sample_dataframe.schema.fields
        )
        assert mock_spark_session.storage.table_exists("test_schema", "test_table")

    def test_error_handling(self, mock_spark_session):
        """Test error handling with mock Spark."""
        # Test table not found error
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent.table")

        # Test invalid parameters - this should raise an exception
        with pytest.raises(Exception):
            mock_spark_session.createDataFrame("invalid_data", "invalid_schema")
