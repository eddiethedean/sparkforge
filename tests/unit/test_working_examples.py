"""
Working unit tests that use actual SparkForge APIs.
"""

import os

import pytest

from pipeline_builder.execution import (
    ExecutionEngine,
    ExecutionMode,
    StepStatus,
    StepType,
)
from pipeline_builder.models import PipelineConfig, ValidationThresholds
from pipeline_builder.pipeline.builder import PipelineBuilder
from pipeline_builder.validation.pipeline_validation import (
    UnifiedValidator,
    ValidationResult,
)
from pipeline_builder.writer.core import LogWriter
from pipeline_builder.writer.models import LogLevel, WriteMode, WriterConfig


class TestWorkingExamples:
    """Working tests that use actual SparkForge APIs."""

    def test_pipeline_builder_basic(self, spark):
        """Test basic pipeline builder functionality."""
        builder = PipelineBuilder(spark=spark, schema="test_schema")
        assert builder.spark == spark
        assert builder.schema == "test_schema"

    def test_pipeline_builder_with_quality_rates(self, spark):
        """Test pipeline builder with custom quality rates."""
        builder = PipelineBuilder(
            spark=spark,
            schema="test_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=99.0,
        )
        # Check that the builder was created successfully
        assert builder.spark == spark
        assert builder.schema == "test_schema"

    def test_execution_engine_with_config(self, spark):
        """Test execution engine with proper config."""
        # Create proper config
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            verbose=True,
        )

        engine = ExecutionEngine(spark=spark, config=config)
        assert engine.spark == spark
        assert engine.config == config

    def test_unified_validator_basic(self, spark):
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

    def test_log_writer_with_config(self, spark):
        """Test LogWriter with proper config."""
        writer = LogWriter(
            spark=spark, schema="test_schema", table_name="test_logs"
        )
        assert writer.spark == spark
        assert writer.config.table_schema == "test_schema"
        assert writer.config.table_name == "test_logs"

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

        from pipeline_builder.execution import StepExecutionResult

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

        from pipeline_builder.execution import ExecutionResult, StepExecutionResult

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
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            verbose=True,
        )

        assert config.schema == "test_schema"
        assert config.thresholds == thresholds
        assert config.verbose is True

    def test_validation_thresholds_creation(self):
        """Test creating ValidationThresholds."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)

        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0

    def test_parallel_config_creation(self):
        """Test removed - ParallelConfig no longer exists."""
        # This test is no longer relevant as ParallelConfig has been removed
        pass

    def test_mock_spark_integration(self, spark, sample_dataframe):
        """Test integration with mock Spark session."""

        # Test basic DataFrame operations
        assert sample_dataframe.count() > 0
        assert len(sample_dataframe.columns) > 0

        # Test schema operations using standard Spark SQL
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
        spark.sql("DROP TABLE IF EXISTS test_schema.test_table")

        # Test table operations using standard Spark operations
        sample_dataframe.write.saveAsTable("test_schema.test_table")

        # Verify table exists using standard Spark operations
        table_df = spark.table("test_schema.test_table")
        assert table_df is not None
        assert table_df.count() > 0

    def test_error_handling(self, spark, spark_mode):
        """Test error handling with mock Spark-like engine."""
        from sparkless.testing import Mode
        if spark_mode == Mode.PYSPARK:
            pytest.skip("Mock-spark-specific test (uses mock-spark exceptions)")
        from sparkless.errors import AnalysisException  # type: ignore[import]
        from sparkless.sql.utils import IllegalArgumentException  # type: ignore[import]

        # Test table not found error
        with pytest.raises(AnalysisException):
            spark.table("nonexistent.table")

        # Test invalid parameters - sparkless raises TypeError, PySpark may raise IllegalArgumentException
        with pytest.raises((TypeError, IllegalArgumentException)):
            spark.createDataFrame("invalid_data", "invalid_schema")
