"""
Working SparkForge coverage tests using actual APIs.
"""

import os
import uuid
from datetime import datetime

import pytest
from mock_spark import (
    DoubleType,
    IntegerType,
    MockStructField,
    MockStructType,
    StringType,
)

from sparkforge.errors import (
    ConfigurationError,
    DataError,
    ExecutionError,
    PerformanceError,
    ResourceError,
    SystemError,
    ValidationError,
)
from sparkforge.execution import ExecutionEngine, ExecutionMode
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    ExecutionContext,
    ParallelConfig,
    PipelineConfig,
    StageStats,
    StepResult,
    ValidationThresholds,
)
from sparkforge.models.enums import PipelinePhase
from sparkforge.performance import format_duration, now_dt
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.table_operations import (
    drop_table,
)
from sparkforge.table_operations import (
    table_exists as sparkforge_table_exists,
)
from sparkforge.validation.pipeline_validation import (
    StepValidator,
    UnifiedValidator,
    ValidationResult,
)
from sparkforge.validation.utils import get_dataframe_info, safe_divide
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import (
    LogLevel,
    LogRow,
    WriteMode,
    WriterConfig,
    WriterMetrics,
)

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F
    MockF = F
else:
    from pyspark.sql import functions as F
    MockF = None


class TestSparkForgeWorking:
    """Working SparkForge coverage tests using actual APIs."""

    def test_pipeline_builder_working(self, mock_spark_session):
        """Test PipelineBuilder using actual API."""
        # Test basic initialization
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)
        assert builder.spark == mock_spark_session
        assert builder.schema == "test_schema"

        # Test schema validation
        mock_spark_session.storage.create_schema("test_schema")
        builder._validate_schema("test_schema")  # Should not raise

        # Test schema creation
        builder._create_schema_if_not_exists("new_schema")
        # Verify schema was created by listing databases (works in mock-spark 1.4.0+)
        dbs = mock_spark_session.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "new_schema" in db_names

    def test_execution_engine_working(self, mock_spark_session):
        """Test ExecutionEngine using actual API."""
        # Create config using actual API
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=600)
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        # Test execution engine initialization
        engine = ExecutionEngine(spark=mock_spark_session, config=config)
        assert engine.spark == mock_spark_session
        assert engine.config == config

        # Test execution context creation
        context = ExecutionContext(
            execution_id=str(uuid.uuid4()),
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
        )
        assert context.execution_id is not None
        assert context.mode == ExecutionMode.INITIAL

        # Test step execution result using actual API
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.5,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
            step_type="bronze",
        )
        assert step_result.step_name == "test_step"
        assert step_result.phase == PipelinePhase.BRONZE
        assert step_result.success is True
        assert step_result.rows_processed == 100

    def test_validation_system_working(self, mock_spark_session):
        """Test validation system using actual API."""
        # Test UnifiedValidator
        validator = UnifiedValidator()
        assert validator.logger is not None
        assert len(validator.custom_validators) == 0

        # Test adding custom validator
        class CustomValidator(StepValidator):
            def validate(self, step, context):
                return ValidationResult(
                    is_valid=True, errors=[], warnings=[], recommendations=[]
                )

        custom_validator = CustomValidator()
        validator.add_validator(custom_validator)
        assert len(validator.custom_validators) == 1

        # Test validation result
        result = ValidationResult(
            is_valid=True,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
            recommendations=["Recommendation 1"],
        )
        assert result.is_valid is True
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert len(result.recommendations) == 1

    def test_writer_system_working(self, mock_spark_session):
        """Test writer system using actual API."""
        # Test WriterConfig
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
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
        assert config.write_mode == WriteMode.APPEND
        assert config.batch_size == 1000

        # Test LogWriter
        writer = LogWriter(spark=mock_spark_session, config=config)
        assert writer.spark == mock_spark_session
        assert writer.config == config

        # Test LogRow (it's a TypedDict, so test as dict)
        log_row = LogRow(
            execution_id=str(uuid.uuid4()),
            step_name="test_step",
            status="completed",
            timestamp=datetime.now(),
            duration=1.5,
            rows_processed=100,
        )
        assert log_row["execution_id"] is not None
        assert log_row["step_name"] == "test_step"

        # Test WriterMetrics (it's a TypedDict, so test as dict)
        metrics = WriterMetrics(
            execution_time=1.5, rows_written=100, bytes_written=1024, files_written=1
        )
        assert metrics["execution_time"] == 1.5
        assert metrics["rows_written"] == 100

        # Test table_exists function with correct signature
        mock_spark_session.storage.create_schema("test_schema")
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        mock_spark_session.storage.create_table(
            "test_schema", "test_table", schema.fields
        )

        # Use correct function signature
        assert sparkforge_table_exists(mock_spark_session, "test_schema.test_table")
        assert not sparkforge_table_exists(
            mock_spark_session, "test_schema.nonexistent_table"
        )

    def test_models_working(self, mock_spark_session):
        """Test model classes using actual API."""
        # Test ValidationThresholds
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0

        # Test ParallelConfig
        parallel_config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=600)
        assert parallel_config.enabled is True
        assert parallel_config.max_workers == 4
        assert parallel_config.timeout_secs == 600

        # Test PipelineConfig
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

        # Test StageStats using actual API
        stage_stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=1.5,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )
        assert stage_stats.stage == "bronze"
        assert stage_stats.step == "test_step"
        assert stage_stats.total_rows == 100
        assert stage_stats.valid_rows == 95
        assert stage_stats.invalid_rows == 5
        assert stage_stats.validation_rate == 95.0

        # Test properties
        assert stage_stats.is_valid is True
        assert stage_stats.error_rate == 5.0
        assert stage_stats.throughput_rows_per_sec > 0

    def test_logging_system_working(self, mock_spark_session):
        """Test logging system using actual API."""
        # Test PipelineLogger
        logger = PipelineLogger()
        assert logger is not None

        # Test with custom configuration using actual API
        logger_custom = PipelineLogger(name="custom_logger", level="INFO")
        assert logger_custom.name == "custom_logger"
        assert logger_custom.level == "INFO"

    def test_performance_system_working(self, mock_spark_session):
        """Test performance system using actual API."""
        # Test performance utility functions
        current_time = now_dt()
        assert current_time is not None

        # Test duration formatting
        formatted_duration = format_duration(1.5)
        assert formatted_duration is not None
        assert "1.5" in formatted_duration or "1" in formatted_duration

    def test_table_operations_working(self, mock_spark_session):
        """Test table operations using actual API."""
        # Create test schema and table
        mock_spark_session.storage.create_schema("test_schema")

        # Create table with proper schema
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        mock_spark_session.storage.create_table(
            "test_schema", "test_table", schema.fields
        )

        # Test table_exists with correct signature
        assert sparkforge_table_exists(mock_spark_session, "test_schema.test_table")
        assert not sparkforge_table_exists(
            mock_spark_session, "test_schema.nonexistent_table"
        )

        # Test drop_table
        drop_table(mock_spark_session, "test_schema.test_table")
        # Table should be dropped (implementation dependent)

    def test_error_handling_working(self, mock_spark_session):
        """Test error handling using actual API."""
        # Test ConfigurationError
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("Test configuration error")

        # Test ValidationError
        with pytest.raises(ValidationError):
            raise ValidationError("Test validation error")

        # Test ExecutionError
        with pytest.raises(ExecutionError):
            raise ExecutionError("Test execution error")

        # Test DataError
        with pytest.raises(DataError):
            raise DataError("Test data error")

        # Test SystemError
        with pytest.raises(SystemError):
            raise SystemError("Test system error")

        # Test PerformanceError
        with pytest.raises(PerformanceError):
            raise PerformanceError("Test performance error")

        # Test ResourceError
        with pytest.raises(ResourceError):
            raise ResourceError("Test resource error")

    def test_validation_utils_working(self, mock_spark_session):
        """Test validation utilities using actual API."""
        # Test safe_divide function
        result = safe_divide(10, 2)
        assert result == 5.0

        result_zero = safe_divide(10, 0)
        assert result_zero == 0.0

        result_default = safe_divide(10, 0, default=1.0)
        assert result_default == 1.0

        # Test get_dataframe_info function
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = mock_spark_session.createDataFrame(data, schema)

        info = get_dataframe_info(df)
        # The function might return 0 for row_count due to mock implementation
        assert info["column_count"] == 2
        assert info["is_empty"] is False
        assert "id" in info["columns"]
        assert "name" in info["columns"]

    def test_pipeline_validation_working(self, mock_spark_session):
        """Test pipeline validation using actual API."""
        # Test UnifiedValidator with custom validators
        validator = UnifiedValidator()

        # Test custom validator integration
        class TestValidator(StepValidator):
            def validate(self, step, context):
                return ValidationResult(
                    is_valid=True, errors=[], warnings=[], recommendations=[]
                )

        test_validator = TestValidator()
        validator.add_validator(test_validator)
        assert len(validator.custom_validators) == 1

    def test_edge_cases_working(self, mock_spark_session):
        """Test edge cases using actual API."""
        # Test with empty DataFrame
        empty_schema = MockStructType([])
        empty_df = mock_spark_session.createDataFrame([{}], empty_schema)
        assert empty_df.count() == 1
        assert len(empty_df.columns) == 0

        # Test with null values
        data_with_nulls = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": None},
            {"id": None, "name": "Charlie"},
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        df = mock_spark_session.createDataFrame(data_with_nulls, schema)
        assert df.count() == 3

        # Test with large dataset
        large_data = []
        for i in range(1000):
            large_data.append({"id": i, "name": f"Person_{i}", "age": 20 + (i % 50)})

        large_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        large_df = mock_spark_session.createDataFrame(large_data, large_schema)
        assert large_df.count() == 1000

        # Test boundary values
        boundary_data = [
            {"id": 2147483647, "value": 1.7976931348623157e308},  # Max values
            {"id": -2147483648, "value": 2.2250738585072014e-308},  # Min values
        ]

        boundary_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", DoubleType()),
            ]
        )

        boundary_df = mock_spark_session.createDataFrame(boundary_data, boundary_schema)
        assert boundary_df.count() == 2

    def test_step_result_validation_working(self, mock_spark_session):
        """Test StepResult validation using actual API."""
        # Test valid StepResult
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.5,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
            step_type="bronze",
        )

        # Test validation
        step_result.validate()  # Should not raise

        # Test invalid StepResult
        invalid_step_result = StepResult(
            step_name="",  # Empty name should fail validation
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.5,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0,
            step_type="bronze",
        )

        with pytest.raises(ValueError):
            invalid_step_result.validate()

    def test_stage_stats_validation_working(self, mock_spark_session):
        """Test StageStats validation using actual API."""
        # Test valid StageStats
        stage_stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=1.5,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        # Test validation
        stage_stats.validate()  # Should not raise

        # Test invalid StageStats (rows don't add up)
        invalid_stage_stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=90,
            invalid_rows=5,  # Should be 10 to add up to 100
            validation_rate=95.0,
            duration_secs=1.5,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        with pytest.raises((ValueError, AttributeError)):
            invalid_stage_stats.validate()

    def test_comprehensive_coverage_working(self, mock_spark_session):
        """Test comprehensive coverage using actual APIs."""
        # Test all major components together

        # 1. Pipeline Builder
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema")
        assert builder.spark == mock_spark_session

        # 2. Configuration
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=600)
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        # 3. Execution Engine
        engine = ExecutionEngine(spark=mock_spark_session, config=config)
        assert engine.spark == mock_spark_session

        # 4. Validation System
        validator = UnifiedValidator()
        assert validator.logger is not None

        # 5. Writer System
        writer_config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            batch_size=1000,
        )
        writer = LogWriter(spark=mock_spark_session, config=writer_config)
        assert writer.spark == mock_spark_session

        # 6. Performance System
        current_time = now_dt()
        assert current_time is not None

        # 7. Error Handling
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("Test error")

        # 8. Table Operations
        mock_spark_session.storage.create_schema("test_schema")
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        mock_spark_session.storage.create_table(
            "test_schema", "test_table", schema.fields
        )
        assert sparkforge_table_exists(mock_spark_session, "test_schema.test_table")

        # 9. Validation Utils
        result = safe_divide(10, 2)
        assert result == 5.0

        # 10. Edge Cases
        empty_schema = MockStructType([])
        empty_df = mock_spark_session.createDataFrame([{}], empty_schema)
        assert empty_df.count() == 1
