"""
Comprehensive tests for sparkforge.execution module.

This module tests all execution engine functionality including step execution,
pipeline execution, error handling, and various execution modes.
"""

import os
from datetime import datetime
from unittest.mock import patch

import pytest
from mock_spark import (
    IntegerType,
    MockStructField,
    MockStructType,
    StringType,
)

from sparkforge.errors import ExecutionError
from sparkforge.execution import (
    ExecutionEngine,
    ExecutionMode,
    ExecutionResult,
    StepExecutionResult,
    StepStatus,
    StepType,
)
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    BronzeStep,
    ParallelConfig,
    PipelineConfig,
    ValidationThresholds,
)

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F
    MockF = F
else:
    from pyspark.sql import functions as F
    MockF = None


class TestExecutionMode:
    """Test ExecutionMode enum."""

    def test_execution_mode_values(self):
        """Test ExecutionMode enum values."""
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

    def test_execution_mode_enumeration(self):
        """Test ExecutionMode enumeration."""
        modes = list(ExecutionMode)
        assert len(modes) == 4
        assert ExecutionMode.INITIAL in modes
        assert ExecutionMode.INCREMENTAL in modes
        assert ExecutionMode.FULL_REFRESH in modes
        assert ExecutionMode.VALIDATION_ONLY in modes


class TestStepStatus:
    """Test StepStatus enum."""

    def test_step_status_values(self):
        """Test StepStatus enum values."""
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

    def test_step_status_enumeration(self):
        """Test StepStatus enumeration."""
        statuses = list(StepStatus)
        assert len(statuses) == 5
        assert StepStatus.PENDING in statuses
        assert StepStatus.RUNNING in statuses
        assert StepStatus.COMPLETED in statuses
        assert StepStatus.FAILED in statuses
        assert StepStatus.SKIPPED in statuses


class TestStepType:
    """Test StepType enum."""

    def test_step_type_values(self):
        """Test StepType enum values."""
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

    def test_step_type_enumeration(self):
        """Test StepType enumeration."""
        types = list(StepType)
        assert len(types) == 3
        assert StepType.BRONZE in types
        assert StepType.SILVER in types
        assert StepType.GOLD in types


class TestStepExecutionResult:
    """Test StepExecutionResult dataclass."""

    def test_step_execution_result_creation(self):
        """Test creating StepExecutionResult."""
        start_time = datetime.now()
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.RUNNING,
            start_time=start_time,
        )

        assert result.step_name == "test_step"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.RUNNING
        assert result.start_time == start_time
        assert result.end_time is None
        assert result.duration is None
        assert result.error is None
        assert result.rows_processed is None
        assert result.output_table is None

    def test_step_execution_result_with_end_time(self):
        """Test StepExecutionResult with end_time calculates duration."""
        start_time = datetime.now()
        end_time = datetime.now()

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
        )

        assert result.duration is not None
        assert result.duration >= 0

    def test_step_execution_result_with_all_fields(self):
        """Test StepExecutionResult with all fields."""
        start_time = datetime.now()
        end_time = datetime.now()

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.SILVER,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
            error="Test error",
            rows_processed=100,
            output_table="test_schema.test_table",
        )

        assert result.step_name == "test_step"
        assert result.step_type == StepType.SILVER
        assert result.status == StepStatus.COMPLETED
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.duration is not None
        assert result.error == "Test error"
        assert result.rows_processed == 100
        assert result.output_table == "test_schema.test_table"


class TestExecutionResult:
    """Test ExecutionResult dataclass."""

    def test_execution_result_creation(self):
        """Test creating ExecutionResult."""
        start_time = datetime.now()
        result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
        )

        assert result.execution_id == "test_execution"
        assert result.mode == ExecutionMode.INITIAL
        assert result.start_time == start_time
        assert result.end_time is None
        assert result.duration is None
        assert result.status == "running"
        assert result.steps == []
        assert result.error is None

    def test_execution_result_with_end_time(self):
        """Test ExecutionResult with end_time calculates duration."""
        start_time = datetime.now()
        end_time = datetime.now()

        result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
        )

        assert result.duration is not None
        assert result.duration >= 0

    def test_execution_result_with_steps(self):
        """Test ExecutionResult with steps."""
        start_time = datetime.now()
        step_result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
        )

        result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            steps=[step_result],
        )

        assert len(result.steps) == 1
        assert result.steps[0] == step_result


class TestExecutionEngineInitialization:
    """Test ExecutionEngine initialization."""

    def test_execution_engine_initialization(self, spark_session):
        """Test basic ExecutionEngine initialization."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )

        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        assert engine.spark == spark_session
        assert engine.config == config
        assert isinstance(engine.logger, PipelineLogger)

    def test_execution_engine_with_custom_logger(self, spark_session):
        """Test ExecutionEngine with custom logger."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        custom_logger = PipelineLogger()

        engine = ExecutionEngine(
            spark=spark_session, config=config, logger=custom_logger, functions=MockF
        )

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger == custom_logger

    def test_execution_engine_with_none_logger(self, spark_session):
        """Test ExecutionEngine with None logger creates default."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )

        engine = ExecutionEngine(spark=spark_session, config=config, logger=None, functions=MockF)

        assert engine.spark == spark_session
        assert engine.config == config
        assert isinstance(engine.logger, PipelineLogger)


class TestExecuteStep:
    """Test execute_step method."""

    def test_execute_bronze_step_success(self, spark_session):
        """Test successful bronze step execution."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create bronze step
        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})

        # Execute step
        context = {"test_bronze": test_df}
        result = engine.execute_step(bronze_step, context, ExecutionMode.INITIAL)

        assert result.step_name == "test_bronze"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.COMPLETED
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.duration is not None
        assert result.rows_processed == 2
        assert result.output_table is None  # Bronze steps don't write to tables

    def test_execute_step_unknown_step_type(self, spark_session):
        """Test execute_step with unknown step type."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create mock step with unknown type
        class UnknownStep:
            def __init__(self):
                self.name = "unknown_step"

        unknown_step = UnknownStep()
        context = {}

        with pytest.raises(ValueError, match="Unknown step type"):
            engine.execute_step(unknown_step, context, ExecutionMode.INITIAL)

    def test_execute_bronze_step_missing_context(self, spark_session):
        """Test bronze step execution with missing context data."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})
        context = {}  # Empty context

        with pytest.raises(
            ExecutionError, match="Bronze step 'test_bronze' requires data"
        ):
            engine.execute_step(bronze_step, context, ExecutionMode.INITIAL)


class TestExecutePipeline:
    """Test execute_pipeline method."""

    def test_execute_pipeline_with_none_context(self, spark_session):
        """Test pipeline execution with None context."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})

        # Execute pipeline with None context
        result = engine.execute_pipeline(
            steps=[bronze_step], mode=ExecutionMode.INITIAL, context=None
        )

        assert result.execution_id is not None
        assert result.mode == ExecutionMode.INITIAL
        assert result.status == "failed"  # Should fail due to missing context
        assert len(result.steps) == 1
        assert result.steps[0].status == StepStatus.FAILED

    def test_execute_pipeline_invalid_context_type(self, spark_session):
        """Test pipeline execution with invalid context type."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})

        # Execute pipeline with invalid context type
        with pytest.raises(
            ExecutionError,
            match="Pipeline execution failed: context must be a dictionary",
        ):
            engine.execute_pipeline(
                steps=[bronze_step],
                mode=ExecutionMode.INITIAL,
                context="invalid_context",  # String instead of dict
            )

    def test_execute_pipeline_empty_steps(self, spark_session):
        """Test pipeline execution with empty steps list."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Execute pipeline with empty steps
        result = engine.execute_pipeline(
            steps=[], mode=ExecutionMode.INITIAL, context={}
        )

        assert result.status == "completed"
        assert len(result.steps) == 0


class TestPrivateMethods:
    """Test private execution methods."""

    def test_execute_bronze_step_empty_dataframe(self, spark_session):
        """Test bronze step execution with empty DataFrame."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create empty DataFrame
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        empty_df = spark_session.createDataFrame([], schema)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})
        context = {"test_bronze": empty_df}

        # Should not raise exception, just log warning
        with patch.object(engine.logger, "warning") as mock_warning:
            result = engine._execute_bronze_step(bronze_step, context)
            assert result == empty_df
            mock_warning.assert_called_once()


class TestBackwardCompatibility:
    """Test backward compatibility aliases."""

    def test_unified_execution_engine_alias(self):
        """Test UnifiedExecutionEngine alias."""
        from sparkforge.execution import UnifiedExecutionEngine

        assert UnifiedExecutionEngine == ExecutionEngine

    def test_unified_step_execution_result_alias(self):
        """Test UnifiedStepExecutionResult alias."""
        from sparkforge.execution import UnifiedStepExecutionResult

        assert UnifiedStepExecutionResult == StepExecutionResult


class TestExecutionIntegration:
    """Test execution engine integration scenarios."""

    def test_different_execution_modes(self, spark_session):
        """Test different execution modes."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})
        context = {"test_bronze": test_df}

        # Test different execution modes
        for mode in ExecutionMode:
            result = engine.execute_step(bronze_step, context, mode)
            assert result.status == StepStatus.COMPLETED
            assert result.step_name == "test_bronze"

    def test_logging_integration(self, spark_session):
        """Test logging integration during execution."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        bronze_step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})
        context = {"test_bronze": test_df}

        # Test logging during step execution
        with patch.object(engine.logger, "info") as mock_info:
            engine.execute_step(bronze_step, context, ExecutionMode.INITIAL)

            # Verify logging calls
            assert mock_info.call_count >= 2  # Start and completion
            # Check for new logging format with emojis and uppercase
            mock_info.assert_any_call("🚀 Starting BRONZE step: test_bronze")
            # Note: Completed message includes timing and metrics, just check it was called
            assert any("Completed BRONZE step: test_bronze" in str(call) for call in mock_info.call_args_list)
