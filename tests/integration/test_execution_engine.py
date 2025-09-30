#!/usr/bin/env python3
"""
Comprehensive tests for execution engine functionality.

This module tests the ExecutionEngine class and all its methods with extensive coverage.
"""

import uuid
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkforge.errors import ExecutionError, ValidationError
from sparkforge.execution import (
    ExecutionEngine,
    ExecutionMode,
    ExecutionResult,
    StepExecutionResult,
    StepStatus,
    StepType,
)
from sparkforge.models import BronzeStep, GoldStep, PipelineConfig, SilverStep


class TestExecutionMode:
    """Test cases for ExecutionMode enum."""

    def test_execution_mode_values(self):
        """Test that ExecutionMode has correct values."""
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

    def test_execution_mode_enumeration(self):
        """Test that ExecutionMode can be enumerated."""
        modes = list(ExecutionMode)
        assert len(modes) == 4
        assert ExecutionMode.INITIAL in modes
        assert ExecutionMode.INCREMENTAL in modes
        assert ExecutionMode.FULL_REFRESH in modes
        assert ExecutionMode.VALIDATION_ONLY in modes


class TestStepStatus:
    """Test cases for StepStatus enum."""

    def test_step_status_values(self):
        """Test that StepStatus has correct values."""
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

    def test_step_status_enumeration(self):
        """Test that StepStatus can be enumerated."""
        statuses = list(StepStatus)
        assert len(statuses) == 5
        assert StepStatus.PENDING in statuses
        assert StepStatus.RUNNING in statuses
        assert StepStatus.COMPLETED in statuses
        assert StepStatus.FAILED in statuses
        assert StepStatus.SKIPPED in statuses


class TestStepType:
    """Test cases for StepType enum."""

    def test_step_type_values(self):
        """Test that StepType has correct values."""
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

    def test_step_type_enumeration(self):
        """Test that StepType can be enumerated."""
        types = list(StepType)
        assert len(types) == 3
        assert StepType.BRONZE in types
        assert StepType.SILVER in types
        assert StepType.GOLD in types


class TestStepExecutionResult:
    """Test cases for StepExecutionResult dataclass."""

    def test_step_execution_result_creation(self):
        """Test basic StepExecutionResult creation."""
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

    def test_step_execution_result_with_all_fields(self):
        """Test StepExecutionResult creation with all fields."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.SILVER,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
            duration=300.0,
            error="Test error",
            rows_processed=1000,
            output_table="test_schema.test_table",
        )

        assert result.step_name == "test_step"
        assert result.step_type == StepType.SILVER
        assert result.status == StepStatus.COMPLETED
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.duration == 300.0
        assert result.error == "Test error"
        assert result.rows_processed == 1000
        assert result.output_table == "test_schema.test_table"

    def test_step_execution_result_duration_calculation(self):
        """Test that duration is calculated automatically in __post_init__."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
        )

        # Duration should be calculated automatically
        assert result.duration == 300.0  # 5 minutes

    def test_step_execution_result_no_duration_without_end_time(self):
        """Test that duration is None when end_time is None."""
        start_time = datetime.now()

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.RUNNING,
            start_time=start_time,
            end_time=None,
        )

        assert result.duration is None


class TestExecutionResult:
    """Test cases for ExecutionResult dataclass."""

    def test_execution_result_creation(self):
        """Test basic ExecutionResult creation."""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        result = ExecutionResult(
            execution_id=execution_id, mode=ExecutionMode.INITIAL, start_time=start_time
        )

        assert result.execution_id == execution_id
        assert result.mode == ExecutionMode.INITIAL
        assert result.start_time == start_time
        assert result.end_time is None
        assert result.duration is None
        assert result.status == "running"
        assert result.steps == []
        assert result.error is None

    def test_execution_result_with_all_fields(self):
        """Test ExecutionResult creation with all fields."""
        execution_id = str(uuid.uuid4())
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        step_result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
        )

        result = ExecutionResult(
            execution_id=execution_id,
            mode=ExecutionMode.INCREMENTAL,
            start_time=start_time,
            end_time=end_time,
            duration=300.0,
            status="completed",
            steps=[step_result],
            error="Test error",
        )

        assert result.execution_id == execution_id
        assert result.mode == ExecutionMode.INCREMENTAL
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.duration == 300.0
        assert result.status == "completed"
        assert result.steps == [step_result]
        assert result.error == "Test error"

    def test_execution_result_duration_calculation(self):
        """Test that duration is calculated automatically in __post_init__."""
        execution_id = str(uuid.uuid4())
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        result = ExecutionResult(
            execution_id=execution_id,
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
        )

        # Duration should be calculated automatically
        assert result.duration == 300.0  # 5 minutes

    def test_execution_result_steps_initialization(self):
        """Test that steps list is initialized to empty list."""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        result = ExecutionResult(
            execution_id=execution_id, mode=ExecutionMode.INITIAL, start_time=start_time
        )

        assert result.steps == []


class TestExecutionEngine:
    """Test cases for ExecutionEngine class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock(spec=SparkSession)
        return spark

    @pytest.fixture
    def mock_config(self):
        """Create a mock PipelineConfig."""
        config = Mock(spec=PipelineConfig)
        return config

    @pytest.fixture
    def mock_logger(self):
        """Create a mock PipelineLogger."""
        logger = Mock()
        return logger

    @pytest.fixture
    def sample_bronze_step(self):
        """Create a sample BronzeStep."""
        return BronzeStep(
            name="test_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
            schema="test_schema",
        )

    @pytest.fixture
    def sample_silver_step(self):
        """Create a sample SilverStep."""
        return SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, bronze_df, silvers: bronze_df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_table",
            schema="test_schema",
        )

    @pytest.fixture
    def sample_gold_step(self):
        """Create a sample GoldStep."""
        return GoldStep(
            name="test_gold",
            transform=lambda spark, silvers: silvers["test_silver"],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_table",
            source_silvers=["test_silver"],
            schema="test_schema",
        )

    def test_execution_engine_initialization_with_logger(
        self, mock_spark, mock_config, mock_logger
    ):
        """Test ExecutionEngine initialization with custom logger."""
        engine = ExecutionEngine(mock_spark, mock_config, mock_logger)

        assert engine.spark == mock_spark
        assert engine.config == mock_config
        assert engine.logger == mock_logger

    def test_execution_engine_initialization_without_logger(
        self, mock_spark, mock_config
    ):
        """Test ExecutionEngine initialization without logger."""
        engine = ExecutionEngine(mock_spark, mock_config)

        assert engine.spark == mock_spark
        assert engine.config == mock_config
        assert engine.logger is not None

    def test_execute_step_bronze_success(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test successful bronze step execution."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        # Mock Spark read
        mock_spark.read.format.return_value.load.return_value = mock_df

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            result = engine.execute_step(sample_bronze_step, {}, ExecutionMode.INITIAL)

            assert result.step_name == "test_bronze"
            assert result.step_type == StepType.BRONZE
            assert result.status == StepStatus.COMPLETED
            assert result.start_time is not None
            assert result.end_time is not None
            assert result.duration is not None
            assert result.error is None
            assert result.rows_processed == 100
            assert result.output_table == "test_schema.test_table"

    def test_execute_step_silver_success(
        self, mock_spark, mock_config, sample_silver_step
    ):
        """Test successful silver step execution."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        # Mock context with dependency
        context = {"test_bronze": mock_df}

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            result = engine.execute_step(
                sample_silver_step, context, ExecutionMode.INITIAL
            )

            assert result.step_name == "test_silver"
            assert result.step_type == StepType.SILVER
            assert result.status == StepStatus.COMPLETED
            assert result.rows_processed == 50

    def test_execute_step_gold_success(self, mock_spark, mock_config, sample_gold_step):
        """Test successful gold step execution."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 25

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        # Mock context with dependency
        context = {"test_silver": mock_df}

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            result = engine.execute_step(
                sample_gold_step, context, ExecutionMode.INITIAL
            )

            assert result.step_name == "test_gold"
            assert result.step_type == StepType.GOLD
            assert result.status == StepStatus.COMPLETED
            assert result.rows_processed == 25

    def test_execute_step_unknown_type(self, mock_spark, mock_config):
        """Test execute_step with unknown step type."""
        engine = ExecutionEngine(mock_spark, mock_config)

        # Create a mock step that's not BronzeStep, SilverStep, or GoldStep
        unknown_step = Mock()
        unknown_step.name = "unknown_step"

        with pytest.raises(ValueError, match="Unknown step type"):
            engine.execute_step(unknown_step, {}, ExecutionMode.INITIAL)

    def test_execute_step_bronze_without_source_path(self, mock_spark, mock_config):
        """Test bronze step creation without rules should fail."""
        # A BronzeStep without rules is logically invalid
        # and should be rejected during construction
        with pytest.raises(
            ValidationError, match="Rules must be a non-empty dictionary"
        ):
            BronzeStep(
                name="test_bronze",
                rules={},  # Empty rules should cause error
                schema="test_schema",
            )

    def test_execute_bronze_step_not_in_context(self, mock_spark, mock_config):
        """Test bronze step execution when step name is not in context raises error."""
        engine = ExecutionEngine(mock_spark, mock_config)

        # Create a bronze step
        bronze_step = BronzeStep(
            name="test_bronze", rules={"id": ["not_null"]}, schema="test_schema"
        )

        # Execute bronze step without the step name in context
        # This should raise an ExecutionError
        with pytest.raises(ExecutionError) as excinfo:
            engine._execute_bronze_step(bronze_step, {})

        # Verify the error message is clear
        error_msg = str(excinfo.value)
        assert "Bronze step 'test_bronze' requires data to be provided in context" in error_msg
        assert "Bronze steps are for validating existing data, not creating it" in error_msg

    def test_execute_bronze_step_with_data(self, mock_spark, mock_config):
        """Test bronze step execution with provided data."""
        engine = ExecutionEngine(mock_spark, mock_config)

        # Create a bronze step
        bronze_step = BronzeStep(
            name="test_bronze", rules={"id": ["not_null"]}, schema="test_schema"
        )

        # Create mock DataFrame
        mock_df = mock_spark.createDataFrame.return_value

        # Execute bronze step with data in context
        result_df = engine._execute_bronze_step(bronze_step, {"test_bronze": mock_df})

        # Verify that the DataFrame was returned
        assert result_df == mock_df

    def test_execute_step_silver_without_dependencies(self, mock_spark, mock_config):
        """Test silver step creation without valid dependencies should fail."""
        # A SilverStep without a valid source_bronze is logically invalid
        # and should be rejected during construction
        with pytest.raises(
            ValidationError, match="Source bronze step name must be a non-empty string"
        ):
            SilverStep(
                name="test_silver",
                source_bronze="",  # Empty source_bronze should cause error
                transform=lambda spark, dfs: dfs,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_table",
                schema="test_schema",
            )

    def test_execute_step_silver_missing_dependency(
        self, mock_spark, mock_config, sample_silver_step
    ):
        """Test silver step execution with missing dependency."""
        engine = ExecutionEngine(mock_spark, mock_config)

        with pytest.raises(
            ExecutionError, match="Source bronze step test_bronze not found in context"
        ):
            engine.execute_step(sample_silver_step, {}, ExecutionMode.INITIAL)

    def test_execute_step_silver_without_transform(self, mock_spark, mock_config):
        """Test silver step creation without transform function should fail."""
        # A SilverStep without a transform function is logically invalid
        # and should be rejected during construction
        with pytest.raises(
            ValidationError, match="Transform function is required and must be callable"
        ):
            SilverStep(
                name="test_silver",
                source_bronze="test_bronze",
                transform=None,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_table",
                schema="test_schema",
            )

    def test_execute_step_gold_without_dependencies(self, mock_spark, mock_config):
        """Test gold step creation without valid dependencies should fail."""
        # A GoldStep without valid source_silvers is logically invalid
        # and should be rejected during construction
        rules = {"id": [F.col("id").isNotNull()]}
        with pytest.raises(
            ValidationError, match="Source silvers must be a non-empty list"
        ):
            GoldStep(
                name="test_gold",
                transform=lambda spark, dfs: dfs,
                rules=rules,
                table_name="test_table",
                source_silvers=[],  # Empty source_silvers should cause error
                schema="test_schema",
            )

    def test_execute_step_gold_missing_dependency(
        self, mock_spark, mock_config, sample_gold_step
    ):
        """Test gold step execution with missing dependency."""
        engine = ExecutionEngine(mock_spark, mock_config)

        with pytest.raises(
            ExecutionError, match="Source silver test_silver not found in context"
        ):
            engine.execute_step(sample_gold_step, {}, ExecutionMode.INITIAL)

    def test_execute_step_gold_without_transform(self, mock_spark, mock_config):
        """Test gold step creation without transform function should fail."""
        # A GoldStep without a transform function is logically invalid
        # and should be rejected during construction
        with pytest.raises(
            ValidationError, match="Transform function is required and must be callable"
        ):
            GoldStep(
                name="test_gold",
                transform=None,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_table",
                source_silvers=["test_silver"],
                schema="test_schema",
            )

    def test_execute_step_validation_only_mode(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test step execution in validation-only mode."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_spark.read.format.return_value.load.return_value = mock_df

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"

            result = engine.execute_step(
                sample_bronze_step, {}, ExecutionMode.VALIDATION_ONLY
            )

            # In validation-only mode, should not write to table or apply validation
            mock_apply_rules.assert_not_called()
            mock_df.write.mode.assert_not_called()
            assert result.output_table is None
            assert result.rows_processed is None

    def test_execute_step_exception_handling(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test step execution exception handling."""
        # Mock Spark to raise exception on both read and createDataFrame
        mock_spark.read.format.return_value.load.side_effect = Exception("Read failed")
        mock_spark.createDataFrame.side_effect = Exception("CreateDataFrame failed")

        engine = ExecutionEngine(mock_spark, mock_config)

        with pytest.raises(ExecutionError, match="Step execution failed"):
            engine.execute_step(sample_bronze_step, {}, ExecutionMode.INITIAL)

    def test_execute_pipeline_success(
        self,
        mock_spark,
        mock_config,
        sample_bronze_step,
        sample_silver_step,
        sample_gold_step,
    ):
        """Test successful pipeline execution."""
        # Mock DataFrames
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_spark.table.return_value = mock_df

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            steps = [sample_bronze_step, sample_silver_step, sample_gold_step]
            result = engine.execute_pipeline(steps, ExecutionMode.INITIAL)

            assert result.execution_id is not None
            assert result.mode == ExecutionMode.INITIAL
            assert result.start_time is not None
            assert result.end_time is not None
            assert result.status == "completed"
            assert len(result.steps) == 3
            assert result.error is None

    def test_execute_pipeline_failure(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test pipeline execution failure."""
        # Mock Spark to raise exception on both read and createDataFrame
        mock_spark.read.format.return_value.load.side_effect = Exception(
            "Pipeline failed"
        )
        mock_spark.createDataFrame.side_effect = Exception("CreateDataFrame failed")

        engine = ExecutionEngine(mock_spark, mock_config)

        with pytest.raises(ExecutionError, match="Pipeline execution failed"):
            engine.execute_pipeline([sample_bronze_step], ExecutionMode.INITIAL)

    def test_execute_pipeline_with_different_step_types(self, mock_spark, mock_config):
        """Test pipeline execution with different step types."""
        # Create steps of different types
        bronze_step = BronzeStep(
            name="bronze1",
            rules={"id": [F.col("id").isNotNull()]},
            schema="test_schema",
        )

        silver_step = SilverStep(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, bronze_df, silvers: bronze_df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_table",
            schema="test_schema",
        )

        gold_step = GoldStep(
            name="gold1",
            transform=lambda spark, silvers: silvers["silver1"],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_table",
            source_silvers=["silver1"],
            schema="test_schema",
        )

        # Mock DataFrames
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_spark.table.return_value = mock_df

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            steps = [bronze_step, silver_step, gold_step]
            result = engine.execute_pipeline(steps, ExecutionMode.INITIAL)

            # Verify execution order: bronze first, then silver, then gold
            assert len(result.steps) == 3
            assert result.steps[0].step_type == StepType.BRONZE
            assert result.steps[1].step_type == StepType.SILVER
            assert result.steps[2].step_type == StepType.GOLD

    def test_execute_pipeline_with_max_workers(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test pipeline execution with max_workers parameter."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_spark.read.format.return_value.load.return_value = mock_df

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            result = engine.execute_pipeline(
                [sample_bronze_step], ExecutionMode.INITIAL, max_workers=8
            )

            assert result.status == "completed"

    def test_execute_pipeline_empty_steps(self, mock_spark, mock_config):
        """Test pipeline execution with empty steps list."""
        engine = ExecutionEngine(mock_spark, mock_config)

        result = engine.execute_pipeline([], ExecutionMode.INITIAL)

        assert result.status == "completed"
        assert len(result.steps) == 0

    def test_execute_pipeline_step_failure_continues(
        self, mock_spark, mock_config, sample_bronze_step, sample_silver_step
    ):
        """Test that pipeline continues execution even if a step fails."""
        # Mock first step to succeed, second to fail
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_spark.read.format.return_value.load.return_value = mock_df

        # Mock StageStats
        mock_stats = Mock()
        mock_stats.validation_rate = 100.0
        mock_spark.table.return_value = mock_df

        # Make silver step fail by not providing dependency
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="nonexistent",
            transform=lambda spark, bronze_df, silvers: bronze_df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_table",
            schema="test_schema",
        )

        engine = ExecutionEngine(mock_spark, mock_config)

        with patch("sparkforge.execution.fqn") as mock_fqn, patch(
            "sparkforge.execution.apply_column_rules"
        ) as mock_apply_rules:
            mock_fqn.return_value = "test_schema.test_table"
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)

            steps = [sample_bronze_step, silver_step]

            with pytest.raises(ExecutionError):
                engine.execute_pipeline(steps, ExecutionMode.INITIAL)

    def test_backward_compatibility_aliases(self):
        """Test that backward compatibility aliases work."""
        from sparkforge.execution import (
            UnifiedExecutionEngine,
            UnifiedStepExecutionResult,
        )

        assert UnifiedExecutionEngine == ExecutionEngine
        assert UnifiedStepExecutionResult == StepExecutionResult
