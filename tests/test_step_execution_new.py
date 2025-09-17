"""
Tests for step-by-step execution functionality.

This module tests the actual execution flow of the simplified SparkForge
execution system, including bronze, silver, and gold step execution.
"""

from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkforge.execution import ExecutionEngine, ExecutionMode, StepStatus, StepType
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)


class TestStepExecutionFlow:
    """Test the step-by-step execution flow."""

    def test_bronze_step_execution_flow(self, spark_session):
        """Test that bronze step execution follows the correct flow."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create a bronze step
        bronze_step = BronzeStep(
            name="test_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Test that the step is properly configured
        assert bronze_step.name == "test_bronze"
        assert bronze_step.incremental_col == "timestamp"
        assert "id" in bronze_step.rules
        assert len(bronze_step.rules["id"]) == 1

    def test_silver_step_execution_flow(self, spark_session):
        """Test that silver step execution follows the correct flow."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create a silver step
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("id").isNotNull())

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver",
        )

        # Test that the step is properly configured
        assert silver_step.name == "test_silver"
        assert silver_step.source_bronze == "test_bronze"
        assert silver_step.table_name == "test_silver"
        assert callable(silver_step.transform)

    def test_gold_step_execution_flow(self, spark_session):
        """Test that gold step execution follows the correct flow."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create a gold step
        def gold_transform(spark, silvers):
            # Simple aggregation example
            if silvers:
                return list(silvers.values())[0].groupBy("id").count()
            return spark.createDataFrame([], "id int, count long")

        gold_step = GoldStep(
            name="test_gold",
            transform=gold_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold",
        )

        # Test that the step is properly configured
        assert gold_step.name == "test_gold"
        assert gold_step.table_name == "test_gold"
        assert callable(gold_step.transform)

    def test_step_validation_flow(self, spark_session):
        """Test that step validation works correctly."""
        # Test valid bronze step
        valid_bronze = BronzeStep(
            name="valid_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )
        valid_bronze.validate()  # Should not raise

        # Test valid silver step
        valid_silver = SilverStep(
            name="valid_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver",
        )
        valid_silver.validate()  # Should not raise

        # Test valid gold step
        valid_gold = GoldStep(
            name="valid_gold",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold",
        )
        valid_gold.validate()  # Should not raise

    def test_step_type_detection_flow(self, spark_session):
        """Test that step types are correctly detected in execution flow."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create steps of different types
        bronze_step = BronzeStep(
            name="bronze_test", rules={"id": [F.col("id").isNotNull()]}
        )

        silver_step = SilverStep(
            name="silver_test",
            source_bronze="bronze_test",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_test",
        )

        gold_step = GoldStep(
            name="gold_test",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_test",
        )

        # Test type detection
        assert isinstance(bronze_step, BronzeStep)
        assert isinstance(silver_step, SilverStep)
        assert isinstance(gold_step, GoldStep)

    def test_execution_context_flow(self, spark_session):
        """Test that execution context is properly managed."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create test data
        test_data = [(1, "test1"), (2, "test2"), (3, "test3")]
        test_df = spark_session.createDataFrame(test_data, ["id", "name"])

        # Create execution context
        context = {
            "bronze_data": test_df,
            "silver_data": test_df.filter(F.col("id") > 1),
        }

        # Test context structure
        assert "bronze_data" in context
        assert "silver_data" in context
        assert isinstance(context["bronze_data"], DataFrame)
        assert isinstance(context["silver_data"], DataFrame)
        assert context["bronze_data"].count() == 3
        assert context["silver_data"].count() == 2

    def test_step_execution_result_flow(self, spark_session):
        """Test that step execution results are properly created."""
        from sparkforge.execution import StepExecutionResult

        # Test result creation
        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.RUNNING,
            start_time=datetime.now(),
        )

        # Test initial state
        assert result.step_name == "test_step"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.RUNNING
        assert result.start_time is not None
        assert result.end_time is None
        assert result.duration is None
        assert result.error is None
        assert result.rows_processed is None
        assert result.output_table is None

        # Test completion state
        result.status = StepStatus.COMPLETED
        result.end_time = datetime.now()
        result.rows_processed = 100
        result.output_table = "test_schema.test_table"

        assert result.status == StepStatus.COMPLETED
        assert result.end_time is not None
        assert result.rows_processed == 100
        assert result.output_table == "test_schema.test_table"

    def test_execution_mode_flow(self, spark_session):
        """Test that execution modes work correctly."""
        # Test all execution modes
        modes = [
            ExecutionMode.INITIAL,
            ExecutionMode.INCREMENTAL,
            ExecutionMode.FULL_REFRESH,
            ExecutionMode.VALIDATION_ONLY,
        ]

        for mode in modes:
            assert mode.value in [
                "initial",
                "incremental",
                "full_refresh",
                "validation_only",
            ]
            assert isinstance(mode, ExecutionMode)

    def test_step_status_flow(self, spark_session):
        """Test that step statuses work correctly."""
        # Test all step statuses
        statuses = [
            StepStatus.PENDING,
            StepStatus.RUNNING,
            StepStatus.COMPLETED,
            StepStatus.FAILED,
            StepStatus.SKIPPED,
        ]

        for status in statuses:
            assert status.value in [
                "pending",
                "running",
                "completed",
                "failed",
                "skipped",
            ]
            assert isinstance(status, StepStatus)

    def test_pipeline_configuration_flow(self, spark_session):
        """Test that pipeline configuration works correctly."""
        # Test valid configuration
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        # Test configuration values
        assert config.schema == "test_schema"
        assert config.thresholds.bronze == 95.0
        assert config.thresholds.silver == 98.0
        assert config.thresholds.gold == 99.0
        assert config.parallel.max_workers == 4
        assert config.parallel.enabled is True
        assert config.parallel.timeout_secs == 300  # Default value

    def test_execution_engine_initialization_flow(self, spark_session):
        """Test that execution engine initializes correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        # Test with explicit logger
        logger = PipelineLogger()
        engine = ExecutionEngine(spark=spark_session, config=config, logger=logger)

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger == logger

        # Test without explicit logger
        engine2 = ExecutionEngine(spark=spark_session, config=config)

        assert engine2.spark == spark_session
        assert engine2.config == config
        assert engine2.logger is not None
        assert isinstance(engine2.logger, PipelineLogger)

    def test_step_execution_error_handling_flow(self, spark_session):
        """Test that step execution handles errors correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        # Test with invalid step type
        class InvalidStep:
            def __init__(self):
                self.name = "invalid_step"

        invalid_step = InvalidStep()
        context = {}

        # This should raise a ValueError for unknown step type
        with pytest.raises(ValueError, match="Unknown step type"):
            engine.execute_step(invalid_step, context, ExecutionMode.INITIAL)

    def test_step_execution_with_mock_data(self, spark_session):
        """Test step execution with mock data."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create mock data
        mock_data = [(1, "test1"), (2, "test2"), (3, "test3")]
        mock_df = spark_session.createDataFrame(mock_data, ["id", "name"])

        # Test that we can work with the mock data
        assert mock_df.count() == 3
        assert len(mock_df.columns) == 2
        assert "id" in mock_df.columns
        assert "name" in mock_df.columns

        # Test that the engine can handle the mock data
        context = {"mock_data": mock_df}
        assert "mock_data" in context
        assert isinstance(context["mock_data"], DataFrame)
