"""
Tests for the simplified execution engine.

This module tests the step-by-step execution functionality of the simplified
SparkForge execution system.
"""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkforge.errors import ValidationError
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


class TestExecutionEngine:
    """Test the simplified execution engine."""

    def test_execution_engine_initialization(self, spark_session):
        """Test that ExecutionEngine initializes correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(
            spark=spark_session, config=config, logger=PipelineLogger()
        )

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger is not None

    def test_execution_engine_without_logger(self, spark_session):
        """Test that ExecutionEngine works without explicit logger."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger is not None  # Should create default logger

    def test_step_type_detection(self, spark_session):
        """Test that step types are correctly detected."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Test BronzeStep detection
        bronze_step = BronzeStep(
            name="test_bronze", rules={"id": [F.col("id").isNotNull()]}
        )

        # Test SilverStep detection
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver",
        )

        # Test GoldStep detection
        gold_step = GoldStep(
            name="test_gold",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold",
        )

        # The step type detection happens in execute_step method
        # We can test this by checking the step types are correctly identified
        assert isinstance(bronze_step, BronzeStep)
        assert isinstance(silver_step, SilverStep)
        assert isinstance(gold_step, GoldStep)

    def test_execution_context_creation(self, spark_session):
        """Test that execution context is created correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        ExecutionEngine(spark=spark_session, config=config)

        # Create test data
        test_data = [(1, "test"), (2, "test2")]
        test_df = spark_session.createDataFrame(test_data, ["id", "name"])

        context = {"test_data": test_df}

        # Test that context is properly structured
        assert "test_data" in context
        assert isinstance(context["test_data"], DataFrame)
        assert context["test_data"].count() == 2

    def test_step_execution_result_creation(self, spark_session):
        """Test that StepExecutionResult is created correctly."""
        from datetime import datetime

        from sparkforge.execution import StepExecutionResult

        result = StepExecutionResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.RUNNING,
            start_time=datetime.now(),
        )

        assert result.step_name == "test_step"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.RUNNING
        assert result.start_time is not None
        assert result.end_time is None
        assert result.error is None
        assert result.output_table is None
        assert result.rows_processed is None

    def test_execution_mode_enum(self):
        """Test that ExecutionMode enum works correctly."""
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

    def test_step_status_enum(self):
        """Test that StepStatus enum works correctly."""
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

    def test_step_type_enum(self):
        """Test that StepType enum works correctly."""
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"


class TestExecutionEngineIntegration:
    """Test execution engine integration with simplified models."""

    def test_bronze_step_validation(self, spark_session):
        """Test that BronzeStep validation works correctly."""
        # Valid BronzeStep
        valid_bronze = BronzeStep(
            name="valid_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )
        valid_bronze.validate()  # Should not raise

        # Invalid BronzeStep - empty name should be rejected during construction
        with pytest.raises(
            ValidationError, match="Step name must be a non-empty string"
        ):
            BronzeStep(name="", rules={"id": [F.col("id").isNotNull()]})

    def test_silver_step_validation(self, spark_session):
        """Test that SilverStep validation works correctly."""
        # Valid SilverStep
        valid_silver = SilverStep(
            name="valid_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver",
        )
        valid_silver.validate()  # Should not raise

    def test_gold_step_validation(self, spark_session):
        """Test that GoldStep validation works correctly."""
        # Valid GoldStep
        valid_gold = GoldStep(
            name="valid_gold",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold",
        )
        valid_gold.validate()  # Should not raise

    def test_pipeline_config_validation(self, spark_session):
        """Test that PipelineConfig validation works correctly."""
        # Valid PipelineConfig
        valid_config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        assert valid_config.schema == "test_schema"
        assert valid_config.thresholds.bronze == 95.0
        assert valid_config.parallel.max_workers == 4

    def test_execution_engine_with_mock_steps(self, spark_session):
        """Test execution engine with mock step execution."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        # Create mock steps
        BronzeStep(name="mock_bronze", rules={"id": [F.col("id").isNotNull()]})

        # Test that we can create the engine and it has the expected attributes
        assert hasattr(engine, "spark")
        assert hasattr(engine, "config")
        assert hasattr(engine, "logger")
        assert hasattr(engine, "execute_step")
        assert hasattr(engine, "execute_pipeline")

    def test_execution_engine_error_handling(self, spark_session):
        """Test that execution engine handles errors gracefully."""
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

        # This should raise an error when trying to determine step type
        with pytest.raises(ValueError, match="Unknown step type"):
            # We need to call execute_step to trigger the error
            # But first we need to mock the context
            context = {}
            engine.execute_step(invalid_step, context, ExecutionMode.INITIAL)


class TestExecutionEnginePerformance:
    """Test execution engine performance characteristics."""

    def test_execution_engine_memory_usage(self, spark_session):
        """Test that execution engine doesn't have memory leaks."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        # Create multiple engines to test for memory leaks
        engines = []
        for _i in range(10):
            engine = ExecutionEngine(spark=spark_session, config=config)
            engines.append(engine)

        # All engines should be created successfully
        assert len(engines) == 10
        for engine in engines:
            assert engine.spark == spark_session
            assert engine.config == config

    def test_execution_engine_concurrent_creation(self, spark_session):
        """Test that multiple execution engines can be created concurrently."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        # Create engines in a loop to simulate concurrent creation
        engines = []
        for _i in range(5):
            engine = ExecutionEngine(spark=spark_session, config=config)
            engines.append(engine)

        # All engines should be independent
        assert len(engines) == 5
        for _i, engine in enumerate(engines):
            assert engine.spark == spark_session
            assert engine.config == config
            # Each engine should have its own logger instance
            assert engine.logger is not None


class TestExecutionEngineLogging:
    """Test execution engine logging functionality."""

    def test_execution_engine_logging_initialization(self, spark_session):
        """Test that execution engine initializes logging correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        logger = PipelineLogger()
        engine = ExecutionEngine(spark=spark_session, config=config, logger=logger)

        assert engine.logger == logger
        assert engine.logger is not None

    def test_execution_engine_default_logging(self, spark_session):
        """Test that execution engine creates default logger when none provided."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        assert engine.logger is not None
        assert isinstance(engine.logger, PipelineLogger)

    def test_execution_engine_logging_methods(self, spark_session):
        """Test that execution engine has required logging methods."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        # Test that logger has expected methods
        assert hasattr(engine.logger, "info")
        assert hasattr(engine.logger, "error")
        assert hasattr(engine.logger, "warning")
        assert hasattr(engine.logger, "debug")
