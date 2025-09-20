#!/usr/bin/env python3
"""
Tests for pipeline runner functionality.

This module tests the SimplePipelineRunner class and its methods.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkforge.execution import (
    ExecutionMode,
    ExecutionResult,
    StepExecutionResult,
    StepStatus,
    StepType,
)
from sparkforge.models import BronzeStep, GoldStep, PipelineConfig, SilverStep
from sparkforge.pipeline.models import PipelineMode, PipelineStatus
from sparkforge.pipeline.runner import PipelineRunner, SimplePipelineRunner


class TestSimplePipelineRunner:
    """Test cases for SimplePipelineRunner."""

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
            schema="test_schema",
        )

    @pytest.fixture
    def sample_silver_step(self):
        """Create a sample SilverStep."""
        return SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, dfs: dfs,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_table",
            schema="test_schema",
        )

    @pytest.fixture
    def sample_gold_step(self):
        """Create a sample GoldStep."""
        return GoldStep(
            name="test_gold",
            transform=lambda spark, dfs: dfs,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_table",
            source_silvers=["test_silver"],
            schema="test_schema",
        )

    def test_runner_initialization_with_all_parameters(
        self, mock_spark, mock_config, mock_logger
    ):
        """Test runner initialization with all parameters."""
        bronze_steps = {"bronze1": Mock(spec=BronzeStep)}
        silver_steps = {"silver1": Mock(spec=SilverStep)}
        gold_steps = {"gold1": Mock(spec=GoldStep)}

        runner = SimplePipelineRunner(
            spark=mock_spark,
            config=mock_config,
            bronze_steps=bronze_steps,
            silver_steps=silver_steps,
            gold_steps=gold_steps,
            logger=mock_logger,
        )

        assert runner.spark == mock_spark
        assert runner.config == mock_config
        assert runner.bronze_steps == bronze_steps
        assert runner.silver_steps == silver_steps
        assert runner.gold_steps == gold_steps
        assert runner.logger == mock_logger
        assert runner.execution_engine is not None

    def test_runner_initialization_with_minimal_parameters(
        self, mock_spark, mock_config
    ):
        """Test runner initialization with minimal parameters."""
        runner = SimplePipelineRunner(spark=mock_spark, config=mock_config)

        assert runner.spark == mock_spark
        assert runner.config == mock_config
        assert runner.bronze_steps == {}
        assert runner.silver_steps == {}
        assert runner.gold_steps == {}
        assert runner.logger is not None
        assert runner.execution_engine is not None

    def test_runner_initialization_with_none_steps(self, mock_spark, mock_config):
        """Test runner initialization with None step dictionaries."""
        runner = SimplePipelineRunner(
            spark=mock_spark,
            config=mock_config,
            bronze_steps=None,
            silver_steps=None,
            gold_steps=None,
        )

        assert runner.bronze_steps == {}
        assert runner.silver_steps == {}
        assert runner.gold_steps == {}

    def test_convert_mode_initial(self, mock_spark, mock_config):
        """Test mode conversion for INITIAL mode."""
        runner = SimplePipelineRunner(mock_spark, mock_config)
        result = runner._convert_mode(PipelineMode.INITIAL)
        assert result == ExecutionMode.INITIAL

    def test_convert_mode_incremental(self, mock_spark, mock_config):
        """Test mode conversion for INCREMENTAL mode."""
        runner = SimplePipelineRunner(mock_spark, mock_config)
        result = runner._convert_mode(PipelineMode.INCREMENTAL)
        assert result == ExecutionMode.INCREMENTAL

    def test_convert_mode_full_refresh(self, mock_spark, mock_config):
        """Test mode conversion for FULL_REFRESH mode."""
        runner = SimplePipelineRunner(mock_spark, mock_config)
        result = runner._convert_mode(PipelineMode.FULL_REFRESH)
        assert result == ExecutionMode.FULL_REFRESH

    def test_convert_mode_validation_only(self, mock_spark, mock_config):
        """Test mode conversion for VALIDATION_ONLY mode."""
        runner = SimplePipelineRunner(mock_spark, mock_config)
        result = runner._convert_mode(PipelineMode.VALIDATION_ONLY)
        assert result == ExecutionMode.VALIDATION_ONLY

    def test_convert_mode_unknown(self, mock_spark, mock_config):
        """Test mode conversion for unknown mode."""
        runner = SimplePipelineRunner(mock_spark, mock_config)
        # Test with a mock mode that's not in the mapping
        unknown_mode = Mock()
        unknown_mode.name = "UNKNOWN"
        result = runner._convert_mode(unknown_mode)
        assert result == ExecutionMode.INITIAL  # Default fallback

    @patch("sparkforge.pipeline.runner.datetime")
    def test_run_pipeline_success(
        self, mock_datetime, mock_spark, mock_config, sample_bronze_step
    ):
        """Test successful pipeline execution."""
        # Mock datetime
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)
        mock_datetime.now.side_effect = [start_time, end_time]

        # Mock execution engine
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[
                StepExecutionResult(
                    step_name="test_bronze",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                )
            ],
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        # Run pipeline
        result = runner.run_pipeline([sample_bronze_step], PipelineMode.INITIAL)

        assert result.pipeline_id.startswith("pipeline_")
        assert result.status == PipelineStatus.COMPLETED
        assert result.mode == PipelineMode.INITIAL
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.metrics.total_steps == 1
        assert result.metrics.successful_steps == 1
        assert result.metrics.failed_steps == 0

    def test_run_pipeline_with_bronze_sources(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test pipeline execution with bronze sources."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        bronze_sources = {"test_bronze": Mock(spec=DataFrame)}

        runner.run_pipeline([sample_bronze_step], PipelineMode.INITIAL, bronze_sources)

        # Verify execution engine was called
        mock_execution_engine.execute_pipeline.assert_called_once()

    def test_run_pipeline_without_bronze_sources(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test pipeline execution without bronze sources."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        runner.run_pipeline([sample_bronze_step], PipelineMode.INITIAL)

        # Verify execution engine was called
        mock_execution_engine.execute_pipeline.assert_called_once()

    def test_run_pipeline_execution_failure(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test pipeline execution failure."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.side_effect = Exception(
            "Execution failed"
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        result = runner.run_pipeline([sample_bronze_step], PipelineMode.INITIAL)

        assert result.status == PipelineStatus.FAILED
        assert "Execution failed" in result.errors[0]

    def test_run_initial_load_with_steps(
        self, mock_spark, mock_config, sample_bronze_step
    ):
        """Test run_initial_load with provided steps."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        runner.run_initial_load([sample_bronze_step])

        # Verify it was called with INITIAL mode
        mock_execution_engine.execute_pipeline.assert_called_once()
        call_args = mock_execution_engine.execute_pipeline.call_args
        assert (
            call_args[0][1] == ExecutionMode.INITIAL
        )  # Second argument should be mode

    def test_run_initial_load_without_steps(self, mock_spark, mock_config):
        """Test run_initial_load without provided steps using stored steps."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
        )

        bronze_step = Mock(spec=BronzeStep)
        silver_step = Mock(spec=SilverStep)
        gold_step = Mock(spec=GoldStep)

        runner = SimplePipelineRunner(
            mock_spark,
            mock_config,
            bronze_steps={"bronze1": bronze_step},
            silver_steps={"silver1": silver_step},
            gold_steps={"gold1": gold_step},
        )
        runner.execution_engine = mock_execution_engine

        runner.run_initial_load()

        # Verify execution engine was called with all stored steps
        mock_execution_engine.execute_pipeline.assert_called_once()
        call_args = mock_execution_engine.execute_pipeline.call_args
        steps = call_args[0][0]  # First argument should be steps list
        assert len(steps) == 3  # bronze + silver + gold

    def test_run_incremental(self, mock_spark, mock_config, sample_bronze_step):
        """Test run_incremental method."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.INCREMENTAL,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        runner.run_incremental([sample_bronze_step])

        # Verify it was called with INCREMENTAL mode
        mock_execution_engine.execute_pipeline.assert_called_once()
        call_args = mock_execution_engine.execute_pipeline.call_args
        assert call_args[0][1] == ExecutionMode.INCREMENTAL

    def test_run_full_refresh(self, mock_spark, mock_config, sample_bronze_step):
        """Test run_full_refresh method."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.FULL_REFRESH,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        runner.run_full_refresh([sample_bronze_step])

        # Verify it was called with FULL_REFRESH mode
        mock_execution_engine.execute_pipeline.assert_called_once()
        call_args = mock_execution_engine.execute_pipeline.call_args
        assert call_args[0][1] == ExecutionMode.FULL_REFRESH

    def test_run_validation_only(self, mock_spark, mock_config, sample_bronze_step):
        """Test run_validation_only method."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_pipeline.return_value = ExecutionResult(
            execution_id="test",
            mode=ExecutionMode.VALIDATION_ONLY,
            start_time=datetime.now(),
            status="completed",
        )

        runner = SimplePipelineRunner(mock_spark, mock_config)
        runner.execution_engine = mock_execution_engine

        runner.run_validation_only([sample_bronze_step])

        # Verify it was called with VALIDATION_ONLY mode
        mock_execution_engine.execute_pipeline.assert_called_once()
        call_args = mock_execution_engine.execute_pipeline.call_args
        assert call_args[0][1] == ExecutionMode.VALIDATION_ONLY

    def test_create_pipeline_report_success(self, mock_spark, mock_config):
        """Test creating pipeline report for successful execution."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[
                StepExecutionResult(
                    step_name="step1",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                ),
                StepExecutionResult(
                    step_name="step2",
                    step_type=StepType.SILVER,
                    status=StepStatus.FAILED,
                    start_time=start_time,
                    end_time=end_time,
                    error="Test error",
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        assert report.pipeline_id == "test_pipeline"
        assert report.status == PipelineStatus.COMPLETED
        assert report.mode == PipelineMode.INITIAL
        assert report.start_time == start_time
        assert report.end_time == end_time
        assert report.metrics.total_steps == 2
        assert report.metrics.successful_steps == 1
        assert report.metrics.failed_steps == 1
        assert "Test error" in report.errors

    def test_create_pipeline_report_failure(self, mock_spark, mock_config):
        """Test creating pipeline report for failed execution."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="failed",
            steps=[],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        assert report.status == PipelineStatus.FAILED

    def test_create_pipeline_report_without_end_time(self, mock_spark, mock_config):
        """Test creating pipeline report without end time."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)

        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=None,
            status="completed",
            steps=[],
        )

        with patch("sparkforge.pipeline.runner.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15, 10, 35, 0)

            report = runner._create_pipeline_report(
                pipeline_id="test_pipeline",
                mode=PipelineMode.INITIAL,
                start_time=start_time,
                execution_result=execution_result,
            )

            assert report.end_time is not None

    def test_create_error_report(self, mock_spark, mock_config):
        """Test creating error report."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)

        with patch("sparkforge.pipeline.runner.datetime") as mock_datetime:
            end_time = datetime(2024, 1, 15, 10, 35, 0)
            mock_datetime.now.return_value = end_time

            report = runner._create_error_report(
                pipeline_id="test_pipeline",
                mode=PipelineMode.INITIAL,
                start_time=start_time,
                error="Test error message",
            )

            assert report.pipeline_id == "test_pipeline"
            assert report.status == PipelineStatus.FAILED
            assert report.mode == PipelineMode.INITIAL
            assert report.start_time == start_time
            assert report.end_time == end_time
            assert report.metrics.total_steps == 0
            assert report.metrics.successful_steps == 0
            assert report.metrics.failed_steps == 0
            assert "Test error message" in report.errors
            # Success rate calculation would be 0.0 for failed pipeline

    def test_pipeline_runner_alias(self):
        """Test that PipelineRunner alias works correctly."""
        # Test that the alias is properly defined
        assert PipelineRunner == SimplePipelineRunner

        # Test instantiation through alias
        mock_spark = Mock(spec=SparkSession)
        mock_config = Mock(spec=PipelineConfig)
        runner = PipelineRunner(mock_spark, mock_config)
        assert isinstance(runner, SimplePipelineRunner)

    def test_create_pipeline_report_with_empty_steps(self, mock_spark, mock_config):
        """Test creating pipeline report with empty steps list."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        assert report.metrics.total_steps == 0
        assert report.metrics.successful_steps == 0
        assert report.metrics.failed_steps == 0
        # Success rate calculation would be 0.0 for empty pipeline
