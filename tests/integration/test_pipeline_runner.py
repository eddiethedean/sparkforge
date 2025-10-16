#!/usr/bin/env python3
"""
Tests for pipeline runner functionality.

This module tests the SimplePipelineRunner class and its methods.
"""

import os
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import MockDataFrame as DataFrame
    from mock_spark import functions as F
else:
    from pyspark.sql import DataFrame
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

    def test_report_metrics_row_counts_accuracy(self, mock_spark, mock_config):
        """Test that report accurately aggregates row counts from step results."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)

        # Create step results with specific row counts
        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[
                StepExecutionResult(
                    step_name="bronze_events",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=1000,  # Bronze processes but doesn't write
                    output_table=None,  # Bronze doesn't write to tables
                    duration=2.5,
                ),
                StepExecutionResult(
                    step_name="silver_enriched",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=856,
                    output_table="analytics.silver_enriched",
                    duration=3.2,
                ),
                StepExecutionResult(
                    step_name="gold_summary",
                    step_type=StepType.GOLD,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=42,
                    output_table="analytics.gold_summary",
                    duration=1.8,
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        # Verify row counts are accurately aggregated
        assert report.metrics.total_rows_processed == 1898  # 1000 + 856 + 42
        # Only Silver and Gold write to tables (have output_table)
        assert report.metrics.total_rows_written == 898  # 856 + 42
        assert report.metrics.total_rows_written != 0  # Should not be zero!

    def test_report_metrics_duration_by_layer_accuracy(self, mock_spark, mock_config):
        """Test that report accurately calculates durations by layer."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 40, 0)

        # Create steps with durations by not passing end_time, letting duration be set directly
        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[
                # Two Bronze steps
                StepExecutionResult(
                    step_name="bronze_events",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,  # Let duration be set directly
                    duration=2.5,
                    rows_processed=1000,
                ),
                StepExecutionResult(
                    step_name="bronze_users",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    duration=1.5,
                    rows_processed=500,
                ),
                # Three Silver steps
                StepExecutionResult(
                    step_name="silver_events",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    duration=3.2,
                    rows_processed=856,
                    output_table="analytics.silver_events",
                ),
                StepExecutionResult(
                    step_name="silver_users",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    duration=2.8,
                    rows_processed=450,
                    output_table="analytics.silver_users",
                ),
                StepExecutionResult(
                    step_name="silver_joined",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    duration=4.1,
                    rows_processed=800,
                    output_table="analytics.silver_joined",
                ),
                # One Gold step
                StepExecutionResult(
                    step_name="gold_summary",
                    step_type=StepType.GOLD,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    duration=5.3,
                    rows_processed=42,
                    output_table="analytics.gold_summary",
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        # Verify durations by layer
        assert report.metrics.bronze_duration == 4.0  # 2.5 + 1.5
        assert report.metrics.silver_duration == 10.1  # 3.2 + 2.8 + 4.1
        assert report.metrics.gold_duration == 5.3  # 5.3
        assert report.metrics.total_duration == 600.0  # 10 minutes

    def test_report_metrics_with_failed_steps(self, mock_spark, mock_config):
        """Test that report correctly handles row counts when some steps fail."""
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
                    step_name="bronze_events",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=1000,
                    duration=2.5,
                ),
                StepExecutionResult(
                    step_name="silver_enriched",
                    step_type=StepType.SILVER,
                    status=StepStatus.FAILED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=None,  # Failed step has no rows
                    output_table=None,
                    duration=1.0,
                    error="Transformation failed",
                ),
                StepExecutionResult(
                    step_name="gold_summary",
                    step_type=StepType.GOLD,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=42,
                    output_table="analytics.gold_summary",
                    duration=1.8,
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        # Verify row counts handle None values from failed steps
        assert report.metrics.total_rows_processed == 1042  # 1000 + 0 + 42
        assert report.metrics.total_rows_written == 42  # Only gold completed with table
        assert report.metrics.failed_steps == 1
        assert report.metrics.successful_steps == 2

    def test_report_metrics_with_no_rows_processed(self, mock_spark, mock_config):
        """Test that report handles steps with zero rows gracefully."""
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
                    step_name="bronze_empty",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=0,  # Empty dataset
                    duration=1.0,
                ),
                StepExecutionResult(
                    step_name="silver_empty",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=0,
                    output_table="analytics.silver_empty",
                    duration=1.0,
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        # Verify zero row counts are handled correctly
        assert report.metrics.total_rows_processed == 0
        assert report.metrics.total_rows_written == 0
        assert report.metrics.successful_steps == 2

    def test_report_metrics_mixed_layers_comprehensive(self, mock_spark, mock_config):
        """Comprehensive test with all layer types and various row counts."""
        runner = SimplePipelineRunner(mock_spark, mock_config)

        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 45, 0)

        execution_result = ExecutionResult(
            execution_id="test_execution",
            mode=ExecutionMode.INITIAL,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            steps=[
                # Bronze layer: processes 10,000 rows total
                StepExecutionResult(
                    step_name="bronze_transactions",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,  # Let duration be set directly
                    rows_processed=7500,
                    duration=3.5,
                ),
                StepExecutionResult(
                    step_name="bronze_customers",
                    step_type=StepType.BRONZE,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    rows_processed=2500,
                    duration=1.5,
                ),
                # Silver layer: writes 8,000 rows total
                StepExecutionResult(
                    step_name="silver_transactions",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    rows_processed=6000,
                    output_table="analytics.silver_transactions",
                    duration=5.0,
                ),
                StepExecutionResult(
                    step_name="silver_customers",
                    step_type=StepType.SILVER,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    rows_processed=2000,
                    output_table="analytics.silver_customers",
                    duration=3.0,
                ),
                # Gold layer: writes 150 rows total
                StepExecutionResult(
                    step_name="gold_daily_summary",
                    step_type=StepType.GOLD,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    rows_processed=100,
                    output_table="analytics.gold_daily_summary",
                    duration=4.5,
                ),
                StepExecutionResult(
                    step_name="gold_customer_metrics",
                    step_type=StepType.GOLD,
                    status=StepStatus.COMPLETED,
                    start_time=start_time,
                    end_time=None,
                    rows_processed=50,
                    output_table="analytics.gold_customer_metrics",
                    duration=2.5,
                ),
            ],
        )

        report = runner._create_pipeline_report(
            pipeline_id="test_pipeline",
            mode=PipelineMode.INITIAL,
            start_time=start_time,
            execution_result=execution_result,
        )

        # Verify comprehensive metrics
        assert report.metrics.total_rows_processed == 18150  # Sum of all rows
        assert report.metrics.total_rows_written == 8150  # Silver + Gold only
        assert report.metrics.total_steps == 6
        assert report.metrics.successful_steps == 6
        assert report.metrics.failed_steps == 0
        
        # Verify layer durations
        assert report.metrics.bronze_duration == 5.0  # 3.5 + 1.5
        assert report.metrics.silver_duration == 8.0  # 5.0 + 3.0
        assert report.metrics.gold_duration == 7.0  # 4.5 + 2.5
        
        # Verify total duration is based on wall-clock time
        assert report.metrics.total_duration == 900.0  # 15 minutes
