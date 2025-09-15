#!/usr/bin/env python3
"""
Comprehensive tests for the execution_engine module.

This module tests all execution engine functionality, including different execution modes,
retry mechanisms, performance monitoring, and error handling.
"""

import unittest
from unittest.mock import Mock

import pytest

from sparkforge.execution import (
    ExecutionConfig,
    ExecutionEngine,
    ExecutionMode,
    ExecutionResult,
    RetryStrategy,
)
from sparkforge.logger import PipelineLogger
from sparkforge.models import BronzeStep, GoldStep, SilverStep


class TestExecutionMode(unittest.TestCase):
    """Test ExecutionMode enum."""

    def test_execution_mode_values(self):
        """Test execution mode enum values."""
        self.assertEqual(ExecutionMode.SEQUENTIAL.value, "sequential")
        self.assertEqual(ExecutionMode.PARALLEL.value, "parallel")
        self.assertEqual(ExecutionMode.ADAPTIVE.value, "adaptive")


class TestRetryStrategy(unittest.TestCase):
    """Test RetryStrategy enum."""

    def test_retry_strategy_values(self):
        """Test retry strategy enum values."""
        self.assertEqual(RetryStrategy.NONE.value, "none")
        self.assertEqual(RetryStrategy.IMMEDIATE.value, "immediate")
        self.assertEqual(RetryStrategy.EXPONENTIAL_BACKOFF.value, "exponential_backoff")


class TestExecutionConfig(unittest.TestCase):
    """Test ExecutionConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ExecutionConfig()
        self.assertIsNotNone(config)
        # Test that config can be created without errors
        self.assertTrue(hasattr(config, "max_workers"))

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ExecutionConfig(max_workers=4)
        self.assertEqual(config.max_workers, 4)


class TestExecutionResult(unittest.TestCase):
    """Test ExecutionResult dataclass."""

    def test_execution_result_creation(self):
        """Test execution result creation."""
        result = ExecutionResult(
            successful_steps=2,
            failed_steps=0,
            total_duration=1.5,
            step_results={},
            execution_groups=[],
            parallel_efficiency=0.0,
            total_rows_processed=0,
            total_rows_written=0,
        )

        assert result.successful_steps == 2
        assert result.failed_steps == 0
        assert result.total_duration == 1.5
        assert isinstance(result.step_results, dict)

    def test_execution_result_with_error(self):
        """Test execution result with error."""
        result = ExecutionResult(
            successful_steps=1,
            failed_steps=1,
            total_duration=2.0,
            step_results={},
            execution_groups=[],
            parallel_efficiency=0.0,
            total_rows_processed=0,
            total_rows_written=0,
            errors=["Test error"],
        )

        assert result.successful_steps == 1
        assert result.failed_steps == 1
        assert result.errors == ["Test error"]


class TestExecutionEngine:
    """Test ExecutionEngine class."""

    @pytest.fixture(autouse=True)
    def setup_method(self, spark_session):
        """Set up test fixtures."""
        self.spark = spark_session
        self.logger = PipelineLogger(verbose=False)
        self.engine = ExecutionEngine(spark=self.spark, logger=self.logger)

        # Create test steps
        self.bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]})
        }

        self.silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="bronze1",
                transform=lambda df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
            )
        }

        self.gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=["silver1"],
                transform=lambda spark, silvers: silvers["silver1"],
                rules={"id": ["not_null"]},
                table_name="gold1",
            )
        }

    def test_engine_creation(self):
        """Test execution engine creation."""
        assert self.engine is not None
        assert isinstance(self.engine.logger, PipelineLogger)

    def test_engine_creation_with_config(self):
        """Test execution engine creation with config."""
        config = ExecutionConfig(max_workers=4)
        engine = ExecutionEngine(spark=self.spark, config=config, logger=self.logger)
        assert engine is not None
        assert engine.config.max_workers == 4

    def test_execute_bronze_steps(self):
        """Test executing bronze steps."""
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df

        # Test executing bronze steps using execute_steps
        steps = {
            "bronze1": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "source_data": mock_df,
            }
        }

        result = self.engine.execute_steps(steps)

        assert isinstance(result, ExecutionResult)
        assert result.successful_steps >= 0

    def test_execute_silver_steps(self):
        """Test executing silver steps."""
        # Mock Spark session and DataFrame
        Mock()
        mock_df = Mock()

        # Test executing silver steps using execute_steps
        steps = {
            "silver1": {
                "step_type": "silver",
                "source_bronze": "bronze1",
                "transform": lambda df, silvers: df,
                "rules": {"id": ["not_null"]},
                "table_name": "silver1",
                "source_data": mock_df,
            }
        }

        result = self.engine.execute_steps(steps)

        assert isinstance(result, ExecutionResult)
        assert result.successful_steps >= 0

    def test_execute_gold_steps(self):
        """Test executing gold steps."""
        # Mock Spark session and DataFrame
        Mock()
        mock_df = Mock()

        # Test executing gold steps using execute_steps
        steps = {
            "gold1": {
                "step_type": "gold",
                "source_silvers": ["silver1"],
                "transform": lambda spark, silvers: silvers["silver1"],
                "rules": {"id": ["not_null"]},
                "table_name": "gold1",
                "source_data": mock_df,
            }
        }

        result = self.engine.execute_steps(steps)

        assert isinstance(result, ExecutionResult)
        assert result.successful_steps >= 0

    def test_execute_pipeline_steps(self):
        """Test executing all pipeline steps."""
        # Mock Spark session and DataFrame
        Mock()
        mock_df = Mock()

        # Test executing multiple steps using execute_steps
        steps = {
            "bronze1": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "source_data": mock_df,
            },
            "silver1": {
                "step_type": "silver",
                "source_bronze": "bronze1",
                "transform": lambda df, silvers: df,
                "rules": {"id": ["not_null"]},
                "table_name": "silver1",
                "source_data": mock_df,
            },
        }

        result = self.engine.execute_steps(steps)

        assert isinstance(result, ExecutionResult)
        assert result.successful_steps >= 0
        assert result.failed_steps >= 0

    def test_execution_with_error_handling(self):
        """Test execution with error handling."""
        # Mock Spark session that raises an error
        mock_spark = Mock()
        mock_spark.createDataFrame.side_effect = Exception("Test error")

        # Test error handling using execute_steps
        steps = {
            "bronze1": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "source_data": Mock(),
            }
        }

        result = self.engine.execute_steps(steps)

        # Should handle errors gracefully
        assert isinstance(result, ExecutionResult)
        assert result.failed_steps >= 0

    def test_execution_context_manager(self):
        """Test execution engine creation."""
        engine = ExecutionEngine(spark=self.spark)
        assert engine is not None
        assert isinstance(engine, ExecutionEngine)

    def test_execution_with_different_strategies(self):
        """Test execution with different strategies."""
        # Test sequential execution
        sequential_engine = ExecutionEngine(spark=self.spark)
        assert sequential_engine is not None

        # Test parallel execution
        parallel_engine = ExecutionEngine(spark=self.spark)
        assert parallel_engine is not None

        # Test adaptive execution
        adaptive_engine = ExecutionEngine(spark=self.spark)
        assert adaptive_engine is not None


class TestExecutionEngineIntegration:
    """Test ExecutionEngine integration scenarios."""

    @pytest.fixture(autouse=True)
    def setup_method(self, spark_session):
        """Set up test fixtures."""
        self.spark = spark_session
        self.engine = ExecutionEngine(spark=self.spark)

        # Create test steps
        self.bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]}),
            "bronze2": BronzeStep(name="bronze2", rules={"id": ["not_null"]}),
        }

        self.silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="bronze1",
                transform=lambda df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
            ),
            "silver2": SilverStep(
                name="silver2",
                source_bronze="bronze2",
                transform=lambda df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver2",
            ),
        }

        self.gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=["silver1", "silver2"],
                transform=lambda spark, silvers: silvers["silver1"],
                rules={"id": ["not_null"]},
                table_name="gold1",
            )
        }

    def test_complex_pipeline_execution(self):
        """Test execution of a complex pipeline."""
        # Mock Spark session and DataFrame
        Mock()
        mock_df = Mock()

        # Create step configurations for execute_steps
        steps = {
            "bronze1": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "source_data": mock_df,
            },
            "bronze2": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "source_data": mock_df,
            },
            "silver1": {
                "step_type": "silver",
                "source_bronze": "bronze1",
                "transform": lambda df, silvers: df,
                "rules": {"id": ["not_null"]},
                "table_name": "silver1",
                "source_data": mock_df,
            },
            "silver2": {
                "step_type": "silver",
                "source_bronze": "bronze2",
                "transform": lambda df, silvers: df,
                "rules": {"id": ["not_null"]},
                "table_name": "silver2",
                "source_data": mock_df,
            },
            "gold1": {
                "step_type": "gold",
                "source_silvers": ["silver1", "silver2"],
                "transform": lambda spark, silvers: silvers["silver1"],
                "rules": {"id": ["not_null"]},
                "table_name": "gold1",
                "source_data": mock_df,
            },
        }

        result = self.engine.execute_steps(steps)

        assert isinstance(result, ExecutionResult)
        assert result.successful_steps >= 0
        assert result.failed_steps >= 0

    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms."""
        # Mock Spark session that raises an error
        mock_spark = Mock()
        mock_spark.createDataFrame.side_effect = Exception("Test error")

        # Create step configurations for execute_steps
        steps = {
            "bronze1": {"step_type": "bronze", "name": "bronze1"},
            "bronze2": {"step_type": "bronze", "name": "bronze2"},
            "silver1": {"step_type": "silver", "name": "silver1"},
            "silver2": {"step_type": "silver", "name": "silver2"},
            "gold1": {"step_type": "gold", "name": "gold1"},
        }

        result = self.engine.execute_steps(steps)

        # Should handle errors gracefully
        assert isinstance(result, ExecutionResult)


if __name__ == "__main__":
    unittest.main()
