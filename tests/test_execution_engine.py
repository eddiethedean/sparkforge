#!/usr/bin/env python3
"""
Comprehensive tests for the execution_engine module.

This module tests all execution engine functionality, including different execution modes,
retry mechanisms, performance monitoring, and error handling.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
import time
from concurrent.futures import Future

from sparkforge.execution_engine import (
    ExecutionEngine, ExecutionMode, RetryStrategy, ExecutionConfig,
    ExecutionResult, ExecutionStats
)
from sparkforge.models import SilverStep, StageStats, ExecutionContext
from sparkforge.logger import PipelineLogger


class TestExecutionMode(unittest.TestCase):
    """Test ExecutionMode enum."""
    
    def test_execution_mode_values(self):
        """Test execution mode enum values."""
        self.assertEqual(ExecutionMode.SEQUENTIAL.value, "sequential")
        self.assertEqual(ExecutionMode.PARALLEL.value, "parallel")
        self.assertEqual(ExecutionMode.ADAPTIVE.value, "adaptive")
        self.assertEqual(ExecutionMode.BATCH.value, "batch")


class TestRetryStrategy(unittest.TestCase):
    """Test RetryStrategy enum."""
    
    def test_retry_strategy_values(self):
        """Test retry strategy enum values."""
        self.assertEqual(RetryStrategy.NONE.value, "none")
        self.assertEqual(RetryStrategy.IMMEDIATE.value, "immediate")
        self.assertEqual(RetryStrategy.EXPONENTIAL_BACKOFF.value, "exponential_backoff")
        self.assertEqual(RetryStrategy.LINEAR_BACKOFF.value, "linear_backoff")


class TestExecutionConfig(unittest.TestCase):
    """Test ExecutionConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = ExecutionConfig()
        
        self.assertEqual(config.mode, ExecutionMode.ADAPTIVE)
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.retry_strategy, RetryStrategy.EXPONENTIAL_BACKOFF)
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_delay, 1.0)
        self.assertIsNone(config.timeout_seconds)
        self.assertTrue(config.enable_caching)
        self.assertTrue(config.enable_monitoring)
        self.assertEqual(config.batch_size, 10)
        self.assertEqual(config.adaptive_threshold, 0.5)
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = ExecutionConfig(
            mode=ExecutionMode.PARALLEL,
            max_workers=8,
            retry_strategy=RetryStrategy.LINEAR_BACKOFF,
            max_retries=5,
            retry_delay=2.0,
            timeout_seconds=300,
            enable_caching=False,
            enable_monitoring=False,
            batch_size=5,
            adaptive_threshold=0.8
        )
        
        self.assertEqual(config.mode, ExecutionMode.PARALLEL)
        self.assertEqual(config.max_workers, 8)
        self.assertEqual(config.retry_strategy, RetryStrategy.LINEAR_BACKOFF)
        self.assertEqual(config.max_retries, 5)
        self.assertEqual(config.retry_delay, 2.0)
        self.assertEqual(config.timeout_seconds, 300)
        self.assertFalse(config.enable_caching)
        self.assertFalse(config.enable_monitoring)
        self.assertEqual(config.batch_size, 5)
        self.assertEqual(config.adaptive_threshold, 0.8)


class TestExecutionResult(unittest.TestCase):
    """Test ExecutionResult dataclass."""
    
    def test_execution_result_creation(self):
        """Test execution result creation."""
        result = ExecutionResult(
            step_name="test_step",
            success=True,
            duration_seconds=1.5,
            rows_processed=1000,
            retry_count=0,
            metadata={"key": "value"}
        )
        
        self.assertEqual(result.step_name, "test_step")
        self.assertTrue(result.success)
        self.assertEqual(result.duration_seconds, 1.5)
        self.assertEqual(result.rows_processed, 1000)
        self.assertIsNone(result.error)
        self.assertEqual(result.retry_count, 0)
        self.assertEqual(result.metadata, {"key": "value"})
    
    def test_execution_result_with_error(self):
        """Test execution result with error."""
        result = ExecutionResult(
            step_name="failed_step",
            success=False,
            duration_seconds=0.5,
            rows_processed=0,
            error="Test error",
            retry_count=2
        )
        
        self.assertEqual(result.step_name, "failed_step")
        self.assertFalse(result.success)
        self.assertEqual(result.duration_seconds, 0.5)
        self.assertEqual(result.rows_processed, 0)
        self.assertEqual(result.error, "Test error")
        self.assertEqual(result.retry_count, 2)


class TestExecutionStats(unittest.TestCase):
    """Test ExecutionStats dataclass."""
    
    def test_execution_stats_creation(self):
        """Test execution stats creation."""
        stats = ExecutionStats(
            total_steps=10,
            successful_steps=8,
            failed_steps=1,
            skipped_steps=1,
            total_duration=5.0,
            parallel_efficiency=0.8,
            average_step_duration=0.5,
            retry_count=2,
            cache_hits=5,
            cache_misses=5
        )
        
        self.assertEqual(stats.total_steps, 10)
        self.assertEqual(stats.successful_steps, 8)
        self.assertEqual(stats.failed_steps, 1)
        self.assertEqual(stats.skipped_steps, 1)
        self.assertEqual(stats.total_duration, 5.0)
        self.assertEqual(stats.parallel_efficiency, 0.8)
        self.assertEqual(stats.average_step_duration, 0.5)
        self.assertEqual(stats.retry_count, 2)
        self.assertEqual(stats.cache_hits, 5)
        self.assertEqual(stats.cache_misses, 5)


class TestExecutionEngine(unittest.TestCase):
    """Test ExecutionEngine class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.logger = PipelineLogger(verbose=False)
        self.thresholds = {"silver": 95.0}
        self.schema = "test_schema"
        
        # Create test silver step
        self.silver_step = SilverStep(
            name="test_step",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver1"
        )
        
        # Create test DataFrames
        self.bronze_df = Mock()
        self.bronze_df.count.return_value = 100
        self.prior_silvers = {}
        
        # Create execution engine
        self.engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema
        )
    
    def test_engine_creation(self):
        """Test execution engine creation."""
        self.assertEqual(self.engine.spark, self.spark)
        self.assertEqual(self.engine.logger, self.logger)
        self.assertEqual(self.engine.thresholds, self.thresholds)
        self.assertEqual(self.engine.schema, self.schema)
        self.assertIsInstance(self.engine.config, ExecutionConfig)
    
    def test_engine_creation_with_custom_config(self):
        """Test execution engine creation with custom config."""
        config = ExecutionConfig(mode=ExecutionMode.PARALLEL, max_workers=8)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        self.assertEqual(engine.config, config)
    
    def test_execute_silver_step_success(self):
        """Test successful silver step execution."""
        # Mock the transform function
        def mock_transform(spark, df):
            return df
        
        self.silver_step.transform = mock_transform
        
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.execution_engine.apply_column_rules') as mock_apply_rules:
            mock_stats = StageStats(
                stage="silver",
                step="test_step",
                total_rows=100,
                valid_rows=95,
                invalid_rows=5,
                validation_rate=95.0,
                duration_secs=1.0
            )
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)
            
            # Mock write operation
            with patch('sparkforge.execution_engine.time_write_operation') as mock_write:
                mock_write.return_value = (100, 0.5, None, None)
                
                step_name, result = self.engine.execute_silver_step(
                    "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
                )
                
                self.assertEqual(step_name, "test_step")
                self.assertIn("table_fqn", result)
                self.assertIn("transform", result)
                self.assertIn("validation", result)
                self.assertIn("write", result)
                self.assertFalse(result.get("skipped", False))
                self.assertNotIn("error", result)
    
    def test_execute_silver_step_error(self):
        """Test silver step execution with error."""
        # Mock transform function that raises exception
        def mock_transform(spark, df):
            raise ValueError("Test error")
        
        self.silver_step.transform = mock_transform
        
        step_name, result = self.engine.execute_silver_step(
            "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
        )
        
        self.assertEqual(step_name, "test_step")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Test error")
        self.assertFalse(result.get("skipped", False))
    
    def test_execute_silver_step_skipped(self):
        """Test silver step execution when skipped."""
        # Set up for incremental mode with no rows
        self.bronze_df.count.return_value = 0
        
        step_name, result = self.engine.execute_silver_step(
            "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "incremental"
        )
        
        self.assertEqual(step_name, "test_step")
        self.assertTrue(result.get("skipped", False))
        self.assertIn("transform", result)
        self.assertIn("validation", result)
        self.assertIn("write", result)
    
    def test_execute_existing_silver(self):
        """Test execution of existing silver step."""
        self.silver_step.existing = True
        
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.execution_engine.apply_column_rules') as mock_apply_rules:
            mock_stats = StageStats(
                stage="silver",
                step="test_step",
                total_rows=100,
                valid_rows=95,
                invalid_rows=5,
                validation_rate=95.0,
                duration_secs=1.0
            )
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)
            
            # Mock write operation
            with patch('sparkforge.execution_engine.time_write_operation') as mock_write:
                mock_write.return_value = (100, 0.5, None, None)
                
                step_name, result = self.engine.execute_silver_step(
                    "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
                )
                
                self.assertEqual(step_name, "test_step")
                self.assertIn("table_fqn", result)
                self.assertIn("validation", result)
                self.assertIn("write", result)
                self.assertFalse(result.get("skipped", False))
    
    def test_execute_silver_steps_sequential(self):
        """Test sequential execution of silver steps."""
        config = ExecutionConfig(mode=ExecutionMode.SEQUENTIAL)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        silver_steps = {"test_step": self.silver_step}
        bronze_valid = {"bronze1": self.bronze_df}
        
        # Mock successful execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.return_value = ("test_step", {"success": True})
            
            results = engine.execute_silver_steps(
                ["test_step"], silver_steps, bronze_valid, self.prior_silvers, "initial"
            )
            
            self.assertEqual(len(results), 1)
            self.assertIn("test_step", results)
            mock_execute.assert_called_once()
    
    def test_execute_silver_steps_parallel(self):
        """Test parallel execution of silver steps."""
        config = ExecutionConfig(mode=ExecutionMode.PARALLEL, max_workers=2)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        silver_steps = {"test_step": self.silver_step}
        bronze_valid = {"bronze1": self.bronze_df}
        
        # Mock successful execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.return_value = ("test_step", {"success": True})
            
            results = engine.execute_silver_steps(
                ["test_step"], silver_steps, bronze_valid, self.prior_silvers, "initial"
            )
            
            self.assertEqual(len(results), 1)
            self.assertIn("test_step", results)
            mock_execute.assert_called_once()
    
    def test_execute_silver_steps_adaptive(self):
        """Test adaptive execution of silver steps."""
        config = ExecutionConfig(mode=ExecutionMode.ADAPTIVE)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        silver_steps = {"test_step": self.silver_step}
        bronze_valid = {"bronze1": self.bronze_df}
        
        # Mock successful execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.return_value = ("test_step", {"success": True})
            
            results = engine.execute_silver_steps(
                ["test_step"], silver_steps, bronze_valid, self.prior_silvers, "initial"
            )
            
            self.assertEqual(len(results), 1)
            self.assertIn("test_step", results)
            mock_execute.assert_called_once()
    
    def test_execute_silver_steps_batch(self):
        """Test batch execution of silver steps."""
        config = ExecutionConfig(mode=ExecutionMode.BATCH, batch_size=2)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        silver_steps = {"test_step": self.silver_step}
        bronze_valid = {"bronze1": self.bronze_df}
        
        # Mock successful execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.return_value = ("test_step", {"success": True})
            
            results = engine.execute_silver_steps(
                ["test_step"], silver_steps, bronze_valid, self.prior_silvers, "initial"
            )
            
            self.assertEqual(len(results), 1)
            self.assertIn("test_step", results)
            mock_execute.assert_called_once()
    
    def test_retry_step_immediate(self):
        """Test immediate retry strategy."""
        config = ExecutionConfig(
            retry_strategy=RetryStrategy.IMMEDIATE,
            max_retries=2
        )
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        # Mock successful retry
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.side_effect = [
                ("test_step", {"error": "First failure"}),
                ("test_step", {"success": True})
            ]
            
            step_name, result = engine.retry_step(
                "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
            )
            
            self.assertEqual(step_name, "test_step")
            self.assertEqual(result["success"], True)
            self.assertEqual(mock_execute.call_count, 2)
    
    def test_retry_step_exponential_backoff(self):
        """Test exponential backoff retry strategy."""
        config = ExecutionConfig(
            retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            max_retries=3,
            retry_delay=1.0
        )
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        # Mock all retries fail
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.return_value = ("test_step", {"error": "Persistent failure"})
            
            step_name, result = engine.retry_step(
                "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
            )
            
            self.assertEqual(step_name, "test_step")
            self.assertIn("error", result)
            self.assertIn("Failed after", result["error"])
            self.assertEqual(mock_execute.call_count, 3)
    
    def test_retry_step_none(self):
        """Test no retry strategy."""
        config = ExecutionConfig(retry_strategy=RetryStrategy.NONE)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        step_name, result = engine.retry_step(
            "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
        )
        
        self.assertEqual(step_name, "test_step")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Retry disabled")
    
    def test_caching_functionality(self):
        """Test execution caching."""
        config = ExecutionConfig(enable_caching=True)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.execution_engine.apply_column_rules') as mock_apply_rules:
            mock_stats = StageStats(
                stage="silver",
                step="test_step",
                total_rows=100,
                valid_rows=95,
                invalid_rows=5,
                validation_rate=95.0,
                duration_secs=1.0
            )
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)
            
            # Mock write operation
            with patch('sparkforge.execution_engine.time_write_operation') as mock_write:
                mock_write.return_value = (100, 0.5, None, None)
                
                # First execution
                step_name, result1 = engine.execute_silver_step(
                    "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
                )
                
                # Second execution should use cache
                step_name, result2 = engine.execute_silver_step(
                    "test_step", self.silver_step, self.bronze_df, self.prior_silvers, "initial"
                )
                
                # Should only execute once due to caching
                self.assertEqual(mock_apply_rules.call_count, 1)
                self.assertEqual(result1, result2)
    
    def test_get_execution_stats(self):
        """Test getting execution statistics."""
        stats = self.engine.get_execution_stats()
        
        self.assertIsInstance(stats, ExecutionStats)
        self.assertEqual(stats.total_steps, 0)
        self.assertEqual(stats.successful_steps, 0)
        self.assertEqual(stats.failed_steps, 0)
        self.assertEqual(stats.skipped_steps, 0)
    
    def test_get_step_performance(self):
        """Test getting step performance metrics."""
        # Simulate some step executions
        self.engine._step_timings["test_step"] = [1.0, 1.5, 2.0]
        
        performance = self.engine.get_step_performance("test_step")
        
        self.assertEqual(performance["step_name"], "test_step")
        self.assertEqual(performance["execution_count"], 3)
        self.assertEqual(performance["average_duration"], 1.5)
        self.assertEqual(performance["min_duration"], 1.0)
        self.assertEqual(performance["max_duration"], 2.0)
        self.assertEqual(performance["total_duration"], 4.5)
    
    def test_get_step_performance_nonexistent(self):
        """Test getting performance for non-existent step."""
        performance = self.engine.get_step_performance("nonexistent_step")
        
        self.assertEqual(performance, {})
    
    def test_clear_cache(self):
        """Test clearing execution cache."""
        # Add something to cache
        self.engine._execution_cache["test_step"] = {"cached": True}
        
        self.engine.clear_cache()
        
        self.assertEqual(len(self.engine._execution_cache), 0)
    
    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        # Simulate cache hits and misses
        self.engine._execution_stats.cache_hits = 5
        self.engine._execution_stats.cache_misses = 5
        
        stats = self.engine.get_cache_stats()
        
        self.assertEqual(stats["cache_size"], 0)
        self.assertEqual(stats["cache_hits"], 5)
        self.assertEqual(stats["cache_misses"], 5)
        self.assertEqual(stats["hit_ratio"], 0.5)
    
    def test_stop_execution(self):
        """Test stopping execution."""
        # Add some mock futures
        mock_future = Mock(spec=Future)
        self.engine._active_futures.add(mock_future)
        
        self.engine.stop_execution()
        
        mock_future.cancel.assert_called_once()
        self.assertEqual(len(self.engine._active_futures), 0)
    
    def test_optimize_configuration(self):
        """Test configuration optimization."""
        # Simulate some step timings
        self.engine._step_timings = {
            "fast_step": [0.1, 0.2, 0.15],
            "slow_step": [5.0, 6.0, 5.5]
        }
        
        optimized_config = self.engine.optimize_configuration()
        
        self.assertIsInstance(optimized_config, ExecutionConfig)
        self.assertIn(optimized_config.mode, [ExecutionMode.PARALLEL, ExecutionMode.SEQUENTIAL, ExecutionMode.ADAPTIVE])
    
    def test_execution_context_manager(self):
        """Test execution context manager."""
        context = ExecutionContext(
            mode="initial",
            start_time=time.time(),
            end_time=None,
            duration_secs=0.0,
            run_id="test_execution"
        )
        
        with self.engine.execution_context(context):
            # Engine should be available in context
            self.assertIsNotNone(self.engine)
        
        # Execution should be stopped after context
        self.assertEqual(len(self.engine._active_futures), 0)


class TestExecutionEngineIntegration(unittest.TestCase):
    """Test ExecutionEngine integration scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.logger = PipelineLogger(verbose=False)
        self.thresholds = {"silver": 95.0}
        self.schema = "test_schema"
    
    def test_complex_pipeline_execution(self):
        """Test execution of a complex pipeline."""
        # Create multiple silver steps
        silver_steps = {
            "step1": SilverStep(
                name="step1",
                source_bronze="bronze1",
                transform=lambda spark, df: df,
                rules={"id": ["not_null"]},
                table_name="silver1"
            ),
            "step2": SilverStep(
                name="step2",
                source_bronze="bronze2",
                transform=lambda spark, df: df,
                rules={"id": ["not_null"]},
                table_name="silver2"
            ),
            "step3": SilverStep(
                name="step3",
                source_bronze="bronze3",
                transform=lambda spark, df: df,
                rules={"id": ["not_null"]},
                table_name="silver3"
            )
        }
        
        # Create bronze DataFrames
        bronze_valid = {
            "bronze1": Mock(),
            "bronze2": Mock(),
            "bronze3": Mock()
        }
        
        for df in bronze_valid.values():
            df.count.return_value = 100
        
        # Create execution engine
        config = ExecutionConfig(mode=ExecutionMode.PARALLEL, max_workers=2)
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        # Mock successful execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.side_effect = [
                ("step1", {"success": True}),
                ("step2", {"success": True}),
                ("step3", {"success": True})
            ]
            
            results = engine.execute_silver_steps(
                ["step1", "step2", "step3"], silver_steps, bronze_valid, {}, "initial"
            )
            
            self.assertEqual(len(results), 3)
            self.assertIn("step1", results)
            self.assertIn("step2", results)
            self.assertIn("step3", results)
            self.assertEqual(mock_execute.call_count, 3)
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms."""
        config = ExecutionConfig(
            retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            max_retries=2
        )
        engine = ExecutionEngine(
            spark=self.spark,
            logger=self.logger,
            thresholds=self.thresholds,
            schema=self.schema,
            config=config
        )
        
        # Mock failing execution
        with patch.object(engine, 'execute_silver_step') as mock_execute:
            mock_execute.side_effect = [
                ("test_step", {"error": "First failure"}),
                ("test_step", {"error": "Second failure"}),
                ("test_step", {"error": "Third failure"})
            ]
            
            step_name, result = engine.retry_step(
                "test_step", SilverStep(
                    name="test_step",
                    source_bronze="bronze1",
                    transform=lambda spark, df: df,
                    rules={"id": ["not_null"]},
                    table_name="silver1"
                ), Mock(), {}, "initial"
            )
            
            self.assertEqual(step_name, "test_step")
            self.assertIn("error", result)
            self.assertIn("Failed after", result["error"])
            self.assertEqual(mock_execute.call_count, 2)  # max_retries = 2, so 2 calls


def run_execution_engine_tests():
    """Run all execution engine tests."""
    print("🧪 Running Execution Engine Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestExecutionMode,
        TestRetryStrategy,
        TestExecutionConfig,
        TestExecutionResult,
        TestExecutionStats,
        TestExecutionEngine,
        TestExecutionEngineIntegration
    ]
    
    for test_class in test_classes:
        test_suite.addTest(unittest.makeSuite(test_class))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n❌ Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_execution_engine_tests()
    if success:
        print("\n🎉 All execution engine tests passed!")
    else:
        print("\n❌ Some execution engine tests failed!")
