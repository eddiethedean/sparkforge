#!/usr/bin/env python3
"""
Simple tests for dynamic parallel execution module.

These tests focus on basic functionality and API compatibility.
"""

import unittest
import time
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from sparkforge.parallel_execution import (
    DynamicWorkerPool, DynamicParallelExecutor, ExecutionTask, TaskPriority,
    TaskMetrics, WorkerMetrics, SystemResources, create_execution_task,
    get_dynamic_executor
)


class TestExecutionTask(unittest.TestCase):
    """Test ExecutionTask dataclass."""

    def test_execution_task_creation(self):
        """Test creating execution task."""
        def test_function(x, y):
            return x + y
        
        task = ExecutionTask(
            task_id="test_task",
            function=test_function,
            args=(1, 2),
            priority=TaskPriority.HIGH
        )
        
        self.assertEqual(task.task_id, "test_task")
        self.assertEqual(task.function, test_function)
        self.assertEqual(task.args, (1, 2))
        self.assertEqual(task.priority, TaskPriority.HIGH)
        self.assertEqual(task.kwargs, {})
        self.assertTrue(task.is_ready)

    def test_execution_task_with_dependencies(self):
        """Test execution task with dependencies."""
        def test_function():
            return "result"
        
        task = ExecutionTask(
            task_id="test_task",
            function=test_function,
            dependencies={"dep1", "dep2"}
        )
        
        self.assertEqual(task.dependencies, {"dep1", "dep2"})
        self.assertFalse(task.is_ready)

    def test_create_execution_task_function(self):
        """Test create_execution_task helper function."""
        def test_function(x, y, z=10):
            return x + y + z
        
        task = create_execution_task(
            "helper_task",
            test_function,
            1, 2,  # args
            priority=TaskPriority.CRITICAL,
            z=5  # kwargs
        )
        
        self.assertEqual(task.task_id, "helper_task")
        self.assertEqual(task.args, (1, 2))
        self.assertEqual(task.kwargs, {"z": 5})
        self.assertEqual(task.priority, TaskPriority.CRITICAL)


class TestTaskMetrics(unittest.TestCase):
    """Test TaskMetrics dataclass."""

    def test_task_metrics_creation(self):
        """Test creating task metrics."""
        now = datetime.utcnow()
        metrics = TaskMetrics(
            task_id="test_task",
            start_time=now,
            result="test_result"
        )
        
        self.assertEqual(metrics.task_id, "test_task")
        self.assertEqual(metrics.start_time, now)
        self.assertEqual(metrics.result, "test_result")
        self.assertFalse(metrics.is_completed)
        self.assertEqual(metrics.throughput_rows_per_second, 0.0)

    def test_task_metrics_completion(self):
        """Test task metrics completion."""
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(seconds=2)
        
        metrics = TaskMetrics(
            task_id="test_task",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=2.0,
            success=True,
            rows_processed=100
        )
        
        self.assertTrue(metrics.is_completed)
        self.assertEqual(metrics.throughput_rows_per_second, 50.0)


class TestSystemResources(unittest.TestCase):
    """Test SystemResources dataclass."""

    def test_system_resources_creation(self):
        """Test creating system resources."""
        resources = SystemResources(
            total_memory_mb=8192.0,
            available_memory_mb=4096.0,
            total_cpu_cores=8,
            cpu_usage_percent=50.0,
            memory_usage_percent=50.0
        )
        
        self.assertEqual(resources.total_memory_mb, 8192.0)
        self.assertEqual(resources.available_memory_mb, 4096.0)
        self.assertEqual(resources.total_cpu_cores, 8)
        self.assertEqual(resources.memory_utilization, 50.0)
        self.assertFalse(resources.is_under_load)

    def test_system_resources_under_load(self):
        """Test system resources under load."""
        resources = SystemResources(
            total_memory_mb=8192.0,
            available_memory_mb=1000.0,
            total_cpu_cores=8,
            cpu_usage_percent=90.0,
            memory_usage_percent=87.5
        )
        
        self.assertTrue(resources.is_under_load)


class TestDynamicWorkerPool(unittest.TestCase):
    """Test DynamicWorkerPool class."""

    def setUp(self):
        """Set up test fixtures."""
        self.worker_pool = DynamicWorkerPool(
            min_workers=1,
            max_workers=4,
            logger=Mock()
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.worker_pool.shutdown()

    def test_worker_pool_initialization(self):
        """Test worker pool initialization."""
        self.assertEqual(self.worker_pool.min_workers, 1)
        self.assertEqual(self.worker_pool.max_workers, 4)
        self.assertEqual(self.worker_pool.current_worker_count, 1)
        self.assertEqual(self.worker_pool.active_workers, 1)
        self.assertFalse(self.worker_pool.shutdown_flag)

    def test_submit_simple_task(self):
        """Test submitting a simple task."""
        def simple_function():
            return "success"
        
        task = ExecutionTask(
            task_id="simple_task",
            function=simple_function
        )
        
        task_id = self.worker_pool.submit_task(task)
        self.assertEqual(task_id, "simple_task")
        
        # Wait for completion
        success = self.worker_pool.wait_for_completion(timeout=5.0)
        self.assertTrue(success)
        
        # Check results
        self.assertIn("simple_task", self.worker_pool.completed_tasks)
        task_metrics = self.worker_pool.completed_tasks["simple_task"]
        self.assertTrue(task_metrics.success)
        self.assertEqual(task_metrics.result, "success")

    def test_submit_task_with_args(self):
        """Test submitting a task with arguments."""
        def add_function(x, y):
            return x + y
        
        task = ExecutionTask(
            task_id="add_task",
            function=add_function,
            args=(5, 3)
        )
        
        task_id = self.worker_pool.submit_task(task)
        self.assertEqual(task_id, "add_task")
        
        # Wait for completion
        success = self.worker_pool.wait_for_completion(timeout=5.0)
        self.assertTrue(success)
        
        # Check results
        self.assertIn("add_task", self.worker_pool.completed_tasks)
        task_metrics = self.worker_pool.completed_tasks["add_task"]
        self.assertTrue(task_metrics.success)
        self.assertEqual(task_metrics.result, 8)

    def test_submit_task_with_kwargs(self):
        """Test submitting a task with keyword arguments."""
        def multiply_function(x, y, multiplier=1):
            return (x + y) * multiplier
        
        task = ExecutionTask(
            task_id="multiply_task",
            function=multiply_function,
            args=(2, 3),
            kwargs={"multiplier": 4}
        )
        
        task_id = self.worker_pool.submit_task(task)
        self.assertEqual(task_id, "multiply_task")
        
        # Wait for completion
        success = self.worker_pool.wait_for_completion(timeout=5.0)
        self.assertTrue(success)
        
        # Check results
        self.assertIn("multiply_task", self.worker_pool.completed_tasks)
        task_metrics = self.worker_pool.completed_tasks["multiply_task"]
        self.assertTrue(task_metrics.success)
        self.assertEqual(task_metrics.result, 20)

    def test_task_priority(self):
        """Test task priority execution."""
        results = []
        
        def low_priority_task():
            time.sleep(0.1)
            results.append("low")
            return "low"
        
        def high_priority_task():
            time.sleep(0.1)
            results.append("high")
            return "high"
        
        # Submit low priority first
        low_task = ExecutionTask(
            task_id="low_task",
            function=low_priority_task,
            priority=TaskPriority.LOW
        )
        self.worker_pool.submit_task(low_task)
        
        # Submit high priority second
        high_task = ExecutionTask(
            task_id="high_task",
            function=high_priority_task,
            priority=TaskPriority.HIGH
        )
        self.worker_pool.submit_task(high_task)
        
        # Wait for completion
        success = self.worker_pool.wait_for_completion(timeout=5.0)
        self.assertTrue(success)
        
        # High priority should complete first due to priority queue
        self.assertEqual(len(results), 2)
        self.assertIn("high", results)
        self.assertIn("low", results)

    def test_task_failure(self):
        """Test task failure handling."""
        def failing_function():
            raise ValueError("Test error")
        
        task = ExecutionTask(
            task_id="failing_task",
            function=failing_function,
            max_retries=0  # No retries for test
        )
        
        task_id = self.worker_pool.submit_task(task)
        self.assertEqual(task_id, "failing_task")
        
        # Wait for completion
        success = self.worker_pool.wait_for_completion(timeout=5.0)
        self.assertTrue(success)
        
        # Check failure
        self.assertIn("failing_task", self.worker_pool.failed_tasks)
        task_metrics = self.worker_pool.failed_tasks["failing_task"]
        self.assertFalse(task_metrics.success)
        self.assertIn("Test error", task_metrics.error_message)

    def test_get_performance_metrics(self):
        """Test getting performance metrics."""
        def test_function():
            return "test"
        
        task = ExecutionTask(
            task_id="test_task",
            function=test_function
        )
        
        self.worker_pool.submit_task(task)
        self.worker_pool.wait_for_completion(timeout=5.0)
        
        metrics = self.worker_pool.get_performance_metrics()
        
        self.assertIn("total_tasks", metrics)
        self.assertIn("successful_tasks", metrics)
        self.assertIn("success_rate", metrics)
        self.assertIn("worker_count", metrics)
        self.assertIn("active_workers", metrics)
        self.assertIn("queue_size", metrics)
        self.assertIn("running_tasks", metrics)
        self.assertIn("average_efficiency", metrics)


class TestDynamicParallelExecutor(unittest.TestCase):
    """Test DynamicParallelExecutor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = DynamicParallelExecutor(
            logger=Mock()
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.executor.shutdown()

    def test_executor_initialization(self):
        """Test executor initialization."""
        self.assertIsNotNone(self.executor.config)
        self.assertIsNotNone(self.executor.worker_pool)
        self.assertTrue(self.executor.optimization_enabled)

    def test_execute_parallel_simple(self):
        """Test simple parallel execution."""
        def task1():
            return "task1_result"
        
        def task2():
            return "task2_result"
        
        tasks = [
            ExecutionTask(task_id="task1", function=task1),
            ExecutionTask(task_id="task2", function=task2)
        ]
        
        result = self.executor.execute_parallel(tasks, wait_for_completion=True)
        
        self.assertTrue(result["success"])
        self.assertIn("execution_time", result)
        self.assertIn("metrics", result)
        self.assertIn("task_ids", result)
        self.assertEqual(len(result["task_ids"]), 2)

    def test_execute_parallel_with_priority(self):
        """Test parallel execution with different priorities."""
        results = []
        
        def low_task():
            time.sleep(0.1)
            results.append("low")
            return "low"
        
        def high_task():
            time.sleep(0.1)
            results.append("high")
            return "high"
        
        tasks = [
            ExecutionTask(task_id="low", function=low_task, priority=TaskPriority.LOW),
            ExecutionTask(task_id="high", function=high_task, priority=TaskPriority.HIGH)
        ]
        
        result = self.executor.execute_parallel(tasks, wait_for_completion=True)
        
        self.assertTrue(result["success"])
        self.assertEqual(len(results), 2)

    def test_get_performance_metrics(self):
        """Test getting performance metrics."""
        def test_function():
            return "test"
        
        task = ExecutionTask(task_id="test", function=test_function)
        self.executor.execute_parallel([task], wait_for_completion=True)
        
        metrics = self.executor.get_performance_metrics()
        
        self.assertIn("total_tasks", metrics)
        self.assertIn("successful_tasks", metrics)
        self.assertIn("success_rate", metrics)

    def test_get_optimization_recommendations(self):
        """Test getting optimization recommendations."""
        # With no execution history
        recommendations = self.executor.get_optimization_recommendations()
        self.assertIn("Insufficient data", recommendations[0])
        
        # Add some execution history
        self.executor.execution_history = [
            {"success_rate": 80.0, "efficiency": 0.5, "worker_count": 2, "execution_time": 10.0},
            {"success_rate": 85.0, "efficiency": 0.6, "worker_count": 2, "execution_time": 12.0},
            {"success_rate": 90.0, "efficiency": 0.7, "worker_count": 2, "execution_time": 15.0}
        ]
        
        recommendations = self.executor.get_optimization_recommendations()
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)


class TestGlobalFunctions(unittest.TestCase):
    """Test global utility functions."""

    def test_get_dynamic_executor(self):
        """Test getting global dynamic executor."""
        executor1 = get_dynamic_executor()
        executor2 = get_dynamic_executor()
        
        # Should return the same instance
        self.assertIs(executor1, executor2)
        
        # Clean up
        executor1.shutdown()

    def test_create_execution_task(self):
        """Test create_execution_task function."""
        def test_function(x, y, z=10):
            return x + y + z
        
        task = create_execution_task(
            "test",
            test_function,
            1, 2,
            priority=TaskPriority.HIGH,
            z=5
        )
        
        self.assertEqual(task.task_id, "test")
        self.assertEqual(task.args, (1, 2))
        self.assertEqual(task.kwargs, {"z": 5})
        self.assertEqual(task.priority, TaskPriority.HIGH)


if __name__ == "__main__":
    unittest.main()
