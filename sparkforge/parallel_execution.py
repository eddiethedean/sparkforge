#!/usr/bin/env python3
"""
Simplified parallel execution system for SparkForge.

This module provides a clean, reliable parallel execution system with
dynamic worker allocation and performance monitoring.

Key Features:
- Simple API for parallel execution
- Dynamic worker allocation based on workload
- Performance monitoring and metrics
- Reliable task execution and error handling
- Easy-to-use configuration
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Callable, Set, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
import queue
import psutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from collections import defaultdict, deque
import statistics
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .models import PipelineConfig, ValidationThresholds, ParallelConfig
from .logger import PipelineLogger
from .performance_cache import get_performance_cache, PerformanceCache


class TaskPriority(Enum):
    """Priority levels for task execution."""
    CRITICAL = 1  # Must complete first
    HIGH = 2      # High priority
    NORMAL = 3    # Normal priority
    LOW = 4       # Low priority
    BACKGROUND = 5  # Background tasks


@dataclass
class TaskMetrics:
    """Metrics for a single task execution."""
    task_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    rows_processed: int = 0
    rows_written: int = 0
    success: bool = False
    error_message: Optional[str] = None
    retry_count: int = 0
    result: Any = None
    
    @property
    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self.end_time is not None
    
    @property
    def throughput_rows_per_second(self) -> float:
        """Calculate throughput in rows per second."""
        if self.duration_seconds and self.duration_seconds > 0:
            return self.rows_processed / self.duration_seconds
        return 0.0


@dataclass
class WorkerMetrics:
    """Metrics for a worker thread."""
    worker_id: str
    tasks_completed: int = 0
    total_duration: float = 0.0
    total_memory_usage: float = 0.0
    total_cpu_usage: float = 0.0
    current_task: Optional[str] = None
    is_idle: bool = True
    last_activity: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def average_task_duration(self) -> float:
        """Calculate average task duration."""
        if self.tasks_completed > 0:
            return self.total_duration / self.tasks_completed
        return 0.0
    
    @property
    def efficiency_score(self) -> float:
        """Calculate worker efficiency score."""
        if self.tasks_completed == 0:
            return 0.0
        
        # Efficiency based on task completion rate and resource usage
        completion_rate = self.tasks_completed / max(self.total_duration, 1.0)
        resource_efficiency = 1.0 / max(self.total_memory_usage + self.total_cpu_usage, 1.0)
        return completion_rate * resource_efficiency


@dataclass
class ExecutionTask:
    """Represents a task to be executed."""
    task_id: str
    function: Callable
    args: Tuple[Any, ...] = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    dependencies: Set[str] = field(default_factory=set)
    estimated_duration: float = 0.0
    memory_requirement_mb: float = 0.0
    cpu_requirement_percent: float = 0.0
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    result: Any = None
    
    @property
    def is_ready(self) -> bool:
        """Check if task is ready to execute (dependencies satisfied)."""
        return len(self.dependencies) == 0


@dataclass
class SystemResources:
    """System resource information."""
    total_memory_mb: float
    available_memory_mb: float
    total_cpu_cores: int
    cpu_usage_percent: float
    memory_usage_percent: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def memory_utilization(self) -> float:
        """Calculate memory utilization percentage."""
        return (self.total_memory_mb - self.available_memory_mb) / self.total_memory_mb * 100
    
    @property
    def is_under_load(self) -> bool:
        """Check if system is under high load."""
        return self.cpu_usage_percent > 80 or self.memory_utilization > 80


class DynamicWorkerPool:
    """Simplified dynamic worker pool with intelligent allocation."""
    
    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 16,
        logger: Optional[PipelineLogger] = None
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.logger = logger or PipelineLogger()
        
        # Worker management
        self.workers: Dict[str, WorkerMetrics] = {}
        self.worker_pool: Optional[ThreadPoolExecutor] = None
        self.current_worker_count = min_workers
        
        # Task management
        self.task_queue = queue.PriorityQueue()
        self.running_tasks: Dict[str, Future] = {}
        self.completed_tasks: Dict[str, TaskMetrics] = {}
        self.failed_tasks: Dict[str, TaskMetrics] = {}
        
        # Performance tracking
        self.performance_history: List[Dict[str, Any]] = []
        self.optimization_enabled = True
        
        # Resource monitoring
        self.resource_monitor = ResourceMonitor()
        self.last_optimization = datetime.utcnow()
        self.optimization_interval = timedelta(seconds=30)
        
        # Threading
        self._lock = threading.RLock()
        self._shutdown = False
        self._monitor_thread: Optional[threading.Thread] = None
        
        # Add missing attributes for compatibility
        self.active_workers = 0
        self.shutdown_flag = False
        
        self._initialize_workers()
    
    def _initialize_workers(self) -> None:
        """Initialize the worker pool."""
        self.worker_pool = ThreadPoolExecutor(
            max_workers=self.current_worker_count,
            thread_name_prefix="DynamicWorker"
        )
        
        # Initialize worker metrics
        for i in range(self.current_worker_count):
            worker_id = f"worker_{i}"
            self.workers[worker_id] = WorkerMetrics(worker_id=worker_id)
        
        self.active_workers = self.current_worker_count
        self.logger.info(f"Initialized dynamic worker pool with {self.current_worker_count} workers")
    
    def submit_task(self, task: ExecutionTask) -> str:
        """Submit a task for execution."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Worker pool is shutting down")
            
            # Add task to queue with priority
            priority_value = (task.priority.value, task.created_at.timestamp())
            self.task_queue.put((priority_value, task))
            
            self.logger.debug(f"Submitted task {task.task_id} with priority {task.priority.name}")
            
            # Start monitoring if not already running
            if self._monitor_thread is None or not self._monitor_thread.is_alive():
                self._start_monitoring()
            
            return task.task_id
    
    def _start_monitoring(self) -> None:
        """Start the monitoring thread."""
        self._monitor_thread = threading.Thread(
            target=self._monitor_execution,
            daemon=True,
            name="WorkerMonitor"
        )
        self._monitor_thread.start()
    
    def _monitor_execution(self) -> None:
        """Monitor task execution and optimize worker allocation."""
        while not self._shutdown:
            try:
                with self._lock:
                    self._process_ready_tasks()
                    self._optimize_worker_allocation()
                    self._cleanup_completed_tasks()
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error in monitoring thread: {e}")
                time.sleep(5)  # Wait before retrying
    
    def _process_ready_tasks(self) -> None:
        """Process tasks that are ready to execute."""
        ready_tasks = []
        
        # Get ready tasks from queue
        while not self.task_queue.empty():
            try:
                priority, task = self.task_queue.get_nowait()
                if task.is_ready:
                    ready_tasks.append(task)
                else:
                    # Put back in queue if not ready
                    self.task_queue.put((priority, task))
                    break
            except queue.Empty:
                break
        
        # Submit ready tasks to workers
        for task in ready_tasks:
            if len(self.running_tasks) < self.current_worker_count:
                self._submit_task_to_worker(task)
            else:
                # Put back in queue if no workers available
                priority_value = (task.priority.value, task.created_at.timestamp())
                self.task_queue.put((priority_value, task))
                break
    
    def _submit_task_to_worker(self, task: ExecutionTask) -> None:
        """Submit a task to an available worker."""
        if self.worker_pool is None:
            return
        
        # Find available worker
        available_worker = None
        for worker_id, metrics in self.workers.items():
            if metrics.is_idle:
                available_worker = worker_id
                break
        
        if available_worker is None:
            return
        
        # Update worker metrics
        self.workers[available_worker].is_idle = False
        self.workers[available_worker].current_task = task.task_id
        self.workers[available_worker].last_activity = datetime.utcnow()
        
        # Submit to worker pool
        future = self.worker_pool.submit(self._execute_task, task, available_worker)
        self.running_tasks[task.task_id] = future
        
        self.logger.debug(f"Submitted task {task.task_id} to worker {available_worker}")
    
    def _execute_task(self, task: ExecutionTask, worker_id: str) -> TaskMetrics:
        """Execute a task and return metrics."""
        task_metrics = TaskMetrics(
            task_id=task.task_id,
            start_time=datetime.utcnow()
        )
        
        try:
            # Monitor resource usage
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            start_cpu = psutil.cpu_percent()
            
            # Execute the task - fix the function call
            if task.kwargs:
                result = task.function(*task.args, **task.kwargs)
            else:
                result = task.function(*task.args)
            
            # Store result in task
            task.result = result
            task_metrics.result = result
            
            # Calculate metrics
            task_metrics.end_time = datetime.utcnow()
            task_metrics.duration_seconds = (task_metrics.end_time - task_metrics.start_time).total_seconds()
            task_metrics.success = True
            
            # Update resource usage
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            end_cpu = psutil.cpu_percent()
            task_metrics.memory_usage_mb = end_memory - start_memory
            task_metrics.cpu_usage_percent = (start_cpu + end_cpu) / 2
            
            # Update worker metrics
            with self._lock:
                worker_metrics = self.workers[worker_id]
                worker_metrics.tasks_completed += 1
                worker_metrics.total_duration += task_metrics.duration_seconds
                worker_metrics.total_memory_usage += task_metrics.memory_usage_mb
                worker_metrics.total_cpu_usage += task_metrics.cpu_usage_percent
                worker_metrics.is_idle = True
                worker_metrics.current_task = None
                worker_metrics.last_activity = datetime.utcnow()
            
            self.logger.debug(f"Task {task.task_id} completed successfully in {task_metrics.duration_seconds:.2f}s")
            
        except Exception as e:
            task_metrics.end_time = datetime.utcnow()
            task_metrics.duration_seconds = (task_metrics.end_time - task_metrics.start_time).total_seconds()
            task_metrics.success = False
            task_metrics.error_message = str(e)
            
            self.logger.error(f"Task {task.task_id} failed: {e}")
            
            # Handle retries
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task_metrics.retry_count = task.retry_count
                
                # Resubmit task with exponential backoff
                delay = 2 ** task.retry_count
                time.sleep(delay)
                
                with self._lock:
                    priority_value = (task.priority.value, task.created_at.timestamp())
                    self.task_queue.put((priority_value, task))
            
            # Update worker metrics
            with self._lock:
                worker_metrics = self.workers[worker_id]
                worker_metrics.is_idle = True
                worker_metrics.current_task = None
                worker_metrics.last_activity = datetime.utcnow()
        
        return task_metrics
    
    def _optimize_worker_allocation(self) -> None:
        """Optimize worker allocation based on current workload and performance."""
        if not self.optimization_enabled:
            return
        
        now = datetime.utcnow()
        if now - self.last_optimization < self.optimization_interval:
            return
        
        self.last_optimization = now
        
        # Get current system resources
        resources = self.resource_monitor.get_current_resources()
        
        # Calculate optimal worker count
        optimal_workers = self._calculate_optimal_workers(resources)
        
        # Adjust worker count if needed
        if optimal_workers != self.current_worker_count:
            self._adjust_worker_count(optimal_workers)
    
    def _calculate_optimal_workers(self, resources: SystemResources) -> int:
        """Calculate optimal number of workers based on current conditions."""
        # Base calculation on system resources
        available_cores = resources.total_cpu_cores
        memory_per_worker = 512  # MB per worker
        available_workers_by_memory = int(resources.available_memory_mb / memory_per_worker)
        
        # Consider current workload
        queue_size = self.task_queue.qsize()
        running_tasks = len(self.running_tasks)
        
        # Dynamic allocation based on current workload
        if queue_size > running_tasks * 2:
            # High queue, increase workers
            optimal = min(self.max_workers, self.current_worker_count + 2)
        elif queue_size < running_tasks:
            # Low queue, decrease workers
            optimal = max(self.min_workers, self.current_worker_count - 1)
        else:
            optimal = self.current_worker_count
        
        # Apply constraints
        optimal = max(self.min_workers, min(optimal, self.max_workers))
        optimal = min(optimal, available_workers_by_memory)
        
        return optimal
    
    def _adjust_worker_count(self, new_count: int) -> None:
        """Adjust the number of workers."""
        if new_count == self.current_worker_count:
            return
        
        self.logger.info(f"Adjusting worker count from {self.current_worker_count} to {new_count}")
        
        if new_count > self.current_worker_count:
            # Add workers
            self._add_workers(new_count - self.current_worker_count)
        else:
            # Remove workers (gracefully)
            self._remove_workers(self.current_worker_count - new_count)
        
        self.current_worker_count = new_count
        self.active_workers = new_count
    
    def _add_workers(self, count: int) -> None:
        """Add new workers to the pool."""
        # Create new worker pool with increased size
        old_pool = self.worker_pool
        self.worker_pool = ThreadPoolExecutor(
            max_workers=self.current_worker_count + count,
            thread_name_prefix="DynamicWorker"
        )
        
        # Add new worker metrics
        for i in range(count):
            worker_id = f"worker_{self.current_worker_count + i}"
            self.workers[worker_id] = WorkerMetrics(worker_id=worker_id)
        
        # Shutdown old pool
        if old_pool:
            old_pool.shutdown(wait=False)
    
    def _remove_workers(self, count: int) -> None:
        """Remove workers from the pool."""
        # Mark workers as idle and let them finish current tasks
        workers_to_remove = []
        for worker_id, metrics in self.workers.items():
            if metrics.is_idle and len(workers_to_remove) < count:
                workers_to_remove.append(worker_id)
        
        # Remove worker metrics
        for worker_id in workers_to_remove:
            del self.workers[worker_id]
    
    def _cleanup_completed_tasks(self) -> None:
        """Clean up completed tasks and update metrics."""
        completed_task_ids = []
        
        for task_id, future in self.running_tasks.items():
            if future.done():
                try:
                    task_metrics = future.result()
                    if task_metrics.success:
                        self.completed_tasks[task_id] = task_metrics
                    else:
                        self.failed_tasks[task_id] = task_metrics
                    completed_task_ids.append(task_id)
                except Exception as e:
                    self.logger.error(f"Error getting result for task {task_id}: {e}")
                    completed_task_ids.append(task_id)
        
        # Remove completed tasks from running tasks
        for task_id in completed_task_ids:
            del self.running_tasks[task_id]
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        with self._lock:
            total_tasks = len(self.completed_tasks) + len(self.failed_tasks) + len(self.running_tasks)
            successful_tasks = len(self.completed_tasks)
            failed_tasks = len(self.failed_tasks)
            
            success_rate = successful_tasks / max(total_tasks, 1) * 100
            
            # Calculate average task duration
            durations = [t.duration_seconds for t in self.completed_tasks.values() if t.duration_seconds]
            avg_duration = statistics.mean(durations) if durations else 0.0
            
            # Calculate worker efficiency
            worker_efficiencies = [w.efficiency_score for w in self.workers.values()]
            avg_efficiency = statistics.mean(worker_efficiencies) if worker_efficiencies else 0.0
            
            return {
                "total_tasks": total_tasks,
                "successful_tasks": successful_tasks,
                "failed_tasks": failed_tasks,
                "success_rate": success_rate,
                "average_duration": avg_duration,
                "worker_count": self.current_worker_count,
                "active_workers": self.active_workers,
                "queue_size": self.task_queue.qsize(),
                "running_tasks": len(self.running_tasks),
                "average_efficiency": avg_efficiency,
                "worker_efficiencies": {w.worker_id: w.efficiency_score for w in self.workers.values()}
            }
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all tasks to complete."""
        start_time = time.time()
        
        while True:
            with self._lock:
                if self.task_queue.empty() and len(self.running_tasks) == 0:
                    return True
            
            if timeout and (time.time() - start_time) > timeout:
                return False
            
            time.sleep(0.1)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the worker pool."""
        self._shutdown = True
        self.shutdown_flag = True
        
        if self.worker_pool:
            self.worker_pool.shutdown(wait=wait)
        
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)


class ResourceMonitor:
    """Monitors system resources for optimization decisions."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def get_current_resources(self) -> SystemResources:
        """Get current system resource information."""
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        return SystemResources(
            total_memory_mb=memory.total / 1024 / 1024,
            available_memory_mb=memory.available / 1024 / 1024,
            total_cpu_cores=psutil.cpu_count(),
            cpu_usage_percent=cpu_percent,
            memory_usage_percent=memory.percent
        )
    
    def get_resource_trends(self, duration_minutes: int = 5) -> Dict[str, List[float]]:
        """Get resource usage trends over time."""
        # This would typically store historical data
        # For now, return current values
        resources = self.get_current_resources()
        
        return {
            "cpu_usage": [resources.cpu_usage_percent],
            "memory_usage": [resources.memory_usage_percent],
            "timestamps": [resources.timestamp.timestamp()]
        }


class DynamicParallelExecutor:
    """Simplified interface for dynamic parallel execution."""
    
    def __init__(
        self,
        config: Optional[ParallelConfig] = None,
        logger: Optional[PipelineLogger] = None
    ):
        self.config = config or ParallelConfig(enabled=True, max_workers=4, timeout_secs=300)
        self.logger = logger or PipelineLogger()
        
        # Initialize worker pool
        self.worker_pool = DynamicWorkerPool(
            min_workers=1,
            max_workers=self.config.max_workers,
            logger=self.logger
        )
        
        # Performance tracking
        self.execution_history: List[Dict[str, Any]] = []
        self.optimization_enabled = True
    
    def execute_parallel(
        self,
        tasks: List[ExecutionTask],
        wait_for_completion: bool = True,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Execute tasks in parallel with dynamic optimization."""
        if not self.config.enabled:
            self.logger.warning("Parallel execution is disabled")
            return {"success": False, "error": "Parallel execution disabled"}
        
        start_time = time.time()
        self.logger.info(f"Starting parallel execution of {len(tasks)} tasks")
        
        # Submit all tasks
        task_ids = []
        for task in tasks:
            task_id = self.worker_pool.submit_task(task)
            task_ids.append(task_id)
        
        # Wait for completion if requested
        if wait_for_completion:
            success = self.worker_pool.wait_for_completion(timeout)
            if not success:
                self.logger.warning("Parallel execution timed out")
        
        # Get results
        execution_time = time.time() - start_time
        metrics = self.worker_pool.get_performance_metrics()
        
        # Store execution history
        execution_record = {
            "timestamp": datetime.utcnow(),
            "task_count": len(tasks),
            "execution_time": execution_time,
            "success_rate": metrics["success_rate"],
            "worker_count": metrics["worker_count"],
            "efficiency": metrics["average_efficiency"]
        }
        self.execution_history.append(execution_record)
        
        # Log results
        self.logger.info(
            f"Parallel execution completed: {metrics['successful_tasks']}/{metrics['total_tasks']} "
            f"tasks successful ({metrics['success_rate']:.1f}%) in {execution_time:.2f}s"
        )
        
        return {
            "success": True,
            "execution_time": execution_time,
            "metrics": metrics,
            "task_ids": task_ids
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        return self.worker_pool.get_performance_metrics()
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get recommendations for optimizing parallel execution."""
        recommendations = []
        
        if len(self.execution_history) < 2:
            return ["Insufficient data for optimization recommendations"]
        
        recent_executions = self.execution_history[-10:]  # Last 10 executions
        
        # Analyze success rate
        avg_success_rate = statistics.mean([e["success_rate"] for e in recent_executions])
        if avg_success_rate < 90:
            recommendations.append("Low success rate detected. Consider reducing parallelism or increasing timeouts.")
        
        # Analyze efficiency
        avg_efficiency = statistics.mean([e["efficiency"] for e in recent_executions])
        if avg_efficiency < 0.7:
            recommendations.append("Low efficiency detected. Consider adjusting worker allocation strategy.")
        
        # Analyze worker utilization
        avg_workers = statistics.mean([e["worker_count"] for e in recent_executions])
        if avg_workers < self.config.max_workers * 0.5:
            recommendations.append("Workers underutilized. Consider reducing max_workers or increasing task complexity.")
        
        # Analyze execution time trends
        if len(recent_executions) >= 3:
            recent_times = [e["execution_time"] for e in recent_executions[-3:]]
            if all(recent_times[i] > recent_times[i-1] for i in range(1, len(recent_times))):
                recommendations.append("Increasing execution times detected. Consider optimizing task functions.")
        
        return recommendations
    
    def shutdown(self) -> None:
        """Shutdown the parallel executor."""
        self.worker_pool.shutdown(wait=True)
        self.logger.info("Dynamic parallel executor shutdown complete")


# Global instance for easy access
_dynamic_executor: Optional[DynamicParallelExecutor] = None


def get_dynamic_executor() -> DynamicParallelExecutor:
    """Get global dynamic executor instance."""
    global _dynamic_executor
    if _dynamic_executor is None:
        _dynamic_executor = DynamicParallelExecutor()
    return _dynamic_executor


def create_execution_task(
    task_id: str,
    function: Callable,
    *args,
    priority: TaskPriority = TaskPriority.NORMAL,
    dependencies: Optional[Set[str]] = None,
    estimated_duration: float = 0.0,
    memory_requirement_mb: float = 0.0,
    timeout_seconds: Optional[int] = None,
    **kwargs
) -> ExecutionTask:
    """Create an execution task with the given parameters."""
    return ExecutionTask(
        task_id=task_id,
        function=function,
        args=args,
        kwargs=kwargs,
        priority=priority,
        dependencies=dependencies or set(),
        estimated_duration=estimated_duration,
        memory_requirement_mb=memory_requirement_mb,
        timeout_seconds=timeout_seconds
    )