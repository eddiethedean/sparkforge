#!/usr/bin/env python3
"""
Performance monitoring infrastructure for SparkForge testing.

This module provides tools for measuring and tracking performance
of SparkForge components including timing, memory usage, and regression detection.
"""

import gc
import os
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import psutil


@dataclass
class PerformanceResult:
    """Container for performance measurement results."""

    function_name: str
    execution_time: float
    memory_usage_mb: float
    peak_memory_mb: float
    iterations: int
    timestamp: float
    success: bool
    error_message: Optional[str] = None

    @property
    def throughput(self) -> float:
        """Calculate throughput (iterations per second)."""
        if self.execution_time > 0:
            return self.iterations / self.execution_time
        return 0.0

    @property
    def avg_time_per_iteration(self) -> float:
        """Calculate average time per iteration in milliseconds."""
        if self.iterations > 0:
            return (self.execution_time * 1000) / self.iterations
        return 0.0


class PerformanceMonitor:
    """Performance monitoring and regression detection."""

    def __init__(self, baseline_file: str = "performance_baseline.json"):
        """Initialize performance monitor with baseline storage."""
        self.baseline_file = baseline_file
        self.baselines: Dict[str, PerformanceResult] = {}
        self.results: List[PerformanceResult] = []
        self.load_baselines()

    def load_baselines(self) -> None:
        """Load performance baselines from file."""
        import json

        if os.path.exists(self.baseline_file):
            try:
                with open(self.baseline_file) as f:
                    baseline_data = json.load(f)
                    for name, data in baseline_data.items():
                        self.baselines[name] = PerformanceResult(**data)
            except (json.JSONDecodeError, KeyError, TypeError):
                # If baseline file is corrupted, start fresh
                self.baselines = {}

    def save_baselines(self) -> None:
        """Save current baselines to file."""
        import json

        baseline_data = {}
        for name, result in self.baselines.items():
            baseline_data[name] = {
                "function_name": result.function_name,
                "execution_time": result.execution_time,
                "memory_usage_mb": result.memory_usage_mb,
                "peak_memory_mb": result.peak_memory_mb,
                "iterations": result.iterations,
                "timestamp": result.timestamp,
                "success": result.success,
                "error_message": result.error_message,
            }

        with open(self.baseline_file, "w") as f:
            json.dump(baseline_data, f, indent=2)

    @contextmanager
    def measure_performance(self, function_name: str, iterations: int = 1):
        """Context manager for measuring function performance."""
        # Start memory tracing
        tracemalloc.start()
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.time()
        start_timestamp = time.time()

        result = PerformanceResult(
            function_name=function_name,
            execution_time=0.0,
            memory_usage_mb=0.0,
            peak_memory_mb=0.0,
            iterations=iterations,
            timestamp=start_timestamp,
            success=False,
        )

        try:
            yield result

            # Measure final metrics
            end_time = time.time()
            current, peak = tracemalloc.get_traced_memory()
            process.memory_info().rss / 1024 / 1024  # MB

            result.execution_time = end_time - start_time
            result.memory_usage_mb = (current / 1024 / 1024) - initial_memory
            result.peak_memory_mb = (peak / 1024 / 1024) - initial_memory
            result.success = True

        except Exception as e:
            result.error_message = str(e)
            result.success = False

        finally:
            tracemalloc.stop()
            self.results.append(result)

    def run_performance_test(
        self,
        function: Callable,
        function_name: str,
        iterations: int = 1,
        args: Tuple = (),
        kwargs: Dict[str, Any] = None,
    ) -> PerformanceResult:
        """Run a performance test on a function."""
        if kwargs is None:
            kwargs = {}

        with self.measure_performance(function_name, iterations) as result:
            for _ in range(iterations):
                function(*args, **kwargs)

        return result

    def check_regression(
        self, function_name: str, tolerance: float = 0.2
    ) -> Dict[str, Any]:
        """Check for performance regression against baseline."""
        # Find the most recent result for this function
        recent_results = [r for r in self.results if r.function_name == function_name]
        if not recent_results:
            return {"status": "no_data", "message": "No recent results found"}

        latest_result = recent_results[-1]

        if function_name not in self.baselines:
            return {
                "status": "no_baseline",
                "message": "No baseline found for this function",
                "current": latest_result,
            }

        baseline = self.baselines[function_name]

        # Check for regressions
        time_regression = latest_result.execution_time > baseline.execution_time * (
            1 + tolerance
        )
        memory_regression = latest_result.memory_usage_mb > baseline.memory_usage_mb * (
            1 + tolerance
        )

        regression_info = {
            "status": "ok",
            "function_name": function_name,
            "baseline": baseline,
            "current": latest_result,
            "time_regression": time_regression,
            "memory_regression": memory_regression,
            "time_change_percent": (
                (latest_result.execution_time - baseline.execution_time)
                / baseline.execution_time
            )
            * 100,
            "memory_change_percent": (
                (latest_result.memory_usage_mb - baseline.memory_usage_mb)
                / baseline.memory_usage_mb
            )
            * 100,
        }

        if time_regression or memory_regression:
            regression_info["status"] = "regression_detected"

        return regression_info

    def update_baseline(self, function_name: str) -> None:
        """Update baseline for a function with the most recent result."""
        recent_results = [
            r for r in self.results if r.function_name == function_name and r.success
        ]
        if recent_results:
            self.baselines[function_name] = recent_results[-1]
            self.save_baselines()

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of all performance results."""
        if not self.results:
            return {"message": "No performance data available"}

        successful_results = [r for r in self.results if r.success]

        summary = {
            "total_tests": len(self.results),
            "successful_tests": len(successful_results),
            "failed_tests": len(self.results) - len(successful_results),
            "functions_tested": len({r.function_name for r in self.results}),
            "total_execution_time": sum(r.execution_time for r in successful_results),
            "avg_execution_time": (
                sum(r.execution_time for r in successful_results)
                / len(successful_results)
                if successful_results
                else 0
            ),
            "total_memory_used": sum(r.memory_usage_mb for r in successful_results),
            "results": [r.__dict__ for r in self.results],
        }

        return summary

    def benchmark_function(
        self,
        function: Callable,
        function_name: str,
        iterations: int = 100,
        args: Tuple = (),
        kwargs: Dict[str, Any] = None,
        warmup_iterations: int = 10,
    ) -> PerformanceResult:
        """Benchmark a function with warmup and multiple iterations."""
        if kwargs is None:
            kwargs = {}

        # Warmup runs
        for _ in range(warmup_iterations):
            function(*args, **kwargs)

        # Force garbage collection
        gc.collect()

        # Actual benchmark
        return self.run_performance_test(
            function, function_name, iterations, args, kwargs
        )


# Global performance monitor instance
performance_monitor = PerformanceMonitor()
