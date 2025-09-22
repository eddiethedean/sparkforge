"""
Performance benchmarking for SparkForge.

This module provides comprehensive performance benchmarking capabilities including:
- Function benchmarking with statistical analysis
- Performance regression detection
- Load testing and stress testing
- Performance comparison and ranking
- Automated performance testing
"""

import concurrent.futures
import gc
import json
import logging
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import psutil


@dataclass
class BenchmarkResult:
    """Benchmark result data structure."""

    function_name: str
    execution_time: float
    memory_usage: float
    cpu_usage: float
    iterations: int
    timestamp: datetime
    success: bool = True
    error_message: Optional[str] = None


@dataclass
class BenchmarkStats:
    """Benchmark statistics data structure."""

    function_name: str
    min_time: float
    max_time: float
    mean_time: float
    median_time: float
    std_dev: float
    p95_time: float
    p99_time: float
    total_iterations: int
    success_rate: float
    memory_stats: Dict[str, float]
    cpu_stats: Dict[str, float]


@dataclass
class PerformanceRegression:
    """Performance regression detection result."""

    function_name: str
    baseline_time: float
    current_time: float
    regression_percent: float
    severity: str
    confidence: float
    timestamp: datetime


@dataclass
class LoadTestResult:
    """Load test result data structure."""

    test_name: str
    concurrent_users: int
    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_response_time: float
    p95_response_time: float
    p99_response_time: float
    throughput_rps: float
    error_rate: float
    duration: float


class PerformanceBenchmark:
    """Comprehensive performance benchmarking system."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._default_config()
        self.benchmark_results: List[BenchmarkResult] = []
        self.baseline_results: Dict[str, BenchmarkStats] = {}
        self.regression_threshold = 0.15  # 15% regression threshold
        self.logger = logging.getLogger("performance_benchmark")

    def _default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "default_iterations": 100,
            "warmup_iterations": 10,
            "timeout_seconds": 300,
            "memory_tracking": True,
            "cpu_tracking": True,
            "statistical_analysis": True,
            "regression_detection": True,
        }

    def benchmark_function(
        self,
        func: Callable,
        *args,
        iterations: Optional[int] = None,
        warmup_iterations: Optional[int] = None,
        **kwargs,
    ) -> BenchmarkStats:
        """Benchmark a function with statistical analysis."""
        iterations = iterations or self.config.get("default_iterations", 100)
        warmup_iterations = warmup_iterations or self.config.get(
            "warmup_iterations", 10
        )

        function_name = f"{func.__module__}.{func.__name__}"
        results = []
        memory_usage = []
        cpu_usage = []

        # Warmup iterations
        self.logger.info(
            f"Warming up {function_name} with {warmup_iterations} iterations"
        )
        for _ in range(warmup_iterations):
            try:
                func(*args, **kwargs)
            except Exception as e:
                self.logger.warning(f"Warmup iteration failed: {e}")

        # Force garbage collection before benchmarking
        gc.collect()

        # Benchmark iterations
        self.logger.info(f"Benchmarking {function_name} with {iterations} iterations")
        for i in range(iterations):
            try:
                # Measure execution time
                start_time = time.perf_counter()
                start_memory = self._get_memory_usage()
                start_cpu = self._get_cpu_usage()

                func(*args, **kwargs)

                end_time = time.perf_counter()
                end_memory = self._get_memory_usage()
                end_cpu = self._get_cpu_usage()

                execution_time = (
                    end_time - start_time
                ) * 1000  # Convert to milliseconds
                memory_delta = end_memory - start_memory
                cpu_delta = end_cpu - start_cpu

                # Store result
                benchmark_result = BenchmarkResult(
                    function_name=function_name,
                    execution_time=execution_time,
                    memory_usage=memory_delta,
                    cpu_usage=cpu_delta,
                    iterations=1,
                    timestamp=datetime.now(),
                    success=True,
                )

                self.benchmark_results.append(benchmark_result)
                results.append(execution_time)
                memory_usage.append(memory_delta)
                cpu_usage.append(cpu_delta)

            except Exception as e:
                error_result = BenchmarkResult(
                    function_name=function_name,
                    execution_time=0,
                    memory_usage=0,
                    cpu_usage=0,
                    iterations=1,
                    timestamp=datetime.now(),
                    success=False,
                    error_message=str(e),
                )

                self.benchmark_results.append(error_result)
                self.logger.error(f"Benchmark iteration {i} failed: {e}")

        # Calculate statistics
        stats = self._calculate_benchmark_stats(
            function_name, results, memory_usage, cpu_usage
        )

        return stats

    def _calculate_benchmark_stats(
        self,
        function_name: str,
        execution_times: List[float],
        memory_usage: List[float],
        cpu_usage: List[float],
    ) -> BenchmarkStats:
        """Calculate comprehensive benchmark statistics."""
        if not execution_times:
            return BenchmarkStats(
                function_name=function_name,
                min_time=0,
                max_time=0,
                mean_time=0,
                median_time=0,
                std_dev=0,
                p95_time=0,
                p99_time=0,
                total_iterations=0,
                success_rate=0,
                memory_stats={},
                cpu_stats={},
            )

        # Execution time statistics
        min_time = min(execution_times)
        max_time = max(execution_times)
        mean_time = statistics.mean(execution_times)
        median_time = statistics.median(execution_times)
        std_dev = statistics.stdev(execution_times) if len(execution_times) > 1 else 0
        p95_time = self._percentile(execution_times, 95)
        p99_time = self._percentile(execution_times, 99)

        # Memory statistics
        memory_stats = {}
        if memory_usage:
            memory_stats = {
                "min_mb": min(memory_usage),
                "max_mb": max(memory_usage),
                "mean_mb": statistics.mean(memory_usage),
                "median_mb": statistics.median(memory_usage),
            }

        # CPU statistics
        cpu_stats = {}
        if cpu_usage:
            cpu_stats = {
                "min_percent": min(cpu_usage),
                "max_percent": max(cpu_usage),
                "mean_percent": statistics.mean(cpu_usage),
                "median_percent": statistics.median(cpu_usage),
            }

        # Success rate
        total_results = len(self.benchmark_results)
        successful_results = len([r for r in self.benchmark_results if r.success])
        success_rate = (
            (successful_results / total_results * 100) if total_results > 0 else 0
        )

        return BenchmarkStats(
            function_name=function_name,
            min_time=min_time,
            max_time=max_time,
            mean_time=mean_time,
            median_time=median_time,
            std_dev=std_dev,
            p95_time=p95_time,
            p99_time=p99_time,
            total_iterations=len(execution_times),
            success_rate=success_rate,
            memory_stats=memory_stats,
            cpu_stats=cpu_stats,
        )

    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of values."""
        if not values:
            return 0.0

        sorted_values = sorted(values)
        index = (percentile / 100.0) * (len(sorted_values) - 1)

        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower = sorted_values[int(index)]
            upper = sorted_values[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0

    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            return psutil.cpu_percent()
        except Exception:
            return 0.0

    def compare_benchmarks(
        self, baseline_stats: BenchmarkStats, current_stats: BenchmarkStats
    ) -> Dict[str, Any]:
        """Compare two benchmark results."""
        time_regression = (
            (
                (current_stats.mean_time - baseline_stats.mean_time)
                / baseline_stats.mean_time
                * 100
            )
            if baseline_stats.mean_time > 0
            else 0
        )

        memory_regression = 0
        if baseline_stats.memory_stats and current_stats.memory_stats:
            baseline_memory = baseline_stats.memory_stats.get("mean_mb", 0)
            current_memory = current_stats.memory_stats.get("mean_mb", 0)
            memory_regression = (
                ((current_memory - baseline_memory) / baseline_memory * 100)
                if baseline_memory > 0
                else 0
            )

        return {
            "function_name": current_stats.function_name,
            "time_regression_percent": time_regression,
            "memory_regression_percent": memory_regression,
            "baseline_time": baseline_stats.mean_time,
            "current_time": current_stats.mean_time,
            "baseline_memory": baseline_stats.memory_stats.get("mean_mb", 0),
            "current_memory": current_stats.memory_stats.get("mean_mb", 0),
            "improvement": time_regression < 0,
            "regression_severity": self._calculate_regression_severity(time_regression),
        }

    def _calculate_regression_severity(self, regression_percent: float) -> str:
        """Calculate regression severity based on percentage."""
        if regression_percent > 50:
            return "critical"
        elif regression_percent > 25:
            return "high"
        elif regression_percent > 10:
            return "medium"
        elif regression_percent > 5:
            return "low"
        else:
            return "negligible"

    def detect_performance_regressions(self) -> List[PerformanceRegression]:
        """Detect performance regressions compared to baseline."""
        regressions = []

        # Group results by function name
        function_results = {}
        for result in self.benchmark_results:
            if result.function_name not in function_results:
                function_results[result.function_name] = []
            function_results[result.function_name].append(result)

        # Check each function for regressions
        for function_name, results in function_results.items():
            if function_name in self.baseline_results:
                baseline_stats = self.baseline_results[function_name]
                current_stats = self._calculate_benchmark_stats(
                    function_name,
                    [r.execution_time for r in results if r.success],
                    [r.memory_usage for r in results if r.success],
                    [r.cpu_usage for r in results if r.success],
                )

                # Check for regression
                time_regression = (
                    (
                        (current_stats.mean_time - baseline_stats.mean_time)
                        / baseline_stats.mean_time
                        * 100
                    )
                    if baseline_stats.mean_time > 0
                    else 0
                )

                if time_regression > (self.regression_threshold * 100):
                    regression = PerformanceRegression(
                        function_name=function_name,
                        baseline_time=baseline_stats.mean_time,
                        current_time=current_stats.mean_time,
                        regression_percent=time_regression,
                        severity=self._calculate_regression_severity(time_regression),
                        confidence=min(95, 70 + abs(time_regression) * 0.5),
                        timestamp=datetime.now(),
                    )
                    regressions.append(regression)

        return regressions

    def set_baseline(self, stats: BenchmarkStats) -> None:
        """Set baseline benchmark statistics."""
        self.baseline_results[stats.function_name] = stats
        self.logger.info(
            f"Set baseline for {stats.function_name}: {stats.mean_time:.2f}ms"
        )

    def load_test(
        self,
        func: Callable,
        concurrent_users: int,
        total_requests: int,
        *args,
        **kwargs,
    ) -> LoadTestResult:
        """Perform load testing with concurrent users."""
        test_name = f"{func.__module__}.{func.__name__}"
        self.logger.info(
            f"Starting load test: {test_name} with {concurrent_users} concurrent users"
        )

        results = []
        errors = []
        start_time = time.time()

        # Create thread pool for concurrent execution
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=concurrent_users
        ) as executor:
            # Submit all requests
            futures = []
            for _i in range(total_requests):
                future = executor.submit(self._execute_request, func, *args, **kwargs)
                futures.append(future)

            # Collect results
            for future in concurrent.futures.as_completed(futures):
                try:
                    execution_time = future.result()
                    results.append(execution_time)
                except Exception as e:
                    errors.append(str(e))

        end_time = time.time()
        duration = end_time - start_time

        # Calculate load test metrics
        successful_requests = len(results)
        failed_requests = len(errors)

        if results:
            avg_response_time = statistics.mean(results)
            p95_response_time = self._percentile(results, 95)
            p99_response_time = self._percentile(results, 99)
        else:
            avg_response_time = 0
            p95_response_time = 0
            p99_response_time = 0

        throughput_rps = successful_requests / duration if duration > 0 else 0
        error_rate = (
            (failed_requests / total_requests * 100) if total_requests > 0 else 0
        )

        return LoadTestResult(
            test_name=test_name,
            concurrent_users=concurrent_users,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_response_time=avg_response_time,
            p95_response_time=p95_response_time,
            p99_response_time=p99_response_time,
            throughput_rps=throughput_rps,
            error_rate=error_rate,
            duration=duration,
        )

    def _execute_request(self, func: Callable, *args, **kwargs) -> float:
        """Execute a single request and return execution time."""
        start_time = time.perf_counter()
        func(*args, **kwargs)
        end_time = time.perf_counter()
        return (end_time - start_time) * 1000  # Convert to milliseconds

    def stress_test(
        self,
        func: Callable,
        max_concurrent_users: int,
        duration_seconds: int,
        *args,
        **kwargs,
    ) -> List[LoadTestResult]:
        """Perform stress testing with increasing concurrent users."""
        test_name = f"{func.__module__}.{func.__name__}"
        self.logger.info(
            f"Starting stress test: {test_name} for {duration_seconds} seconds"
        )

        results = []
        concurrent_users = 1

        while concurrent_users <= max_concurrent_users:
            # Run load test for this level
            requests_per_level = concurrent_users * 10  # 10 requests per user
            result = self.load_test(
                func, concurrent_users, requests_per_level, *args, **kwargs
            )
            results.append(result)

            # Check if we should continue
            if result.error_rate > 50:  # More than 50% errors
                self.logger.warning(
                    f"High error rate at {concurrent_users} users: {result.error_rate:.1f}%"
                )
                break

            if result.avg_response_time > 10000:  # More than 10 seconds
                self.logger.warning(
                    f"High response time at {concurrent_users} users: {result.avg_response_time:.1f}ms"
                )
                break

            concurrent_users *= 2  # Double concurrent users

        return results

    def benchmark_suite(
        self, benchmarks: List[Tuple[Callable, tuple, dict]]
    ) -> Dict[str, BenchmarkStats]:
        """Run a suite of benchmarks."""
        results = {}

        for func, args, kwargs in benchmarks:
            function_name = f"{func.__module__}.{func.__name__}"
            self.logger.info(f"Running benchmark: {function_name}")

            try:
                stats = self.benchmark_function(func, *args, **kwargs)
                results[function_name] = stats
            except Exception as e:
                self.logger.error(f"Benchmark failed for {function_name}: {e}")

        return results

    def export_benchmark_report(self, output_file: Optional[Path] = None) -> Path:
        """Export comprehensive benchmark report."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(f"benchmark_report_{timestamp}.json")

        # Group results by function
        function_results = {}
        for result in self.benchmark_results:
            if result.function_name not in function_results:
                function_results[result.function_name] = []
            function_results[result.function_name].append(result)

        # Calculate statistics for each function
        function_stats = {}
        for function_name, results in function_results.items():
            successful_results = [r for r in results if r.success]
            if successful_results:
                execution_times = [r.execution_time for r in successful_results]
                memory_usage = [r.memory_usage for r in successful_results]
                cpu_usage = [r.cpu_usage for r in successful_results]

                stats = self._calculate_benchmark_stats(
                    function_name, execution_times, memory_usage, cpu_usage
                )
                function_stats[function_name] = asdict(stats)

        # Detect regressions
        regressions = self.detect_performance_regressions()

        # Create report
        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_benchmarks": len(self.benchmark_results),
                "functions_benchmarked": len(function_stats),
                "config": self.config,
            },
            "function_statistics": function_stats,
            "performance_regressions": [asdict(r) for r in regressions],
            "baseline_results": {
                name: asdict(stats) for name, stats in self.baseline_results.items()
            },
        }

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        return output_file


# Convenience functions
def benchmark(func: Callable, *args, iterations: int = 100, **kwargs) -> BenchmarkStats:
    """Convenience function to benchmark a single function."""
    benchmarker = PerformanceBenchmark()
    return benchmarker.benchmark_function(func, *args, iterations=iterations, **kwargs)


def compare_benchmarks(
    func1: Callable, func2: Callable, *args, iterations: int = 100, **kwargs
) -> Dict[str, Any]:
    """Compare two functions with benchmarking."""
    benchmarker = PerformanceBenchmark()

    stats1 = benchmarker.benchmark_function(
        func1, *args, iterations=iterations, **kwargs
    )
    stats2 = benchmarker.benchmark_function(
        func2, *args, iterations=iterations, **kwargs
    )

    return benchmarker.compare_benchmarks(stats1, stats2)


def load_test(
    func: Callable, concurrent_users: int, total_requests: int, *args, **kwargs
) -> LoadTestResult:
    """Convenience function for load testing."""
    benchmarker = PerformanceBenchmark()
    return benchmarker.load_test(
        func, concurrent_users, total_requests, *args, **kwargs
    )


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Performance Benchmarking")
    parser.add_argument("--benchmark", help="Function to benchmark")
    parser.add_argument(
        "--iterations", type=int, default=100, help="Number of iterations"
    )
    parser.add_argument("--load-test", action="store_true", help="Run load test")
    parser.add_argument(
        "--concurrent-users",
        type=int,
        default=10,
        help="Concurrent users for load test",
    )
    parser.add_argument(
        "--total-requests", type=int, default=100, help="Total requests for load test"
    )
    parser.add_argument("--output", type=Path, help="Output file for report")

    args = parser.parse_args()

    benchmarker = PerformanceBenchmark()

    if args.benchmark:
        # This would need to be implemented based on the specific function
        print(f"Benchmarking function: {args.benchmark}")
        print(f"Iterations: {args.iterations}")
    else:
        # Generate general report
        report_file = benchmarker.export_benchmark_report(args.output)
        print(f"Benchmark report saved to: {report_file}")

        # Print summary
        print("\nBenchmark Summary:")
        print(f"Total benchmarks: {len(benchmarker.benchmark_results)}")
        print(
            f"Functions benchmarked: {len({r.function_name for r in benchmarker.benchmark_results})}"
        )

        # Show regressions
        regressions = benchmarker.detect_performance_regressions()
        if regressions:
            print(f"Performance regressions detected: {len(regressions)}")
            for regression in regressions:
                print(
                    f"  - {regression.function_name}: {regression.regression_percent:.1f}% regression"
                )
        else:
            print("No performance regressions detected")
