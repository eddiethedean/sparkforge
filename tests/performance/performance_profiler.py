"""
Performance profiler for SparkForge.

This module provides comprehensive performance profiling capabilities including:
- Function-level profiling
- Memory usage profiling
- Execution time analysis
- Resource utilization monitoring
- Performance bottleneck identification
"""

import cProfile
import io
import json
import pstats
import threading
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import psutil


@dataclass
class PerformanceMetrics:
    """Performance metrics data structure."""

    function_name: str
    execution_time: float
    memory_usage: float
    cpu_usage: float
    call_count: int
    timestamp: datetime
    args_size: int = 0
    return_size: int = 0
    exception_count: int = 0


@dataclass
class ProfilerReport:
    """Profiler report data structure."""

    total_execution_time: float
    total_memory_usage: float
    total_cpu_usage: float
    function_metrics: List[PerformanceMetrics]
    bottlenecks: List[Dict[str, Any]]
    recommendations: List[str]
    timestamp: datetime


class PerformanceProfiler:
    """Comprehensive performance profiler for SparkForge."""

    def __init__(
        self, enable_memory_tracking: bool = True, enable_cpu_tracking: bool = True
    ):
        self.enable_memory_tracking = enable_memory_tracking
        self.enable_cpu_tracking = enable_cpu_tracking
        self.metrics: List[PerformanceMetrics] = []
        self.active_profiles: Dict[str, Dict[str, Any]] = {}
        self.thread_local = threading.local()

        # Performance thresholds
        self.thresholds = {
            "slow_function_ms": 1000,  # Functions slower than 1 second
            "high_memory_mb": 100,  # Functions using more than 100MB
            "high_cpu_percent": 80,  # Functions using more than 80% CPU
            "frequent_calls": 1000,  # Functions called more than 1000 times
            "memory_leak_threshold": 50,  # Memory increase threshold in MB
        }

        # Start memory tracking if enabled
        if self.enable_memory_tracking:
            tracemalloc.start()

    def profile_function(self, func: Callable) -> Callable:
        """Decorator to profile a function."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            function_name = f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            start_memory = self._get_memory_usage()
            start_cpu = self._get_cpu_usage()

            # Track exceptions
            exception_count = 0
            result = None

            try:
                result = func(*args, **kwargs)
            except Exception:
                exception_count += 1
                raise
            finally:
                end_time = time.time()
                end_memory = self._get_memory_usage()
                end_cpu = self._get_cpu_usage()

                # Calculate metrics
                execution_time = (
                    end_time - start_time
                ) * 1000  # Convert to milliseconds
                memory_usage = end_memory - start_memory
                cpu_usage = end_cpu - start_cpu

                # Estimate argument and return sizes
                args_size = self._estimate_size(args) + self._estimate_size(kwargs)
                return_size = self._estimate_size(result) if result is not None else 0

                # Create metrics
                metrics = PerformanceMetrics(
                    function_name=function_name,
                    execution_time=execution_time,
                    memory_usage=memory_usage,
                    cpu_usage=cpu_usage,
                    call_count=1,
                    timestamp=datetime.now(),
                    args_size=args_size,
                    return_size=return_size,
                    exception_count=exception_count,
                )

                self.metrics.append(metrics)

            return result

        return wrapper

    def profile_class(self, cls: type) -> type:
        """Decorator to profile all methods of a class."""
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name)
            if callable(attr) and not attr_name.startswith("_"):
                setattr(cls, attr_name, self.profile_function(attr))
        return cls

    def start_profile(self, profile_name: str) -> None:
        """Start profiling with a specific name."""
        self.active_profiles[profile_name] = {
            "start_time": time.time(),
            "start_memory": self._get_memory_usage(),
            "start_cpu": self._get_cpu_usage(),
            "metrics_count": len(self.metrics),
        }

    def end_profile(self, profile_name: str) -> Dict[str, Any]:
        """End profiling and return results."""
        if profile_name not in self.active_profiles:
            raise ValueError(f"Profile '{profile_name}' not found")

        profile_data = self.active_profiles[profile_name]
        end_time = time.time()
        end_memory = self._get_memory_usage()
        end_cpu = self._get_cpu_usage()

        # Calculate profile metrics
        total_time = (end_time - profile_data["start_time"]) * 1000
        total_memory = end_memory - profile_data["start_memory"]
        total_cpu = end_cpu - profile_data["start_cpu"]

        # Get metrics collected during this profile
        profile_metrics = self.metrics[profile_data["metrics_count"] :]

        profile_result = {
            "profile_name": profile_name,
            "total_execution_time": total_time,
            "total_memory_usage": total_memory,
            "total_cpu_usage": total_cpu,
            "function_count": len(profile_metrics),
            "metrics": [asdict(m) for m in profile_metrics],
            "timestamp": datetime.now().isoformat(),
        }

        # Clean up
        del self.active_profiles[profile_name]

        return profile_result

    def profile_pipeline(
        self, pipeline_func: Callable, *args, **kwargs
    ) -> Tuple[Any, ProfilerReport]:
        """Profile a complete pipeline execution."""
        self.start_profile("pipeline_execution")

        try:
            result = pipeline_func(*args, **kwargs)
        finally:
            self.end_profile("pipeline_execution")

        # Generate comprehensive report
        report = self.generate_report()

        return result, report

    def profile_spark_operations(
        self, spark_func: Callable, *args, **kwargs
    ) -> Tuple[Any, ProfilerReport]:
        """Profile Spark-specific operations."""
        self.start_profile("spark_operations")

        try:
            result = spark_func(*args, **kwargs)
        finally:
            self.end_profile("spark_operations")

        # Generate Spark-specific report
        report = self.generate_spark_report()

        return result, report

    def generate_report(self) -> ProfilerReport:
        """Generate comprehensive performance report."""
        if not self.metrics:
            return ProfilerReport(
                total_execution_time=0,
                total_memory_usage=0,
                total_cpu_usage=0,
                function_metrics=[],
                bottlenecks=[],
                recommendations=[],
                timestamp=datetime.now(),
            )

        # Aggregate metrics by function
        function_aggregates = {}
        for metric in self.metrics:
            func_name = metric.function_name
            if func_name not in function_aggregates:
                function_aggregates[func_name] = {
                    "execution_time": 0,
                    "memory_usage": 0,
                    "cpu_usage": 0,
                    "call_count": 0,
                    "exception_count": 0,
                    "args_size": 0,
                    "return_size": 0,
                }

            agg = function_aggregates[func_name]
            agg["execution_time"] += metric.execution_time
            agg["memory_usage"] += metric.memory_usage
            agg["cpu_usage"] += metric.cpu_usage
            agg["call_count"] += metric.call_count
            agg["exception_count"] += metric.exception_count
            agg["args_size"] += metric.args_size
            agg["return_size"] += metric.return_size

        # Create aggregated metrics
        aggregated_metrics = []
        for func_name, agg in function_aggregates.items():
            aggregated_metrics.append(
                PerformanceMetrics(
                    function_name=func_name,
                    execution_time=agg["execution_time"],
                    memory_usage=agg["memory_usage"],
                    cpu_usage=agg["cpu_usage"],
                    call_count=agg["call_count"],
                    timestamp=datetime.now(),
                    args_size=agg["args_size"],
                    return_size=agg["return_size"],
                    exception_count=agg["exception_count"],
                )
            )

        # Sort by execution time (descending)
        aggregated_metrics.sort(key=lambda x: x.execution_time, reverse=True)

        # Calculate totals
        total_execution_time = sum(m.execution_time for m in aggregated_metrics)
        total_memory_usage = sum(m.memory_usage for m in aggregated_metrics)
        total_cpu_usage = sum(m.cpu_usage for m in aggregated_metrics)

        # Identify bottlenecks
        bottlenecks = self._identify_bottlenecks(aggregated_metrics)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            aggregated_metrics, bottlenecks
        )

        return ProfilerReport(
            total_execution_time=total_execution_time,
            total_memory_usage=total_memory_usage,
            total_cpu_usage=total_cpu_usage,
            function_metrics=aggregated_metrics,
            bottlenecks=bottlenecks,
            recommendations=recommendations,
            timestamp=datetime.now(),
        )

    def generate_spark_report(self) -> ProfilerReport:
        """Generate Spark-specific performance report."""
        report = self.generate_report()

        # Add Spark-specific analysis
        spark_metrics = [
            m
            for m in report.function_metrics
            if any(
                spark_indicator in m.function_name.lower()
                for spark_indicator in ["spark", "dataframe", "rdd", "sql"]
            )
        ]

        # Add Spark-specific recommendations
        spark_recommendations = []

        # Check for DataFrame operations
        df_ops = [m for m in spark_metrics if "dataframe" in m.function_name.lower()]
        if df_ops:
            total_df_time = sum(m.execution_time for m in df_ops)
            if total_df_time > self.thresholds["slow_function_ms"] * 2:
                spark_recommendations.append(
                    "Consider optimizing DataFrame operations - they are taking significant time"
                )

        # Check for SQL operations
        sql_ops = [m for m in spark_metrics if "sql" in m.function_name.lower()]
        if sql_ops:
            spark_recommendations.append(
                "Review SQL queries for optimization opportunities"
            )

        # Check for caching opportunities
        frequent_ops = [m for m in spark_metrics if m.call_count > 10]
        if frequent_ops:
            spark_recommendations.append(
                "Consider caching frequently accessed DataFrames or RDDs"
            )

        # Update recommendations
        report.recommendations.extend(spark_recommendations)

        return report

    def _identify_bottlenecks(
        self, metrics: List[PerformanceMetrics]
    ) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks."""
        bottlenecks = []

        for metric in metrics:
            bottleneck_reasons = []

            # Check execution time
            if metric.execution_time > self.thresholds["slow_function_ms"]:
                bottleneck_reasons.append(
                    f"Slow execution: {metric.execution_time:.2f}ms"
                )

            # Check memory usage
            if metric.memory_usage > self.thresholds["high_memory_mb"]:
                bottleneck_reasons.append(
                    f"High memory usage: {metric.memory_usage:.2f}MB"
                )

            # Check CPU usage
            if metric.cpu_usage > self.thresholds["high_cpu_percent"]:
                bottleneck_reasons.append(f"High CPU usage: {metric.cpu_usage:.2f}%")

            # Check call frequency
            if metric.call_count > self.thresholds["frequent_calls"]:
                bottleneck_reasons.append(
                    f"Frequently called: {metric.call_count} times"
                )

            # Check for memory leaks (simplified)
            if metric.memory_usage > self.thresholds["memory_leak_threshold"]:
                bottleneck_reasons.append(
                    f"Potential memory leak: {metric.memory_usage:.2f}MB"
                )

            if bottleneck_reasons:
                bottlenecks.append(
                    {
                        "function": metric.function_name,
                        "reasons": bottleneck_reasons,
                        "severity": self._calculate_bottleneck_severity(metric),
                        "impact_score": self._calculate_impact_score(metric),
                    }
                )

        # Sort by impact score
        bottlenecks.sort(key=lambda x: x["impact_score"], reverse=True)

        return bottlenecks

    def _calculate_bottleneck_severity(self, metric: PerformanceMetrics) -> str:
        """Calculate bottleneck severity."""
        score = 0

        if metric.execution_time > self.thresholds["slow_function_ms"] * 2:
            score += 3
        elif metric.execution_time > self.thresholds["slow_function_ms"]:
            score += 2

        if metric.memory_usage > self.thresholds["high_memory_mb"] * 2:
            score += 3
        elif metric.memory_usage > self.thresholds["high_memory_mb"]:
            score += 2

        if metric.cpu_usage > self.thresholds["high_cpu_percent"] * 1.5:
            score += 3
        elif metric.cpu_usage > self.thresholds["high_cpu_percent"]:
            score += 2

        if score >= 6:
            return "critical"
        elif score >= 4:
            return "high"
        elif score >= 2:
            return "medium"
        else:
            return "low"

    def _calculate_impact_score(self, metric: PerformanceMetrics) -> float:
        """Calculate impact score for prioritization."""
        # Weight different factors
        time_weight = 0.4
        memory_weight = 0.3
        cpu_weight = 0.2
        frequency_weight = 0.1

        # Normalize values (simplified)
        time_score = min(
            metric.execution_time / (self.thresholds["slow_function_ms"] * 2), 1.0
        )
        memory_score = min(
            metric.memory_usage / (self.thresholds["high_memory_mb"] * 2), 1.0
        )
        cpu_score = min(
            metric.cpu_usage / (self.thresholds["high_cpu_percent"] * 1.5), 1.0
        )
        frequency_score = min(
            metric.call_count / (self.thresholds["frequent_calls"] * 2), 1.0
        )

        return (
            time_score * time_weight
            + memory_score * memory_weight
            + cpu_score * cpu_weight
            + frequency_score * frequency_weight
        )

    def _generate_recommendations(
        self, metrics: List[PerformanceMetrics], bottlenecks: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []

        # General recommendations based on bottlenecks
        if bottlenecks:
            critical_bottlenecks = [
                b for b in bottlenecks if b["severity"] == "critical"
            ]
            if critical_bottlenecks:
                recommendations.append(
                    f"Address {len(critical_bottlenecks)} critical performance bottlenecks immediately"
                )

            high_bottlenecks = [b for b in bottlenecks if b["severity"] == "high"]
            if high_bottlenecks:
                recommendations.append(
                    f"Review and optimize {len(high_bottlenecks)} high-priority bottlenecks"
                )

        # Specific recommendations
        slow_functions = [
            m for m in metrics if m.execution_time > self.thresholds["slow_function_ms"]
        ]
        if slow_functions:
            recommendations.append(
                f"Optimize {len(slow_functions)} slow functions (>{self.thresholds['slow_function_ms']}ms)"
            )

        memory_intensive = [
            m for m in metrics if m.memory_usage > self.thresholds["high_memory_mb"]
        ]
        if memory_intensive:
            recommendations.append(
                f"Optimize memory usage in {len(memory_intensive)} memory-intensive functions"
            )

        frequent_calls = [
            m for m in metrics if m.call_count > self.thresholds["frequent_calls"]
        ]
        if frequent_calls:
            recommendations.append(
                f"Consider caching or optimization for {len(frequent_calls)} frequently called functions"
            )

        # Top function recommendation
        if metrics:
            top_function = metrics[0]
            recommendations.append(
                f"Focus optimization efforts on '{top_function.function_name}' "
                f"(takes {top_function.execution_time:.2f}ms, {top_function.call_count} calls)"
            )

        # Add general recommendations
        recommendations.extend(
            [
                "Consider implementing function-level caching for frequently called functions",
                "Review data structures and algorithms for optimization opportunities",
                "Profile memory usage patterns to identify potential leaks",
                "Consider parallelization for CPU-intensive operations",
                "Implement lazy evaluation where appropriate",
            ]
        )

        return recommendations

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        if not self.enable_memory_tracking:
            return 0.0

        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # Convert to MB
        except Exception:
            return 0.0

    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        if not self.enable_cpu_tracking:
            return 0.0

        try:
            process = psutil.Process()
            return process.cpu_percent()
        except Exception:
            return 0.0

    def _estimate_size(self, obj: Any) -> int:
        """Estimate the size of an object in bytes."""
        try:
            import sys

            return sys.getsizeof(obj)
        except Exception:
            return 0

    def get_memory_snapshot(self) -> Dict[str, Any]:
        """Get detailed memory snapshot."""
        if not self.enable_memory_tracking:
            return {}

        try:
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")

            return {
                "current_memory_mb": self._get_memory_usage(),
                "peak_memory_mb": tracemalloc.get_traced_memory()[1] / 1024 / 1024,
                "top_memory_usage": [
                    {
                        "filename": stat.traceback.format()[0],
                        "size_mb": stat.size / 1024 / 1024,
                        "count": stat.count,
                    }
                    for stat in top_stats[:10]
                ],
            }
        except Exception as e:
            return {"error": str(e)}

    def reset_metrics(self) -> None:
        """Reset all performance metrics."""
        self.metrics.clear()
        self.active_profiles.clear()

        # Restart memory tracking if enabled
        if self.enable_memory_tracking:
            tracemalloc.stop()
            tracemalloc.start()

    def export_report(self, output_file: Optional[Path] = None) -> Path:
        """Export performance report to file."""
        report = self.generate_report()

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(f"performance_report_{timestamp}.json")

        # Convert to serializable format
        report_data = {
            "report_metadata": {
                "generated_at": report.timestamp.isoformat(),
                "total_functions_profiled": len(report.function_metrics),
                "profiler_settings": {
                    "memory_tracking": self.enable_memory_tracking,
                    "cpu_tracking": self.enable_cpu_tracking,
                    "thresholds": self.thresholds,
                },
            },
            "summary": {
                "total_execution_time_ms": report.total_execution_time,
                "total_memory_usage_mb": report.total_memory_usage,
                "total_cpu_usage_percent": report.total_cpu_usage,
                "bottlenecks_count": len(report.bottlenecks),
                "recommendations_count": len(report.recommendations),
            },
            "function_metrics": [asdict(m) for m in report.function_metrics],
            "bottlenecks": report.bottlenecks,
            "recommendations": report.recommendations,
            "memory_snapshot": self.get_memory_snapshot(),
        }

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        return output_file

    def profile_with_cprofile(self, func: Callable, *args, **kwargs) -> Tuple[Any, str]:
        """Profile function using cProfile for detailed analysis."""
        profiler = cProfile.Profile()

        try:
            result = profiler.runcall(func, *args, **kwargs)
        finally:
            profiler.disable()

        # Generate stats
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s)
        ps.sort_stats("cumulative")
        ps.print_stats(20)  # Top 20 functions

        return result, s.getvalue()


# Convenience functions for easy profiling
def profile_function(func: Callable) -> Callable:
    """Convenience decorator for profiling functions."""
    profiler = PerformanceProfiler()
    return profiler.profile_function(func)


def profile_class(cls: type) -> type:
    """Convenience decorator for profiling classes."""
    profiler = PerformanceProfiler()
    return profiler.profile_class(cls)


def quick_profile(func: Callable, *args, **kwargs) -> Tuple[Any, ProfilerReport]:
    """Quick profiling of a single function call."""
    profiler = PerformanceProfiler()
    return profiler.profile_pipeline(func, *args, **kwargs)


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Performance Profiler")
    parser.add_argument("--function", help="Function to profile")
    parser.add_argument("--output", type=Path, help="Output file for report")
    parser.add_argument(
        "--memory-tracking", action="store_true", help="Enable memory tracking"
    )
    parser.add_argument(
        "--cpu-tracking", action="store_true", help="Enable CPU tracking"
    )

    args = parser.parse_args()

    profiler = PerformanceProfiler(
        enable_memory_tracking=args.memory_tracking,
        enable_cpu_tracking=args.cpu_tracking,
    )

    if args.function:
        # Profile specific function
        try:
            # This would need to be implemented based on the specific function
            print(f"Profiling function: {args.function}")
        except Exception as e:
            print(f"Error profiling function: {e}")
    else:
        # Generate general report
        report = profiler.generate_report()
        report_file = profiler.export_report(args.output)
        print(f"Performance report saved to: {report_file}")

        # Print summary
        print("\nPerformance Summary:")
        print(f"Total execution time: {report.total_execution_time:.2f}ms")
        print(f"Total memory usage: {report.total_memory_usage:.2f}MB")
        print(f"Total CPU usage: {report.total_cpu_usage:.2f}%")
        print(f"Bottlenecks found: {len(report.bottlenecks)}")
        print(f"Recommendations: {len(report.recommendations)}")
