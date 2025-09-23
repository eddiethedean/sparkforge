"""
Memory optimization utilities for SparkForge.

This module provides memory optimization capabilities including:
- Memory usage analysis and profiling
- Memory leak detection
- Memory optimization strategies
- Garbage collection optimization
- Memory-efficient data structures
"""

import functools
import gc
import logging
import sys
import threading
import time
import tracemalloc
import weakref
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List

import psutil


@dataclass
class MemorySnapshot:
    """Memory snapshot data structure."""

    timestamp: datetime
    current_memory_mb: float
    peak_memory_mb: float
    allocated_blocks: int
    top_allocations: List[Dict[str, Any]]


@dataclass
class MemoryLeak:
    """Memory leak detection result."""

    leak_id: str
    object_type: str
    count: int
    size_bytes: int
    growth_rate: float
    first_detected: datetime
    severity: str


@dataclass
class MemoryOptimizationReport:
    """Memory optimization report."""

    total_memory_mb: float
    memory_efficiency_score: float
    detected_leaks: List[MemoryLeak]
    optimization_opportunities: List[str]
    recommendations: List[str]
    gc_stats: Dict[str, Any]


class MemoryProfiler:
    """Memory profiling and analysis."""

    def __init__(self, enable_tracemalloc: bool = True):
        self.enable_tracemalloc = enable_tracemalloc
        self.snapshots: List[MemorySnapshot] = []
        self.object_counts: Dict[str, int] = defaultdict(int)
        self.object_sizes: Dict[str, int] = defaultdict(int)
        self.leak_detector = MemoryLeakDetector()

        if self.enable_tracemalloc:
            tracemalloc.start()

        self.logger = logging.getLogger("memory_profiler")

    def take_snapshot(self) -> MemorySnapshot:
        """Take a memory snapshot."""
        try:
            # Get current memory usage
            process = psutil.Process()
            current_memory = process.memory_info().rss / 1024 / 1024  # MB

            # Get peak memory if tracemalloc is enabled
            if self.enable_tracemalloc:
                current, peak = tracemalloc.get_traced_memory()
                peak_memory = peak / 1024 / 1024
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics("lineno")

                top_allocations = []
                for stat in top_stats[:10]:
                    top_allocations.append(
                        {
                            "filename": stat.traceback.format()[0],
                            "size_mb": stat.size / 1024 / 1024,
                            "count": stat.count,
                        }
                    )
            else:
                peak_memory = current_memory
                top_allocations = []

            # Count objects
            object_counts = {}
            for obj in gc.get_objects():
                obj_type = type(obj).__name__
                object_counts[obj_type] = object_counts.get(obj_type, 0) + 1
                self.object_counts[obj_type] += 1
                self.object_sizes[obj_type] += sys.getsizeof(obj)

            snapshot = MemorySnapshot(
                timestamp=datetime.now(),
                current_memory_mb=current_memory,
                peak_memory_mb=peak_memory,
                allocated_blocks=len(gc.get_objects()),
                top_allocations=top_allocations,
            )

            self.snapshots.append(snapshot)
            self.leak_detector.analyze_snapshot(snapshot)

            return snapshot

        except Exception as e:
            self.logger.error(f"Error taking memory snapshot: {e}")
            return MemorySnapshot(
                timestamp=datetime.now(),
                current_memory_mb=0,
                peak_memory_mb=0,
                allocated_blocks=0,
                top_allocations=[],
            )

    def get_memory_stats(self) -> Dict[str, Any]:
        """Get comprehensive memory statistics."""
        if not self.snapshots:
            return {}

        latest_snapshot = self.snapshots[-1]

        # Calculate memory trends
        if len(self.snapshots) > 1:
            memory_trend = []
            for snapshot in self.snapshots[-10:]:  # Last 10 snapshots
                memory_trend.append(
                    {
                        "timestamp": snapshot.timestamp.isoformat(),
                        "memory_mb": snapshot.current_memory_mb,
                    }
                )
        else:
            memory_trend = []

        return {
            "current_memory_mb": latest_snapshot.current_memory_mb,
            "peak_memory_mb": latest_snapshot.peak_memory_mb,
            "allocated_blocks": latest_snapshot.allocated_blocks,
            "top_allocations": latest_snapshot.top_allocations,
            "object_counts": dict(self.object_counts),
            "object_sizes": {
                k: v / 1024 / 1024 for k, v in self.object_sizes.items()
            },  # Convert to MB
            "memory_trend": memory_trend,
            "gc_stats": self._get_gc_stats(),
        }

    def _get_gc_stats(self) -> Dict[str, Any]:
        """Get garbage collection statistics."""
        try:
            gc_stats = gc.get_stats()
            return {
                "generation_0": gc_stats[0] if len(gc_stats) > 0 else {},
                "generation_1": gc_stats[1] if len(gc_stats) > 1 else {},
                "generation_2": gc_stats[2] if len(gc_stats) > 2 else {},
                "total_collections": sum(stat["collections"] for stat in gc_stats),
                "total_collected": sum(stat["collected"] for stat in gc_stats),
            }
        except Exception as e:
            self.logger.error(f"Error getting GC stats: {e}")
            return {}


class MemoryLeakDetector:
    """Memory leak detection and analysis."""

    def __init__(self):
        self.object_history: Dict[str, List[int]] = defaultdict(list)
        self.detected_leaks: List[MemoryLeak] = []
        self.analysis_interval = 60  # seconds
        self.last_analysis = datetime.now()

    def analyze_snapshot(self, snapshot: MemorySnapshot) -> None:
        """Analyze memory snapshot for potential leaks."""
        current_time = datetime.now()

        # Only analyze if enough time has passed
        if (current_time - self.last_analysis).total_seconds() < self.analysis_interval:
            return

        self.last_analysis = current_time

        # Track object counts over time
        for allocation in snapshot.top_allocations:
            obj_type = allocation["filename"]
            count = allocation["count"]

            self.object_history[obj_type].append(count)

            # Keep only last 10 measurements
            if len(self.object_history[obj_type]) > 10:
                self.object_history[obj_type] = self.object_history[obj_type][-10:]

            # Detect potential leaks
            if len(self.object_history[obj_type]) >= 5:
                self._detect_leak(obj_type, allocation, current_time)

    def _detect_leak(
        self, obj_type: str, allocation: Dict[str, Any], timestamp: datetime
    ) -> None:
        """Detect potential memory leak for specific object type."""
        history = self.object_history[obj_type]

        # Calculate growth rate
        if len(history) >= 3:
            recent_growth = (
                history[-1] - history[-3]
            ) / 2  # Average growth over last 2 intervals

            # Consider it a leak if growth rate is positive and significant
            if recent_growth > 10:  # More than 10 objects per interval
                leak = MemoryLeak(
                    leak_id=f"leak_{obj_type}_{int(timestamp.timestamp())}",
                    object_type=obj_type,
                    count=history[-1],
                    size_bytes=allocation["size_mb"] * 1024 * 1024,
                    growth_rate=recent_growth,
                    first_detected=timestamp,
                    severity="high" if recent_growth > 50 else "medium",
                )

                # Only add if not already detected
                if not any(
                    leak_item.object_type == obj_type and not leak_item.resolved
                    for leak_item in self.detected_leaks
                ):
                    self.detected_leaks.append(leak)

    def get_detected_leaks(self) -> List[MemoryLeak]:
        """Get list of detected memory leaks."""
        return [
            leak for leak in self.detected_leaks if not getattr(leak, "resolved", False)
        ]


class MemoryOptimizer:
    """Memory optimization strategies and utilities."""

    def __init__(self):
        self.optimization_strategies = {
            "gc_optimization": self._optimize_garbage_collection,
            "object_pooling": self._implement_object_pooling,
            "lazy_evaluation": self._implement_lazy_evaluation,
            "memory_mapping": self._implement_memory_mapping,
            "weak_references": self._use_weak_references,
        }
        self.logger = logging.getLogger("memory_optimizer")

    def optimize_memory_usage(self, target_memory_mb: float) -> Dict[str, Any]:
        """Optimize memory usage to target level."""
        results = {}

        # Get current memory usage
        process = psutil.Process()
        current_memory = process.memory_info().rss / 1024 / 1024

        if current_memory <= target_memory_mb:
            return {"status": "already_optimized", "current_memory_mb": current_memory}

        # Apply optimization strategies
        for strategy_name, strategy_func in self.optimization_strategies.items():
            try:
                result = strategy_func()
                results[strategy_name] = result

                # Check if we've reached target
                new_memory = process.memory_info().rss / 1024 / 1024
                if new_memory <= target_memory_mb:
                    break

            except Exception as e:
                self.logger.error(
                    f"Error applying optimization strategy {strategy_name}: {e}"
                )
                results[strategy_name] = {"error": str(e)}

        final_memory = process.memory_info().rss / 1024 / 1024
        results["final_memory_mb"] = final_memory
        results["memory_reduced_mb"] = current_memory - final_memory

        return results

    def _optimize_garbage_collection(self) -> Dict[str, Any]:
        """Optimize garbage collection settings."""
        # Get current GC settings
        old_thresholds = gc.get_threshold()

        # Optimize thresholds for better memory management
        gc.set_threshold(700, 10, 10)  # More aggressive collection

        # Force garbage collection
        collected = gc.collect()

        return {
            "old_thresholds": old_thresholds,
            "new_thresholds": gc.get_threshold(),
            "objects_collected": collected,
        }

    def _implement_object_pooling(self) -> Dict[str, Any]:
        """Implement object pooling for frequently created objects."""
        # This is a placeholder - actual implementation would depend on specific use cases
        return {
            "strategy": "object_pooling",
            "status": "placeholder_implementation",
            "note": "Object pooling should be implemented based on specific application needs",
        }

    def _implement_lazy_evaluation(self) -> Dict[str, Any]:
        """Implement lazy evaluation for memory-intensive operations."""
        # This is a placeholder - actual implementation would depend on specific use cases
        return {
            "strategy": "lazy_evaluation",
            "status": "placeholder_implementation",
            "note": "Lazy evaluation should be implemented for specific data processing operations",
        }

    def _implement_memory_mapping(self) -> Dict[str, Any]:
        """Implement memory mapping for large data structures."""
        # This is a placeholder - actual implementation would depend on specific use cases
        return {
            "strategy": "memory_mapping",
            "status": "placeholder_implementation",
            "note": "Memory mapping should be implemented for large file-based data structures",
        }

    def _use_weak_references(self) -> Dict[str, Any]:
        """Use weak references to prevent memory leaks."""
        # This is a placeholder - actual implementation would depend on specific use cases
        return {
            "strategy": "weak_references",
            "status": "placeholder_implementation",
            "note": "Weak references should be used for caching and observer patterns",
        }

    def analyze_memory_efficiency(self) -> float:
        """Analyze memory efficiency and return score (0-100)."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()

            # Calculate efficiency based on various factors
            memory_usage_mb = memory_info.rss / 1024 / 1024

            # Base score
            efficiency_score = 100.0

            # Penalize high memory usage
            if memory_usage_mb > 1000:  # > 1GB
                efficiency_score -= min(50, (memory_usage_mb - 1000) / 100 * 10)

            # Check for memory fragmentation
            if hasattr(memory_info, "vms") and memory_info.vms > memory_info.rss * 2:
                efficiency_score -= 20  # High virtual memory usage

            # Check GC efficiency
            gc_stats = gc.get_stats()
            total_collections = sum(stat["collections"] for stat in gc_stats)
            if total_collections > 1000:  # Too many collections
                efficiency_score -= 15

            return max(0, min(100, efficiency_score))

        except Exception as e:
            self.logger.error(f"Error analyzing memory efficiency: {e}")
            return 0.0


class MemoryEfficientDataStructures:
    """Memory-efficient data structures and utilities."""

    @staticmethod
    def create_weak_cache(max_size: int = 1000) -> weakref.WeakValueDictionary:
        """Create a memory-efficient cache using weak references."""
        return weakref.WeakValueDictionary()

    @staticmethod
    def create_object_pool(pool_size: int = 100, factory_func: Callable = None):
        """Create an object pool to reuse expensive objects."""
        pool = []

        def get_object():
            if pool:
                return pool.pop()
            elif factory_func:
                return factory_func()
            else:
                return None

        def return_object(obj):
            if len(pool) < pool_size:
                pool.append(obj)

        return get_object, return_object

    @staticmethod
    def create_memory_efficient_list(max_size: int = 10000):
        """Create a memory-efficient list that automatically manages size."""

        class MemoryEfficientList:
            def __init__(self, max_size):
                self.max_size = max_size
                self.items = []

            def append(self, item):
                self.items.append(item)
                if len(self.items) > self.max_size:
                    self.items = self.items[-self.max_size :]  # Keep only recent items

            def __getitem__(self, index):
                return self.items[index]

            def __len__(self):
                return len(self.items)

            def __iter__(self):
                return iter(self.items)

        return MemoryEfficientList(max_size)

    @staticmethod
    def create_lazy_dict(factory_func: Callable):
        """Create a lazy dictionary that creates values on demand."""

        class LazyDict(dict):
            def __init__(self, factory_func):
                super().__init__()
                self.factory_func = factory_func

            def __missing__(self, key):
                value = self.factory_func(key)
                self[key] = value
                return value

        return LazyDict(factory_func)


def memory_monitor(interval_seconds: int = 30) -> Callable:
    """Decorator to monitor memory usage of functions."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get initial memory
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024

            # Start memory monitoring in background
            monitor_thread = threading.Thread(
                target=_monitor_memory_usage,
                args=(interval_seconds, func.__name__),
                daemon=True,
            )
            monitor_thread.start()

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                # Get final memory
                final_memory = process.memory_info().rss / 1024 / 1024
                memory_delta = final_memory - initial_memory

                if memory_delta > 100:  # More than 100MB increase
                    logging.warning(
                        f"Function {func.__name__} used {memory_delta:.1f}MB of memory"
                    )

        return wrapper

    return decorator


def _monitor_memory_usage(interval_seconds: int, function_name: str) -> None:
    """Background memory monitoring function."""
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024

    while True:
        time.sleep(interval_seconds)
        current_memory = process.memory_info().rss / 1024 / 1024
        memory_delta = current_memory - initial_memory

        if memory_delta > 200:  # More than 200MB increase
            logging.warning(
                f"Memory usage in {function_name} increased by {memory_delta:.1f}MB"
            )


def optimize_spark_memory() -> Dict[str, Any]:
    """Optimize Spark memory configuration."""
    # This would contain Spark-specific memory optimization
    return {
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
        },
        "memory_settings": {
            "spark.driver.memory": "4g",
            "spark.executor.memory": "8g",
            "spark.driver.maxResultSize": "2g",
            "spark.executor.memoryFraction": "0.8",
            "spark.storage.memoryFraction": "0.6",
        },
    }


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Memory Optimization")
    parser.add_argument("--profile", action="store_true", help="Run memory profiling")
    parser.add_argument(
        "--optimize", action="store_true", help="Run memory optimization"
    )
    parser.add_argument(
        "--target-memory", type=float, default=500, help="Target memory in MB"
    )
    parser.add_argument(
        "--duration", type=int, default=60, help="Profiling duration in seconds"
    )

    args = parser.parse_args()

    if args.profile:
        profiler = MemoryProfiler()
        print("Starting memory profiling...")

        # Take initial snapshot
        initial_snapshot = profiler.take_snapshot()
        print(f"Initial memory: {initial_snapshot.current_memory_mb:.1f}MB")

        # Monitor for specified duration
        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:
            print("\nProfiling stopped by user")

        # Take final snapshot
        final_snapshot = profiler.take_snapshot()
        print(f"Final memory: {final_snapshot.current_memory_mb:.1f}MB")

        # Show stats
        stats = profiler.get_memory_stats()
        print("\nMemory Statistics:")
        print(f"Peak memory: {stats.get('peak_memory_mb', 0):.1f}MB")
        print(f"Allocated blocks: {stats.get('allocated_blocks', 0)}")

        # Show detected leaks
        leaks = profiler.leak_detector.get_detected_leaks()
        if leaks:
            print(f"\nDetected Memory Leaks: {len(leaks)}")
            for leak in leaks:
                print(
                    f"  - {leak.object_type}: {leak.count} objects, growth rate: {leak.growth_rate:.1f}"
                )
        else:
            print("\nNo memory leaks detected")

    if args.optimize:
        optimizer = MemoryOptimizer()
        print(f"Optimizing memory to target: {args.target_memory}MB")

        results = optimizer.optimize_memory_usage(args.target_memory)
        print("\nOptimization Results:")
        print(f"Final memory: {results.get('final_memory_mb', 0):.1f}MB")
        print(f"Memory reduced: {results.get('memory_reduced_mb', 0):.1f}MB")

        # Show efficiency score
        efficiency_score = optimizer.analyze_memory_efficiency()
        print(f"Memory efficiency score: {efficiency_score:.1f}/100")

    if not args.profile and not args.optimize:
        print("Use --profile or --optimize to run memory operations")
