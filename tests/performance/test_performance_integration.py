"""
Integration tests for SparkForge performance components.

This module tests the integration of all performance components including:
- Performance profiler
- Caching strategies
- Performance monitoring
- Memory optimization
- Performance benchmarking
"""

import tempfile
import time
from pathlib import Path

import pytest

from tests.performance.caching_strategies import (
    HybridCache,
    MemoryCache,
    PersistentCache,
    cache_manager,
    cache_result,
)
from tests.performance.memory_optimization import (
    MemoryOptimizer,
    MemoryProfiler,
    memory_monitor,
    optimize_spark_memory,
)
from tests.performance.performance_benchmarking import PerformanceBenchmark
from tests.performance.performance_monitoring import PerformanceMonitor

# Import performance components
from tests.performance.performance_profiler import PerformanceProfiler, profile_function


class TestPerformanceIntegration:
    """Integration tests for performance components."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def performance_profiler(self):
        """Create performance profiler instance."""
        return PerformanceProfiler(
            enable_memory_tracking=True, enable_cpu_tracking=True
        )

    @pytest.fixture
    def performance_monitor(self):
        """Create performance monitor instance."""
        config = {
            "monitoring_interval": 1,
            "enable_alerts": True,
            "enable_resource_monitoring": True,
        }
        return PerformanceMonitor(config)

    @pytest.fixture
    def memory_profiler(self):
        """Create memory profiler instance."""
        return MemoryProfiler(enable_tracemalloc=True)

    @pytest.fixture
    def performance_benchmark(self):
        """Create performance benchmark instance."""
        return PerformanceBenchmark()

    def test_performance_profiler_integration(self, performance_profiler):
        """Test performance profiler integration."""

        # Test function profiling
        @performance_profiler.profile_function
        def test_function(x):
            time.sleep(0.01)  # Simulate work
            return x * 2

        # Profile function
        result, report = performance_profiler.profile_pipeline(test_function, 5)

        assert result == 10
        assert report.total_execution_time > 0
        assert len(report.function_metrics) > 0
        assert len(report.recommendations) > 0

    def test_caching_strategies_integration(self, temp_dir):
        """Test caching strategies integration."""
        # Test memory cache
        memory_cache = MemoryCache(max_size_mb=10, default_ttl=60)

        # Test basic caching
        memory_cache.set("test_key", "test_value")
        value = memory_cache.get("test_key")
        assert value == "test_value"

        # Test cache statistics
        stats = memory_cache.get_stats()
        assert stats.total_entries == 1
        assert stats.hit_count == 1

        # Test persistent cache
        persistent_cache = PersistentCache(temp_dir, max_file_size_mb=5)
        persistent_cache.set("persistent_key", "persistent_value")
        persistent_value = persistent_cache.get("persistent_key")
        assert persistent_value == "persistent_value"

        # Test hybrid cache
        hybrid_cache = HybridCache(memory_cache, persistent_cache)
        hybrid_cache.set("hybrid_key", "hybrid_value")
        hybrid_value = hybrid_cache.get("hybrid_key")
        assert hybrid_value == "hybrid_value"

    def test_performance_monitoring_integration(self, performance_monitor):
        """Test performance monitoring integration."""
        # Start monitoring
        performance_monitor.start_monitoring()

        # Record some metrics
        performance_monitor.record_metric("test_metric", 100.0, "ms")
        performance_monitor.record_metric("throughput", 50.0, "rps")

        # Wait for monitoring to collect data
        time.sleep(2)

        # Get dashboard data
        dashboard_data = performance_monitor.get_dashboard_data()
        assert "metrics" in dashboard_data
        assert "resources" in dashboard_data
        assert "alerts" in dashboard_data

        # Stop monitoring
        performance_monitor.stop_monitoring()
        assert not performance_monitor.monitoring_active

    def test_memory_optimization_integration(self, memory_profiler):
        """Test memory optimization integration."""
        # Take initial snapshot
        initial_snapshot = memory_profiler.take_snapshot()
        assert initial_snapshot.current_memory_mb >= 0

        # Create some objects to test memory tracking
        [f"test_object_{i}" for i in range(1000)]

        # Take another snapshot
        final_snapshot = memory_profiler.take_snapshot()
        assert final_snapshot.current_memory_mb >= initial_snapshot.current_memory_mb

        # Get memory stats
        stats = memory_profiler.get_memory_stats()
        assert "current_memory_mb" in stats
        assert "object_counts" in stats

        # Test memory optimizer
        optimizer = MemoryOptimizer()
        efficiency_score = optimizer.analyze_memory_efficiency()
        assert 0 <= efficiency_score <= 100

    def test_performance_benchmarking_integration(self, performance_benchmark):
        """Test performance benchmarking integration."""

        # Test function to benchmark
        def benchmark_function(x):
            time.sleep(0.001)  # Simulate work
            return x * x

        # Benchmark function
        stats = performance_benchmark.benchmark_function(
            benchmark_function, 5, iterations=50
        )

        assert stats.function_name.endswith("benchmark_function")
        assert stats.mean_time > 0
        assert stats.total_iterations == 50
        assert stats.success_rate == 100.0

        # Set baseline
        performance_benchmark.set_baseline(stats)

        # Test regression detection
        regressions = performance_benchmark.detect_performance_regressions()
        assert isinstance(regressions, list)

    def test_load_testing_integration(self, performance_benchmark):
        """Test load testing integration."""

        def load_test_function(x):
            time.sleep(0.001)  # Simulate work
            return x + 1

        # Run load test
        load_result = performance_benchmark.load_test(load_test_function, 5, 50, 10)

        assert load_result.concurrent_users == 5
        assert load_result.total_requests == 50
        assert load_result.successful_requests >= 0
        assert load_result.avg_response_time >= 0
        assert load_result.throughput_rps >= 0

    def test_caching_decorators_integration(self):
        """Test caching decorators integration."""
        call_count = 0

        @cache_result(ttl_seconds=60)
        def cached_function(x):
            nonlocal call_count
            call_count += 1
            time.sleep(0.01)  # Simulate work
            return x * 2

        # First call
        result1 = cached_function(5)
        assert result1 == 10
        assert call_count == 1

        # Second call (should be cached)
        result2 = cached_function(5)
        assert result2 == 10
        assert call_count == 1  # Should not increment

        # Different argument (should not be cached)
        result3 = cached_function(6)
        assert result3 == 12
        assert call_count == 2

    def test_memory_monitoring_decorator_integration(self):
        """Test memory monitoring decorator integration."""

        @memory_monitor(interval_seconds=1)
        def memory_intensive_function():
            # Create some objects to use memory
            objects = [f"object_{i}" for i in range(10000)]
            time.sleep(0.1)
            return len(objects)

        # Execute function
        result = memory_intensive_function()
        assert result == 10000

    def test_performance_profiler_decorator_integration(self):
        """Test performance profiler decorator integration."""

        @profile_function
        def profiled_function(x):
            time.sleep(0.01)
            return x * 3

        # Execute function
        result = profiled_function(4)
        assert result == 12

    def test_comprehensive_performance_workflow(
        self,
        performance_profiler,
        performance_monitor,
        memory_profiler,
        performance_benchmark,
    ):
        """Test comprehensive performance workflow integration."""
        # Step 1: Start monitoring
        performance_monitor.start_monitoring()

        # Step 2: Take memory snapshot
        memory_profiler.take_snapshot()

        # Step 3: Profile a function
        @performance_profiler.profile_function
        def workflow_function(data):
            time.sleep(0.01)
            return [x * 2 for x in data]

        # Step 4: Benchmark the function
        stats = performance_benchmark.benchmark_function(
            workflow_function, [1, 2, 3, 4, 5], iterations=20
        )

        # Step 5: Record performance metrics
        performance_monitor.record_metric(
            "workflow_execution_time", stats.mean_time, "ms"
        )
        performance_monitor.record_metric(
            "workflow_memory_usage",
            memory_profiler.get_memory_stats().get("current_memory_mb", 0),
            "mb",
        )

        # Step 6: Wait for monitoring
        time.sleep(2)

        # Step 7: Get comprehensive results
        dashboard_data = performance_monitor.get_dashboard_data()
        memory_stats = memory_profiler.get_memory_stats()
        profiler_report = performance_profiler.generate_report()

        # Step 8: Verify integration
        assert dashboard_data["monitoring_active"]
        assert "metrics" in dashboard_data
        assert memory_stats["current_memory_mb"] >= 0
        assert profiler_report.total_execution_time > 0

        # Step 9: Stop monitoring
        performance_monitor.stop_monitoring()

    def test_performance_optimization_workflow(self, temp_dir):
        """Test performance optimization workflow."""
        # Create cache manager
        cache_manager.clear_all()

        # Test optimization workflow
        optimizer = MemoryOptimizer()

        # Test Spark memory optimization
        spark_config = optimize_spark_memory()
        assert "spark_config" in spark_config
        assert "memory_settings" in spark_config

        # Test memory efficiency analysis
        efficiency_score = optimizer.analyze_memory_efficiency()
        assert 0 <= efficiency_score <= 100

    def test_performance_reporting_integration(
        self,
        performance_profiler,
        performance_monitor,
        memory_profiler,
        performance_benchmark,
        temp_dir,
    ):
        """Test performance reporting integration."""

        # Generate some performance data
        @performance_profiler.profile_function
        def report_function(x):
            time.sleep(0.01)
            return x**2

        # Profile function
        result, profiler_report = performance_profiler.profile_pipeline(
            report_function, 5
        )

        # Benchmark function
        performance_benchmark.benchmark_function(
            report_function, 5, iterations=10
        )

        # Start monitoring and record metrics
        performance_monitor.start_monitoring()
        performance_monitor.record_metric("report_metric", 100.0, "ms")
        time.sleep(1)

        # Take memory snapshot
        memory_profiler.take_snapshot()

        # Generate reports
        profiler_report_file = performance_profiler.export_report(
            temp_dir / "profiler_report.json"
        )
        benchmark_report_file = performance_benchmark.export_benchmark_report(
            temp_dir / "benchmark_report.json"
        )
        performance_report = performance_monitor.generate_performance_report()
        performance_report_file = performance_monitor.export_report(
            performance_report, temp_dir / "performance_report.json"
        )

        # Verify reports were created
        assert profiler_report_file.exists()
        assert benchmark_report_file.exists()
        assert performance_report_file.exists()

        # Stop monitoring
        performance_monitor.stop_monitoring()

    def test_error_handling_integration(
        self, performance_profiler, performance_monitor
    ):
        """Test error handling in performance components."""

        # Test profiler with failing function
        @performance_profiler.profile_function
        def failing_function():
            raise ValueError("Test error")

        # Should handle error gracefully
        try:
            failing_function()
        except ValueError:
            pass  # Expected

        # Check that error was recorded
        error_results = [r for r in performance_profiler.metrics if not r.success]
        assert len(error_results) > 0

        # Test monitor with invalid metrics
        performance_monitor.start_monitoring()

        # Record invalid metric
        performance_monitor.record_metric("", -1.0, "")  # Invalid name and value

        # Should handle gracefully
        dashboard_data = performance_monitor.get_dashboard_data()
        assert "metrics" in dashboard_data

        performance_monitor.stop_monitoring()

    def test_concurrent_performance_operations(
        self, performance_profiler, performance_monitor
    ):
        """Test concurrent performance operations."""
        import threading

        results = []
        errors = []

        def concurrent_operation(operation_id):
            try:

                @performance_profiler.profile_function
                def concurrent_function(x):
                    time.sleep(0.01)
                    return x + operation_id

                result = concurrent_function(10)
                results.append(result)

                # Record metric
                performance_monitor.record_metric(
                    f"concurrent_metric_{operation_id}", result, "count"
                )

            except Exception as e:
                errors.append(str(e))

        # Start monitoring
        performance_monitor.start_monitoring()

        # Run concurrent operations
        threads = []
        for i in range(5):
            thread = threading.Thread(target=concurrent_operation, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify results
        assert len(results) == 5
        assert len(errors) == 0

        # Check monitoring data
        time.sleep(1)
        dashboard_data = performance_monitor.get_dashboard_data()
        assert dashboard_data["monitoring_active"]

        performance_monitor.stop_monitoring()


# Pytest markers for performance tests
@pytest.mark.performance
class TestPerformanceMarkers:
    """Test performance-specific pytest markers."""

    def test_performance_marker_works(self):
        """Test that performance marker works."""
        assert True

    @pytest.mark.slow
    def test_slow_performance_test(self):
        """Test slow performance test marker."""
        time.sleep(0.1)  # Simulate slow test
        assert True


# Integration test for CI/CD performance pipeline
def test_performance_cicd_integration():
    """Test performance integration with CI/CD pipeline."""
    # Create performance components
    profiler = PerformanceProfiler()
    monitor = PerformanceMonitor({"monitoring_interval": 1})
    benchmark = PerformanceBenchmark()

    # Test function
    def cicd_test_function(data):
        return sum(data)

    # Profile function
    result, profiler_report = profiler.profile_pipeline(
        cicd_test_function, [1, 2, 3, 4, 5]
    )
    assert result == 15

    # Benchmark function
    benchmark_stats = benchmark.benchmark_function(
        cicd_test_function, [1, 2, 3, 4, 5], iterations=10
    )
    assert benchmark_stats.mean_time >= 0
    assert benchmark_stats.success_rate == 100.0

    # Start monitoring
    monitor.start_monitoring()
    monitor.record_metric("cicd_metric", benchmark_stats.mean_time, "ms")
    time.sleep(1)

    # Get results
    dashboard_data = monitor.get_dashboard_data()
    assert dashboard_data["monitoring_active"]

    monitor.stop_monitoring()


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short"])
