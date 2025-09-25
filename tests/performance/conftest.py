#!/usr/bin/env python3
"""
Performance testing configuration for SparkForge.

This module provides pytest fixtures and configuration for performance tests,
including setup/teardown for performance monitoring and baseline management.
"""

import os
import sys
from pathlib import Path

import pytest

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.performance.performance_monitor import performance_monitor  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def setup_performance_testing():
    """Setup performance testing environment."""
    # Ensure we have a clean performance monitor
    performance_monitor.results.clear()

    # Set up environment variables for performance testing
    os.environ["SPARKFORGE_PERFORMANCE_TESTING"] = "true"

    yield

    # Cleanup after all performance tests
    if hasattr(performance_monitor, "results"):
        performance_monitor.results.clear()


@pytest.fixture(scope="function")
def performance_monitor_clean():
    """Provide a clean performance monitor for each test."""
    # Clear results before each test
    performance_monitor.results.clear()
    return performance_monitor


@pytest.fixture(scope="session")
def performance_baseline_file():
    """Provide the path to the performance baseline file."""
    baseline_file = project_root / "performance_baseline.json"
    return str(baseline_file)


@pytest.fixture(scope="session")
def performance_report_file():
    """Provide the path to the performance report file."""
    report_file = project_root / "performance_report.json"
    return str(report_file)


@pytest.fixture(scope="function")
def performance_tolerance():
    """Provide default performance regression tolerance."""
    return 0.2  # 20% tolerance for regressions


@pytest.fixture(scope="function")
def warmup_iterations():
    """Provide default warmup iterations for performance tests."""
    return 10


@pytest.fixture(scope="function")
def benchmark_iterations():
    """Provide default benchmark iterations for performance tests."""
    return 1000


@pytest.fixture(scope="function")
def memory_limit_mb():
    """Provide memory usage limit for performance tests."""
    return 100  # 100MB limit


@pytest.fixture(scope="function")
def time_limit_seconds():
    """Provide time limit for performance tests."""
    return 5.0  # 5 seconds limit


@pytest.fixture(scope="function")
def throughput_minimum():
    """Provide minimum throughput requirements."""
    return 100  # 100 operations per second minimum


# Performance test markers
def pytest_configure(config):
    """Configure performance test markers."""
    config.addinivalue_line("markers", "performance: mark test as a performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "memory: mark test that measures memory usage")
    config.addinivalue_line(
        "markers", "regression: mark test that checks for performance regression"
    )
    config.addinivalue_line("markers", "benchmark: mark test as a benchmark test")


# Performance test collection
def pytest_collection_modifyitems(config, items):
    """Modify test collection for performance tests."""
    for item in items:
        # Add performance marker to all tests in performance directory
        if "performance" in str(item.fspath):
            item.add_marker(pytest.mark.performance)

        # Add slow marker to tests that are expected to be slow
        if any(
            keyword in item.name.lower() for keyword in ["memory", "large", "stress"]
        ):
            item.add_marker(pytest.mark.slow)

        # Add memory marker to tests that measure memory
        if "memory" in item.name.lower():
            item.add_marker(pytest.mark.memory)

        # Add regression marker to tests that check regressions
        if "regression" in item.name.lower():
            item.add_marker(pytest.mark.regression)


# Performance test reporting
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Add performance test summary to terminal output."""
    if not config.getoption("--quiet", False):
        # Only show performance summary if we ran performance tests
        if any(
            "performance" in str(item.fspath)
            for item in terminalreporter.config.getoption("--collect-only", False) or []
        ):
            terminalreporter.write_sep("=", "Performance Test Summary")

            summary = performance_monitor.get_performance_summary()
            if summary.get("total_tests", 0) > 0:
                terminalreporter.write(
                    f"Total performance tests: {summary['total_tests']}\n"
                )
                terminalreporter.write(
                    f"Successful tests: {summary['successful_tests']}\n"
                )
                terminalreporter.write(f"Failed tests: {summary['failed_tests']}\n")
                terminalreporter.write(
                    f"Functions tested: {summary['functions_tested']}\n"
                )
                terminalreporter.write(
                    f"Total execution time: {summary['total_execution_time']:.2f}s\n"
                )

                # Check for regressions
                regressions = []
                for result in performance_monitor.results:
                    if result.success:
                        regression = performance_monitor.check_regression(
                            result.function_name
                        )
                        if regression["status"] == "regression_detected":
                            regressions.append(result.function_name)

                if regressions:
                    terminalreporter.write(
                        f"⚠️  Performance regressions detected in: {', '.join(regressions)}\n"
                    )
                else:
                    terminalreporter.write("✅ No performance regressions detected\n")
            else:
                terminalreporter.write("No performance test results available\n")


# Performance test failure handling
def pytest_runtest_setup(item):
    """Setup for each performance test."""
    if "performance" in str(item.fspath):
        # Ensure clean state for performance tests
        performance_monitor.results.clear()


def pytest_runtest_teardown(item, nextitem):
    """Teardown for each performance test."""
    if "performance" in str(item.fspath):
        # Save results after each test
        pass  # Results are already saved in the monitor


# Performance test timeout handling
def pytest_runtest_makereport(item, call):
    """Make report for performance tests."""
    if "performance" in str(item.fspath):
        if call.when == "call":
            # Check if test exceeded time limits
            for result in performance_monitor.results:
                if result.execution_time > 10.0:  # 10 second timeout
                    pytest.fail(
                        f"Performance test exceeded time limit: {result.execution_time:.2f}s"
                    )


# Environment setup for performance tests
@pytest.fixture(scope="session")
def performance_test_environment():
    """Setup performance testing environment variables."""
    env_vars = {
        "SPARKFORGE_PERFORMANCE_TESTING": "true",
        "SPARKFORGE_PERFORMANCE_BASELINE_FILE": "performance_baseline.json",
        "SPARKFORGE_PERFORMANCE_REPORT_FILE": "performance_report.json",
        "SPARKFORGE_PERFORMANCE_TOLERANCE": "0.2",
    }

    original_env = {}
    for key, value in env_vars.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    yield env_vars

    # Restore original environment
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value
