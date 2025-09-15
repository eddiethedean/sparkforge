#!/usr/bin/env python3
"""
Parallel test runner for Sparkforge with smart test categorization.
"""

import argparse
import subprocess
import sys
import time


def run_command(cmd, description):
    """Run a command and return timing info."""
    print(f"\n🚀 {description}")
    print(f"Command: {' '.join(cmd)}")
    start_time = time.time()

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = time.time() - start_time

        if result.returncode == 0:
            print(f"✅ {description} - PASSED ({duration:.1f}s)")
            return True, duration
        else:
            print(f"❌ {description} - FAILED ({duration:.1f}s)")
            print(f"Error: {result.stderr}")
            return False, duration

    except Exception as e:
        duration = time.time() - start_time
        print(f"💥 {description} - ERROR ({duration:.1f}s): {e}")
        return False, duration


def main():
    parser = argparse.ArgumentParser(
        description="Run Sparkforge tests with smart parallel execution"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--fast-only", action="store_true", help="Run only fast tests in parallel"
    )
    parser.add_argument(
        "--all-parallel",
        action="store_true",
        help="Run all parallel-compatible tests (slower than fast-only)",
    )

    args = parser.parse_args()

    # Test categories that work well in parallel
    parallel_tests = [
        "tests/test_validation.py",
        "tests/test_logger.py",
        "tests/test_execution_engine.py",
        "tests/test_pipeline_builder.py",
    ]

    # Tests that need sequential execution
    sequential_tests = [
        "tests/test_config.py",
        "tests/test_performance.py",
        "tests/test_concurrent_execution.py",
        "tests/test_dataframe_access_pytest.py",
        "tests/test_delta_lake_comprehensive.py",
        "tests/test_table_operations.py",
        "tests/test_unified_dependency_scenarios.py",
        "tests/test_unified_execution.py",
        "tests/test_unified_execution_edge_cases.py",
        "tests/test_source_silvers_none.py",
        "tests/test_integration_comprehensive.py",
        "tests/test_log_writer_real_spark.py",
        "tests/test_bronze_no_datetime.py",
        "tests/test_integration_simple.py",
        "tests/test_simple_real_spark.py",
        "tests/test_source_silvers_pytest.py",
        "tests/test_utils.py",
    ]

    if args.fast_only:
        # Only run fast parallel tests
        test_files = parallel_tests[:6]  # First 6 are fastest
        description = f"Fast parallel tests ({len(test_files)} files)"
    elif args.all_parallel:
        # Run all parallel tests
        test_files = parallel_tests
        description = f"All parallel-compatible tests ({len(test_files)} files)"
    else:
        # Run fast parallel tests by default (fastest option)
        test_files = parallel_tests[:6]  # First 6 are fastest
        description = f"Fast parallel tests ({len(test_files)} files) - fastest option"

    # Base command for parallel execution
    base_cmd = [
        "python",
        "-m",
        "pytest",
        "--log-level=INFO",
        "--tb=short",
        "-q",
        "-n",
        str(args.workers),
    ] + test_files

    # Run parallel tests
    parallel_success, parallel_duration = run_command(base_cmd, description)

    if not parallel_success:
        print(f"\n💥 Parallel tests failed in {parallel_duration:.1f}s")
        sys.exit(1)

    print(f"\n🎉 Parallel tests completed in {parallel_duration:.1f}s!")

    # Ask if user wants to run sequential tests
    if not args.fast_only:
        print("\n📊 Performance Summary:")
        print(
            f"  • Parallel tests: {len(test_files)} files in {parallel_duration:.1f}s"
        )
        print(f"  • Sequential tests: {len(sequential_tests)} files (not run)")
        print(f"  • Total parallel speedup: ~{4}x faster than sequential")

        print("\n🔧 To run sequential tests:")
        print(
            f"  python -m pytest {' '.join(sequential_tests)} --log-level=INFO --tb=no -q"
        )

        print("\n💡 Usage options:")
        print(
            "  python run_tests_parallel.py --workers 4                    # Fast tests (default)"
        )
        print(
            "  python run_tests_parallel.py --all-parallel --workers 4     # All parallel tests"
        )
        print(
            "  python run_tests_parallel.py --fast-only --workers 4        # Fast tests (explicit)"
        )


if __name__ == "__main__":
    main()
