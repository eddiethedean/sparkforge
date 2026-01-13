#!/usr/bin/env python3
"""
Unified test runner for SparkForge test suite.

This script provides a single, flexible interface for running tests with various
configurations including mode selection, layer filtering, parallelization, and coverage.

Usage:
    # Run all unit tests in mock mode (default)
    python run_tests.py

    # Run all tests in real mode
    python run_tests.py --mode real

    # Run specific layer
    python run_tests.py --layer integration

    # Run with parallelization
    python run_tests.py --parallel --workers 10

    # Run with coverage
    python run_tests.py --coverage

    # Run specific test file
    python run_tests.py tests/unit/test_validation.py

    # Run tests matching marker
    python run_tests.py --marker slow

    # Combine options
    python run_tests.py --mode real --layer system --parallel --workers 4 --coverage
"""

import argparse
import os
import subprocess
import sys
import time
from typing import List, Optional


def setup_environment(mode: str) -> dict:
    """Set up environment variables for test execution."""
    env = os.environ.copy()

    # Set Spark mode
    env["SPARK_MODE"] = mode
    env["SPARKFORGE_ENGINE"] = mode

    # Set Java environment if needed for real mode
    if mode == "real":
        java_home = os.environ.get("JAVA_HOME")
        if not java_home or not os.path.exists(java_home):
            # Try to find Java
            for alt_path in [
                "/opt/homebrew/opt/openjdk@17",
                "/opt/homebrew/opt/openjdk@11",
                "/usr/lib/jvm/java-17-openjdk",
                "/usr/lib/jvm/java-11-openjdk",
            ]:
                if os.path.exists(alt_path):
                    java_home = alt_path
                    break

        if java_home:
            env["JAVA_HOME"] = java_home
            env["PATH"] = f"{java_home}/bin:{env.get('PATH', '')}"

    return env


def build_pytest_command(
    mode: str,
    layer: Optional[str],
    parallel: bool,
    workers: int,
    coverage: bool,
    marker: Optional[str],
    test_paths: List[str],
    verbose: bool,
    extra_args: List[str],
) -> List[str]:
    """Build pytest command with all specified options."""
    cmd = [sys.executable, "-m", "pytest"]

    # Add test paths or default to layer
    if test_paths:
        cmd.extend(test_paths)
    elif layer:
        if layer == "all":
            cmd.append("tests/")
        else:
            cmd.append(f"tests/{layer}/")
    else:
        # Default to all tests
        cmd.append("tests/")

    # Add layer-specific ignores
    if layer and layer != "all":
        ignores = {
            "unit": [
                "--ignore=tests/integration",
                "--ignore=tests/system",
                "--ignore=tests/performance",
                "--ignore=tests/security",
            ],
            "integration": [
                "--ignore=tests/unit",
                "--ignore=tests/system",
                "--ignore=tests/performance",
                "--ignore=tests/security",
            ],
            "system": [
                "--ignore=tests/unit",
                "--ignore=tests/integration",
                "--ignore=tests/performance",
                "--ignore=tests/security",
            ],
        }
        if layer in ignores:
            cmd.extend(ignores[layer])

    # Add parallel execution
    if parallel:
        cmd.extend(["-n", str(workers)])
        # Exclude tests that don't work well in parallel
        cmd.extend(
            [
                "-k",
                "not (test_dataframe_access_pytest or test_delta_lake_comprehensive or test_table_operations or test_unified_dependency_scenarios or test_unified_execution or test_source_silvers_none)",
            ]
        )

    # Add coverage
    if coverage:
        cmd.extend(
            [
                "--cov=src/pipeline_builder",
                "--cov=src/pipeline_builder_base",
                "--cov-report=term",
                "--cov-report=html:htmlcov",
            ]
        )

    # Add marker filter
    if marker:
        # If in real mode, also exclude batch_mode tests
        if mode == "real":
            cmd.extend(["-m", f"{marker} and not batch_mode"])
        else:
            cmd.extend(["-m", marker])
    elif mode == "real":
        # Skip batch mode tests in real mode (Delta Lake doesn't support batch mode operations)
        cmd.extend(["-m", "not batch_mode"])

    # Add verbosity
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")

    # Add default pytest options
    cmd.extend(["--tb=short", "--durations=10"])

    # Add extra arguments
    if extra_args:
        cmd.extend(extra_args)

    return cmd


def run_tests(
    mode: str,
    layer: Optional[str],
    parallel: bool,
    workers: int,
    coverage: bool,
    marker: Optional[str],
    test_paths: List[str],
    verbose: bool,
    extra_args: List[str],
) -> tuple[bool, float]:
    """Run tests with specified configuration."""
    env = setup_environment(mode)
    cmd = build_pytest_command(
        mode,
        layer,
        parallel,
        workers,
        coverage,
        marker,
        test_paths,
        verbose,
        extra_args,
    )

    # Print configuration
    print("=" * 70)
    print("🧪 SparkForge Test Runner")
    print("=" * 70)
    print(f"Mode: {mode.upper()}")
    if layer:
        print(f"Layer: {layer}")
    if parallel:
        print(f"Parallel: {workers} workers")
    if coverage:
        print("Coverage: Enabled")
    if marker:
        print(f"Marker: {marker}")
    if test_paths:
        print(f"Paths: {', '.join(test_paths)}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 70)
    print()

    # Run tests
    start_time = time.time()
    try:
        result = subprocess.run(cmd, env=env, check=False)
        duration = time.time() - start_time
        return result.returncode == 0, duration
    except KeyboardInterrupt:
        print("\n❌ Tests interrupted by user")
        return False, time.time() - start_time
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False, time.time() - start_time


def main():
    """Main entry point for test runner."""
    parser = argparse.ArgumentParser(
        description="Unified test runner for SparkForge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["mock", "real", "both"],
        default="mock",
        help="Spark mode: mock (default), real, or both (runs in both modes)",
    )

    # Layer selection
    parser.add_argument(
        "--layer",
        choices=["unit", "integration", "system", "performance", "security", "all"],
        help="Test layer to run (default: all)",
    )

    # Parallel execution
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run tests in parallel with pytest-xdist",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4, requires --parallel)",
    )

    # Coverage
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report",
    )

    # Test filtering
    parser.add_argument(
        "--marker",
        help="Run tests matching marker (e.g., 'slow', 'not slow', 'unit and not slow')",
    )

    # Verbosity
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    # Test paths
    parser.add_argument(
        "test_paths",
        nargs="*",
        help="Specific test files or directories to run",
    )

    # Parse known args to allow passing extra pytest arguments
    args, extra_args = parser.parse_known_args()

    # Validate arguments
    if args.workers < 1:
        print("❌ Error: --workers must be at least 1")
        sys.exit(1)

    if args.parallel and args.workers == 1:
        print(
            "⚠️  Warning: --parallel with 1 worker is equivalent to sequential execution"
        )

    # Run tests
    if args.mode == "both":
        # Run in both modes
        print("🔄 Running tests in both modes...\n")

        # Run mock mode
        print("=" * 70)
        print("MODE 1: MOCK SPARK")
        print("=" * 70)
        mock_success, mock_duration = run_tests(
            "mock",
            args.layer,
            args.parallel,
            args.workers,
            args.coverage,
            args.marker,
            args.test_paths,
            args.verbose,
            extra_args,
        )

        print("\n" + "=" * 70)
        print("MODE 2: REAL SPARK")
        print("=" * 70)
        real_success, real_duration = run_tests(
            "real",
            args.layer,
            args.parallel,
            args.workers,
            args.coverage,
            args.marker,
            args.test_paths,
            args.verbose,
            extra_args,
        )

        # Summary
        print("\n" + "=" * 70)
        print("📊 SUMMARY")
        print("=" * 70)
        print(
            f"Mock mode: {'✅ PASSED' if mock_success else '❌ FAILED'} ({mock_duration:.1f}s)"
        )
        print(
            f"Real mode: {'✅ PASSED' if real_success else '❌ FAILED'} ({real_duration:.1f}s)"
        )
        print(f"Total time: {mock_duration + real_duration:.1f}s")

        sys.exit(0 if (mock_success and real_success) else 1)
    else:
        # Run in single mode
        success, duration = run_tests(
            args.mode,
            args.layer,
            args.parallel,
            args.workers,
            args.coverage,
            args.marker,
            args.test_paths,
            args.verbose,
            extra_args,
        )

        print("\n" + "=" * 70)
        print("📊 RESULTS")
        print("=" * 70)
        print(f"Status: {'✅ PASSED' if success else '❌ FAILED'}")
        print(f"Duration: {duration:.1f}s")
        print("=" * 70)

        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
