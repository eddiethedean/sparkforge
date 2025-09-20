#!/usr/bin/env python3
"""
Unit test runner for SparkForge.

This script runs all unit tests with proper configuration and reporting.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_unit_tests():
    """Run unit tests with mypy type checking."""
    print("🧪 Running SparkForge Unit Tests")
    print("=" * 50)

    # Set up environment
    env = os.environ.copy()
    env["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@11/11.0.28"
    env["PATH"] = f"{env['JAVA_HOME']}/bin:{env['PATH']}"

    start_time = time.time()

    # Run unit tests
    print("📊 Running unit tests...")
    test_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/unit/",
        "-v",
        "--tb=short",
        "--durations=10",
        "--cov=sparkforge",
        "--cov-report=term",
        "--cov-report=html:htmlcov_unit",
        "-m",
        "unit",
    ]

    result = subprocess.run(test_cmd, env=env, capture_output=True, text=True)

    # Run mypy type checking
    print("\n🔍 Running mypy type checking...")
    mypy_cmd = [
        sys.executable,
        "-m",
        "mypy",
        "tests/unit/",
        "--config-file=tests/mypy.ini",
    ]

    mypy_result = subprocess.run(mypy_cmd, env=env, capture_output=True, text=True)

    # Run mypy on source code
    print("🔍 Running mypy on source code...")
    mypy_source_cmd = [
        sys.executable,
        "-m",
        "mypy",
        "sparkforge/",
        "--config-file=mypy.ini",
    ]

    mypy_source_result = subprocess.run(
        mypy_source_cmd, env=env, capture_output=True, text=True
    )

    end_time = time.time()
    duration = end_time - start_time

    # Print results
    print("\n" + "=" * 50)
    print("📊 UNIT TEST RESULTS")
    print("=" * 50)

    if result.returncode == 0:
        print("✅ Unit tests: PASSED")
    else:
        print("❌ Unit tests: FAILED")
        print(result.stdout)
        print(result.stderr)

    if mypy_result.returncode == 0:
        print("✅ Unit tests mypy: PASSED")
    else:
        print("❌ Unit tests mypy: FAILED")
        print(mypy_result.stdout)
        print(mypy_result.stderr)

    if mypy_source_result.returncode == 0:
        print("✅ Source code mypy: PASSED")
    else:
        print("❌ Source code mypy: FAILED")
        print(mypy_source_result.stdout)
        print(mypy_source_result.stderr)

    print(f"⏱️  Total duration: {duration:.2f}s")

    # Return success if all checks passed
    return (
        result.returncode == 0
        and mypy_result.returncode == 0
        and mypy_source_result.returncode == 0
    )


if __name__ == "__main__":
    success = run_unit_tests()
    sys.exit(0 if success else 1)
