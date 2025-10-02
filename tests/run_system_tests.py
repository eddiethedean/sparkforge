#!/usr/bin/env python3
"""
System test runner for SparkForge.

This script runs all system tests with proper configuration and reporting.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_system_tests():
    """Run system tests with mypy type checking."""
    print("🌐 Running SparkForge System Tests")
    print("🔧 Using Mock Spark (faster, more reliable testing)")
    print("=" * 50)

    # Set up environment
    env = os.environ.copy()
    env["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@11/11.0.28"
    env["PATH"] = f"{env['JAVA_HOME']}/bin:{env['PATH']}"
    # Default to mock mode for faster, more reliable testing
    env["SPARK_MODE"] = "mock"

    start_time = time.time()

    # Run system tests
    print("📊 Running system tests...")
    test_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/system/",
        "-v",
        "--tb=short",
        "--durations=10",
        "-m",
        "system",
    ]

    result = subprocess.run(test_cmd, env=env, capture_output=True, text=True)

    # Run mypy type checking on system tests
    print("\n🔍 Running mypy type checking on system tests...")
    mypy_cmd = [
        sys.executable,
        "-m",
        "mypy",
        "tests/system/",
        "--config-file=tests/mypy.ini",
    ]

    mypy_result = subprocess.run(mypy_cmd, env=env, capture_output=True, text=True)

    end_time = time.time()
    duration = end_time - start_time

    # Print results
    print("\n" + "=" * 50)
    print("📊 SYSTEM TEST RESULTS")
    print("=" * 50)

    if result.returncode == 0:
        print("✅ System tests: PASSED")
    else:
        print("❌ System tests: FAILED")
        print(result.stdout)
        print(result.stderr)

    if mypy_result.returncode == 0:
        print("✅ System tests mypy: PASSED")
    else:
        print("❌ System tests mypy: FAILED")
        print(mypy_result.stdout)
        print(mypy_result.stderr)

    print(f"⏱️  Total duration: {duration:.2f}s")

    # Return success if all checks passed
    return result.returncode == 0 and mypy_result.returncode == 0


if __name__ == "__main__":
    success = run_system_tests()
    sys.exit(0 if success else 1)
