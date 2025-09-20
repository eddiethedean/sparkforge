#!/usr/bin/env python3
"""
Integration test runner for SparkForge.

This script runs all integration tests with proper configuration and reporting.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_integration_tests():
    """Run integration tests with mypy type checking."""
    print("🔗 Running SparkForge Integration Tests")
    print("=" * 50)

    # Set up environment
    env = os.environ.copy()
    env["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@11/11.0.28"
    env["PATH"] = f"{env['JAVA_HOME']}/bin:{env['PATH']}"

    start_time = time.time()

    # Run integration tests
    print("📊 Running integration tests...")
    test_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/integration/",
        "-v",
        "--tb=short",
        "--durations=10",
        "-m",
        "integration",
    ]

    result = subprocess.run(test_cmd, env=env, capture_output=True, text=True)

    # Run mypy type checking on integration tests
    print("\n🔍 Running mypy type checking on integration tests...")
    mypy_cmd = [
        sys.executable,
        "-m",
        "mypy",
        "tests/integration/",
        "--config-file=tests/mypy.ini",
    ]

    mypy_result = subprocess.run(mypy_cmd, env=env, capture_output=True, text=True)

    end_time = time.time()
    duration = end_time - start_time

    # Print results
    print("\n" + "=" * 50)
    print("📊 INTEGRATION TEST RESULTS")
    print("=" * 50)

    if result.returncode == 0:
        print("✅ Integration tests: PASSED")
    else:
        print("❌ Integration tests: FAILED")
        print(result.stdout)
        print(result.stderr)

    if mypy_result.returncode == 0:
        print("✅ Integration tests mypy: PASSED")
    else:
        print("❌ Integration tests mypy: FAILED")
        print(mypy_result.stdout)
        print(mypy_result.stderr)

    print(f"⏱️  Total duration: {duration:.2f}s")

    # Return success if all checks passed
    return result.returncode == 0 and mypy_result.returncode == 0


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
