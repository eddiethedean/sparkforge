#!/usr/bin/env python3
"""
Test runner script that allows easy switching between mock and real Spark modes.

Usage:
    python run_tests.py                    # Run with mock Spark (default)
    python run_tests.py --real             # Run with real Spark
    python run_tests.py --mock             # Run with mock Spark (explicit)
    python run_tests.py --real unit/test_validation_standalone.py  # Run specific test with real Spark
    python run_tests.py --mock unit/test_validation_standalone.py  # Run specific test with mock Spark
"""

import argparse
import os
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Run tests with mock or real Spark")
    parser.add_argument(
        "--real", action="store_true", help="Use real Spark instead of mock"
    )
    parser.add_argument("--mock", action="store_true", help="Use mock Spark (default)")
    parser.add_argument(
        "test_path", nargs="*", help="Specific test file or directory to run"
    )

    # Parse known args to separate pytest arguments
    args, pytest_args = parser.parse_known_args()

    # Determine Spark mode
    if args.real:
        spark_mode = "real"
        print("🔧 Running tests with REAL Spark")
    else:
        spark_mode = "mock"
        print("🔧 Running tests with MOCK Spark")

    # Set environment variable
    os.environ["SPARK_MODE"] = spark_mode

    # Build pytest command
    cmd = [sys.executable, "-m", "pytest"]

    # Add test path if specified
    if args.test_path:
        cmd.extend(args.test_path)
    else:
        cmd.append("unit/")  # Default to unit tests

    # Add additional pytest arguments
    if pytest_args:
        cmd.extend(pytest_args)
    else:
        # Default pytest arguments
        cmd.extend(["-v", "--tb=short"])

    print(f"Running command: {' '.join(cmd)}")
    print(f"SPARK_MODE={spark_mode}")
    print("-" * 50)

    # Run the tests
    try:
        result = subprocess.run(cmd, check=False)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        print("\n❌ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
