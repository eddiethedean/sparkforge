#!/usr/bin/env python3
"""
Test runner script that explicitly uses mock Spark mode.

This script ensures that all tests run with mock Spark by default,
providing a consistent testing environment without requiring Spark installation.
"""

import os
import subprocess
import sys


def main():
    """Run tests with mock Spark mode."""
    # Ensure mock mode is set
    os.environ["SPARK_MODE"] = "mock"

    # Get command line arguments
    args = sys.argv[1:] if len(sys.argv) > 1 else []

    # Default test command
    cmd = ["python", "-m", "pytest"] + args

    print("🔧 Running tests with Mock Spark (default mode)")
    print(f"Command: {' '.join(cmd)}")
    print()

    # Run the tests
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
