#!/usr/bin/env python3
"""
Test runner script that uses real Spark mode.

This script runs tests with real Spark and Delta Lake for integration testing.
Requires Spark and Delta Lake to be installed.
"""

import os
import subprocess
import sys


def main():
    """Run tests with real Spark mode."""
    # Set real Spark mode
    os.environ["SPARK_MODE"] = "real"

    # Get command line arguments
    args = sys.argv[1:] if len(sys.argv) > 1 else []

    # Default test command
    cmd = ["python", "-m", "pytest"] + args

    print("🔧 Running tests with Real Spark (requires Spark installation)")
    print(f"Command: {' '.join(cmd)}")
    print()

    # Run the tests
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
