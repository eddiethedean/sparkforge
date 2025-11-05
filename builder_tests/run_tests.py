#!/usr/bin/env python3
"""
Test runner for builder tests.

This script provides an easy way to run the builder tests with proper configuration
and reporting.
"""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Run the builder tests with proper configuration."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Change to project root directory
    os.chdir(project_root)

    # Build the pytest command
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "builder_tests/",
        "-v",
        "--tb=short",
        "--strict-markers",
        "--disable-warnings",
    ]

    # Add coverage if requested
    if "--coverage" in sys.argv:
        cmd.extend(
            [
                "--cov=pipeline_builder",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov_builder",
            ]
        )
        sys.argv.remove("--coverage")

    # Add any additional arguments
    if len(sys.argv) > 1:
        cmd.extend(sys.argv[1:])

    print(f"Running command: {' '.join(cmd)}")
    print("=" * 80)

    # Run the tests
    result = subprocess.run(cmd)

    print("=" * 80)
    if result.returncode == 0:
        print("✅ All builder tests passed!")
    else:
        print("❌ Some builder tests failed!")

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
