#!/usr/bin/env python3
"""
Smart parallel test runner that runs problematic tests sequentially
and the rest concurrently to avoid race conditions.

This script uses pytest markers to identify tests:
1. Runs tests marked with @pytest.mark.sequential one at a time
2. Runs all other tests concurrently
3. Ensures sequential and concurrent tests don't overlap
"""

import os
import subprocess
import sys
from pathlib import Path


def collect_sequential_tests(spark_mode):
    """Collect all tests marked with @pytest.mark.sequential."""
    # Use pytest --collect-only to get sequential tests
    cmd = [
        sys.executable, "-m", "pytest",
        "--collect-only",
        "-m", "sequential",
        "-q",
    ]
    
    env = os.environ.copy()
    env["SPARK_MODE"] = spark_mode
    
    result = subprocess.run(
        cmd,
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        env=env
    )
    
    sequential_tests = []
    if result.returncode == 0 or result.returncode == 2:  # 2 = errors but tests collected
        # Parse output to extract test paths
        for line in result.stdout.split('\n'):
            line = line.strip()
            # Look for test paths (format: path::Class::test_method)
            # Skip error messages and other non-test lines
            if '::' in line and line.startswith('tests/') and '<' not in line:
                # Remove any trailing spaces or extra info
                test_path = line.split()[0] if line.split() else line
                if test_path not in sequential_tests:
                    sequential_tests.append(test_path)
    
    return sequential_tests


def run_command(cmd, description, capture_output=False):
    """Run a command and return success status and output."""
    print(f"\n{'='*80}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*80}\n")
    
    if capture_output:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    else:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
        return result.returncode == 0, None, None


def main():
    """Main execution function."""
    # Get script arguments or use defaults
    # Default to 10 workers for parallel runs
    num_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    spark_mode = os.environ.get("SPARK_MODE", "real")
    
    print(f"Smart Parallel Test Runner")
    print(f"Mode: {spark_mode}")
    print(f"Concurrent workers: {num_workers}")
    
    # Ensure SPARK_MODE is set
    env = os.environ.copy()
    env["SPARK_MODE"] = spark_mode
    
    # Collect tests marked with @pytest.mark.sequential
    print("\nCollecting tests marked with @pytest.mark.sequential...")
    sequential_tests = collect_sequential_tests(spark_mode)
    
    if not sequential_tests:
        print("⚠️  No tests found with @pytest.mark.sequential marker")
        print("   All tests will run concurrently")
    else:
        print(f"✅ Found {len(sequential_tests)} tests marked for sequential execution")
    
    # Step 1: Run sequential tests (one at a time)
    print(f"\n{'='*80}")
    print("STEP 1: Running sequential tests (marked with @pytest.mark.sequential)")
    print(f"{'='*80}")
    
    sequential_failed = []
    for i, test in enumerate(sequential_tests, 1):
        print(f"\n[{i}/{len(sequential_tests)}] Running: {test}")
        cmd = [
            sys.executable, "-m", "pytest",
            test,
            "-v",
            "--tb=short",
        ]
        success = run_command(cmd, f"Sequential test {i}/{len(sequential_tests)}: {test}")
        if not success:
            sequential_failed.append(test)
            print(f"❌ FAILED: {test}")
        else:
            print(f"✅ PASSED: {test}")
    
    # Step 2: Run all other tests concurrently (excluding sequential ones)
    print(f"\n{'='*80}")
    print("STEP 2: Running all other tests concurrently (excluding sequential)")
    print(f"{'='*80}")
    
    cmd = [
        sys.executable, "-m", "pytest",
        "-n", str(num_workers),
        "-v",
        "--tb=line",
        "-m", "not sequential",  # Exclude sequential tests using marker
    ]
    
    if sequential_tests:
        print(f"Excluding {len(sequential_tests)} sequential tests from concurrent run")
    
    # Run concurrent tests and capture output to parse failures
    concurrent_success, concurrent_stdout, concurrent_stderr = run_command(
        cmd, "Concurrent tests (excluding problematic ones)", capture_output=True
    )
    
    # Parse concurrent test failures from the output
    concurrent_failed = []
    if not concurrent_success and concurrent_stdout:
        # Extract failed test names from pytest output
        lines = concurrent_stdout.split('\n')
        for line in lines:
            if 'FAILED' in line and '::' in line:
                # Extract test path from lines like: "FAILED tests/path/to/test.py::TestClass::test_method"
                test_path = line.strip().replace('FAILED ', '').split()[0] if 'FAILED' in line else None
                if test_path and test_path not in concurrent_failed:
                    concurrent_failed.append(test_path)
        
        if concurrent_failed:
            print(f"\n⚠️  Found {len(concurrent_failed)} failed concurrent tests:")
            for test in concurrent_failed:
                print(f"  ❌ {test}")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    if sequential_tests:
        print(f"Sequential tests: {len(sequential_tests) - len(sequential_failed)}/{len(sequential_tests)} passed")
    if sequential_failed:
        print(f"\nFailed sequential tests:")
        for test in sequential_failed:
            print(f"  ❌ {test}")
    
    print(f"\nConcurrent tests: {'✅ PASSED' if concurrent_success else '❌ FAILED'}")
    
    # Return appropriate exit code
    if sequential_failed or not concurrent_success:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

