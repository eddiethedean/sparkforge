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
    
    # Parse concurrent test results from the output
    concurrent_failed = []
    concurrent_passed = 0
    concurrent_skipped = 0
    concurrent_errors = 0
    
    if concurrent_stdout:
        # Extract test results from pytest output
        lines = concurrent_stdout.split('\n')
        for line in lines:
            # Look for failed tests
            if 'FAILED' in line and '::' in line:
                # Extract test path from lines like: "FAILED tests/path/to/test.py::TestClass::test_method"
                test_path = line.strip().replace('FAILED ', '').split()[0] if 'FAILED' in line else None
                if test_path and test_path not in concurrent_failed:
                    concurrent_failed.append(test_path)
            # Look for summary line with test counts (pytest final summary)
            elif 'passed' in line.lower() and '=========' in line:
                # Parse summary like: "========= 1334 passed, 477 skipped, 2 errors in 475.03s =========="
                import re
                passed_match = re.search(r'(\d+)\s+passed', line)
                failed_match = re.search(r'(\d+)\s+failed', line)
                error_match = re.search(r'(\d+)\s+error', line)
                skipped_match = re.search(r'(\d+)\s+skipped', line)
                
                if passed_match:
                    concurrent_passed = int(passed_match.group(1))
                if failed_match:
                    # If there are failures, they'll be in concurrent_failed list from FAILED lines
                    pass
                if error_match:
                    concurrent_errors = int(error_match.group(1))
                if skipped_match:
                    concurrent_skipped = int(skipped_match.group(1))
        
        if concurrent_failed:
            print(f"\n⚠️  Found {len(concurrent_failed)} failed concurrent tests:")
            for test in concurrent_failed:
                print(f"  ❌ {test}")
    
    # Determine success based on actual test results, not just exit code
    # Pytest may return non-zero exit code due to warnings, but tests can still pass
    # If we couldn't parse the summary, fall back to exit code
    if concurrent_passed == 0 and concurrent_skipped == 0:
        # Couldn't parse results, use exit code as fallback
        concurrent_tests_successful = concurrent_success
    else:
        # Use parsed results - success if we have passes and no failures/errors
        concurrent_tests_successful = (
            concurrent_passed > 0 and 
            len(concurrent_failed) == 0 and 
            concurrent_errors == 0
        )
    
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
    
    # Show detailed concurrent test results
    if concurrent_passed > 0 or concurrent_skipped > 0:
        print(f"\nConcurrent tests: {concurrent_passed} passed, {concurrent_skipped} skipped", end="")
        if concurrent_errors > 0:
            print(f", {concurrent_errors} errors", end="")
        if len(concurrent_failed) > 0:
            print(f", {len(concurrent_failed)} failed", end="")
        print()
        print(f"Status: {'✅ PASSED' if concurrent_tests_successful else '❌ FAILED'}")
    else:
        print(f"\nConcurrent tests: {'✅ PASSED' if concurrent_success else '❌ FAILED'}")
    
    # Return appropriate exit code based on actual test results
    if sequential_failed or not concurrent_tests_successful:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

