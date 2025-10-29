#!/usr/bin/env python3
"""
Test runner for write_mode regression prevention tests.

This script runs the specific tests that prevent the write_mode bug from being reintroduced.
The bug was that incremental mode was incorrectly using "overwrite" instead of "append",
causing data loss in incremental pipelines.
"""

import os
import subprocess
import sys


def run_write_mode_tests():
    """Run all write_mode regression prevention tests."""
    print("🧪 Running Write Mode Regression Prevention Tests")
    print("=" * 60)

    # Change to the project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_dir)

    test_files = [
        "tests/unit/test_execution_write_mode.py",
        "tests/unit/test_pipeline_runner_write_mode.py",
        "tests/integration/test_write_mode_integration.py",
    ]

    all_passed = True

    for test_file in test_files:
        print(f"\n📋 Running tests in {test_file}")
        print("-" * 40)

        try:
            # Run the specific test file
            result = subprocess.run(
                [sys.executable, "-m", "pytest", test_file, "-v", "--tb=short"],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode == 0:
                print(f"✅ All tests in {test_file} PASSED")
            else:
                print(f"❌ Some tests in {test_file} FAILED")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                all_passed = False

        except subprocess.TimeoutExpired:
            print(f"⏰ Tests in {test_file} timed out")
            all_passed = False
        except Exception as e:
            print(f"💥 Error running tests in {test_file}: {e}")
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL WRITE MODE REGRESSION TESTS PASSED!")
        print("✅ The write_mode bug fix is working correctly")
        print("✅ Incremental mode correctly uses 'append'")
        print("✅ Initial mode correctly uses 'overwrite'")
        print("✅ No data loss issues detected")
        return 0
    else:
        print("🚨 SOME WRITE MODE REGRESSION TESTS FAILED!")
        print("❌ The write_mode bug may have been reintroduced")
        print("❌ Please check the test output above")
        print("❌ Incremental mode should use 'append', not 'overwrite'")
        return 1


def run_critical_regression_test():
    """Run the most critical regression test."""
    print("🔥 Running Critical Write Mode Regression Test")
    print("=" * 60)

    # Change to the project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_dir)

    try:
        # Run the most critical test
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/unit/test_execution_write_mode.py::TestWriteModeRegression::test_incremental_mode_never_uses_overwrite",
                "-v",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            print("✅ CRITICAL REGRESSION TEST PASSED!")
            print("✅ Incremental mode correctly uses 'append' (no data loss)")
            return 0
        else:
            print("🚨 CRITICAL REGRESSION TEST FAILED!")
            print("❌ Incremental mode is using 'overwrite' - DATA LOSS RISK!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return 1

    except subprocess.TimeoutExpired:
        print("⏰ Critical regression test timed out")
        return 1
    except Exception as e:
        print(f"💥 Error running critical regression test: {e}")
        return 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--critical":
        exit_code = run_critical_regression_test()
    else:
        exit_code = run_write_mode_tests()

    sys.exit(exit_code)
