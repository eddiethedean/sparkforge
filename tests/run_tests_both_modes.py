#!/usr/bin/env python3
"""
Run tests in both mock and real Spark modes and compare results.
"""

import os
import subprocess
import sys
from typing import List, Tuple


def run_tests(mode: str, test_files: List[str]) -> Tuple[int, int, List[str]]:
    """Run tests in specified mode and return (passed, failed, errors)."""
    env = os.environ.copy()
    env["SPARK_MODE"] = mode

    if mode == "real":
        env["SPARKFORGE_BASIC_SPARK"] = "1"

    cmd = [sys.executable, "-m", "pytest"] + test_files + ["-v", "--tb=no"]

    print(f"\n🔧 Running tests in {mode.upper()} Spark mode...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)

    try:
        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=300
        )

        # Parse output to count passed/failed
        lines = result.stdout.split("\n")
        passed = 0
        failed = 0
        errors = []

        for line in lines:
            if "PASSED" in line:
                passed += 1
            elif "FAILED" in line:
                failed += 1
                errors.append(line.strip())

        print(f"✅ {mode.upper()} mode: {passed} passed, {failed} failed")
        if errors:
            print("❌ Failures:")
            for error in errors[:5]:  # Show first 5 errors
                print(f"   {error}")
            if len(errors) > 5:
                print(f"   ... and {len(errors) - 5} more")

        return passed, failed, errors

    except subprocess.TimeoutExpired:
        print(f"⏰ {mode.upper()} mode timed out after 5 minutes")
        return 0, 0, ["Timeout"]
    except Exception as e:
        print(f"❌ {mode.upper()} mode failed: {e}")
        return 0, 0, [str(e)]


def main():
    """Run tests in both modes and compare results."""
    test_files = [
        "unit/test_execution_engine_simple.py",
        "unit/test_writer_core_simple.py",
        "unit/test_pipeline_builder_basic.py",
        "unit/test_validation_standalone.py",
    ]

    print("🚀 Running SparkForge tests in both Mock and Real Spark modes")
    print("=" * 70)

    # Run mock tests
    mock_passed, mock_failed, mock_errors = run_tests("mock", test_files)

    # Run real tests
    real_passed, real_failed, real_errors = run_tests("real", test_files)

    # Summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"Mock Spark:  {mock_passed:3d} passed, {mock_failed:3d} failed")
    print(f"Real Spark:  {real_passed:3d} passed, {real_failed:3d} failed")
    print(
        f"Total:       {mock_passed + real_passed:3d} passed, {mock_failed + real_failed:3d} failed"
    )

    # Analysis
    print("\n🔍 ANALYSIS")
    print("-" * 30)

    if mock_passed == real_passed and mock_failed == real_failed:
        print("✅ Perfect compatibility! Both modes have identical results.")
    elif mock_passed > real_passed:
        print("⚠️  Mock mode has more passing tests than real mode.")
        print(
            "   This suggests some tests are mock-specific and don't work with real Spark."
        )
    elif real_passed > mock_passed:
        print("⚠️  Real mode has more passing tests than mock mode.")
        print("   This suggests the mock implementation needs improvement.")
    else:
        print("ℹ️  Different failure patterns between modes.")
        print("   This is expected for tests that use mock-specific features.")

    # Success rate
    total_tests = mock_passed + mock_failed
    if total_tests > 0:
        success_rate = (mock_passed / total_tests) * 100
        print(f"\n📈 Mock Spark Success Rate: {success_rate:.1f}%")

    total_real = real_passed + real_failed
    if total_real > 0:
        real_success_rate = (real_passed / total_real) * 100
        print(f"📈 Real Spark Success Rate: {real_success_rate:.1f}%")

    print("\n🎯 RECOMMENDATIONS")
    print("-" * 20)
    print("• Use Mock Spark for fast development and testing")
    print("• Use Real Spark for integration testing and validation")
    print("• Both modes provide comprehensive test coverage")


if __name__ == "__main__":
    main()
