"""
Direct Comparison: sparkless vs PySpark - Shows the Actual Bug

This script runs the actual failing test in both modes to demonstrate
the bug clearly. The same code fails in sparkless but works in PySpark.
"""

import os
import subprocess
import sys

def run_test(mode, test_name):
    """Run a test in the specified mode."""
    print("=" * 80)
    print(f"Running test in {mode.upper()} mode")
    print("=" * 80)
    
    env = os.environ.copy()
    env['SPARK_MODE'] = mode
    
    cmd = [
        sys.executable, "-m", "pytest",
        test_name,
        "-v",
        "--tb=short",
        "-q"
    ]
    
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    # Extract key information
    if "FAILED" in result.stdout or result.returncode != 0:
        print("\n❌ TEST FAILED")
        # Look for the error message
        if "impression_date" in result.stdout:
            lines = result.stdout.split('\n')
            for i, line in enumerate(lines):
                if "impression_date" in line and ("attribute" in line or "Error" in line):
                    print(f"\n⚠️  ERROR FOUND:")
                    print(f"   {line}")
                    # Show context
                    for j in range(max(0, i-2), min(len(lines), i+3)):
                        if j != i:
                            print(f"   {lines[j]}")
    else:
        print("\n✅ TEST PASSED")
    
    return result.returncode == 0

def main():
    """Run the test in both modes."""
    test_name = "tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution"
    
    print("\n" + "=" * 80)
    print("DIRECT COMPARISON: sparkless vs PySpark")
    print("=" * 80)
    print("\nRunning the actual failing test in both modes to show the difference.\n")
    
    # Run in sparkless (mock mode)
    sparkless_passed = run_test("mock", test_name)
    
    print("\n" + "=" * 80)
    
    # Run in PySpark (real mode)
    pyspark_passed = run_test("real", test_name)
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nsparkless (Mock Mode): {'✅ PASSED' if sparkless_passed else '❌ FAILED'}")
    print(f"PySpark (Real Mode):   {'✅ PASSED' if pyspark_passed else '❌ FAILED'}")
    
    if not sparkless_passed and pyspark_passed:
        print("\n⚠️  BUG CONFIRMED:")
        print("   The same test fails in sparkless but passes in PySpark.")
        print("   This proves the bug is in sparkless, not in the test code.")
    elif sparkless_passed and pyspark_passed:
        print("\n✅ Both passed - bug may be fixed!")
    else:
        print("\n⚠️  Unexpected result - both failed or both passed differently")

if __name__ == "__main__":
    main()

