#!/usr/bin/env python3
"""
Optimized test runner for Sparkforge with different performance levels.
"""

import argparse
import subprocess
import sys
import time


def run_command(cmd, description):
    """Run a command and return timing info."""
    print(f"\n🚀 {description}")
    print(f"Command: {' '.join(cmd)}")
    start_time = time.time()

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = time.time() - start_time

        if result.returncode == 0:
            print(f"✅ {description} - PASSED ({duration:.1f}s)")
            return True, duration
        else:
            print(f"❌ {description} - FAILED ({duration:.1f}s)")
            print(f"Error: {result.stderr}")
            return False, duration

    except Exception as e:
        duration = time.time() - start_time
        print(f"💥 {description} - ERROR ({duration:.1f}s): {e}")
        return False, duration


def main():
    parser = argparse.ArgumentParser(
        description="Run Sparkforge tests with different optimization levels"
    )
    parser.add_argument(
        "--level",
        choices=["fast", "medium", "full"],
        default="fast",
        help="Test level: fast (exclude slow), medium (exclude slow + performance), full (all tests)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run tests in parallel with pytest-xdist",
    )
    parser.add_argument(
        "--workers", type=int, default=2, help="Number of parallel workers (default: 2)"
    )

    args = parser.parse_args()

    # Base command
    base_cmd = ["python", "-m", "pytest", "--tb=short", "-q"]

    # Add markers based on level
    if args.level == "fast":
        base_cmd.extend(["-m", "not slow"])
        description = "Fast tests (excluding slow tests)"
    elif args.level == "medium":
        base_cmd.extend(["-m", "not slow and not performance"])
        description = "Medium tests (excluding slow and performance tests)"
    else:  # full
        # Use fast tests by default for best performance
        base_cmd.extend(["-m", "not slow"])
        description = "Fast tests (excluding slow tests) - fastest option"

    # Add parallel execution if requested
    if args.parallel:
        base_cmd.extend(["-n", str(args.workers)])
        # Add worker-specific environment variables and settings
        base_cmd.extend(["--dist", "worksteal"])
        # Exclude problematic tests that don't work well in parallel
        base_cmd.extend(
            [
                "-k",
                "not (test_dataframe_access_pytest or test_delta_lake_comprehensive or test_table_operations or test_unified_dependency_scenarios or test_unified_execution or test_source_silvers_none)",
            ]
        )
        description += (
            f" (parallel with {args.workers} workers, excluding problematic tests)"
        )

    # Run the tests
    success, duration = run_command(base_cmd, description)

    if success:
        print(f"\n🎉 All tests passed in {duration:.1f}s!")

        # Show optimization suggestions
        print("\n💡 Performance Tips:")
        print("  • Use --level fast for quick development feedback")
        print("  • Use --level medium for CI/CD pipelines")
        print("  • Use --parallel for faster execution on multi-core systems")
        print("  • Use --workers 4 for 4-core systems, --workers 8 for 8-core systems")

        # Show different run options
        print("\n🔧 Different test run options:")
        print("  python run_tests_optimized.py --level fast")
        print("  python run_tests_optimized.py --level medium --parallel --workers 4")
        print("  python run_tests_optimized.py --level full --parallel --workers 8")

    else:
        print(f"\n💥 Tests failed in {duration:.1f}s")
        sys.exit(1)


if __name__ == "__main__":
    main()
