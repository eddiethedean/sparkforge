#!/usr/bin/env python3
"""
Store test failures and rerun only failed tests.
Uses pytest's cache mechanism for reliable tracking.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import List


def get_cache_dir() -> Path:
    """Get pytest cache directory."""
    return Path(".pytest_cache/v/cache")


def store_failures(mode: str = "pyspark") -> List[str]:
    """Run tests and store failures."""
    print(f"🧪 Running tests in {mode} mode and storing failures...")
    print("=" * 70)

    # Set environment variables
    env = os.environ.copy()
    env["SPARKFORGE_ENGINE"] = mode
    env["SPARK_MODE"] = "real"
    if mode == "pyspark":
        env["SPARKFORGE_BASIC_SPARK"] = "1"

    # Run pytest with --lf flag to track failures
    # We also use --tb=no -q for faster output
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/",
        "--tb=no",
        "-q",
        "-n",
        "10",  # Parallel execution
        "--cache-clear",  # Start fresh
    ]

    print(f"Command: {' '.join(cmd)}")
    print("-" * 70)

    # Run tests
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
    )

    output = result.stdout + result.stderr

    # Extract failed test names from output
    failed_tests = []
    for line in output.splitlines():
        if "FAILED" in line or "ERROR" in line:
            # Extract test path (format: tests/path/to/test.py::TestClass::test_name FAILED)
            parts = line.strip().split()
            if parts:
                test_path = parts[0]
                if test_path.startswith("tests/") and (
                    "FAILED" in line or "ERROR" in line
                ):
                    failed_tests.append(test_path)

    # Also use pytest's cache to get lastfailed
    failed_tests = list(set(failed_tests))  # Deduplicate
    failed_tests.sort()

    # Write to file
    failed_tests_file = f"failed_tests_{mode}.txt"
    with open(failed_tests_file, "w") as f:
        f.write(f"# Failed tests from {mode} mode\n")
        f.write(
            f"# Generated on: {__import__('datetime').datetime.now().isoformat()}\n"
        )
        f.write(f"# Total failures: {len(failed_tests)}\n\n")
        for test in failed_tests:
            f.write(f"{test}\n")

    # Parse summary
    passed = 0
    failed = 0
    skipped = 0

    for line in output.splitlines():
        if "passed" in line.lower():
            import re

            match = re.search(r"(\d+)\s+passed", line)
            if match:
                passed = int(match.group(1))
        if "failed" in line.lower() and "passed" not in line:
            import re

            match = re.search(r"(\d+)\s+failed", line)
            if match:
                failed = int(match.group(1))
        if "skipped" in line.lower():
            import re

            match = re.search(r"(\d+)\s+skipped", line)
            if match:
                skipped = int(match.group(1))

    print("\n" + "=" * 70)
    print("📊 Test Summary:")
    print(f"  ✅ Passed:  {passed}")
    print(f"  ❌ Failed:  {failed}")
    print(f"  ⏭️  Skipped: {skipped}")
    print(f"\n✅ Stored {len(failed_tests)} failed tests to {failed_tests_file}")
    print("=" * 70)

    return failed_tests


def rerun_failures(mode: str = "pyspark") -> bool:
    """Rerun only stored failures."""
    failed_tests_file = f"failed_tests_{mode}.txt"

    if not os.path.exists(failed_tests_file):
        print(f"❌ No stored failures found in {failed_tests_file}")
        print("   Run 'store' first to capture failures")
        return False

    # Read failed tests
    failed_tests = []
    with open(failed_tests_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                failed_tests.append(line)

    if not failed_tests:
        print("✅ No failed tests to rerun!")
        return True

    print(f"🔄 Rerunning {len(failed_tests)} failed tests in {mode} mode...")
    print("=" * 70)

    # Set environment variables
    env = os.environ.copy()
    env["SPARKFORGE_ENGINE"] = mode
    env["SPARK_MODE"] = "real"
    if mode == "pyspark":
        env["SPARKFORGE_BASIC_SPARK"] = "1"

    # Run only failed tests
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *failed_tests,
        "-v",
        "--tb=short",
        "-n",
        "5",  # Fewer workers for rerun
    ]

    print(f"Command: {' '.join(cmd[:5])} ... ({len(failed_tests)} tests)")
    print("-" * 70)

    result = subprocess.run(cmd, env=env)

    print("\n" + "=" * 70)
    if result.returncode == 0:
        print("🎉 All previously failed tests now pass!")
        print(f"   You can run 'clear {mode}' to reset the failure list")
        return True
    else:
        print(
            "⚠️  Some tests still failing. Run 'store' again to update the failure list"
        )
        return False


def clear_failures(mode: str = "pyspark"):
    """Clear stored failures."""
    failed_tests_file = f"failed_tests_{mode}.txt"

    if os.path.exists(failed_tests_file):
        os.remove(failed_tests_file)
        print(f"✅ Cleared {failed_tests_file}")
    else:
        print(f"ℹ️  No file to clear: {failed_tests_file}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/store_and_rerun_failed_tests.py {store|rerun|clear} [mode]"
        )
        print("")
        print("Commands:")
        print("  store <mode>  - Run all tests and store failures (default: pyspark)")
        print(
            "  rerun <mode>  - Rerun only previously stored failures (default: pyspark)"
        )
        print("  clear <mode>  - Clear stored failure list (default: pyspark)")
        print("")
        print("Modes:")
        print("  pyspark      - PySpark mode (default)")
        print("  mock         - Mock Spark mode")
        sys.exit(1)

    command = sys.argv[1].lower()
    mode = sys.argv[2].lower() if len(sys.argv) > 2 else "pyspark"

    if command == "store":
        store_failures(mode)
    elif command == "rerun":
        rerun_failures(mode)
    elif command == "clear":
        clear_failures(mode)
    else:
        print(f"❌ Unknown command: {command}")
        print("   Use: store, rerun, or clear")
        sys.exit(1)


if __name__ == "__main__":
    main()
