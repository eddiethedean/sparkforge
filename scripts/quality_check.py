#!/usr/bin/env python3

"""
Quality check script for SparkForge.

This script runs all quality checks and provides a comprehensive report.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


class QualityChecker:
    """Comprehensive quality checker for SparkForge."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.results: Dict[str, Any] = {}

    def run_command(self, cmd: List[str], description: str) -> Tuple[bool, str]:
        """Run a command and return success status and output."""
        try:
            print(f"🔍 {description}...")
            result = subprocess.run(
                cmd, cwd=self.project_root, capture_output=True, text=True, check=False
            )
            success = result.returncode == 0
            output = result.stdout + result.stderr
            return success, output
        except Exception as e:
            print(f"❌ Error running {description}: {e}")
            return False, str(e)

    def check_black(self) -> bool:
        """Check code formatting with Black."""
        success, output = self.run_command(
            ["python", "-m", "black", "--check", "--diff", "sparkforge/", "tests/"],
            "Checking code formatting with Black",
        )
        self.results["black"] = {"success": success, "output": output}
        return success

    def check_isort(self) -> bool:
        """Check import sorting with isort."""
        success, output = self.run_command(
            [
                "python",
                "-m",
                "isort",
                "--check-only",
                "--diff",
                "sparkforge/",
                "tests/",
            ],
            "Checking import sorting with isort",
        )
        self.results["isort"] = {"success": success, "output": output}
        return success

    def check_ruff(self) -> bool:
        """Check code quality with Ruff."""
        success, output = self.run_command(
            ["python", "-m", "ruff", "check", "sparkforge/", "tests/"],
            "Checking code quality with Ruff",
        )
        self.results["ruff"] = {"success": success, "output": output}
        return success

    def check_mypy(self) -> bool:
        """Check type annotations with mypy."""
        success, output = self.run_command(
            ["python", "-m", "mypy", "sparkforge/"],
            "Checking type annotations with mypy",
        )
        self.results["mypy"] = {"success": success, "output": output}
        return success

    def check_pylint(self) -> bool:
        """Check code quality with pylint."""
        success, output = self.run_command(
            ["python", "-m", "pylint", "sparkforge/"],
            "Checking code quality with pylint",
        )
        self.results["pylint"] = {"success": success, "output": output}
        return success

    def check_bandit(self) -> bool:
        """Check security issues with bandit."""
        success, output = self.run_command(
            ["python", "-m", "bandit", "-r", "sparkforge/", "-f", "json"],
            "Checking security issues with bandit",
        )
        self.results["bandit"] = {"success": success, "output": output}
        return success

    def check_tests(self) -> bool:
        """Run tests with pytest."""
        success, output = self.run_command(
            ["python", "-m", "pytest", "tests/", "-v", "--tb=short"],
            "Running tests with pytest",
        )
        self.results["tests"] = {"success": success, "output": output}
        return success

    def run_all_checks(self) -> bool:
        """Run all quality checks."""
        print("🚀 Starting comprehensive quality checks for SparkForge")
        print("=" * 60)

        checks = [
            self.check_black,
            self.check_isort,
            self.check_ruff,
            self.check_mypy,
            self.check_pylint,
            self.check_bandit,
            self.check_tests,
        ]

        results = []
        for check in checks:
            try:
                success = check()
                results.append(success)
            except Exception as e:
                print(f"❌ Error in {check.__name__}: {e}")
                results.append(False)

        # Print summary
        print("\n" + "=" * 60)
        print("📊 QUALITY CHECK SUMMARY")
        print("=" * 60)

        passed = sum(results)
        total = len(results)

        for i, check in enumerate(checks):
            status = "✅ PASS" if results[i] else "❌ FAIL"
            print(f"{status} {check.__name__.replace('check_', '').upper()}")

        print(f"\n🎯 Overall: {passed}/{total} checks passed")

        if passed == total:
            print("🎉 All quality checks passed! Your code is ready for production.")
            return True
        else:
            print("⚠️  Some quality checks failed. Please fix the issues above.")
            return False

    def generate_report(self) -> None:
        """Generate a detailed quality report."""
        report_file = self.project_root / "quality_report.json"
        with open(report_file, "w") as f:
            json.dump(self.results, f, indent=2)
        print(f"📄 Detailed report saved to: {report_file}")


def main():
    """Main function."""
    checker = QualityChecker()

    try:
        success = checker.run_all_checks()
        checker.generate_report()

        if not success:
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⏹️  Quality checks interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
