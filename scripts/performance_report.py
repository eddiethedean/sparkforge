#!/usr/bin/env python3
"""
Performance reporting script for SparkForge.

This script generates performance reports and can be integrated
into CI/CD pipelines for performance monitoring.
"""

import argparse
import json
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tests.performance.performance_monitor import performance_monitor  # noqa: E402


def generate_performance_report(output_file: str = "performance_report.json") -> None:
    """Generate a comprehensive performance report."""
    summary = performance_monitor.get_performance_summary()

    # Add regression analysis
    regression_analysis = {}
    for result in performance_monitor.results:
        if result.success:
            regression_info = performance_monitor.check_regression(result.function_name)
            regression_analysis[result.function_name] = regression_info

    report = {
        "summary": summary,
        "regression_analysis": regression_analysis,
        "baselines": {name: result.__dict__ for name, result in performance_monitor.baselines.items()}
    }

    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"Performance report generated: {output_file}")
    return report


def print_performance_summary() -> None:
    """Print a human-readable performance summary."""
    summary = performance_monitor.get_performance_summary()

    print("\n" + "="*60)
    print("SPARKFORGE PERFORMANCE TEST SUMMARY")
    print("="*60)

    if "message" in summary:
        print(f"Status: {summary['message']}")
        return

    print(f"Total Tests Run: {summary.get('total_tests', 0)}")
    print(f"Successful Tests: {summary.get('successful_tests', 0)}")
    print(f"Failed Tests: {summary.get('failed_tests', 0)}")
    print(f"Functions Tested: {summary.get('functions_tested', 0)}")
    print(f"Total Execution Time: {summary.get('total_execution_time', 0):.2f} seconds")
    print(f"Average Execution Time: {summary.get('avg_execution_time', 0):.4f} seconds")
    print(f"Total Memory Used: {summary.get('total_memory_used', 0):.2f} MB")

    print("\n" + "-"*60)
    print("PERFORMANCE RESULTS BY FUNCTION")
    print("-"*60)

    # Group results by function
    function_results = {}
    for result in performance_monitor.results:
        if result.function_name not in function_results:
            function_results[result.function_name] = []
        function_results[result.function_name].append(result)

    for function_name, results in function_results.items():
        successful_results = [r for r in results if r.success]
        if successful_results:
            avg_time = sum(r.avg_time_per_iteration for r in successful_results) / len(successful_results)
            avg_throughput = sum(r.throughput for r in successful_results) / len(successful_results)
            avg_memory = sum(r.memory_usage_mb for r in successful_results) / len(successful_results)

            print(f"\n{function_name}:")
            print(f"  Avg Time/Iteration: {avg_time:.4f} ms")
            print(f"  Avg Throughput: {avg_throughput:.0f} ops/sec")
            print(f"  Avg Memory Usage: {avg_memory:.2f} MB")
            print(f"  Total Iterations: {sum(r.iterations for r in successful_results)}")

            # Check for regressions
            regression = performance_monitor.check_regression(function_name)
            if regression["status"] == "regression_detected":
                print("  ⚠️  REGRESSION DETECTED!")
                print(f"     Time change: {regression['time_change_percent']:.1f}%")
                print(f"     Memory change: {regression['memory_change_percent']:.1f}%")
            elif regression["status"] == "ok":
                print("  ✅ Performance OK")
            elif regression["status"] == "no_baseline":
                print("  📊 No baseline (new test)")

    print("\n" + "-"*60)
    print("BASELINE FUNCTIONS")
    print("-"*60)

    for function_name, baseline in performance_monitor.baselines.items():
        print(f"{function_name}:")
        print(f"  Baseline Time: {baseline.avg_time_per_iteration:.4f} ms")
        print(f"  Baseline Memory: {baseline.memory_usage_mb:.2f} MB")
        print(f"  Baseline Throughput: {baseline.throughput:.0f} ops/sec")


def check_regressions() -> bool:
    """Check for performance regressions and return True if any found."""
    regressions_found = False

    print("\n" + "="*60)
    print("PERFORMANCE REGRESSION CHECK")
    print("="*60)

    for result in performance_monitor.results:
        if result.success:
            regression = performance_monitor.check_regression(result.function_name)
            if regression["status"] == "regression_detected":
                regressions_found = True
                print(f"\n❌ REGRESSION in {result.function_name}:")
                print(f"   Time change: {regression['time_change_percent']:.1f}%")
                print(f"   Memory change: {regression['memory_change_percent']:.1f}%")
                print(f"   Current: {regression['current'].avg_time_per_iteration:.4f} ms")
                print(f"   Baseline: {regression['baseline'].avg_time_per_iteration:.4f} ms")
            elif regression["status"] == "ok":
                print(f"✅ {result.function_name}: OK")

    if not regressions_found:
        print("\n🎉 No performance regressions detected!")

    return regressions_found


def update_baselines() -> None:
    """Update performance baselines with current results."""
    updated_functions = []

    for result in performance_monitor.results:
        if result.success:
            performance_monitor.update_baseline(result.function_name)
            if result.function_name not in updated_functions:
                updated_functions.append(result.function_name)

    print(f"\n📊 Updated baselines for {len(updated_functions)} functions:")
    for func_name in updated_functions:
        print(f"  - {func_name}")


def main():
    """Main entry point for the performance reporting script."""
    parser = argparse.ArgumentParser(description="Generate SparkForge performance reports")
    parser.add_argument("--report", "-r", action="store_true", help="Generate performance report")
    parser.add_argument("--summary", "-s", action="store_true", help="Print performance summary")
    parser.add_argument("--check-regressions", "-c", action="store_true", help="Check for regressions")
    parser.add_argument("--update-baselines", "-u", action="store_true", help="Update baselines")
    parser.add_argument("--output", "-o", default="performance_report.json", help="Output file for report")
    parser.add_argument("--all", "-a", action="store_true", help="Run all operations")

    args = parser.parse_args()

    if args.all or not any([args.report, args.summary, args.check_regressions, args.update_baselines]):
        # Default behavior: run all operations
        args.report = True
        args.summary = True
        args.check_regressions = True

    if args.summary:
        print_performance_summary()

    if args.check_regressions:
        regressions_found = check_regressions()
        if regressions_found:
            sys.exit(1)  # Exit with error code if regressions found

    if args.report:
        generate_performance_report(args.output)

    if args.update_baselines:
        update_baselines()


if __name__ == "__main__":
    main()
