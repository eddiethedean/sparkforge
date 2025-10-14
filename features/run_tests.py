#!/usr/bin/env python3
"""
SparkForge BDD Test Runner

This script provides a convenient way to run BDD tests with various options
and configurations.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def setup_environment():
    """Set up the test environment."""
    print("🔧 Setting up BDD test environment...")

    # Set up Java environment
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        java_home = '/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home'
        os.environ['JAVA_HOME'] = java_home
        print(f"✅ Set JAVA_HOME to {java_home}")

    # Set Spark local IP
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    print("✅ Set SPARK_LOCAL_IP to 127.0.0.1")

    # Create reports directory
    reports_dir = Path('features/reports')
    reports_dir.mkdir(exist_ok=True)
    print(f"✅ Created reports directory: {reports_dir}")


def install_dependencies():
    """Install BDD testing dependencies."""
    print("📦 Installing BDD testing dependencies...")

    try:
        subprocess.run([
            sys.executable, '-m', 'pip', 'install', '-r', 'features/requirements.txt'
        ], check=True, capture_output=True, text=True)
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        print(f"Error output: {e.stderr}")
        return False

    return True


def run_tests(feature=None, tags=None, format_type='pretty', verbose=False,
              junit=False, html=False, json=False, stop_on_failure=False):
    """Run the BDD tests."""
    print("🚀 Running BDD tests...")

    # Build behave command
    cmd = [sys.executable, '-m', 'behave']

    # Add feature file if specified
    if feature:
        cmd.append(f'features/{feature}')
    else:
        cmd.append('features/')

    # Add format
    cmd.extend(['--format', format_type])

    # Add tags if specified
    if tags:
        cmd.extend(['--tags', tags])

    # Add verbose flag
    if verbose:
        cmd.append('--verbose')

    # Add JUnit reporting
    if junit:
        cmd.extend(['--junit', '--junit-directory', 'features/reports/junit'])

    # Add HTML reporting
    if html:
        cmd.extend(['--format', 'html', '--outfile', 'features/reports/behave_report.html'])

    # Add JSON reporting
    if json:
        cmd.extend(['--format', 'json', '--outfile', 'features/reports/behave_report.json'])

    # Add stop on failure
    if stop_on_failure:
        cmd.append('--stop')

    # Add configuration file
    cmd.extend(['--config', 'features/behave.ini'])

    print(f"Running command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Failed to run tests: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='SparkForge BDD Test Runner')

    parser.add_argument('--feature', '-f', help='Run specific feature file')
    parser.add_argument('--tags', '-t', help='Run tests with specific tags')
    parser.add_argument('--format', choices=['pretty', 'plain', 'json', 'html'],
                       default='pretty', help='Output format')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--junit', action='store_true',
                       help='Generate JUnit XML reports')
    parser.add_argument('--html', action='store_true',
                       help='Generate HTML reports')
    parser.add_argument('--json', action='store_true',
                       help='Generate JSON reports')
    parser.add_argument('--stop', action='store_true',
                       help='Stop on first failure')
    parser.add_argument('--install-deps', action='store_true',
                       help='Install dependencies before running tests')
    parser.add_argument('--setup-only', action='store_true',
                       help='Only set up environment, do not run tests')

    args = parser.parse_args()

    print("🧪 SparkForge BDD Test Runner")
    print("=" * 50)

    # Set up environment
    setup_environment()

    # Install dependencies if requested
    if args.install_deps:
        if not install_dependencies():
            sys.exit(1)

    # Exit if setup only
    if args.setup_only:
        print("✅ Environment setup complete")
        return

    # Run tests
    success = run_tests(
        feature=args.feature,
        tags=args.tags,
        format_type=args.format,
        verbose=args.verbose,
        junit=args.junit,
        html=args.html,
        json=args.json,
        stop_on_failure=args.stop
    )

    if success:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()
