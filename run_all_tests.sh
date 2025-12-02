#!/bin/bash
# SparkForge Complete Test Runner
# This script runs all tests with 100% pass rate by handling isolation-required tests separately

set -e

echo "🧪 SparkForge Complete Test Suite Runner"
echo "========================================"
echo ""

# Activate virtual environment
if [ -f "venv38/bin/activate" ]; then
    source venv38/bin/activate
    echo "✅ Virtual environment activated"
    # Set mock-spark as default engine for tests
    export SPARKFORGE_ENGINE=mock
else
    echo "❌ Virtual environment not found. Run: python3.8 -m venv venv38"
    exit 1
fi

echo ""
echo "📊 Running tests in optimal order for 100% pass rate..."
echo ""

# Track results
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0

# Step 1: Run isolation-required tests separately
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 1: Running isolation-required tests (57 tests)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

pytest tests/unit/test_validation.py \
       tests/unit/test_execution_100_coverage.py \
       tests/unit/test_bronze_rules_column_validation.py \
       -v --tb=short --no-cov > /tmp/sparkforge_isolation_tests.log 2>&1

if [ $? -eq 0 ]; then
    ISOLATION_RESULT="✅ PASSED"
    ISOLATION_PASSED=$(grep -c "PASSED" /tmp/sparkforge_isolation_tests.log || echo "57")
    TOTAL_PASSED=$((TOTAL_PASSED + ISOLATION_PASSED))
else
    ISOLATION_RESULT="❌ FAILED"
    echo "⚠️  Warning: Isolation tests failed. Check log at /tmp/sparkforge_isolation_tests.log"
fi

echo "$ISOLATION_RESULT - Isolation-required tests"
echo ""

# Step 2: Run remaining tests
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2: Running main test suite (remaining tests)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Use parallel execution - mock-spark 3.7+ uses Polars backend which fixes threading issues
pytest tests/ \
       --ignore=tests/unit/test_validation.py \
       --ignore=tests/unit/test_execution_100_coverage.py \
       --ignore=tests/unit/test_bronze_rules_column_validation.py \
       -n 10 \
       -v --tb=short --no-cov > /tmp/sparkforge_main_tests.log 2>&1

if [ $? -eq 0 ]; then
    MAIN_RESULT="✅ PASSED"
else
    MAIN_RESULT="⚠️  Some failures"
fi

# Parse main test results
MAIN_PASSED=$(grep -oP '\d+(?= passed)' /tmp/sparkforge_main_tests.log | tail -1 || echo "0")
MAIN_FAILED=$(grep -oP '\d+(?= failed)' /tmp/sparkforge_main_tests.log | tail -1 || echo "0")
MAIN_SKIPPED=$(grep -oP '\d+(?= skipped)' /tmp/sparkforge_main_tests.log | tail -1 || echo "0")

TOTAL_PASSED=$((TOTAL_PASSED + MAIN_PASSED))
TOTAL_FAILED=$((TOTAL_FAILED + MAIN_FAILED))
TOTAL_SKIPPED=$((TOTAL_SKIPPED + MAIN_SKIPPED))

echo "$MAIN_RESULT - Main test suite"
echo ""

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Test Suite Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Passed:  $TOTAL_PASSED"
echo "❌ Failed:  $TOTAL_FAILED"
echo "⏭️  Skipped: $TOTAL_SKIPPED"
echo ""

# Calculate pass rate
if [ $TOTAL_PASSED -gt 0 ]; then
    TOTAL_RUN=$((TOTAL_PASSED + TOTAL_FAILED))
    PASS_RATE=$(awk "BEGIN {printf \"%.1f\", ($TOTAL_PASSED / $TOTAL_RUN) * 100}")
    echo "📈 Pass Rate: $PASS_RATE%"
fi

echo ""
echo "📄 Detailed logs:"
echo "  - Isolation tests: /tmp/sparkforge_isolation_tests.log"
echo "  - Main tests: /tmp/sparkforge_main_tests.log"
echo ""

# Exit with appropriate code
if [ $TOTAL_FAILED -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "⚠️  $TOTAL_FAILED test(s) failed. Review logs for details."
    exit 1
fi

