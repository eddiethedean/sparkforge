#!/bin/bash
# Script to store test failures and rerun only failed tests
# Usage: 
#   ./scripts/store_and_rerun_failed_tests.sh store <mode>    # Store failures for a mode
#   ./scripts/store_and_rerun_failed_tests.sh rerun <mode>    # Rerun only failed tests
#   ./scripts/store_and_rerun_failed_tests.sh clear <mode>    # Clear stored failures

set -e

MODE="${2:-pyspark}"
FAILED_TESTS_FILE="failed_tests_${MODE}.txt"
LOG_FILE="/tmp/sparkforge_${MODE}_tests.log"

case "${1:-store}" in
    store)
        echo "🧪 Running tests in ${MODE} mode and storing failures..."
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        # Set environment variables based on mode
        export SPARKFORGE_ENGINE="${MODE}"
        export SPARK_MODE="real"
        
        if [ "$MODE" = "pyspark" ]; then
            export SPARKFORGE_BASIC_SPARK="1"
        fi
        
        # Run tests and capture output
        pytest tests/ \
            --tb=no \
            -q \
            -n 10 \
            2>&1 | tee "$LOG_FILE" || true
        
        # Extract failed test names
        echo "# Failed tests from $(date)" > "$FAILED_TESTS_FILE"
        echo "# Run: pytest $(grep -E 'FAILED|ERROR' "$LOG_FILE" | sed 's/.* //' | sed "s|::|::|g" | tr '\n' ' ')" >> "$FAILED_TESTS_FILE"
        echo "" >> "$FAILED_TESTS_FILE"
        
        # Parse failed tests (format: tests/path/to/test.py::TestClass::test_name)
        grep -E 'FAILED|ERROR' "$LOG_FILE" | \
            sed 's/FAILED //' | \
            sed 's/ERROR //' | \
            awk '{print $1}' | \
            grep -v '^$' | \
            sort -u >> "$FAILED_TESTS_FILE"
        
        FAILED_COUNT=$(grep -c -E '^tests/' "$FAILED_TESTS_FILE" || echo "0")
        echo "✅ Stored $FAILED_COUNT failed tests to $FAILED_TESTS_FILE"
        
        # Show summary
        PASSED=$(grep -oP '\d+(?= passed)' "$LOG_FILE" | tail -1 || echo "0")
        FAILED=$(grep -oP '\d+(?= failed)' "$LOG_FILE" | tail -1 || echo "0")
        SKIPPED=$(grep -oP '\d+(?= skipped)' "$LOG_FILE" | tail -1 || echo "0")
        
        echo ""
        echo "📊 Test Summary:"
        echo "  ✅ Passed:  $PASSED"
        echo "  ❌ Failed:  $FAILED"
        echo "  ⏭️  Skipped: $SKIPPED"
        ;;
        
    rerun)
        if [ ! -f "$FAILED_TESTS_FILE" ]; then
            echo "❌ No stored failures found in $FAILED_TESTS_FILE"
            echo "   Run 'store' first to capture failures"
            exit 1
        fi
        
        FAILED_TESTS=$(grep -E '^tests/' "$FAILED_TESTS_FILE" | grep -v '^#' | grep -v '^$' | tr '\n' ' ')
        
        if [ -z "$FAILED_TESTS" ]; then
            echo "✅ No failed tests to rerun!"
            exit 0
        fi
        
        FAILED_COUNT=$(echo "$FAILED_TESTS" | wc -w)
        echo "🔄 Rerunning $FAILED_COUNT failed tests in ${MODE} mode..."
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        # Set environment variables
        export SPARKFORGE_ENGINE="${MODE}"
        export SPARK_MODE="real"
        
        if [ "$MODE" = "pyspark" ]; then
            export SPARKFORGE_BASIC_SPARK="1"
        fi
        
        # Run only failed tests
        pytest $FAILED_TESTS -v --tb=short -n 5
        
        # Check if all passed
        if [ $? -eq 0 ]; then
            echo ""
            echo "🎉 All previously failed tests now pass!"
            echo "   Consider running 'clear' to reset the failure list"
        else
            echo ""
            echo "⚠️  Some tests still failing. Run 'store' again to update the failure list"
        fi
        ;;
        
    clear)
        if [ -f "$FAILED_TESTS_FILE" ]; then
            rm "$FAILED_TESTS_FILE"
            echo "✅ Cleared $FAILED_TESTS_FILE"
        else
            echo "ℹ️  No file to clear: $FAILED_TESTS_FILE"
        fi
        ;;
        
    *)
        echo "Usage: $0 {store|rerun|clear} [mode]"
        echo ""
        echo "Commands:"
        echo "  store <mode>  - Run all tests and store failures (default: pyspark)"
        echo "  rerun <mode>  - Rerun only previously stored failures (default: pyspark)"
        echo "  clear <mode>  - Clear stored failure list (default: pyspark)"
        echo ""
        echo "Modes:"
        echo "  pyspark      - PySpark mode (default)"
        echo "  mock         - Mock Spark mode"
        exit 1
        ;;
esac
