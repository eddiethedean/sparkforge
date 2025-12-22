#!/bin/bash
# Script to run the same test in both mock and real mode to demonstrate the bug

echo "=================================================================================="
echo "REPRODUCING: Pipeline validation failure in sparkless 3.18.2 vs PySpark"
echo "=================================================================================="
echo ""
echo "This script runs the same test in both modes to demonstrate:"
echo "  - sparkless 3.18.2 (mock mode) - validation fails silently (0% valid)"
echo "  - PySpark (real mode) - validation works correctly"
echo ""

cd "$(dirname "$0")/.."

echo "=================================================================================="
echo "TEST 1: SPARKLESS MOCK MODE (sparkless 3.18.2)"
echo "=================================================================================="
echo ""
SPARK_MODE=mock python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|Validation completed|rows processed|validation_rate|ERROR|invalid series)" | head -20

echo ""
echo "=================================================================================="
echo "TEST 2: PYSPARK REAL MODE (real Spark)"
echo "=================================================================================="
echo ""
SPARK_MODE=real python -m pytest tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|Validation completed|rows processed|validation_rate|ERROR|invalid series)" | head -20

echo ""
echo "=================================================================================="
echo "SUMMARY"
echo "=================================================================================="
echo ""
echo "Compare the results above:"
echo "  - Mock mode: Should show 0% valid, 0 rows processed"
echo "  - Real mode: Should show 100% valid, > 0 rows processed"
echo ""

