#!/bin/bash
# Script to investigate all test failures in both mock and real modes

echo "=========================================="
echo "Investigating All Test Failures"
echo "=========================================="
echo ""

# List of all failing tests
FAILING_TESTS=(
    "tests/system/test_full_pipeline_with_logging.py::TestFullPipelineWithLogging::test_full_pipeline_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestMinimalPipelines::test_minimal_pipeline_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestLargePipelines::test_large_pipeline_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestExecutionModes::test_pipeline_sequential_execution_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestIncrementalScenarios::test_pipeline_multiple_incremental_runs"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestIncrementalScenarios::test_pipeline_incremental_with_gaps"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestHighVolume::test_pipeline_high_volume_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestComplexDependencies::test_pipeline_complex_dependencies_with_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestParallelStress::test_pipeline_parallel_execution_stress"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestSchemaEvolution::test_pipeline_with_schema_evolution_logging"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestDataTypes::test_pipeline_data_type_variations"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestWriteModes::test_pipeline_write_mode_variations"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestMixedSuccessFailure::test_pipeline_mixed_success_failure"
    "tests/system/test_full_pipeline_with_logging_variations.py::TestLongRunning::test_pipeline_long_running_with_logging"
    "tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution"
    "tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution"
    "tests/builder_tests/test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution"
    "tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution"
    "tests/builder_tests/test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution"
)

# Results file
RESULTS_FILE="test_failure_investigation_results.txt"
echo "Test Failure Investigation Results" > "$RESULTS_FILE"
echo "Generated: $(date)" >> "$RESULTS_FILE"
echo "==========================================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

for test in "${FAILING_TESTS[@]}"; do
    echo "Testing: $test"
    echo "----------------------------------------" >> "$RESULTS_FILE"
    echo "Test: $test" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
    
    # Test in mock mode
    echo "  Mock mode..."
    echo "MOCK MODE:" >> "$RESULTS_FILE"
    SPARK_MODE=mock python -m pytest "$test" -v --tb=line 2>&1 | grep -E "(PASSED|FAILED|Error:|invalid series|unable to find|datetime|AttributeError)" | head -5 >> "$RESULTS_FILE" || echo "FAILED" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
    
    # Test in real mode
    echo "  Real mode..."
    echo "REAL MODE:" >> "$RESULTS_FILE"
    SPARK_MODE=real python -m pytest "$test" -v --tb=line 2>&1 | grep -E "(PASSED|FAILED|Error:)" | head -3 >> "$RESULTS_FILE" || echo "FAILED" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
done

echo ""
echo "Results saved to: $RESULTS_FILE"
cat "$RESULTS_FILE"

