# sparkless 3.18.7 Full Test Results

## Summary

**Total Tests**: 1788
- ✅ **Passed**: 1713
- ❌ **Failed**: 5
- ⏭️ **Skipped**: 70

## Status: ✅ Original Bug Fixed!

The original bug (`cannot resolve 'impression_date'` when column is dropped) has been **FIXED** in sparkless 3.18.7.

### Evidence:
1. ✅ No more "cannot resolve 'impression_date'" errors
2. ✅ Standalone reproduction script works with both 2 and 150 rows
3. ✅ Silver steps complete successfully (previously failed with "cannot resolve")

## Remaining Failures (5 tests)

The same 5 tests still fail, but with **different errors**:

1. `test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
2. `test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
3. `test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution`
4. `test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
5. `test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution`

### New Error Pattern

These tests now fail with:
- **Validation failures**: 0.0% valid (all rows fail validation)
- **0 rows processed**: Downstream steps have no input rows
- **Different from original bug**: No "cannot resolve" errors

This suggests a **different issue** - likely related to:
- Validation logic
- Data quality checks
- Or how sparkless handles validation after the fix

## Comparison

### Before (sparkless 3.18.5/3.18.6)
- Error: `cannot resolve 'impression_date' given input columns: [...]`
- Silver steps failed during execution
- Bug in column resolution during materialization

### After (sparkless 3.18.7)
- ✅ No "cannot resolve" errors
- ✅ Silver steps complete successfully
- ❌ New issue: Validation failures (0.0% valid)

## Conclusion

✅ **The original bug is FIXED in sparkless 3.18.7**

The remaining failures are **different issues** that need separate investigation.

## Test Results File

Full results saved to: `test_results_sparkless_3.18.7.txt`

