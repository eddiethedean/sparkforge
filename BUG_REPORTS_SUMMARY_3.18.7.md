# Bug Reports Summary: sparkless 3.18.7

## Overview

After investigating 5 failing tests in sparkless 3.18.7, I identified and reported 4 confirmed bugs to the sparkless repository.

## Confirmed Bugs Reported

### 1. Validation fails with "cannot resolve" when validating after transform that drops columns
**Issue**: #163  
**Status**: Reported  
**Description**: When validating a DataFrame after a transform that uses a column and then drops it, sparkless tries to resolve the dropped column during validation, causing a "cannot resolve" error.

**Reproduction**: `reproduce_sparkless_bugs_3.18.7/bug_1_validation_cannot_resolve_dropped_column.py`

### 2. Type comparison error: "cannot compare string with numeric type (i32)"
**Issue**: #164  
**Status**: Reported  
**Description**: sparkless treats numeric columns as strings, causing type comparison errors when comparing numeric columns with numbers.

**Reproduction**: `reproduce_sparkless_bugs_3.18.7/bug_3_type_comparison_error.py`

### 3. to_date() requires StringType or DateType input, got TimestampType
**Issue**: #165  
**Status**: Reported  
**Description**: `to_date()` function in sparkless doesn't accept `TimestampType` as input, even though PySpark does.

**Reproduction**: `reproduce_sparkless_bugs_3.18.7/bug_4_to_date_type_error.py`

### 4. Unsupported function: unix_timestamp
**Issue**: #166  
**Status**: Reported  
**Description**: The `unix_timestamp()` function is not supported in sparkless, even though it's a standard PySpark function.

**Reproduction**: `reproduce_sparkless_bugs_3.18.7/bug_5_unix_timestamp_unsupported.py`

## Bugs Not Reproducing

### to_timestamp() returns None
**Status**: NOT REPRODUCING  
**Note**: Initial investigation suggested `to_timestamp()` returns None, but reproduction script shows it works correctly. The issue may be context-specific or related to the validation bug.

### Column resolution error - "unable to find column 'source'"
**Status**: NOT REPRODUCING IN ISOLATION  
**Note**: The error occurs in the data quality pipeline test, but cannot be reproduced with pure sparkless code. It may be specific to the pipeline context or related to the validation bug.

## Test Results

**Total Tests**: 1788
- ✅ **Passed**: 1713
- ❌ **Failed**: 5
- ⏭️ **Skipped**: 70

## Failing Tests

1. `test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
   - Affected by: Bug #1 (validation cannot resolve), Bug #2 (type comparison)

2. `test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
   - Affected by: Bug #1 (validation cannot resolve)

3. `test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution`
   - Affected by: Bug #1 (validation cannot resolve)

4. `test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
   - Affected by: Bug #3 (to_date type error), Bug #4 (unix_timestamp unsupported)

5. `test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution`
   - Affected by: Bug #1 (validation cannot resolve) or context-specific issue

## Next Steps

1. Monitor GitHub issues for responses from sparkless developers
2. Test fixes when new sparkless versions are released
3. Update reproduction scripts if needed based on developer feedback

