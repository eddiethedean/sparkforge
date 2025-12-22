# Bug Reports Summary: sparkless 3.19.0

## Overview

After investigating 5 failing tests in sparkless 3.19.0, I identified and reported 3 confirmed bugs with detailed reproduction scripts and comprehensive issue descriptions.

## Confirmed Bugs Reported

### 1. Validation fails: "cannot resolve 'impression_date'" when validating after transform that drops column
**Issue**: #168  
**Status**: Reported  
**Description**: When validating a DataFrame after a transform that uses a column and then drops it, sparkless tries to resolve the dropped column during validation, causing a `SparkColumnNotFoundError`.

**Reproduction**: `reproduce_sparkless_bugs_3.19.0/bug_1_validation_fails_after_dropping_column.py`

**Impact**:
- Marketing Pipeline: `processed_impressions` validation fails with 0.0% valid
- Healthcare Pipeline: Similar validation failures
- Supply Chain Pipeline: Similar validation failures

**Root Cause**: sparkless's column validation checks the entire execution plan history, not just the current DataFrame schema.

### 2. 'NoneType' object has no attribute 'collect' after transform with to_timestamp()
**Issue**: #169  
**Status**: Reported  
**Description**: After applying a transform that uses `to_timestamp()`, attempting to materialize the DataFrame fails with `'NoneType' object has no attribute 'collect'`.

**Reproduction**: `reproduce_sparkless_bugs_3.19.0/bug_2_none_type_collect_error.py`

**Impact**:
- Healthcare Pipeline: `normalized_labs` and `processed_diagnoses` steps fail
- Any transform that uses `to_timestamp()` followed by materialization

**Root Cause**: An internal operation returns `None` instead of a DataFrame, likely because `to_timestamp()` fails silently.

### 3. to_date() on TimestampType fails with 'NoneType' object has no attribute 'collect'
**Issue**: #170  
**Status**: Reported  
**Description**: When using `to_date()` on a `TimestampType` column, sparkless fails with `'NoneType' object has no attribute 'collect'`. PySpark's `to_date()` accepts `TimestampType`, but sparkless does not.

**Reproduction**: `reproduce_sparkless_bugs_3.19.0/bug_3_to_date_datetime_type_error.py`

**Impact**:
- Streaming Hybrid Pipeline: `unified_analytics` step fails
- Any transform that uses `to_date()` on a `TimestampType` column

**Root Cause**: sparkless's `to_date()` function only accepts `StringType` or `DateType`, not `TimestampType`. When called on `TimestampType`, it fails silently and returns `None`.

**Related Issue**: #165 (reported for 3.18.7, still present in 3.19.0 with different error)

## Test Results

**Total Tests**: 1788
- ✅ **Passed**: 1713
- ❌ **Failed**: 5
- ⏭️ **Skipped**: 70

## Failing Tests

1. `test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
   - Affected by: Bug #168 (validation cannot resolve dropped column)

2. `test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
   - Affected by: Bug #168 (validation cannot resolve), Bug #169 (NoneType collect error)

3. `test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution`
   - Affected by: Bug #168 (validation cannot resolve dropped column)

4. `test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
   - Affected by: Bug #170 (to_date() on TimestampType)

5. `test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution`
   - Affected by: Bug #168 (validation cannot resolve) or context-specific issue

## Improvements Over Previous Reports

### More Detailed Reproduction Scripts
- Each script includes step-by-step comments explaining what's happening
- Scripts match the exact transforms from the failing tests
- Scripts include checks for intermediate results (e.g., checking if `to_timestamp()` worked)

### Comprehensive Issue Descriptions
- Clear summary at the top
- Minimal reproduction code
- Full reproduction script reference
- Expected vs. actual behavior
- Root cause analysis
- Impact assessment
- Workarounds (if any)
- Additional context (related issues, test cases, etc.)

### Better Error Analysis
- Full error messages and tracebacks
- Explanation of what the error means
- Possible causes listed
- Comparison with PySpark behavior where relevant

## Files Created

### Reproduction Scripts
- `reproduce_sparkless_bugs_3.19.0/bug_1_validation_fails_after_dropping_column.py`
- `reproduce_sparkless_bugs_3.19.0/bug_2_none_type_collect_error.py`
- `reproduce_sparkless_bugs_3.19.0/bug_3_to_date_datetime_type_error.py`
- `reproduce_sparkless_bugs_3.19.0/bug_4_column_resolution_source.py` (not reproducing in isolation)

### Issue Descriptions
- `reproduce_sparkless_bugs_3.19.0/ISSUE_1_validation_cannot_resolve_dropped_column.md`
- `reproduce_sparkless_bugs_3.19.0/ISSUE_2_none_type_collect_error.md`
- `reproduce_sparkless_bugs_3.19.0/ISSUE_3_to_date_datetime_type_error.md`

## Next Steps

1. Monitor GitHub issues for responses from sparkless developers
2. Test fixes when new sparkless versions are released
3. Update reproduction scripts if needed based on developer feedback

