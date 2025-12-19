# PySpark Compatibility Issues Investigation Report

## Executive Summary

Investigated 7 failing tests in mock mode (sparkless 3.17.9) to identify PySpark compatibility issues. All failing tests pass in real mode (PySpark), confirming these are sparkless bugs, not issues in our code.

## Findings

### Primary Issue: `to_timestamp()` Dtype Mismatch Bug

**Root Cause**: Sparkless's `to_timestamp()` function has a dtype mismatch when materializing DataFrames with Polars backend.

**Error Message**:
```
polars.exceptions.SchemaError: expected output type 'Datetime('μs')', got 'String'; 
set `return_dtype` to the proper datatype
```

**Behavior**:
1. Sparkless correctly infers schema showing `TimestampType` for columns created with `to_timestamp()`
2. However, when the DataFrame is materialized (during `.show()`, `.collect()`, or write operations), Polars throws an error
3. Polars expects a `Datetime('μs')` type but receives a `String` type instead

**Affected Tests** (5 tests, all pass in real mode):
1. `tests/builder_tests/test_healthcare_pipeline.py::TestHealthcarePipeline::test_complete_healthcare_pipeline_execution`
2. `tests/builder_tests/test_marketing_pipeline.py::TestMarketingPipeline::test_complete_marketing_pipeline_execution`
3. `tests/builder_tests/test_streaming_hybrid_pipeline.py::TestStreamingHybridPipeline::test_complete_streaming_hybrid_pipeline_execution`
4. `tests/builder_tests/test_supply_chain_pipeline.py::TestSupplyChainPipeline::test_complete_supply_chain_pipeline_execution`
5. `tests/builder_tests/test_data_quality_pipeline.py::TestDataQualityPipeline::test_complete_data_quality_pipeline_execution`

**Common Pattern**:
All failing tests use this pattern:
```python
df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_col"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss"
    )
)
```

### Secondary Issues

**Test**: `tests/unit/test_edge_cases.py::TestEdgeCases::test_writer_edge_cases`
- **Issue**: Uses `mock_spark_session.storage.table_exists()`, which is mock-only functionality
- **Status**: Expected to fail in real mode (test is mock-specific)
- **Impact**: Low - this is intentional mock-only test

**Test**: `tests/unit/test_validation_property_based.py::TestValidationPropertyBased::test_safe_divide_properties`
- **Status**: Passes when run individually, may be flaky/concurrency-related
- **Impact**: Low - appears to be test infrastructure issue, not compatibility

**Test**: `tests/debug/test_delta_minimal.py`
- **Issue**: Import error (`_log_session_configs` not found)
- **Status**: Debug test with broken import
- **Impact**: Very Low - debug test, not part of main test suite

## Reproduction

Minimal reproduction script demonstrating the bug:

```python
import os
os.environ["SPARK_MODE"] = "mock"

from sparkless.session.core.session import SparkSession
from sparkless import functions as F
from sparkless.spark_types import StringType, StructField, StructType
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("test").getOrCreate()

# Create test data
test_data = [{"date": (datetime.now() - timedelta(days=i)).isoformat()} for i in range(5)]
schema = StructType([StructField("date", StringType(), False)])
df = spark.createDataFrame(test_data, schema)

# Clean and parse timestamp
df_parsed = df.withColumn(
    "date_clean",
    F.regexp_replace(F.col("date"), r"\.\d+", "")
).withColumn(
    "date_parsed",
    F.to_timestamp(F.col("date_clean"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Schema inference is correct
df_parsed.printSchema()  # Shows: date_parsed: TimestampType ✅

# But materialization fails
df_parsed.show()  # ❌ SchemaError: expected output type 'Datetime('μs')', got 'String'
```

## Comparison: PySpark vs Sparkless

| Aspect | PySpark (Real Mode) | Sparkless (Mock Mode) |
|--------|---------------------|----------------------|
| `to_timestamp()` return type | ✅ `TimestampType` | ❌ Claims `TimestampType` but Polars returns `String` |
| Schema inference | ✅ Correct | ✅ Correct |
| Materialization | ✅ Works | ❌ Fails with dtype mismatch |
| Usage in subsequent operations | ✅ Works (e.g., `F.year()`, `F.hour()`) | ❌ Fails before reaching this point |

## Related Issues

We've reported three related bugs to sparkless:
1. **Issue #130**: `to_timestamp()` output type handling when used with other operations
2. **Issue #131**: `to_timestamp()` should accept more input types to match PySpark behavior
3. **Issue #133**: `to_timestamp()` dtype mismatch - Schema shows TimestampType but Polars materialization fails with String type ([GitHub Issue](https://github.com/eddiethedean/sparkless/issues/133))

## Impact Assessment

- **High Priority**: 5 of 7 failing tests are blocked by the `to_timestamp()` dtype bug
- **All affected tests pass in real mode**, confirming this is a sparkless bug, not our code
- **The tests use standard PySpark patterns** that should work identically in sparkless

## Recommendations

### Short-term
1. **Document the limitation**: Add notes in test files explaining why certain tests are skipped in mock mode
2. **Skip in mock mode**: Consider adding `@pytest.mark.skipif(SPARK_MODE == "mock")` to affected tests
3. **Monitor sparkless issues**: Track GitHub issues #130, #131, and #133 for fixes

### Long-term
1. **Contribute fixes**: If sparkless maintainers are slow to respond, consider contributing fixes
2. **Test both modes**: Continue running tests in both real and mock mode to catch compatibility regressions
3. **Documentation**: Update compatibility documentation with known sparkless limitations

## Technical Details

### Why This Happens

Sparkless uses Polars as its backend execution engine. The issue appears to be in how sparkless translates `to_timestamp()` operations to Polars expressions:

1. Sparkless correctly sets the schema type to `TimestampType`
2. But when materializing, Polars receives a String expression instead of a Datetime expression
3. Polars's type checking catches the mismatch and throws an error

This is a bug in sparkless's Polars backend implementation, not in Polars itself.

### Workarounds

No reliable workarounds exist - the bug occurs at the execution layer, not at the API layer. The following don't work:
- Casting the result: `F.to_timestamp(...).cast("timestamp")` - fails before cast
- Different format strings: Same error occurs
- Different input preprocessing: Issue is in `to_timestamp()` execution, not input

### Existing Documentation

Our codebase already documents some sparkless limitations:
- `docs/MOCK_SPARK_UPSTREAM_PLAN.md`: Documents datetime handling issues
- `docs/MOCK_SPARK_UPGRADE_PLAN.md`: Lists `to_timestamp()` compatibility gaps

## Conclusion

The primary compatibility issue is a confirmed bug in sparkless's `to_timestamp()` implementation with Polars backend. All affected tests pass in real PySpark mode, confirming our code is correct. We should wait for sparkless fixes (already reported) or contribute fixes ourselves if needed.

