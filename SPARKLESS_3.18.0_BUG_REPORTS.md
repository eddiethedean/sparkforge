# sparkless 3.18.0 Bug Reports

## Summary

After upgrading to sparkless 3.18.0, 5 tests still fail (down from 19 in 3.17.11). All failures are related to `F.to_timestamp()` returning String when Datetime is expected.

## Bug Report

### Issue #149: to_timestamp() returns String when Datetime expected

**GitHub Issue**: https://github.com/eddiethedean/sparkless/issues/149

**Error Message**:
```
expected output type 'Datetime('μs')', got 'String'; set `return_dtype` to the proper datatype
```

**Root Cause**:
In sparkless 3.18.0, `F.to_timestamp()` operations return String columns when sparkless expects Datetime('μs') in validation and type-checking contexts. The column appears as TimestampType in the schema, but sparkless treats it as String internally during validation.

**Affected Tests** (5 total):

1. **Marketing Pipeline** (`test_marketing_pipeline.py`)
   - Steps: `processed_impressions`, `processed_clicks`, `processed_conversions`
   - Error: `expected output type 'Datetime('μs')', got 'String'`
   - Pattern: `F.to_timestamp(F.regexp_replace(...).cast("string"), "yyyy-MM-dd'T'HH:mm:ss")`

2. **Supply Chain Pipeline** (`test_supply_chain_pipeline.py`)
   - Steps: `processed_orders`, `processed_shipments`, `processed_inventory`
   - Error: `expected output type 'Datetime('μs')', got 'String'`
   - Pattern: `F.to_timestamp(F.regexp_replace(...).cast("string"), "yyyy-MM-dd'T'HH:mm:ss")`

3. **Streaming Hybrid Pipeline** (`test_streaming_hybrid_pipeline.py`)
   - Steps: `unified_batch_events`, `unified_streaming_events`
   - Error: `expected output type 'Datetime('μs')', got 'String'`
   - Pattern: `F.to_timestamp(F.regexp_replace(...).cast("string"), "yyyy-MM-dd'T'HH:mm:ss")`

4. **Data Quality Pipeline** (`test_data_quality_pipeline.py`)
   - Steps: `normalized_source_a`, `normalized_source_b`
   - Error: `expected output type 'Datetime('μs')', got 'String'`
   - Pattern: `F.to_timestamp(F.regexp_replace(...).cast("string"), "yyyy-MM-dd'T'HH:mm:ss")`

5. **Healthcare Pipeline** (`test_healthcare_pipeline.py`)
   - Step: `patient_risk_scores` (downstream effect)
   - Error: `0.0% valid` - validation fails, 0 rows processed
   - Root Cause: Upstream steps (`normalized_labs`, `processed_diagnoses`) use `F.to_timestamp()` which returns String, causing downstream validation failures
   - Pattern: `F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss")` (without explicit cast)

**Reproduction Scripts**:
- `reproduce_sparkless_bugs_3.18.0/bug_1_to_timestamp_returns_string.py` - Main pattern with explicit cast
- `reproduce_sparkless_bugs_3.18.0/bug_2_to_timestamp_with_clean_column.py` - Pattern without explicit cast
- `reproduce_sparkless_bugs_3.18.0/bug_3_validation_fails_with_datetime_string.py` - Validation failure scenario

**Minimal Reproduction**:
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("to_timestamp_bug").getOrCreate()

data = [("2024-01-15T10:30:45.123456",)]
df = spark.createDataFrame(data, ["date_string"])

df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

# In pipeline context with validation rules like {"date_parsed": ["not_null"]},
# sparkless reports: ERROR: expected output type 'Datetime('μs')', got 'String'
```

**Expected Behavior**:
`F.to_timestamp()` should return a TimestampType column that sparkless recognizes as Datetime('μs') in all contexts, including validation.

**Actual Behavior**:
The column appears as TimestampType in the schema, but sparkless treats it as String when validating or type-checking, causing validation failures.

**Workaround**:
None currently known. The issue occurs at the sparkless type system level.

**Related Issues**:
- This appears to be a regression or incomplete fix from the datetime handling improvements in 3.18.0
- Previous issues (#135, #145) had the opposite problem: sparkless expected String but received datetime
- This suggests the fix was partially applied but type expectations are now reversed

## Test Results Comparison

| Version | Passed | Failed | Status |
|---------|--------|--------|--------|
| 3.17.11 | 1699 | 19 | ❌ 19 failures |
| 3.18.0 | 1713 | 5 | ✅ 14 fixes, 5 remaining |

**Improvement**: +14 tests passing (73.7% of failures fixed)

## All Tests Pass in Real Mode (PySpark)

All 5 failing tests pass successfully when run with `SPARK_MODE=real` (PySpark), confirming these are sparkless compatibility issues, not bugs in the sparkforge framework or test code.

## Next Steps

1. ✅ Bug reported to sparkless (Issue #149)
2. ⏳ Waiting for sparkless team to address the issue
3. ⏳ Once fixed, re-run tests to verify all 5 tests pass

