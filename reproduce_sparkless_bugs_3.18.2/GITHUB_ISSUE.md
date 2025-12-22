# Bug: to_timestamp() returns None for all values in sparkless 3.18.2

## Summary

In sparkless 3.18.2, `F.to_timestamp()` returns `TimestampType` in the schema (appearing correct), but **all actual values are `None`**. This causes validation to fail silently (0% valid, 0 rows processed) because all rows are `None`, so `isNotNull()` filters return 0 rows.

## Environment

- **sparkless version**: 3.18.2
- **Python version**: 3.11
- **PySpark version**: 3.5.0+

## Root Cause

**This is a VALUE bug, not a type bug:**
- ✅ Schema correctly shows `TimestampType`
- ❌ **All actual values are `None`**
- ❌ Validation fails because all values are `None`

## Minimal Reproduction

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("to_timestamp_bug").getOrCreate()

# Create test data
data = [
    ("imp_001", "2024-01-15T10:30:45.123456"),
    ("imp_002", "2024-01-16T14:20:30.789012"),
    ("imp_003", "2024-01-17T09:15:22.456789"),
]
df = spark.createDataFrame(data, ["id", "date_string"])

# Apply to_timestamp()
df_transformed = df.withColumn(
    "date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

# Check schema (looks correct)
print(df_transformed.schema)  # Shows: date_parsed: TimestampType ✅

# Check actual values (THE BUG!)
collected = df_transformed.select("date_parsed").collect()
for row in collected:
    print(row['date_parsed'])  
    # Output: None, None, None ❌
    # Expected: datetime objects ✅

# Validation fails because all are None
valid_rows = df_transformed.filter(F.col("date_parsed").isNotNull()).count()
print(f"Valid rows: {valid_rows}")  
# Output: 0 ❌
# Expected: 3 ✅
```

## Expected Behavior

1. `to_timestamp()` should parse string dates into timestamp values ✅
2. Schema should show `TimestampType` ✅ (this works)
3. **Actual values should be timestamp objects, not `None`** ❌ (this is broken)
4. Validation should pass when values are not null ✅

## Actual Behavior

1. `to_timestamp()` returns `TimestampType` in schema ✅
2. **`to_timestamp()` returns `None` for ALL values** ❌
3. Validation fails because all values are `None` ❌
4. Result: 0% valid, 0 rows processed ❌

## Direct Evidence

Run the reproduction script:
```bash
SPARK_MODE=mock python reproduce_sparkless_bugs_3.18.2/bug_direct_sparkless_validation.py
```

**Output shows:**
```
Column 'date_parsed' type: TimestampType()  ✅ Schema correct
Row 1: None (type: NoneType)                ❌ Value is None
Row 2: None (type: NoneType)                ❌ Value is None  
Row 3: None (type: NoneType)                ❌ Value is None
Rows passing validation: 0                  ❌ All None, so 0 rows
```

## Why This Causes Pipeline Validation Failures

When used in pipeline validation:
```python
rules = {"date_parsed": ["not_null"]}
```

The validation checks:
```python
df.filter(F.col("date_parsed").isNotNull())
```

Since **ALL values are `None`**, this filter returns 0 rows, resulting in:
- 0 rows processed
- 0% valid
- All rows marked as invalid
- No explicit error (silent failure)

## Comparison with PySpark

**Same code in PySpark (real mode):**
```python
# PySpark correctly parses the dates
collected = df_transformed.select("date_parsed").collect()
for row in collected:
    print(row['date_parsed'])
    # Output: 2024-01-15 10:30:45, 2024-01-16 14:20:30, 2024-01-17 09:15:22 ✅

valid_rows = df_transformed.filter(F.col("date_parsed").isNotNull()).count()
print(f"Valid rows: {valid_rows}")
# Output: 3 ✅
```

**PySpark works correctly** - this proves the code is correct and the bug is in sparkless.

## Impact

This affects all pipeline transforms that use `to_timestamp()` to parse datetime strings:

- Marketing pipelines (impression dates, click dates, conversion dates)
- Supply chain pipelines (order dates, shipment dates, inventory dates)
- Streaming pipelines (event timestamps)
- Data quality pipelines (transaction dates)
- Healthcare pipelines (test dates, diagnosis dates)

**5 pipeline tests fail** with this pattern in our test suite.

## Evidence Summary

| Aspect | Expected | Actual (sparkless 3.18.2) | Status |
|--------|----------|---------------------------|--------|
| Schema type | TimestampType | TimestampType | ✅ Correct |
| **Actual values** | **Timestamp objects** | **ALL None** | ❌ **BUG** |
| Validation (isNotNull) | 3 rows | 0 rows | ❌ **BUG** |
| Validation rate | 100% | 0% | ❌ **BUG** |
| PySpark behavior | Works correctly | Works correctly | ✅ Correct |

## Related Issues

- **Issue #151**: Previously reported as type mismatch, but the actual bug is that values are `None`
- This issue supersedes #151 with the correct root cause

## Suggested Fix

sparkless needs to fix `to_timestamp()` to actually parse string dates into timestamp values, not just return `None` while claiming the type is `TimestampType`.

The function should:
1. Parse the input string according to the format
2. Return actual timestamp/datetime values (not `None`)
3. Maintain `TimestampType` in schema (this already works)

## Reproduction Files

- `bug_direct_sparkless_validation.py` - Direct sparkless code demonstrating the bug
- `run_test_both_modes.sh` - Comparison script (sparkless vs PySpark)
- `BUG_ROOT_CAUSE.md` - Detailed root cause analysis

All files available in: `reproduce_sparkless_bugs_3.18.2/`

## Additional Context

This bug was identified when running tests with sparkless 3.18.2:
- **5 tests fail** with 0% validation, 0 rows processed
- **All 5 tests pass** with real PySpark (`SPARK_MODE=real`)
- Confirms this is a sparkless bug, not a test bug

The bug manifests as silent validation failure (0% valid) rather than an explicit error, making it harder to debug.

