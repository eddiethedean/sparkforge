# Bug Root Cause: to_timestamp() returns None in sparkless 3.18.2

## Critical Finding

**The actual bug is NOT a type mismatch - it's that `to_timestamp()` returns `None` for all values!**

## Direct Evidence

Run the reproduction script:
```bash
SPARK_MODE=mock python reproduce_sparkless_bugs_3.18.2/bug_direct_sparkless_validation.py
```

### What the Script Reveals

1. **Schema says TimestampType** ✅
   ```
   Column 'impression_date_parsed' type: TimestampType()
   ```

2. **But all values are None** ❌
   ```
   Row 1: None (type: NoneType)
   Row 2: None (type: NoneType)
   Row 3: None (type: NoneType)
   ```

3. **Validation fails because all values are None** ❌
   ```
   Rows with non-null date_parsed: 0
   Rows passing validation: 0
   ```

## The Bug

**`to_timestamp()` in sparkless 3.18.2:**
- ✅ Returns `TimestampType` in schema (looks correct)
- ❌ Returns `None` for ALL actual values (the bug!)
- ❌ This causes validation to fail (all rows are None, so `isNotNull()` returns 0 rows)

## Why This Causes 0% Validation

When pipeline validation runs:
```python
rules = {"impression_date_parsed": ["not_null"]}
```

The validation checks:
```python
df.filter(F.col("impression_date_parsed").isNotNull())
```

Since ALL values are `None`, this filter returns 0 rows, resulting in:
- 0 rows processed
- 0% valid
- All rows marked as invalid

## Comparison with String Column

The script also shows that string columns work correctly:

```python
# String column (no to_timestamp)
df_string.withColumn("impression_date_clean", ...)  # StringType
# Validation: 3 rows pass ✅

# Timestamp column (with to_timestamp)
df_transformed.withColumn("impression_date_parsed", F.to_timestamp(...))  # TimestampType
# Validation: 0 rows pass ❌ (all are None!)
```

## Why This Was Hard to Detect

1. **Schema looks correct**: `TimestampType` in schema suggests it works
2. **No explicit error**: sparkless doesn't throw an error, just returns None
3. **Silent failure**: Validation fails silently (0% valid) instead of throwing

## The Fix Needed

sparkless needs to fix `to_timestamp()` to actually parse the string dates into timestamp values, not just return `None` while claiming the type is `TimestampType`.

## Evidence Summary

| Aspect | Expected | Actual (sparkless 3.18.2) | Status |
|--------|----------|---------------------------|--------|
| Schema type | TimestampType | TimestampType | ✅ Correct |
| Actual values | Timestamp objects | **ALL None** | ❌ **BUG** |
| Validation (isNotNull) | 3 rows | 0 rows | ❌ **BUG** |
| Validation rate | 100% | 0% | ❌ **BUG** |

## Code to Reproduce

See: `bug_direct_sparkless_validation.py`

Key section:
```python
# Apply to_timestamp()
df_transformed = df.withColumn(
    "impression_date_parsed",
    F.to_timestamp(
        F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
)

# Check values
collected = df_transformed.select("impression_date_parsed").collect()
for row in collected:
    print(row['impression_date_parsed'])  # All None! ❌
```

## Conclusion

The bug is that `to_timestamp()` doesn't actually parse dates in sparkless 3.18.2 - it just returns `None` for all rows while claiming the type is `TimestampType`. This causes all validation to fail because all values are `None`.

This is a **value bug**, not a **type bug**.

