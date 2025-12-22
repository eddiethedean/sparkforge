# Improved Bug Reproduction for sparkless Issue #160

## Critical Finding: Bug Depends on Dataset Size

The bug **ONLY occurs with larger datasets (150+ rows)** but **NOT with small datasets (2 rows)**.

This explains why the bug might not reproduce in all environments - it depends on the dataset size!

## Standalone Reproduction Script

Here's a complete, standalone reproduction script that clearly demonstrates the bug:

```python
#!/usr/bin/env python3
"""
Reproduction script for sparkless bug: cannot resolve dropped columns

Run: python reproduction.py
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

# Create test data (150 rows - bug occurs with larger datasets)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile", "tablet"][i % 3],
    })

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",  # This column will be dropped
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print("Step 1: Created DataFrame")
print(f"  Columns: {bronze_df.columns}")
print(f"  'impression_date' present: {'impression_date' in bronze_df.columns}")

# Apply transform that uses impression_date then drops it
print("\nStep 2: Apply transform")
print("  - Uses 'impression_date' in F.regexp_replace(F.col('impression_date'), ...)")
print("  - Creates new column 'impression_date_parsed'")
print("  - Drops 'impression_date' via .select() (not in select list)")

silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
    .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
    .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # New column
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",
        "cost_per_impression",
        "device_type",
        "is_mobile",
        # NOTE: impression_date is DROPPED - not in select list
    )
)

print(f"  Columns after transform: {silver_df.columns}")
print(f"  'impression_date' present: {'impression_date' in silver_df.columns}")
print(f"  'impression_date_parsed' present: {'impression_date_parsed' in silver_df.columns}")

# Test operations that trigger materialization
print("\nStep 3: Test operations (these will FAIL)")
print("=" * 80)

try:
    count = silver_df.count()
    print(f"❌ count() should have failed but succeeded: {count}")
except Exception as e:
    print(f"✅ count() failed as expected")
    print(f"   Error: {e}")
    print(f"\n   Full traceback:")
    import traceback
    traceback.print_exc()

try:
    cached = silver_df.cache()
    print(f"❌ cache() should have failed but succeeded")
except Exception as e:
    print(f"✅ cache() failed as expected")
    print(f"   Error: {e}")

try:
    collected = silver_df.collect()
    print(f"❌ collect() should have failed but succeeded: {len(collected)} rows")
except Exception as e:
    print(f"✅ collect() failed as expected")
    print(f"   Error: {e}")

spark.stop()
```

## Expected Output

When run, this script will fail with:

```
sparkless.core.exceptions.operation.SparkColumnNotFoundError: 
cannot resolve 'impression_date' given input columns: 
[impression_id, campaign_id, customer_id, impression_date_parsed, hour_of_day, 
 day_of_week, channel, ad_id, cost_per_impression, device_type, is_mobile]
```

## Full Error Traceback

The error occurs in sparkless's column validation during materialization:

```
Traceback (most recent call last):
  File "...", line 107, in test_with_rows
    result = op_func(silver_df)
  File "...", line 98, in <lambda>
    ("count()", lambda df: df.count()),
  File ".../sparkless/dataframe/dataframe.py", line 511, in count
    materialized = self._materialize_if_lazy()
  File ".../sparkless/dataframe/dataframe.py", line 827, in _materialize_if_lazy
    return cast("SupportsDataFrameOps", lazy_engine.materialize(self))
  File ".../sparkless/dataframe/lazy.py", line 501, in materialize
    return LazyEvaluationEngine._materialize_manual(df)
  File ".../sparkless/dataframe/lazy.py", line 1511, in _materialize_manual
    raise e
  File ".../sparkless/dataframe/lazy.py", line 907, in _materialize_manual
    current = cast("DataFrame", current_ops.withColumn(col_name, col))
  File ".../sparkless/dataframe/dataframe.py", line 276, in withColumn
    return self._transformations.withColumn(col_name, col)
  File ".../sparkless/dataframe/services/transformation_service.py", line 263, in withColumn
    self._df._validate_expression_columns(col, "withColumn")
  File ".../sparkless/dataframe/dataframe.py", line 1057, in _validate_expression_columns
    self._get_validation_handler().validate_expression_columns(
  File ".../sparkless/dataframe/validation_handler.py", line 90, in validate_expression_columns
    ColumnValidator.validate_expression_columns(
  File ".../sparkless/dataframe/validation/column_validator.py", line 213, in validate_expression_columns
    ColumnValidator.validate_expression_columns(
  File ".../sparkless/dataframe/validation/column_validator.py", line 213, in validate_expression_columns
    ColumnValidator.validate_expression_columns(
  File ".../sparkless/dataframe/validation/column_validator.py", line 238, in validate_expression_columns
    ColumnValidator.validate_column_exists(
  File ".../sparkless/dataframe/validation/column_validator.py", line 72, in validate_column_exists
    raise SparkColumnNotFoundError(column_name, column_names)
sparkless.core.exceptions.operation.SparkColumnNotFoundError: 
cannot resolve 'impression_date' given input columns: [...]
```

## Key Error Location

The error occurs in:
- **File**: `sparkless/dataframe/validation/column_validator.py`
- **Line**: 72 (`validate_column_exists`)
- **Called from**: Materialization process when validating expression columns
- **Issue**: Validates ALL column references in execution plan, including dropped columns

## Important: Dataset Size Matters

- **2 rows**: ✅ Works fine
- **150+ rows**: ❌ Fails with error

This suggests the bug is related to:
- Plan optimization that kicks in with larger datasets
- Different evaluation strategy for larger DataFrames
- Or some threshold-based behavior in sparkless

## Root Cause

1. Transform uses a column (`impression_date`) in operations like `F.regexp_replace(F.col("impression_date"), ...)`
2. Transform drops the column via `.select()` (excluding it from final columns)
3. Materialization is triggered (via `count()`, `cache()`, `collect()`, etc.)
4. During materialization, sparkless validates ALL column expressions in the execution plan
5. It tries to resolve the dropped column (`impression_date`) which no longer exists
6. Error: `"cannot resolve 'impression_date' given input columns: [...]"`

## Verification

To verify the bug:
1. Run the reproduction script with 150 rows → Should fail ✅
2. Change `range(150)` to `range(2)` → Should work ✅

This confirms the bug is dataset-size dependent.

