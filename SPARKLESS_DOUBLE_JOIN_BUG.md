# Bug: Columns from empty aggregated DataFrames are lost after second left join

## Summary

In `sparkless` (version 3.17.7), when performing two left joins in sequence where the second join involves an empty aggregated DataFrame (created via `groupBy().agg()` on an empty DataFrame), the columns from the second aggregated DataFrame are completely lost from the result. Additionally, columns from the first join are duplicated.

This breaks any code that needs to join multiple empty aggregated DataFrames, which is common in data pipelines where validation filters out all rows from intermediate steps.

## Environment

- **sparkless version:** 3.17.7
- **Python version:** 3.11.13
- **Operating System:** macOS (darwin 24.6.0)
- **Mode:** Mock mode (default sparkless behavior)

## Minimal Reproduction

```python
from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import (
    StructType, StructField, StringType, BooleanType
)

spark = SparkSession.builder.appName("double_join_bug").getOrCreate()

# Create main DataFrame (has data)
patients_data = [
    ("PAT-001", "John", "Doe", 25, "adult", "M", "ProviderA"),
    ("PAT-002", "Jane", "Smith", 30, "adult", "F", "ProviderB"),
]
patients_df = spark.createDataFrame(
    patients_data,
    ["patient_id", "first_name", "last_name", "age", "age_group", "gender", "insurance_provider"]
)
patients_df = patients_df.withColumn(
    "full_name",
    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
)

# Create empty DataFrames (simulating validation failures)
empty_labs_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_abnormal", BooleanType(), True),
    StructField("result_category", StringType(), True),
])
empty_labs_df = spark.createDataFrame([], empty_labs_schema)

empty_diagnoses_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_chronic", BooleanType(), True),
    StructField("risk_level", StringType(), True),
])
empty_diagnoses_df = spark.createDataFrame([], empty_diagnoses_schema)

# Aggregate empty DataFrames
lab_metrics = empty_labs_df.groupBy("patient_id").agg(
    F.count("*").alias("total_labs"),
    F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias("abnormal_labs"),
    F.sum(
        F.when(
            F.col("result_category").isin(["critical_high", "critical_low"]),
            1,
        ).otherwise(0)
    ).alias("critical_labs"),
)

diagnosis_metrics = empty_diagnoses_df.groupBy("patient_id").agg(
    F.count("*").alias("total_diagnoses"),
    F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias("chronic_conditions"),
    F.sum(
        F.when(F.col("risk_level") == "high", 3)
        .when(F.col("risk_level") == "medium", 2)
        .otherwise(1)
    ).alias("risk_score_sum"),  # This column should exist
)

print(f"lab_metrics columns: {lab_metrics.columns}")
# Output: ['patient_id', 'total_labs', 'abnormal_labs', 'critical_labs'] ✅

print(f"diagnosis_metrics columns: {diagnosis_metrics.columns}")
# Output: ['patient_id', 'total_diagnoses', 'chronic_conditions', 'risk_score_sum'] ✅

# First join (works)
result1 = patients_df.join(lab_metrics, "patient_id", "left")
print(f"After first join: {result1.columns}")
# Output includes: 'total_labs', 'abnormal_labs', 'critical_labs' ✅

# Second join (BUG: columns from diagnosis_metrics are lost)
result2 = result1.join(diagnosis_metrics, "patient_id", "left")
print(f"After second join: {result2.columns}")
# Expected: Should include 'total_diagnoses', 'chronic_conditions', 'risk_score_sum'
# Actual: These columns are MISSING ❌
# Also: 'patient_id', 'total_labs', 'abnormal_labs', 'critical_labs' are duplicated

# Try to use risk_score_sum (FAILS)
try:
    result3 = result2.withColumn(
        "overall_risk_score",
        F.coalesce(F.col("risk_score_sum"), F.lit(0))
    )
except Exception as e:
    print(f"FAILED: {e}")
    # Error: 'DataFrame' object has no attribute 'risk_score_sum'. 
    # Available columns: patient_id, first_name, last_name, age, age_group, 
    # gender, insurance_provider, full_name, total_labs, abnormal_labs, 
    # critical_labs, patient_id, total_labs, abnormal_labs, critical_labs
```

## Expected Behavior

After the second left join, the result should include all columns from both joins:
- Columns from `patients_df`: `patient_id`, `first_name`, `last_name`, `age`, `age_group`, `gender`, `insurance_provider`, `full_name`
- Columns from `lab_metrics`: `total_labs`, `abnormal_labs`, `critical_labs`
- Columns from `diagnosis_metrics`: `total_diagnoses`, `chronic_conditions`, `risk_score_sum`

Even though `diagnosis_metrics` has 0 rows, its columns should be present in the result with NULL values for all rows.

## Actual Behavior

After the second left join:
- ❌ Columns from `diagnosis_metrics` are **completely missing**: `total_diagnoses`, `chronic_conditions`, `risk_score_sum`
- ⚠️ Columns from `lab_metrics` are **duplicated**: `patient_id`, `total_labs`, `abnormal_labs`, `critical_labs` appear twice
- ❌ Attempting to use `F.col("risk_score_sum")` raises: `'DataFrame' object has no attribute 'risk_score_sum'`

## Additional Observations

1. **Single join works:** If you only do one join with an empty aggregated DataFrame, the columns are preserved correctly.
2. **Second join breaks it:** The bug only occurs when doing a second join after a first join.
3. **Column duplication:** The first join's columns are duplicated in the result.
4. **Schema shows columns:** The schema of `diagnosis_metrics` correctly shows `risk_score_sum`, but after joining, it's lost.

## Real-World Impact

This breaks any data pipeline that:
1. Has multiple silver/gold layer transforms that depend on each other
2. Uses validation that can filter out all rows from intermediate steps
3. Needs to join multiple aggregated DataFrames, even when some are empty

### Example from Our Codebase

We have a healthcare pipeline that:
1. Joins patient data with lab metrics (empty after validation)
2. Then joins with diagnosis metrics (empty after validation)
3. Calculates risk scores using `risk_score_sum` from diagnosis metrics

This fails in mock mode because `risk_score_sum` is lost after the second join, causing 5 pipeline tests to fail.

## Reproduction Script

A complete reproduction script is available at:
- File: `reproduce_sparkless_double_join_bug.py`
- Can be run directly: `python reproduce_sparkless_double_join_bug.py`

The script demonstrates:
- `diagnosis_metrics` has `risk_score_sum` column ✅
- After first join, `lab_metrics` columns are present ✅
- After second join, `diagnosis_metrics` columns are MISSING ❌
- Error when trying to use `risk_score_sum`: `'DataFrame' object has no attribute 'risk_score_sum'` ❌

## Requested Fix

Please fix the left join operation in `sparkless` to:
1. Preserve all columns from empty aggregated DataFrames in left joins
2. Handle multiple sequential left joins correctly
3. Avoid column duplication
4. Ensure columns are accessible via `F.col()` even when the source DataFrame is empty

**Specific areas to investigate:**
1. Join implementation for empty aggregated DataFrames
2. Schema merging logic for sequential joins
3. Column deduplication in join results

---

**Priority:** High (blocks multi-step pipelines with validation in mock mode)
**Severity:** High (workarounds don't exist - joins are essential functionality)

