#!/usr/bin/env python3
"""
Reproduce the sparkless bug where columns from empty aggregated DataFrames
are lost after a left join.

The bug: When you groupBy().agg() on an empty DataFrame and then left join it,
the aggregated columns are not available in the result, even though they should
be present as NULL values.

This matches the exact scenario from the failing healthcare pipeline test.
"""

from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

# Create Spark session
spark = SparkSession.builder.appName("join_empty_agg_bug").getOrCreate()

print("=" * 80)
print("Reproducing sparkless join bug with empty aggregated DataFrames")
print("=" * 80)

# Create main DataFrame (clean_patients - this one has data)
print("\n1. Creating main DataFrame (clean_patients)...")
patients_data = [
    ("PAT-001", "John", "Doe", 25, "adult", "M", "ProviderA"),
    ("PAT-002", "Jane", "Smith", 30, "adult", "F", "ProviderB"),
]
patients_df = spark.createDataFrame(
    patients_data,
    ["patient_id", "first_name", "last_name", "age", "age_group", "gender", "insurance_provider"]
)
print(f"   ✅ Created {patients_df.count()} rows")
print(f"   Columns: {patients_df.columns}")

# Create empty DataFrame (normalized_labs - 0 rows due to validation failure)
print("\n2. Creating empty DataFrame (normalized_labs - 0 rows)...")
empty_labs_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_abnormal", BooleanType(), True),
    StructField("result_category", StringType(), True),
])
empty_labs_df = spark.createDataFrame([], empty_labs_schema)
print(f"   ✅ Created empty DataFrame: {empty_labs_df.count()} rows")

# Create empty DataFrame (processed_diagnoses - 0 rows due to validation failure)
print("\n3. Creating empty DataFrame (processed_diagnoses - 0 rows)...")
empty_diagnoses_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_chronic", BooleanType(), True),
    StructField("risk_level", StringType(), True),
])
empty_diagnoses_df = spark.createDataFrame([], empty_diagnoses_schema)
print(f"   ✅ Created empty DataFrame: {empty_diagnoses_df.count()} rows")

# Aggregate empty lab DataFrame (as test does)
print("\n4. Aggregating empty lab DataFrame...")
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
print(f"   ✅ Aggregated: {lab_metrics.count()} rows")
print(f"   Columns: {lab_metrics.columns}")

# Aggregate empty diagnoses DataFrame (as test does)
print("\n5. Aggregating empty diagnoses DataFrame...")
diagnosis_metrics = empty_diagnoses_df.groupBy("patient_id").agg(
    F.count("*").alias("total_diagnoses"),
    F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias("chronic_conditions"),
    F.sum(
        F.when(F.col("risk_level") == "high", 3)
        .when(F.col("risk_level") == "medium", 2)
        .otherwise(1)
    ).alias("risk_score_sum"),  # This column should exist
)
print(f"   ✅ Aggregated: {diagnosis_metrics.count()} rows")
print(f"   Columns: {diagnosis_metrics.columns}")

# Join them together (exact sequence from test)
print("\n6. Joining DataFrames (exact sequence from test)...")
print("   Step 1: patients_df.join(lab_metrics, 'patient_id', 'left')")
try:
    result1 = patients_df.join(lab_metrics, "patient_id", "left")
    print(f"   ✅ First join succeeded: {result1.count()} rows")
    print(f"   Columns after first join: {result1.columns}")
    
    print("\n   Step 2: result1.join(diagnosis_metrics, 'patient_id', 'left')")
    result2 = result1.join(diagnosis_metrics, "patient_id", "left")
    print(f"   ✅ Second join succeeded: {result2.count()} rows")
    print(f"   Columns after second join: {result2.columns}")
    
    # Check if risk_score_sum is available
    if "risk_score_sum" in result2.columns:
        print("\n   ✅ 'risk_score_sum' column is present")
    else:
        print("\n   ❌ BUG: 'risk_score_sum' column is MISSING!")
        print(f"   Expected columns: {set(result2.columns)}")
        print(f"   diagnosis_metrics had: {diagnosis_metrics.columns}")
    
    # Try to use risk_score_sum (this is where it fails in the test)
    print("\n7. Attempting to use 'risk_score_sum' column...")
    try:
        result3 = result2.withColumn(
            "overall_risk_score",
            F.coalesce(F.col("risk_score_sum"), F.lit(0))
        )
        count = result3.count()
        print(f"   ✅ SUCCESS: risk_score_sum column accessible")
        print(f"   Result: {count} rows")
    except AttributeError as e:
        if "risk_score_sum" in str(e):
            print(f"   ❌ BUG REPRODUCED!")
            print(f"   Error: {type(e).__name__}")
            print(f"   Message: {e}")
            print(f"\n   This is the exact bug from the test!")
            print(f"   The column 'risk_score_sum' exists in diagnosis_metrics")
            print(f"   But after joining, it's not available in the result")
            print(f"   Available columns: {result2.columns}")
        else:
            print(f"   ❌ Different error: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"   ❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        
except Exception as e:
    print(f"   ❌ Join failed: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("Expected Behavior (PySpark)")
print("=" * 80)
print("""
In PySpark, this exact code works:

empty_df = spark.createDataFrame([], schema)
agg_df = empty_df.groupBy("patient_id").agg(
    F.sum(F.lit(1)).alias("risk_score_sum")
)
# agg_df has 0 rows but still has the 'risk_score_sum' column

result = patients_df.join(agg_df, "patient_id", "left")
# result.columns includes 'risk_score_sum' even though agg_df was empty
# F.col("risk_score_sum") works and returns NULL for all rows
# F.coalesce(F.col("risk_score_sum"), F.lit(0)) works correctly
""")

