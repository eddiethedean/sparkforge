#!/usr/bin/env python3
"""
Reproduce the sparkless join bug where columns from empty aggregated DataFrames
are not included in the result after a join.

The bug: When you groupBy().agg() on an empty DataFrame and then join it,
the columns from the empty DataFrame are not available in the result.
"""

from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

# Create Spark session
spark = SparkSession.builder.appName("join_bug_reproduction").getOrCreate()

print("=" * 80)
print("Reproducing sparkless join bug with empty aggregated DataFrames")
print("=" * 80)

# Create main DataFrame
print("\n1. Creating main DataFrame (clean_patients)...")
patients_data = [
    ("PAT-001", "John", "Doe", 25),
    ("PAT-002", "Jane", "Smith", 30),
]
patients_df = spark.createDataFrame(
    patients_data,
    ["patient_id", "first_name", "last_name", "age"]
)
print(f"   ✅ Created {patients_df.count()} rows")
print(f"   Columns: {patients_df.columns}")

# Create empty DataFrame (simulating normalized_labs with 0 rows after validation)
print("\n2. Creating empty DataFrame (normalized_labs - 0 rows)...")
empty_labs_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_abnormal", BooleanType(), True),
    StructField("result_category", StringType(), True),
])
empty_labs_df = spark.createDataFrame([], empty_labs_schema)
print(f"   ✅ Created empty DataFrame: {empty_labs_df.count()} rows")
print(f"   Columns: {empty_labs_df.columns}")

# Create empty DataFrame (simulating processed_diagnoses with 0 rows after validation)
print("\n3. Creating empty DataFrame (processed_diagnoses - 0 rows)...")
empty_diagnoses_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_chronic", BooleanType(), True),
    StructField("risk_level", StringType(), True),
])
empty_diagnoses_df = spark.createDataFrame([], empty_diagnoses_schema)
print(f"   ✅ Created empty DataFrame: {empty_diagnoses_df.count()} rows")
print(f"   Columns: {empty_diagnoses_df.columns}")

# Aggregate on empty DataFrame (this is what the test does)
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

# Join them together (as the test does)
print("\n6. Joining DataFrames (as test does)...")
try:
    result = (
        patients_df
        .join(lab_metrics, "patient_id", "left")
        .join(diagnosis_metrics, "patient_id", "left")
    )
    print(f"   ✅ Join succeeded: {result.count()} rows")
    print(f"   Result columns: {result.columns}")
    
    # Try to use risk_score_sum (this is where it fails)
    print("\n7. Attempting to use 'risk_score_sum' column...")
    try:
        result_with_score = result.withColumn(
            "overall_risk_score",
            F.coalesce(F.col("risk_score_sum"), F.lit(0))
        )
        count = result_with_score.count()
        print(f"   ✅ SUCCESS: risk_score_sum column accessible")
        print(f"   Result: {count} rows")
    except Exception as e:
        print(f"   ❌ FAILED: {type(e).__name__}")
        print(f"   Error: {e}")
        print(f"\n   This is the bug!")
        print(f"   'risk_score_sum' was created in diagnosis_metrics")
        print(f"   But after joining, it's not available")
        print(f"   Available columns: {result.columns}")
        
except Exception as e:
    print(f"   ❌ Join failed: {type(e).__name__}: {e}")

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
""")

