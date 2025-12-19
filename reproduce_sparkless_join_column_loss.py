#!/usr/bin/env python3
"""
Reproduce the sparkless bug where columns from empty aggregated DataFrames
are lost after a left join.

The bug: When you groupBy().agg() on an empty DataFrame and then left join it,
the aggregated columns are not available in the result, even though they should
be present as NULL values.

This is a simplified reproduction that avoids the materialization bug.
"""

from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

# Create Spark session
spark = SparkSession.builder.appName("join_column_loss_bug").getOrCreate()

print("=" * 80)
print("Reproducing sparkless join column loss bug")
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

# Create empty DataFrame (processed_diagnoses - 0 rows)
print("\n2. Creating empty DataFrame (processed_diagnoses - 0 rows)...")
empty_diagnoses_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("is_chronic", BooleanType(), True),
    StructField("risk_level", StringType(), True),
])
empty_diagnoses_df = spark.createDataFrame([], empty_diagnoses_schema)
print(f"   ✅ Created empty DataFrame: {empty_diagnoses_df.count()} rows")
print(f"   Columns: {empty_diagnoses_df.columns}")

# Aggregate empty diagnoses DataFrame (as test does)
print("\n3. Aggregating empty diagnoses DataFrame...")
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
print(f"   Columns in diagnosis_metrics: {diagnosis_metrics.columns}")

# Check if we can access the column before joining
print("\n4. Checking if 'risk_score_sum' is accessible in diagnosis_metrics...")
try:
    # Just check the schema without materializing
    schema = diagnosis_metrics.schema
    field_names = [field.name for field in schema.fields]
    print(f"   Schema fields: {field_names}")
    if "risk_score_sum" in field_names:
        print("   ✅ 'risk_score_sum' exists in schema")
    else:
        print("   ❌ 'risk_score_sum' missing from schema!")
except Exception as e:
    print(f"   ❌ Error checking schema: {type(e).__name__}: {e}")

# Try a simple join with non-empty DataFrame first
print("\n5. Testing join with non-empty DataFrame (to see if join works)...")
# Create a simple non-empty aggregated DataFrame
simple_data = [("PAT-001", "high", True)]
simple_df = spark.createDataFrame(
    simple_data,
    ["patient_id", "risk_level", "is_chronic"]
)
simple_agg = simple_df.groupBy("patient_id").agg(
    F.count("*").alias("total_diagnoses"),
    F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias("chronic_conditions"),
    F.sum(
        F.when(F.col("risk_level") == "high", 3)
        .when(F.col("risk_level") == "medium", 2)
        .otherwise(1)
    ).alias("risk_score_sum"),
)
print(f"   Simple agg columns: {simple_agg.columns}")

try:
    simple_join = patients_df.join(simple_agg, "patient_id", "left")
    print(f"   ✅ Simple join succeeded")
    print(f"   Simple join columns: {simple_join.columns}")
    if "risk_score_sum" in simple_join.columns:
        print("   ✅ 'risk_score_sum' present in simple join")
    else:
        print("   ❌ 'risk_score_sum' missing in simple join!")
except Exception as e:
    print(f"   ❌ Simple join failed: {type(e).__name__}: {e}")

# Now try the problematic join with empty aggregated DataFrame
print("\n6. Testing join with EMPTY aggregated DataFrame (the bug scenario)...")
print("   This is where the bug occurs - joining with empty aggregated DataFrame")
try:
    # Don't materialize yet, just build the join
    result = patients_df.join(diagnosis_metrics, "patient_id", "left")
    print(f"   ✅ Join operation created (not materialized yet)")
    
    # Check schema before materialization
    try:
        schema = result.schema
        field_names = [field.name for field in schema.fields]
        print(f"   Schema fields (before materialization): {field_names}")
        if "risk_score_sum" in field_names:
            print("   ✅ 'risk_score_sum' exists in schema")
        else:
            print("   ❌ BUG: 'risk_score_sum' missing from schema!")
            print("   This is the bug - column is lost even in schema")
    except Exception as e:
        print(f"   ❌ Error checking schema: {type(e).__name__}: {e}")
    
    # Try to use the column (this triggers materialization and might fail)
    print("\n7. Attempting to use 'risk_score_sum' column...")
    try:
        result_with_score = result.withColumn(
            "overall_risk_score",
            F.coalesce(F.col("risk_score_sum"), F.lit(0))
        )
        # Try to get schema without materializing
        schema2 = result_with_score.schema
        field_names2 = [field.name for field in schema2.fields]
        print(f"   ✅ Column access succeeded")
        print(f"   Final schema fields: {field_names2}")
        if "risk_score_sum" in field_names2 or "overall_risk_score" in field_names2:
            print("   ✅ 'risk_score_sum' or 'overall_risk_score' present")
        else:
            print("   ❌ Both columns missing!")
    except AttributeError as e:
        if "risk_score_sum" in str(e):
            print(f"   ❌ BUG REPRODUCED!")
            print(f"   Error: {type(e).__name__}")
            print(f"   Message: {e}")
            print(f"\n   This is the exact bug from the test!")
            print(f"   The column 'risk_score_sum' exists in diagnosis_metrics")
            print(f"   But after joining, it's not available")
        else:
            print(f"   ❌ Different error: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"   ❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        
except Exception as e:
    print(f"   ❌ Join operation failed: {type(e).__name__}: {e}")
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

