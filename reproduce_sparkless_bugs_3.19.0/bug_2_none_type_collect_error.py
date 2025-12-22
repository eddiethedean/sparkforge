"""
Bug 2: 'NoneType' object has no attribute 'collect'

This reproduces the exact bug from the healthcare pipeline test where
normalized_labs and processed_diagnoses fail with:
  "'NoneType' object has no attribute 'collect'"

Version: sparkless 3.19.0
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_none_type_collect").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: 'NoneType' object has no attribute 'collect'")
print("=" * 80)
print("This reproduces the exact bug from test_healthcare_pipeline.py")
print()

# Create test data similar to healthcare pipeline
print("STEP 1: Create test data (lab results)")
print("-" * 80)
data = []
for i in range(150):
    data.append({
        "lab_id": f"LAB-{i:08d}",
        "patient_id": f"PAT-{i % 40:04d}",
        "test_date": (datetime.now() - timedelta(days=i % 365)).isoformat(),
        "test_name": ["glucose", "cholesterol", "hemoglobin"][i % 3],
        "result_value": round(70 + (i % 50), 2),
        "unit": ["mg/dL", "mg/dL", "g/dL"][i % 3],
        "reference_range": ["70-100", "0-200", "12-16"][i % 3],
    })

bronze_df = spark.createDataFrame(data, [
    "lab_id",
    "patient_id",
    "test_date",
    "test_name",
    "result_value",
    "unit",
    "reference_range",
])

print(f"✅ Created DataFrame with {bronze_df.count()} rows")

# Apply EXACT transform from test (normalize_lab_results_transform)
print("\nSTEP 2: Apply transform (EXACT from test_healthcare_pipeline.py)")
print("-" * 80)
print("Transform: normalize_lab_results_transform")
print("  1. Parses test_date to test_date_parsed")
print("  2. Calculates is_abnormal based on reference_range")
print("  3. Creates result_category")
print()

try:
    silver_df = (
        bronze_df.withColumn(
            "test_date_clean",
            F.regexp_replace(F.col("test_date"), r"\.\d+", ""),
        )
        .withColumn(
            "test_date_parsed",
            F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .drop("test_date_clean")
        .withColumn(
            "is_abnormal",
            F.when(
                (F.col("test_name") == "glucose")
                & ((F.col("result_value") < 70) | (F.col("result_value") > 100)),
                True,
            )
            .when(
                (F.col("test_name") == "cholesterol")
                & (F.col("result_value") > 200),
                True,
            )
            .when(
                (F.col("test_name") == "hemoglobin")
                & ((F.col("result_value") < 12) | (F.col("result_value") > 16)),
                True,
            )
            .otherwise(False),
        )
        .withColumn(
            "result_category",
            F.when(F.col("is_abnormal"), "abnormal").otherwise("normal"),
        )
        .select(
            "lab_id",
            "patient_id",
            "test_date_parsed",
            "test_name",
            "result_value",
            "unit",
            "is_abnormal",
            "result_category",
        )
    )
    
    print(f"✅ Transform completed")
    print(f"   Columns: {silver_df.columns}")
    
    # Try to materialize (this is where the error occurs)
    print("\nSTEP 3: Materialize DataFrame (THIS TRIGGERS THE BUG)")
    print("-" * 80)
    
    try:
        count = silver_df.count()
        print(f"✅ count() succeeded: {count} rows")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ BUG REPRODUCED: {error_msg[:400]}")
        
        if "'NoneType' object has no attribute 'collect'" in error_msg:
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: 'NoneType' object has no attribute 'collect'")
            print("=" * 80)
            print("This error occurs when:")
            print("  1. A transform operation returns None instead of a DataFrame")
            print("  2. sparkless tries to call .collect() on None")
            print("\nPossible causes:")
            print("  - to_timestamp() returns None")
            print("  - Column operations fail silently and return None")
            print("  - Internal DataFrame materialization issue")
        else:
            print(f"\nUnexpected error: {e}")
            import traceback
            traceback.print_exc()
    
    # Check if to_timestamp worked
    print("\nSTEP 4: Check to_timestamp() results")
    print("-" * 80)
    try:
        sample = silver_df.select("lab_id", "test_date_parsed", "is_abnormal").limit(5).collect()
        none_count = sum(1 for row in sample if row[1] is None)
        print(f"Sample rows:")
        for i, row in enumerate(sample):
            print(f"  Row {i}: test_date_parsed={row[1]}, is_abnormal={row[2]}")
        
        if none_count > 0:
            print(f"\n⚠️  WARNING: {none_count}/5 sample rows have None for test_date_parsed")
    except Exception as e:
        print(f"❌ Error checking results: {e}")
        import traceback
        traceback.print_exc()
        
except Exception as e:
    error_msg = str(e)
    print(f"❌ Transform failed: {error_msg[:400]}")
    import traceback
    traceback.print_exc()

spark.stop()

