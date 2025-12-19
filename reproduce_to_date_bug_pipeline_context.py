#!/usr/bin/env python3
"""
Reproduce the F.to_date() bug in the exact pipeline execution context.

The bug occurs when:
1. DataFrame is created with date strings
2. DataFrame is passed to a pipeline transform function
3. Transform uses F.to_date() with format string
4. The result is materialized/validated during pipeline execution

This matches the exact scenario from the failing healthcare pipeline test.
"""

from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import StructType, StructField, StringType
from datetime import datetime, timedelta

# Create Spark session
spark = SparkSession.builder.appName("to_date_bug_pipeline").getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

print("=" * 80)
print("Reproducing F.to_date() bug in pipeline execution context")
print("=" * 80)

# Create data exactly as the test does
data = []
for i in range(5):
    date_str = (datetime.now() - timedelta(days=365 * (20 + i % 60))).strftime('%Y-%m-%d')
    data.append((f'PAT-{i:06d}', f'Patient{i}', f'LastName{i}', date_str))

schema = StructType([
    StructField('patient_id', StringType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('date_of_birth', StringType(), True),
])

print("\n1. Creating DataFrame with date strings (as test does)...")
df = spark.createDataFrame(data, schema)
print(f"   ✅ Created {df.count()} rows")
print(f"   date_of_birth values: {[row[3] for row in data[:3]]}")

# Simulate the exact transform function from the test
def clean_patient_records_transform(spark, df, silvers):
    """Clean and normalize patient demographics - EXACT code from test."""
    return (
        df.withColumn(
            "birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
        )
        .withColumn(
            "age",
            F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25),
        )
        .drop("birth_date")
        .withColumn(
            "full_name",
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
        )
        .select(
            "patient_id",
            "full_name",
            "date_of_birth",
            "age",
        )
    )

print("\n2. Applying transform function (as pipeline does)...")
print("   This is the exact transform from clean_patient_records_transform")
try:
    # Call the transform function exactly as the pipeline does
    result_df = clean_patient_records_transform(spark, df, {})
    print("   ✅ Transform function returned DataFrame")
    print(f"   Schema: {result_df.schema}")
    
    # Try to materialize/validate (as pipeline validation does)
    print("\n3. Materializing DataFrame (triggering actual execution)...")
    print("   This is where the bug occurs - during materialization")
    
    # Count triggers materialization
    count = result_df.count()
    print(f"   ✅ Count: {count}")
    
    # Try to filter (as validation does)
    filtered = result_df.filter(result_df.age >= 0)
    filtered_count = filtered.count()
    print(f"   ✅ Filter: {filtered_count} rows")
    
    # Try to write to table (as pipeline does)
    print("\n4. Writing to table (as pipeline does)...")
    result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.clean_patients")
    print("   ✅ Write succeeded")
    
    # Read back and verify
    read_back = spark.table("test_schema.clean_patients")
    read_count = read_back.count()
    print(f"   ✅ Read back: {read_count} rows")
    
    print("\n   ✅ SUCCESS: No date conversion error in this context!")
    print("   Note: The bug may occur in different contexts or may have been fixed")
    
    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_schema.clean_patients")
    
except Exception as e:
    error_msg = str(e)
    if "conversion from `str` to `date`" in error_msg:
        print(f"\n   ❌ BUG REPRODUCED!")
        print(f"   Error: {type(e).__name__}")
        print(f"   Message: {error_msg}")
        print("\n   This is the exact bug reported in GitHub issue #126")
    else:
        print(f"\n   ❌ Different error: {type(e).__name__}")
        print(f"   Message: {error_msg}")

print("\n" + "=" * 80)
print("Expected Behavior (PySpark)")
print("=" * 80)
print("""
In PySpark, this exact code works without errors:

def clean_patient_records_transform(spark, df, silvers):
    return (
        df.withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25))
        .drop("birth_date")
    )

result = clean_patient_records_transform(spark, df, {})
count = result.count()  # Works without errors
""")

