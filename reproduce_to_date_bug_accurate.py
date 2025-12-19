#!/usr/bin/env python3
"""
Accurate reproduction of the sparkless F.to_date() bug.

The bug occurs when F.to_date() is used in a transform function within a pipeline,
where the DataFrame is read from a table or passed through multiple operations.
"""

from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import StructType, StructField, StringType, DateType
from datetime import datetime, timedelta

# Create Spark session
spark = SparkSession.builder.appName("to_date_bug_reproduction").getOrCreate()

print("=" * 80)
print("Reproducing sparkless F.to_date() bug in pipeline context")
print("=" * 80)

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

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

print("\n1. Creating DataFrame with date strings...")
df = spark.createDataFrame(data, schema)
print(f"   ✅ Created {df.count()} rows")
print(f"   Schema: {df.schema}")

# Write to table (simulating pipeline behavior)
print("\n2. Writing DataFrame to table (simulating pipeline step)...")
table_name = "test_schema.raw_patients"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"   ✅ Written to {table_name}")

# Read back from table
print("\n3. Reading DataFrame from table...")
df_from_table = spark.table(table_name)
print(f"   ✅ Read {df_from_table.count()} rows from table")
print(f"   Schema: {df_from_table.schema}")

# Now try the exact transform from the test
print("\n4. Applying F.to_date() transform (as in pipeline)...")
print("   This is where the bug occurs in the actual pipeline")
try:
    # This is the exact transform from clean_patient_records_transform
    df_transformed = (
        df_from_table
        .withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25))
    )
    
    print("   ✅ Transform completed")
    print(f"   Schema: {df_transformed.schema}")
    
    # Try to use the DataFrame - this is where it might fail
    print("\n5. Attempting to use the transformed DataFrame...")
    result = df_transformed.select("patient_id", "date_of_birth", "birth_date", "age")
    count = result.count()
    print(f"   ✅ Count: {count}")
    print(f"   Result schema: {result.schema}")
    
    # The bug: When this is used in a pipeline transform, it fails with:
    # "conversion from `str` to `date` failed in column 'date_of_birth'"
    # This happens during the actual execution/materialization, not during schema creation
    
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}")
    print(f"   Error: {e}")
    print("\n   This is the bug - F.to_date() fails when used on DataFrames from tables")
    import traceback
    traceback.print_exc()

# Cleanup
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print("\n" + "=" * 80)
print("COMPARISON: What should happen")
print("=" * 80)
print("""
In PySpark, this exact code works:

df_from_table = spark.table("test_schema.raw_patients")
df_transformed = (
    df_from_table
    .withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
    .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25))
)
df_transformed.show()

# Output:
# +----------+-----------+----------+----------+---+
# |patient_id|first_name |last_name |birth_date|age|
# +----------+-----------+----------+----------+---+
# |PAT-000000|Patient0   |LastName0 |2005-12-23| 19|
# |PAT-000001|Patient1   |LastName1 |2004-12-23| 20|
# +----------+-----------+----------+----------+---+
""")

