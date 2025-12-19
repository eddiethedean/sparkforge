#!/usr/bin/env python3
"""
Minimal reproduction script for sparkless date conversion bug.

This script demonstrates that sparkless (Polars backend) cannot automatically
convert string dates in "YYYY-MM-DD" format to date type, even though this
is a common format that PySpark handles automatically.

Issue: sparkless cannot automatically parse common date formats like "YYYY-MM-DD"
Expected: Should automatically convert string dates to date type (like PySpark does)
Actual: Raises error requiring explicit date parsing
"""

from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType, DateType

# Create Spark session
spark = SparkSession.builder.appName("date_conversion_bug_test").getOrCreate()

print("=" * 80)
print("Reproducing sparkless date conversion bug")
print("=" * 80)

# Create schema with a date column
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("date_of_birth", StringType(), True),  # String input
])

# Create data with dates in "YYYY-MM-DD" format (common ISO format)
data = [
    ("1", "Alice", "2005-12-23"),
    ("2", "Bob", "2004-12-23"),
    ("3", "Charlie", "1996-12-25"),
]

print("\n1. Creating DataFrame with string dates in 'YYYY-MM-DD' format...")
df = spark.createDataFrame(data, schema)
print(f"   ✅ DataFrame created: {df.count()} rows")
print(f"   Schema: {df.schema}")

# Try to convert using to_date (as done in real pipelines)
print("\n2. Attempting to convert 'date_of_birth' using F.to_date()...")
try:
    from sparkless import functions as F
    
    # This is what the pipeline code does - should work but fails in sparkless
    df_with_date = df.withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
    print(f"   Schema after to_date: {df_with_date.schema}")
    # Note: The schema shows birth_date as StringType, not DateType - this is wrong!
    if "DateType" not in str(df_with_date.schema):
        print("   ⚠️  WARNING: birth_date is StringType, not DateType - conversion didn't work!")
    
    # Try to use the column (this will fail)
    print("   Attempting to use the date column...")
    df_with_date.show()
    
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}")
    print(f"   Error: {e}")
    print("\n   This is the bug - sparkless cannot parse 'YYYY-MM-DD' dates even with explicit format")
    print("   PySpark handles this correctly with F.to_date()")

# Also try the cast operation
print("\n3. Attempting to cast 'date_of_birth' from string to date...")
try:
    from sparkless import functions as F
    
    # This should work (like PySpark does), but may also fail in sparkless
    df_with_date2 = df.withColumn("date_of_birth", F.col("date_of_birth").cast("date"))
    print("   ✅ Cast succeeded!")
    print(f"   Result schema: {df_with_date2.schema}")
    df_with_date2.show()
    
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}")
    print(f"   Error: {e}")
    print("\n   This is also a bug - sparkless cannot automatically parse 'YYYY-MM-DD' dates")
    print("   PySpark handles this automatically, but sparkless requires explicit parsing")

# Show what PySpark would do (for comparison)
print("\n" + "=" * 80)
print("COMPARISON: What PySpark does (for reference)")
print("=" * 80)
print("""
In PySpark, the same code works automatically:

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("test").getOrCreate()
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("date_of_birth", StringType(), True),
])

data = [("1", "Alice", "2005-12-23"), ("2", "Bob", "2004-12-23")]
df = spark.createDataFrame(data, schema)

# This works automatically in PySpark:
df_with_date = df.withColumn("date_of_birth", F.col("date_of_birth").cast("date"))
df_with_date.show()

# Output:
# +---+-------+-------------+
# | id|   name|date_of_birth|
# +---+-------+-------------+
# |  1|  Alice|   2005-12-23|
# |  2|    Bob|   2004-12-23|
# +---+-------+-------------+
""")

print("\n" + "=" * 80)
print("WORKAROUND (what users must do currently)")
print("=" * 80)
print("""
Users must explicitly parse dates using strptime or to_date with format:

# Workaround 1: Using strptime
df_with_date = df.withColumn(
    "date_of_birth",
    F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
)

# Workaround 2: Using str.strptime (Polars-style)
df_with_date = df.withColumn(
    "date_of_birth",
    F.col("date_of_birth").str.strptime("date", "%Y-%m-%d")
)

This is inconvenient and breaks compatibility with PySpark code.
""")

