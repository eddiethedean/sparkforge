# Issue: sparkless cannot automatically convert string dates in "YYYY-MM-DD" format to date type

## Summary

sparkless (using Polars backend) cannot automatically convert string dates in common formats like "YYYY-MM-DD" to `DateType`, even though PySpark handles this automatically. This breaks compatibility with PySpark code and requires users to explicitly parse dates.

## Environment

- **sparkless version:** 3.17.6
- **Python version:** 3.11.13
- **Operating System:** macOS (darwin 24.6.0)
- **Backend:** Polars (default sparkless behavior)

## Minimal Reproduction

```python
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType
from sparkless import functions as F

# Create Spark session
spark = SparkSession.builder.appName("date_conversion_bug_test").getOrCreate()

# Create schema with a date column (as string input)
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

# Create DataFrame
df = spark.createDataFrame(data, schema)
print(f"DataFrame created: {df.count()} rows")

# Try to convert using to_date with format (as done in real pipelines)
# This should work (like PySpark does), but fails in sparkless
from sparkless import functions as F
df_with_date = df.withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
```

## Expected Behavior

The `F.to_date()` function with an explicit format should parse the "YYYY-MM-DD" format string and convert it to a date type, just like PySpark does:

```python
# In PySpark, this works correctly:
from pyspark.sql import functions as F
df_with_date = df.withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
df_with_date.show()

# Output:
# +---+-------+-------------+-----------+
# | id|   name|date_of_birth| birth_date|
# +---+-------+-------------+-----------+
# |  1|  Alice|   2005-12-23| 2005-12-23|
# |  2|    Bob|   2004-12-23| 2004-12-23|
# +---+-------+-------------+-----------+
```

The `birth_date` column should be of type `DateType`, not `StringType`.

## Actual Behavior

sparkless fails when using `F.to_date()` with an explicit format string:

```python
from sparkless import functions as F
df_with_date = df.withColumn("birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
# The schema shows birth_date as StringType (wrong!), not DateType
# When trying to use the column, it fails with:

# Error: conversion from `str` to `date` failed in column 'date_of_birth' for 3 out of 3 values: ["2005-12-23", "2004-12-23", "1996-12-25"]

# You might want to try:
# - setting `strict=False` to set values that cannot be converted to `null`
# - using `str.strptime`, `str.to_date`, or `str.to_datetime` and providing a format string
```

**Note:** Interestingly, `cast("date")` works, but `F.to_date()` with format string does not.

## Impact

1. **Breaks PySpark Compatibility:** Code that works in PySpark fails in sparkless
2. **Requires Workarounds:** Users must explicitly parse dates:
   ```python
   # Workaround required:
   df_with_date = df.withColumn(
       "date_of_birth",
       F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
   )
   ```
3. **Affects Real-World Pipelines:** This breaks 5 complete pipeline tests in our codebase that use date columns

## Real-World Example

In our pipeline framework, we have healthcare, marketing, data quality, streaming, and supply chain pipelines that all fail in sparkless due to this issue. The same code works perfectly in PySpark.

**Failing Test Example:**
```python
# Create healthcare data with date_of_birth column
patients_df = data_generator.create_healthcare_patients(spark_session, num_patients=40)
# Contains dates like "2005-12-23" in string format

# Pipeline tries to process this data
# Fails with: conversion from `str` to `date` failed
```

## Workaround

Users must explicitly parse dates:

```python
# Option 1: Using to_date with format
df_with_date = df.withColumn(
    "date_of_birth",
    F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
)

# Option 2: Using str.strptime (Polars-style)
df_with_date = df.withColumn(
    "date_of_birth",
    F.col("date_of_birth").str.strptime("date", "%Y-%m-%d")
)
```

## Requested Fix

Please implement automatic date parsing for common formats (especially "YYYY-MM-DD") in `cast("date")` operations to match PySpark behavior. This would:

1. Improve compatibility with PySpark code
2. Reduce the need for explicit date parsing workarounds
3. Make sparkless more user-friendly for date operations

**Specific areas to investigate:**
1. `cast("date")` operation on string columns
2. Automatic format detection for common date formats (YYYY-MM-DD, MM/DD/YYYY, etc.)
3. Compatibility with PySpark's date casting behavior

## Additional Context

- **Test Results:** 5 complete pipeline tests fail in sparkless but pass in PySpark
- **Error Message:** The current error message is helpful but suggests workarounds rather than fixing the root cause
- **Format Support:** "YYYY-MM-DD" is the ISO 8601 standard format and should be supported by default

---

**Priority:** High (breaks compatibility with PySpark and affects real-world pipelines)  
**Severity:** High (requires workarounds for common date operations)

