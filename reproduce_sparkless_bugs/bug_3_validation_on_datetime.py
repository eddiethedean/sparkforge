#!/usr/bin/env python3
"""
Sparkless Bug #3: Validation Failing on Datetime Columns

Reproduces: Validation rules fail (0.0% valid) when applied to dataframes with datetime columns.

This occurs when:
- Creating datetime columns from string dates
- Applying validation rules that check datetime columns
- All rows are marked as invalid even though they should be valid
"""
from sparkless import SparkSession
from sparkless.functions import col, to_date, datediff, current_date, floor, concat, lit, when

spark = SparkSession.builder.appName("BugRepro3").getOrCreate()

# Create sample patient data
data = [
    ("p1", "John", "Doe", "1990-01-15", "M", "Caucasian", "InsuranceA", "2020-01-01"),
    ("p2", "Jane", "Smith", "1985-05-20", "F", "Hispanic", "InsuranceB", "2020-02-01"),
]
df = spark.createDataFrame(
    data,
    ["patient_id", "first_name", "last_name", "date_of_birth", "gender", "ethnicity", "insurance_provider", "registration_date"]
)

# Transform: Parse dates and calculate age
transformed = (
    df.withColumn("birth_date", to_date(col("date_of_birth"), "yyyy-MM-dd"))
    .withColumn("age", floor(datediff(current_date(), col("birth_date")) / 365.25))
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    .withColumn(
        "age_group",
        when(col("age") < 18, "pediatric")
        .when(col("age") < 65, "adult")
        .otherwise("senior"),
    )
)

print("Attempting to validate with datetime columns...")
try:
    # Validation rules that should pass but fail in sparkless
    validation_result = transformed.filter(
        col("patient_id").isNotNull() &
        col("full_name").isNotNull() &
        col("age").isNotNull() &
        (col("age") >= 0) &
        col("age_group").isNotNull()
    )
    count = validation_result.count()
    print(f"✅ Success: {count} rows passed validation")
    
    # Check if all rows are invalid (the bug)
    total = transformed.count()
    if count == 0 and total > 0:
        print(f"❌ BUG: All {total} rows failed validation when they should pass")
        raise Exception("Validation bug: All rows marked invalid")
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e).__name__}")
    raise

