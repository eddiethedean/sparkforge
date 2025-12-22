#!/usr/bin/env python3
"""
Sparkless Bug #5: Column Rename/Transform Validation Mismatch

Reproduces: unable to find column "transaction_date_parsed"; valid columns: ["id", "customer_id", "date", ...]

This occurs when:
- Renaming a column (date -> transaction_date_parsed)
- Creating a new column with transformation
- Validation rules reference the new column name
- But sparkless validation sees the old column structure
"""
from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace, lit, coalesce, length, trim, when

spark = SparkSession.builder.appName("BugRepro5").getOrCreate()

# Create sample transaction data
data = [
    ("rec1", "cust1", "2024-01-15T10:30:00", 100.0, "completed", "electronics", "US"),
    ("rec2", "cust2", "2024-01-15T11:15:00", 200.0, "pending", "clothing", "CA"),
]
df = spark.createDataFrame(
    data,
    ["record_id", "cust_id", "date", "value", "transaction_status", "item_type", "location"]
)

# Transform: Rename columns and parse datetime
transformed = (
    df.withColumn(
        "transaction_date_parsed",
        to_timestamp(
            regexp_replace(col("date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumnRenamed("record_id", "id")
    .withColumnRenamed("cust_id", "customer_id")
    .withColumnRenamed("value", "amount")
    .withColumnRenamed("transaction_status", "status")
    .withColumnRenamed("item_type", "category")
    .withColumnRenamed("location", "region")
    .withColumn("quality_score", lit(100).cast("int"))
    .withColumn("quality_status", lit("excellent"))
    .withColumn("has_null_customer", lit(False))
    .withColumn("has_invalid_amount", col("amount") <= 0)
    .withColumn("has_empty_category", length(trim(col("category"))) == 0)
    .select(
        "id",
        "customer_id",
        "transaction_date_parsed",  # This column should exist
        "amount",
        "status",
        "category",
        "region",
        "quality_score",
        "quality_status",
        "has_null_customer",
        "has_invalid_amount",
        "has_empty_category",
    )
)

print("Attempting to validate with renamed/transformed columns...")
try:
    # Try to validate using the new column name
    validation_result = transformed.filter(
        col("id").isNotNull() &
        col("customer_id").isNotNull() &
        col("transaction_date_parsed").isNotNull()  # This should work
    )
    count = validation_result.count()
    print(f"✅ Success: {count} rows")
    
    # Check what columns actually exist
    actual_columns = transformed.columns
    print(f"Actual columns: {actual_columns}")
    
    if "transaction_date_parsed" not in actual_columns:
        print("❌ BUG: transaction_date_parsed column missing from output")
        if "date" in actual_columns:
            print("   Original 'date' column still present instead")
        raise Exception("Column transformation bug: expected column not found")
        
except Exception as e:
    error_msg = str(e)
    if "transaction_date_parsed" in error_msg or "unable to find column" in error_msg.lower():
        print(f"❌ BUG: Column validation mismatch - {e}")
        print("This works in PySpark but fails in sparkless")
        raise
    else:
        print(f"❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        raise

