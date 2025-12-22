#!/usr/bin/env python3
"""
Sparkless Bug #4: Column Reference After Transformation

Reproduces: 'DataFrame' object has no attribute 'snapshot_date'. Available columns: ... snapshot_date_parsed, ...

This occurs when:
- Transforming a column (e.g., snapshot_date -> snapshot_date_parsed)
- Dropping the original column
- Later operations try to reference the original column name
- Sparkless doesn't handle this the same way PySpark does
"""
from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace, when, lit

spark = SparkSession.builder.appName("BugRepro4").getOrCreate()

# Create sample inventory data
data = [
    ("inv1", "prod1", "wh1", "2024-01-15T10:30:00", 100, 20, 50, 200),
    ("inv2", "prod2", "wh2", "2024-01-15T11:15:00", 150, 30, 75, 300),
]
df = spark.createDataFrame(
    data,
    ["inventory_id", "product_id", "warehouse_id", "snapshot_date", "quantity_on_hand", "quantity_reserved", "reorder_point", "max_stock"]
)

# Transform: Parse datetime and calculate derived columns
transformed = (
    df.withColumn(
        "snapshot_date_parsed",
        to_timestamp(
            regexp_replace(col("snapshot_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("available_quantity", col("quantity_on_hand") - col("quantity_reserved"))
    .withColumn("is_low_stock", when(col("available_quantity") < col("reorder_point"), lit(True)).otherwise(lit(False)))
    .withColumn("is_overstocked", when(col("quantity_on_hand") > col("max_stock"), lit(True)).otherwise(lit(False)))
    .withColumn(
        "stock_level",
        when(col("is_low_stock"), "low")
        .when(col("is_overstocked"), "high")
        .otherwise("normal"),
    )
    # Drop the original column (this is where the issue occurs)
    .drop("snapshot_date")
)

print("Attempting operations after dropping original column...")
try:
    # This should work - we're using the parsed column
    result1 = transformed.select("inventory_id", "snapshot_date_parsed", "available_quantity")
    count1 = result1.count()
    print(f"✅ Success with parsed column: {count1} rows")
    
    # This should fail gracefully in PySpark but may cause issues in sparkless
    # Try to access a column that was dropped (this might be referenced internally)
    result2 = transformed.filter(col("snapshot_date_parsed").isNotNull())
    count2 = result2.count()
    print(f"✅ Success with filter on parsed column: {count2} rows")
    
except AttributeError as e:
    if "snapshot_date" in str(e):
        print(f"❌ BUG: Column reference error - {e}")
        print("This works in PySpark but fails in sparkless")
        raise
    else:
        raise
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e).__name__}")
    raise

