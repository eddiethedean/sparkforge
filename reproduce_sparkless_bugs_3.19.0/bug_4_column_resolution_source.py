"""
Bug 4: unable to find column "source"

This reproduces the exact bug from the data quality pipeline test where
data_quality_metrics fails with:
  "unable to find column 'source'; valid columns: [...]"

The column is created with F.lit("source_a") but validation can't find it.

Version: sparkless 3.19.0
"""

from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("bug_column_resolution_source").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: unable to find column 'source'")
print("=" * 80)
print("This reproduces the exact bug from test_data_quality_pipeline.py")
print()

# Create test data similar to data quality pipeline
print("STEP 1: Create test data (normalized source)")
print("-" * 80)
data = []
for i in range(80):
    data.append({
        "id": f"ID-{i:08d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "amount": round(10.0 + (i % 100), 2),
        "quality_score": round(70 + (i % 30), 1),
        "quality_status": ["excellent", "good", "fair", "poor"][i % 4],
        "has_null_customer": False,
        "has_invalid_amount": False,
        "has_empty_category": False,
    })

normalized_df = spark.createDataFrame(data, [
    "id",
    "customer_id",
    "amount",
    "quality_score",
    "quality_status",
    "has_null_customer",
    "has_invalid_amount",
    "has_empty_category",
])

print(f"✅ Created DataFrame with {normalized_df.count()} rows")

# Apply EXACT transform from test (data_quality_metrics_transform)
print("\nSTEP 2: Apply transform (EXACT from test_data_quality_pipeline.py)")
print("-" * 80)
print("Transform: data_quality_metrics_transform")
print("  1. Aggregates quality metrics")
print("  2. Creates 'source' column with F.lit('source_a')")
print()

try:
    quality_df = (
        normalized_df.agg(
            F.count("*").alias("total_records"),
            F.avg("quality_score").alias("avg_quality_score"),
            F.sum(F.when(F.col("quality_status") == "excellent", 1).otherwise(0)).alias("excellent_count"),
            F.sum(F.when(F.col("quality_status") == "good", 1).otherwise(0)).alias("good_count"),
            F.sum(F.when(F.col("quality_status") == "fair", 1).otherwise(0)).alias("fair_count"),
            F.sum(F.when(F.col("quality_status") == "poor", 1).otherwise(0)).alias("poor_count"),
            F.sum(F.when(F.col("has_null_customer"), 1).otherwise(0)).alias("null_customer_count"),
            F.sum(F.when(F.col("has_invalid_amount"), 1).otherwise(0)).alias("invalid_amount_count"),
            F.sum(F.when(F.col("has_empty_category"), 1).otherwise(0)).alias("empty_category_count"),
        )
        .withColumn("source", F.lit("source_a"))  # Create 'source' column
    )
    
    print(f"✅ Transform completed")
    print(f"   Columns: {quality_df.columns}")
    print(f"   'source' in columns: {'source' in quality_df.columns}")
    
    # Check if we can access the column
    print("\nSTEP 3: Test column access")
    print("-" * 80)
    
    try:
        # Try to select the column
        result = quality_df.select("source", "total_records").collect()
        print(f"✅ select() succeeded: {result}")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ select() failed: {error_msg[:300]}")
        
        if "unable to find column" in error_msg.lower() and "source" in error_msg:
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: unable to find column 'source'")
            print("=" * 80)
            print("This matches the error in test_data_quality_pipeline.py:")
            print("  'unable to find column \"source\"; valid columns: [...]'")
            print("\nRoot cause:")
            print("  - Column 'source' is created with F.lit('source_a')")
            print("  - Column exists in DataFrame.columns")
            print("  - But sparkless cannot resolve it during operations")
            print("  - This might be a column tracking issue after aggregation")
    
    # Try validation (this is where the error occurs in the test)
    print("\nSTEP 4: Test validation (THIS TRIGGERS THE BUG)")
    print("-" * 80)
    print("Validation rules:")
    print("  source: ['not_null']")
    print("  avg_quality_score: [['gte', 0], ['lte', 100]]")
    print()
    
    try:
        # Build validation predicate
        validation_predicate = (
            F.col("source").isNotNull() &
            (F.col("avg_quality_score") >= 0) &
            (F.col("avg_quality_score") <= 100)
        )
        
        valid_df = quality_df.filter(validation_predicate)
        count = valid_df.count()
        print(f"✅ Validation succeeded: {count} rows")
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ BUG REPRODUCED: {error_msg[:400]}")
        
        if "unable to find column" in error_msg.lower() and "source" in error_msg:
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: unable to find column 'source' during validation")
            print("=" * 80)
            print("This matches the error in test_data_quality_pipeline.py:")
            print("  'unable to find column \"source\"; valid columns: [...]'")
            print("\nThe error occurs when:")
            print("  1. Column 'source' is created with F.lit('source_a') after aggregation")
            print("  2. Validation tries to reference the column")
            print("  3. sparkless cannot resolve the column even though it exists")
            print("\nPossible causes:")
            print("  - Column tracking issue after aggregation")
            print("  - Column resolution bug in sparkless")
            print("  - Issue with F.lit() columns in aggregated DataFrames")
        else:
            print(f"\nUnexpected error: {e}")
            import traceback
            traceback.print_exc()
    
except Exception as e:
    error_msg = str(e)
    print(f"❌ Transform failed: {error_msg[:400]}")
    import traceback
    traceback.print_exc()

spark.stop()

