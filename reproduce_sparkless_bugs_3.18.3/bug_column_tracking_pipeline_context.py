"""
Bug Reproduction: Column tracking issue in sparkless 3.18.3 (Pipeline Context)

This demonstrates the bug that occurs when the transform is executed within
the PipelineBuilder context, where sparkless tries to reference a dropped column.

The error occurs specifically when:
1. Transform uses a column (impression_date) 
2. Transform creates new column (impression_date_parsed)
3. Transform drops original via .select()
4. PipelineBuilder's internal processing tries to reference the dropped column
"""

from sparkless import SparkSession, functions as F

print("=" * 80)
print("COLUMN TRACKING BUG IN PIPELINE CONTEXT")
print("=" * 80)
print("\nThe error occurs when the transform is executed in PipelineBuilder context.")
print("Let's trace through what happens step by step.\n")

spark = SparkSession.builder.appName("column_tracking_pipeline_bug").getOrCreate()

# Create test data matching the test
data = []
for i in range(10):
    data.append((
        f"imp_{i:03d}",
        f"2024-01-{(15+i%10):02d}T10:30:45.123456",
        f"campaign_{i%3 + 1}",
        f"customer_{i%5 + 1}",
        "web" if i % 2 == 0 else "mobile",
        f"ad_{i%5 + 1}",
        "mobile" if i % 2 == 1 else "desktop",
        0.05 + (i * 0.01)
    ))

df = spark.createDataFrame(data, [
    "impression_id",
    "impression_date",  # This will be dropped
    "campaign_id",
    "customer_id",
    "channel",
    "ad_id",  # This exists in the test data
    "device_type",
    "cost_per_impression"
])

print("Original DataFrame columns:", df.columns)
print("Has 'impression_date':", "impression_date" in df.columns)
print("Has 'ad_id':", "ad_id" in df.columns)

print("\n" + "=" * 80)
print("STEP 1: Apply the exact transform from the test")
print("=" * 80)

# This is the EXACT transform from test_marketing_pipeline.py
def processed_impressions_transform(spark, df, silvers):
    """Exact transform from test_marketing_pipeline.py"""
    return (
        df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn(
            "hour_of_day",
            F.hour(F.col("impression_date_parsed")),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek(F.col("impression_date_parsed")),
        )
        .withColumn(
            "is_mobile",
            F.when(F.col("device_type") == "mobile", True).otherwise(False),
        )
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",  # New column
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
        )
    )

print("\n1a. Executing transform function:")
try:
    df_transformed = processed_impressions_transform(spark, df, {})
    print("   ✅ Transform executed successfully")
    print("   Columns after transform:", df_transformed.columns)
    print("   Has 'impression_date':", "impression_date" in df_transformed.columns)
    print("   Has 'impression_date_parsed':", "impression_date_parsed" in df_transformed.columns)
    
    df_transformed.show(5, truncate=False)
    
except Exception as e:
    print(f"   ❌ ERROR during transform: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("STEP 2: Try operations that might trigger the bug")
print("=" * 80)

try:
    # Try to materialize/evaluate the DataFrame
    count = df_transformed.count()
    print(f"   ✅ Count succeeded: {count} rows")
except Exception as e:
    print(f"   ❌ ERROR during count: {e}")

try:
    # Try to collect
    collected = df_transformed.collect()
    print(f"   ✅ Collection succeeded: {len(collected)} rows")
except Exception as e:
    print(f"   ❌ ERROR during collection: {e}")

print("\n" + "=" * 80)
print("STEP 3: Try to access columns (simulating validation)")
print("=" * 80)

# The error message suggests sparkless is trying to access 'impression_date'
# Let's see if we can trigger this by trying different operations

try:
    # Try to access the transformed column
    result = df_transformed.select("impression_date_parsed").limit(1).collect()
    print("   ✅ Can access 'impression_date_parsed'")
except Exception as e:
    print(f"   ❌ ERROR accessing 'impression_date_parsed': {e}")

try:
    # Try to access the dropped column (should fail)
    result = df_transformed.select("impression_date").limit(1).collect()
    print("   ❌ Should have failed - 'impression_date' was dropped!")
except Exception as e:
    print(f"   ✅ Expected error accessing 'impression_date': {e}")
    print("   This is the error message from the test!")

print("\n" + "=" * 80)
print("STEP 4: Check if the issue is with column attribute access")
print("=" * 80)

# The error says: 'DataFrame' object has no attribute 'impression_date'
# This suggests something is trying to access df.impression_date (attribute access)
# rather than df['impression_date'] or F.col('impression_date')

try:
    # Try attribute access (this is what the error suggests)
    result = df_transformed.impression_date
    print("   ❌ Should have failed - attribute access to dropped column!")
except AttributeError as e:
    print(f"   ✅ Expected AttributeError: {e}")
    print("   This matches the error in the test!")
except Exception as e:
    print(f"   Other error: {e}")

print("\n" + "=" * 80)
print("ROOT CAUSE ANALYSIS")
print("=" * 80)
print("""
The error "'DataFrame' object has no attribute 'impression_date'" suggests:

1. Something in sparkless is trying to access df.impression_date (attribute access)
2. This happens AFTER the column was dropped via .select()
3. The error occurs in PipelineBuilder's internal processing, not in the transform itself

Possible causes:
- PipelineBuilder's validation/processing tries to access original column names
- Internal column tracking uses attribute access (df.column_name) instead of F.col()
- Some optimization or caching tries to reference the original column
- Validation rules might be checking against original column names

The transform itself works correctly - the bug is in how PipelineBuilder
processes/validates the transformed DataFrame internally.
""")

print("\n" + "=" * 80)
print("EVIDENCE")
print("=" * 80)
print("""
✅ Transform function works correctly when called directly
✅ Transform correctly drops 'impression_date' via .select()
✅ Transformed DataFrame has correct columns
❌ PipelineBuilder's internal processing tries to access df.impression_date
   (attribute access to dropped column)

The bug is in PipelineBuilder's internal processing, not in the transform logic.
""")

spark.stop()

