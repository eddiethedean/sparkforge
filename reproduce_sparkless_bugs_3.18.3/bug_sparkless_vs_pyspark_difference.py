"""
Bug Reproduction: sparkless vs PySpark difference in column tracking

This script demonstrates the actual bug by showing different behavior
between sparkless and PySpark when processing transforms that drop columns.

The bug: sparkless tries to access dropped columns internally, causing failures.
PySpark handles this correctly.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_sparkless():
    """Test in sparkless - demonstrates the bug."""
    print("=" * 80)
    print("TEST 1: SPARKLESS (Mock Mode)")
    print("=" * 80)
    
    os.environ['SPARK_MODE'] = 'mock'
    
    # Import sparkless functions
    from sparkless import SparkSession as SparklessSession, functions as SparklessF
    
    spark = SparklessSession.builder.appName("sparkless_test").getOrCreate()
    
    # Create test data
    data = [
        ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "mobile", 0.05),
        ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "mobile", 0.03),
        ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3", "web", "desktop", 0.04),
    ]
    
    df = spark.createDataFrame(data, [
        "impression_id",
        "impression_date",  # This will be dropped
        "campaign_id",
        "customer_id",
        "channel",
        "device_type",
        "cost_per_impression"
    ])
    
    print("\n1. Original DataFrame:")
    print(f"   Columns: {df.columns}")
    print(f"   Has 'impression_date': {'impression_date' in df.columns}")
    
    # Apply transform that drops the original column
    print("\n2. Applying transform (drops 'impression_date'):")
    
    df_transformed = (
        df.withColumn(
            "impression_date_parsed",
            SparklessF.to_timestamp(
                SparklessF.regexp_replace(SparklessF.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", SparklessF.hour(SparklessF.col("impression_date_parsed")))
        .withColumn("day_of_week", SparklessF.dayofweek(SparklessF.col("impression_date_parsed")))
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",  # New column
            "hour_of_day",
            "day_of_week",
            "channel",
            "cost_per_impression",
            "device_type",
        )
    )
    
    print(f"   Columns after transform: {df_transformed.columns}")
    print(f"   Has 'impression_date': {'impression_date' in df_transformed.columns}")
    print(f"   Has 'impression_date_parsed': {'impression_date_parsed' in df_transformed.columns}")
    
    # Try operations that might trigger internal column access
    print("\n3. Testing operations that might trigger the bug:")
    
    try:
        # Try to get column names (this might trigger internal processing)
        cols = df_transformed.columns
        print(f"   ✅ Can get columns: {len(cols)} columns")
    except Exception as e:
        print(f"   ❌ ERROR getting columns: {e}")
    
    try:
        # Try to get schema
        schema = df_transformed.schema
        print(f"   ✅ Can get schema: {len(schema.fields)} fields")
    except Exception as e:
        print(f"   ❌ ERROR getting schema: {e}")
    
    try:
        # Try to count (forces evaluation)
        count = df_transformed.count()
        print(f"   ✅ Count succeeded: {count} rows")
    except Exception as e:
        print(f"   ❌ ERROR during count: {e}")
        print("   This might reveal the bug!")
    
    try:
        # Try to collect (forces full evaluation)
        collected = df_transformed.collect()
        print(f"   ✅ Collection succeeded: {len(collected)} rows")
    except Exception as e:
        print(f"   ❌ ERROR during collection: {e}")
        print("   This might reveal the bug!")
    
    # Try to access columns in different ways
    print("\n4. Testing different column access methods:")
    
    try:
        # Method 1: Column expression
        result1 = df_transformed.select(SparklessF.col("impression_date_parsed")).limit(1).collect()
        print("   ✅ Method 1 (F.col()): Works")
    except Exception as e:
        print(f"   ❌ Method 1 failed: {e}")
    
    try:
        # Method 2: String column name
        result2 = df_transformed.select("impression_date_parsed").limit(1).collect()
        print("   ✅ Method 2 (string): Works")
    except Exception as e:
        print(f"   ❌ Method 2 failed: {e}")
    
    try:
        # Method 3: Attribute access (should fail for dropped column)
        result3 = df_transformed.impression_date
        print("   ❌ Method 3 (attribute): Should have failed!")
    except AttributeError as e:
        print(f"   ✅ Method 3 (attribute): Expected error - {str(e)[:100]}")
    
    # Try to access the dropped column (this should fail)
    print("\n5. Trying to access dropped column 'impression_date':")
    
    try:
        # Try column expression on dropped column
        result = df_transformed.select(SparklessF.col("impression_date")).limit(1).collect()
        print("   ❌ Should have failed - column was dropped!")
    except Exception as e:
        print(f"   ✅ Expected error (column expression): {str(e)[:100]}")
    
    try:
        # Try attribute access on dropped column
        result = df_transformed.impression_date
        print("   ❌ Should have failed - column was dropped!")
    except AttributeError as e:
        error_msg = str(e)
        print(f"   ✅ Expected AttributeError: {error_msg[:150]}")
        
        # Check if this is the error from the test
        if "'DataFrame' object has no attribute 'impression_date'" in error_msg:
            print("\n   ⚠️  THIS IS THE ERROR FROM THE TEST!")
            print("   This suggests sparkless is trying attribute access internally!")
    
    # Try operations that might trigger internal column tracking
    print("\n6. Testing operations that might trigger internal column tracking:")
    
    try:
        # Try to filter (might trigger column validation)
        filtered = df_transformed.filter(SparklessF.col("impression_date_parsed").isNotNull())
        count = filtered.count()
        print(f"   ✅ Filter succeeded: {count} rows")
    except Exception as e:
        print(f"   ❌ ERROR during filter: {e}")
        print("   This might reveal the bug!")
    
    try:
        # Try to group by (might trigger column validation)
        grouped = df_transformed.groupBy("campaign_id").count()
        result = grouped.collect()
        print(f"   ✅ GroupBy succeeded: {len(result)} groups")
    except Exception as e:
        print(f"   ❌ ERROR during groupBy: {e}")
        print("   This might reveal the bug!")
    
    spark.stop()
    return "sparkless"

def test_pyspark():
    """Test in PySpark - shows it works correctly."""
    print("\n" + "=" * 80)
    print("TEST 2: PYSPARK (Real Mode)")
    print("=" * 80)
    
    os.environ['SPARK_MODE'] = 'real'
    
    spark = SparkSession.builder \
        .appName("pyspark_test") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    # Create same test data
    data = [
        ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1", "customer_1", "web", "mobile", 0.05),
        ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2", "customer_2", "mobile", "mobile", 0.03),
        ("imp_003", "2024-01-17T09:15:22.456789", "campaign_1", "customer_3", "web", "desktop", 0.04),
    ]
    
    df = spark.createDataFrame(data, [
        "impression_id",
        "impression_date",
        "campaign_id",
        "customer_id",
        "channel",
        "device_type",
        "cost_per_impression"
    ])
    
    print("\n1. Original DataFrame:")
    print(f"   Columns: {df.columns}")
    
    # Apply same transform
    print("\n2. Applying same transform:")
    
    df_transformed = (
        df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
        .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",
            "hour_of_day",
            "day_of_week",
            "channel",
            "cost_per_impression",
            "device_type",
        )
    )
    
    print(f"   Columns after transform: {df_transformed.columns}")
    print(f"   Has 'impression_date': {'impression_date' in df_transformed.columns}")
    
    # Same operations
    print("\n3. Testing same operations:")
    
    try:
        count = df_transformed.count()
        print(f"   ✅ Count succeeded: {count} rows")
    except Exception as e:
        print(f"   ❌ ERROR during count: {e}")
    
    try:
        collected = df_transformed.collect()
        print(f"   ✅ Collection succeeded: {len(collected)} rows")
    except Exception as e:
        print(f"   ❌ ERROR during collection: {e}")
    
    try:
        filtered = df_transformed.filter(F.col("impression_date_parsed").isNotNull())
        count = filtered.count()
        print(f"   ✅ Filter succeeded: {count} rows")
    except Exception as e:
        print(f"   ❌ ERROR during filter: {e}")
    
    try:
        grouped = df_transformed.groupBy("campaign_id").count()
        result = grouped.collect()
        print(f"   ✅ GroupBy succeeded: {len(result)} groups")
    except Exception as e:
        print(f"   ❌ ERROR during groupBy: {e}")
    
    spark.stop()
    return "pyspark"

def main():
    """Run both tests and compare."""
    print("\n" + "=" * 80)
    print("REPRODUCING: sparkless vs PySpark column tracking difference")
    print("=" * 80)
    print("\nThis script shows the actual difference in behavior between")
    print("sparkless and PySpark when processing transforms that drop columns.\n")
    
    sparkless_result = test_sparkless()
    pyspark_result = test_pyspark()
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Compare the results above:

If sparkless shows errors during count/collection/filter/groupBy operations
that PySpark doesn't show, that's the bug!

The bug is that sparkless's internal processing tries to access dropped columns,
causing operations to fail, while PySpark handles this correctly.
""")

if __name__ == "__main__":
    main()

