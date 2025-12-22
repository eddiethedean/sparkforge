"""
Bug Reproduction: PipelineBuilder internal column access in sparkless

This script tries to mimic what PipelineBuilder does internally that causes
the column tracking bug. The error occurs when PipelineBuilder processes
the transform output and tries to access columns.

The key is to find what PipelineBuilder does differently in sparkless vs PySpark.
"""

import os
import sys
sys.path.insert(0, 'src')

from pyspark.sql import SparkSession, functions as F
from pipeline_builder.pipeline.builder import PipelineBuilder

def test_with_pipeline_builder_sparkless():
    """Test with PipelineBuilder in sparkless - should fail."""
    print("=" * 80)
    print("TEST 1: PipelineBuilder in SPARKLESS (Mock Mode)")
    print("=" * 80)
    
    os.environ['SPARK_MODE'] = 'mock'
    
    # Import sparkless functions
    from sparkless import SparkSession as SparklessSession, functions as SparklessF
    
    spark = SparklessSession.builder.appName("sparkless_pipeline").getOrCreate()
    
    # Create test data
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
        "impression_date",
        "campaign_id",
        "customer_id",
        "channel",
        "ad_id",
        "device_type",
        "cost_per_impression"
    ])
    
    print("\n1. Creating PipelineBuilder:")
    builder = PipelineBuilder(
        spark=spark,
        schema="bronze",
        functions=SparklessF,
        min_bronze_rate=95.0,
        min_silver_rate=98.0,
        min_gold_rate=99.0,
        verbose=False,
    )
    
    # Add bronze step
    builder.with_bronze_rules(
        name="raw_impressions",
        rules={
            "impression_id": ["not_null"],
            "impression_date": ["not_null"],
        },
    )
    
    # Add silver transform - this is where the bug occurs
    def processed_impressions_transform(spark, df, silvers):
        """Exact transform from test_marketing_pipeline.py"""
        return (
            df.withColumn(
                "impression_date_parsed",
                SparklessF.to_timestamp(
                    SparklessF.regexp_replace(SparklessF.col("impression_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", SparklessF.hour(SparklessF.col("impression_date_parsed")))
            .withColumn("day_of_week", SparklessF.dayofweek(SparklessF.col("impression_date_parsed")))
            .withColumn("is_mobile", SparklessF.when(SparklessF.col("device_type") == "mobile", True).otherwise(False))
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",  # New column, original impression_date is dropped
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
            )
        )
    
    builder.add_silver_transform(
        name="processed_impressions",
        source_bronze="raw_impressions",
        transform=processed_impressions_transform,
        rules={
            "impression_id": ["not_null"],
            "impression_date_parsed": ["not_null"],  # Validates the new column
            "campaign_id": ["not_null"],
            "customer_id": ["not_null"],
        },
        table_name="processed_impressions",
    )
    
    print("   ✅ PipelineBuilder configured")
    
    print("\n2. Executing pipeline (this is where the bug occurs):")
    try:
        result = builder.execute()
        
        print(f"\n   Pipeline Status: {result.status.value}")
        print(f"   Successful Steps: {result.metrics.successful_steps}")
        print(f"   Failed Steps: {result.metrics.failed_steps}")
        
        if "processed_impressions" in result.silver_results:
            silver = result.silver_results["processed_impressions"]
            print(f"\n   Silver Step 'processed_impressions':")
            print(f"     Status: {silver.get('status')}")
            print(f"     Rows Processed: {silver.get('rows_processed', 0)}")
            
            if silver.get('status') == 'failed':
                print(f"     Error: {silver.get('error', 'No error message')}")
                print("\n   ❌ BUG CONFIRMED: Step failed in sparkless!")
        
        if result.errors:
            print(f"\n   Errors ({len(result.errors)}):")
            for error in result.errors[:3]:
                print(f"     - {error}")
                if "impression_date" in error:
                    print("       ⚠️  THIS IS THE BUG - trying to access dropped column!")
        
        return result
        
    except Exception as e:
        print(f"\n   ❌ ERROR during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        spark.stop()

def test_with_pipeline_builder_pyspark():
    """Test with PipelineBuilder in PySpark - should work."""
    print("\n" + "=" * 80)
    print("TEST 2: PipelineBuilder in PYSPARK (Real Mode)")
    print("=" * 80)
    
    os.environ['SPARK_MODE'] = 'real'
    
    spark = SparkSession.builder \
        .appName("pyspark_pipeline") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    # Create same test data
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
        "impression_date",
        "campaign_id",
        "customer_id",
        "channel",
        "ad_id",
        "device_type",
        "cost_per_impression"
    ])
    
    print("\n1. Creating PipelineBuilder:")
    builder = PipelineBuilder(
        spark=spark,
        schema="bronze",
        functions=F,
        min_bronze_rate=95.0,
        min_silver_rate=98.0,
        min_gold_rate=99.0,
        verbose=False,
    )
    
    builder.with_bronze_rules(
        name="raw_impressions",
        rules={
            "impression_id": ["not_null"],
            "impression_date": ["not_null"],
        },
    )
    
    def processed_impressions_transform(spark, df, silvers):
        """Same transform"""
        return (
            df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
            .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
            .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
            )
        )
    
    builder.add_silver_transform(
        name="processed_impressions",
        source_bronze="raw_impressions",
        transform=processed_impressions_transform,
        rules={
            "impression_id": ["not_null"],
            "impression_date_parsed": ["not_null"],
            "campaign_id": ["not_null"],
            "customer_id": ["not_null"],
        },
        table_name="processed_impressions",
    )
    
    print("   ✅ PipelineBuilder configured")
    
    print("\n2. Executing pipeline (this should work in PySpark):")
    try:
        result = builder.execute()
        
        print(f"\n   Pipeline Status: {result.status.value}")
        print(f"   Successful Steps: {result.metrics.successful_steps}")
        print(f"   Failed Steps: {result.metrics.failed_steps}")
        
        if "processed_impressions" in result.silver_results:
            silver = result.silver_results["processed_impressions"]
            print(f"\n   Silver Step 'processed_impressions':")
            print(f"     Status: {silver.get('status')}")
            print(f"     Rows Processed: {silver.get('rows_processed', 0)}")
            
            if silver.get('status') == 'completed':
                print("\n   ✅ SUCCESS: Step completed in PySpark!")
        
        return result
        
    except Exception as e:
        print(f"\n   ❌ ERROR during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        spark.stop()

def main():
    """Run both tests."""
    print("\n" + "=" * 80)
    print("REPRODUCING: PipelineBuilder column tracking bug")
    print("=" * 80)
    print("\nThis uses the actual PipelineBuilder to demonstrate the bug.")
    print("The same pipeline fails in sparkless but works in PySpark.\n")
    
    sparkless_result = test_with_pipeline_builder_sparkless()
    pyspark_result = test_with_pipeline_builder_pyspark()
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if sparkless_result and sparkless_result.errors:
        print("\nsparkless (Mock Mode):")
        print("  ❌ FAILED")
        for error in sparkless_result.errors[:2]:
            if "impression_date" in error:
                print(f"  Error: {error[:150]}")
                print("  ⚠️  This is the bug - trying to access dropped column!")
    
    if pyspark_result and pyspark_result.status.value == "completed":
        print("\nPySpark (Real Mode):")
        print("  ✅ PASSED")
        print("  This proves the code is correct and the bug is in sparkless!")

if __name__ == "__main__":
    main()

