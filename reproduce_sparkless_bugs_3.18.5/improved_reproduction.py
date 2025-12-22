#!/usr/bin/env python3
"""
IMPROVED REPRODUCTION: sparkless Bug with Dropped Columns

This script reproduces a bug in sparkless where operations fail when:
1. A column is used in transformations
2. The column is then dropped via .select()
3. Materialization is triggered (count, cache, collect, etc.)

The bug occurs with larger datasets (150+ rows) but NOT with small datasets (2 rows).

Run this script with: python improved_reproduction.py
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

def test_with_rows(num_rows, test_name):
    """Test the bug with a specific number of rows."""
    print(f"\n{'=' * 80}")
    print(f"TEST: {test_name} ({num_rows} rows)")
    print(f"{'=' * 80}")
    
    spark = SparkSession.builder.appName(f"bug_test_{num_rows}").getOrCreate()
    
    try:
        # Create test data
        data = []
        for i in range(num_rows):
            data.append({
                "impression_id": f"IMP-{i:08d}",
                "campaign_id": f"CAMP-{i % 10:02d}",
                "customer_id": f"CUST-{i % 40:04d}",
                "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
                "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
                "ad_id": f"AD-{i % 20:03d}",
                "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
                "device_type": ["desktop", "mobile", "tablet"][i % 3],
            })
        
        bronze_df = spark.createDataFrame(data, [
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date",  # This column will be dropped
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
        ])
        
        print(f"✅ Created DataFrame with {num_rows} rows")
        print(f"   Columns: {bronze_df.columns}")
        print(f"   'impression_date' is present: {'impression_date' in bronze_df.columns}")
        
        # Apply transform that uses impression_date then drops it
        print(f"\n📝 Applying transform:")
        print(f"   1. Uses 'impression_date' in F.regexp_replace(F.col('impression_date'), ...)")
        print(f"   2. Creates new column 'impression_date_parsed'")
        print(f"   3. Drops 'impression_date' via .select() (not included in select list)")
        
        silver_df = (
            bronze_df.withColumn(
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
                "impression_date_parsed",  # New column
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
                # NOTE: impression_date is DROPPED - not in select list
            )
        )
        
        print(f"✅ Transform completed")
        print(f"   Columns after transform: {silver_df.columns}")
        print(f"   'impression_date' is present: {'impression_date' in silver_df.columns}")
        print(f"   'impression_date_parsed' is present: {'impression_date_parsed' in silver_df.columns}")
        
        # Test operations that trigger materialization
        print(f"\n🧪 Testing operations that trigger materialization:")
        
        operations = [
            ("count()", lambda df: df.count()),
            ("cache()", lambda df: df.cache()),
            ("cache() + count()", lambda df: df.cache().count()),
            ("collect()", lambda df: len(df.collect())),
        ]
        
        all_passed = True
        for op_name, op_func in operations:
            try:
                result = op_func(silver_df)
                print(f"   ✅ {op_name}: Success (result: {result})")
            except Exception as e:
                error_msg = str(e)
                all_passed = False
                print(f"   ❌ {op_name}: FAILED")
                print(f"      Error: {error_msg[:200]}")
                
                if "cannot resolve 'impression_date'" in error_msg:
                    print(f"\n      ⚠️  THIS IS THE BUG!")
                    print(f"      sparkless is trying to resolve 'impression_date'")
                    print(f"      which was dropped via .select()")
                    print(f"\n      Full error:")
                    import traceback
                    traceback.print_exc()
        
        if all_passed:
            print(f"\n✅ All operations succeeded with {num_rows} rows")
            return True
        else:
            print(f"\n❌ Bug reproduced with {num_rows} rows!")
            return False
            
    finally:
        spark.stop()

def main():
    print("=" * 80)
    print("sparkless Bug Reproduction: Dropped Column Resolution Error")
    print("=" * 80)
    print("\nThis script reproduces a bug where sparkless fails to resolve")
    print("columns that were used in transformations but then dropped via .select()")
    print("\nThe bug occurs with larger datasets (150+ rows) but NOT with small datasets.")
    
    # Test with small dataset (should work)
    small_works = test_with_rows(2, "Small Dataset (Should Work)")
    
    # Test with large dataset (should fail)
    large_fails = not test_with_rows(150, "Large Dataset (Should Fail)")
    
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    print(f"Small dataset (2 rows): {'✅ Works' if small_works else '❌ Fails'}")
    print(f"Large dataset (150 rows): {'❌ Fails (BUG REPRODUCED)' if large_fails else '✅ Works'}")
    
    if large_fails and small_works:
        print(f"\n✅ Bug successfully reproduced!")
        print(f"\nRoot Cause:")
        print(f"  - sparkless's column validation during materialization")
        print(f"  - tries to resolve ALL column references in the execution plan")
        print(f"  - including columns that were dropped via .select()")
        print(f"  - fails with: 'cannot resolve <column_name>'")
        print(f"\nThis is a sparkless bug that needs to be fixed.")
    else:
        print(f"\n⚠️  Unexpected behavior - please investigate")

if __name__ == "__main__":
    main()

