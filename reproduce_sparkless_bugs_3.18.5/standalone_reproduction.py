#!/usr/bin/env python3
"""
STANDALONE REPRODUCTION: sparkless Bug with Dropped Columns

This is a complete, standalone script that reproduces the bug.
No dependencies on PipelineBuilder - pure sparkless code only.

Run: python standalone_reproduction.py
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

def main():
    print("=" * 80)
    print("STANDALONE sparkless BUG REPRODUCTION")
    print("=" * 80)
    print("\nThis script reproduces the bug where sparkless fails to resolve")
    print("columns that were used in transformations but then dropped via .select()\n")
    
    spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()
    
    try:
        # Create test data (150 rows - bug occurs with larger datasets)
        print("STEP 1: Create test data (150 rows)")
        print("-" * 80)
        
        data = []
        for i in range(150):
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
        
        print(f"✅ Created DataFrame with {bronze_df.count()} rows")
        print(f"   Columns: {bronze_df.columns}")
        print(f"   'impression_date' present: {'impression_date' in bronze_df.columns}")
        
        # Apply transform that uses impression_date then drops it
        print("\nSTEP 2: Apply transform")
        print("-" * 80)
        print("  - Uses 'impression_date' in F.regexp_replace(F.col('impression_date'), ...)")
        print("  - Creates new column 'impression_date_parsed'")
        print("  - Drops 'impression_date' via .select() (not in select list)")
        
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
        print(f"   'impression_date' present: {'impression_date' in silver_df.columns}")
        print(f"   'impression_date_parsed' present: {'impression_date_parsed' in silver_df.columns}")
        
        # Test operations that trigger materialization
        print("\nSTEP 3: Test operations that trigger materialization")
        print("-" * 80)
        print("These operations will FAIL if the bug exists:\n")
        
        operations = [
            ("count()", lambda df: df.count()),
            ("cache()", lambda df: df.cache()),
            ("cache() + count()", lambda df: df.cache().count()),
            ("collect()", lambda df: len(df.collect())),
        ]
        
        bug_reproduced = False
        for op_name, op_func in operations:
            try:
                result = op_func(silver_df)
                print(f"✅ {op_name}: Success (result: {result})")
            except Exception as e:
                error_msg = str(e)
                bug_reproduced = True
                print(f"❌ {op_name}: FAILED")
                print(f"   Error: {error_msg[:200]}")
                
                if "cannot resolve 'impression_date'" in error_msg:
                    print(f"\n   ⚠️  BUG REPRODUCED!")
                    print(f"   sparkless is trying to resolve 'impression_date'")
                    print(f"   which was dropped via .select()")
                    print(f"\n   Full traceback:")
                    import traceback
                    traceback.print_exc()
        
        print("\n" + "=" * 80)
        if bug_reproduced:
            print("RESULT: ✅ BUG REPRODUCED")
            print("=" * 80)
            print("\nThe bug exists in this version of sparkless.")
            print("\nRoot Cause:")
            print("  - sparkless's column validation during materialization")
            print("  - tries to resolve ALL column references in the execution plan")
            print("  - including columns that were dropped via .select()")
            print("  - fails with: 'cannot resolve <column_name>'")
        else:
            print("RESULT: ✅ BUG NOT REPRODUCED (May be fixed)")
            print("=" * 80)
            print("\nAll operations succeeded. The bug may have been fixed in this version.")
            print("If you expected the bug to occur, please check:")
            print("  1. sparkless version")
            print("  2. Dataset size (bug occurs with 150+ rows)")
            print("  3. Exact transform pattern (use column, then drop it)")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

