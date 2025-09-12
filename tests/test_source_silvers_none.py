#!/usr/bin/env python3
"""
Test to verify that source_silvers=None works correctly and uses all available silvers.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sparkforge import PipelineBuilder

def test_source_silvers_none(spark_session):
    """Test that source_silvers=None uses all available silvers."""
    print("🧪 Testing source_silvers=None behavior...")
    
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark_session.createDataFrame(
        bronze_data, 
        ["user_id", "action", "timestamp"]
    )
    
    # Define validation rules
    bronze_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
    
    silver_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }
    
    gold_rules = {
        "action": [F.col("action").isNotNull()],
        "total_events": [F.col("total_events") > 0],
        "unique_users": [F.col("unique_users") > 0]
    }
    
    # Track what silvers are received
    received_silvers = {}
    
    def silver_events(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def silver_users(spark, bronze_df):
        return (bronze_df
                .select("user_id")
                .distinct()
                .withColumn("created_at", F.current_timestamp())
               )
    
    def gold_summary(spark, silvers):
        print(f"🔍 Gold transform received silvers: {list(silvers.keys())}")
        received_silvers.update(silvers)
        
        # Verify that we received all available silvers
        expected_silvers = {"silver_events", "silver_users"}
        actual_silvers = set(silvers.keys())
        
        print(f"   Expected silvers: {expected_silvers}")
        print(f"   Actual silvers: {actual_silvers}")
        
        assert actual_silvers == expected_silvers, f"Expected {expected_silvers}, got {actual_silvers}"
        
        # Verify that all silvers are DataFrames
        for name, df in silvers.items():
            assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
        
        # Create a summary using both silvers
        events_df = silvers.get("silver_events")
        users_df = silvers.get("silver_users")
        
        if events_df is not None and users_df is not None:
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("total_events"))
                    .crossJoin(users_df.agg(F.countDistinct("user_id").alias("unique_users")))
                    .select("action", "total_events", "unique_users")
                   )
        else:
            return spark_session.createDataFrame([], ["action", "total_events", "unique_users"])
    
    # Build pipeline with source_silvers=None (should use all silvers)
    builder = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_events,
            rules=silver_rules,
            table_name="silver_events",
            watermark_col="event_date"
        )
        .add_silver_transform(
            name="silver_users",
            source_bronze="test_bronze",
            transform=silver_users,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_users",
            watermark_col="created_at"
        )
        .add_gold_transform(
            name="gold_summary",
            transform=gold_summary,
            rules=gold_rules,
            table_name="gold_summary",
            source_silvers=None  # This should use all available silvers
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    
    try:
        report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
        
        # Verify results
        print(f"✅ Pipeline completed successfully!")
        print(f"📊 Report status: {report.status}")
        print(f"📊 Silver results: {list(report.silver_results.keys())}")
        print(f"📊 Gold results: {list(report.gold_results.keys())}")
        
        # Verify that the gold transform received all silvers
        assert len(received_silvers) == 2, f"Expected 2 silvers, got {len(received_silvers)}"
        assert "silver_events" in received_silvers, "silver_events not found in received silvers"
        assert "silver_users" in received_silvers, "silver_users not found in received silvers"
        
        print("✅ All silvers were passed to the gold transform!")
        print("✅ source_silvers=None correctly uses all available silvers!")
        
        assert True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        assert False

def test_source_silvers_specific(spark_session):
    """Test that source_silvers with specific list works correctly."""
    print("\n🧪 Testing source_silvers with specific list...")
    
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
    ]
    
    bronze_df = spark_session.createDataFrame(
        bronze_data, 
        ["user_id", "action", "timestamp"]
    )
    
    # Define validation rules
    bronze_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
    
    silver_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }
    
    gold_rules = {
        "action": [F.col("action").isNotNull()],
        "event_count": [F.col("event_count") > 0]
    }
    
    # Track what silvers are received
    received_silvers = {}
    
    def silver_events(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def silver_users(spark, bronze_df):
        return (bronze_df
                .select("user_id")
                .distinct()
                .withColumn("created_at", F.current_timestamp())
               )
    
    def gold_events_only(spark, silvers):
        print(f"🔍 Gold transform received silvers: {list(silvers.keys())}")
        received_silvers.update(silvers)
        
        # Verify that we only received the specified silver
        expected_silvers = {"silver_events"}
        actual_silvers = set(silvers.keys())
        
        print(f"   Expected silvers: {expected_silvers}")
        print(f"   Actual silvers: {actual_silvers}")
        
        assert actual_silvers == expected_silvers, f"Expected {expected_silvers}, got {actual_silvers}"
        
        # Verify that the silver is a DataFrame
        for name, df in silvers.items():
            assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
        
        # Create a summary using only the specified silver
        events_df = silvers.get("silver_events")
        
        if events_df is not None:
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            return spark_session.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline with source_silvers=["silver_events"] (should use only specified silver)
    builder = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_events,
            rules=silver_rules,
            table_name="silver_events",
            watermark_col="event_date"
        )
        .add_silver_transform(
            name="silver_users",
            source_bronze="test_bronze",
            transform=silver_users,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_users",
            watermark_col="created_at"
        )
        .add_gold_transform(
            name="gold_events_only",
            transform=gold_events_only,
            rules=gold_rules,
            table_name="gold_events_only",
            source_silvers=["silver_events"]  # This should use only the specified silver
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    
    try:
        report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
        
        # Verify results
        print(f"✅ Pipeline completed successfully!")
        print(f"📊 Report status: {report.status}")
        print(f"📊 Silver results: {list(report.silver_results.keys())}")
        print(f"📊 Gold results: {list(report.gold_results.keys())}")
        
        # Verify that the gold transform received only the specified silver
        assert len(received_silvers) == 1, f"Expected 1 silver, got {len(received_silvers)}"
        assert "silver_events" in received_silvers, "silver_events not found in received silvers"
        assert "silver_users" not in received_silvers, "silver_users should not be in received silvers"
        
        print("✅ Only the specified silver was passed to the gold transform!")
        print("✅ source_silvers with specific list works correctly!")
        
        assert True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        assert False

if __name__ == "__main__":
    print("🚀 Testing source_silvers behavior...")
    print("=" * 50)
    
    success1 = test_source_silvers_none()
    success2 = test_source_silvers_specific()
    
    if success1 and success2:
        print("\n🎉 All tests PASSED!")
        print("✅ source_silvers=None correctly uses all available silvers!")
        print("✅ source_silvers with specific list works correctly!")
    else:
        print("\n💥 Some tests FAILED!")
        exit(1)
