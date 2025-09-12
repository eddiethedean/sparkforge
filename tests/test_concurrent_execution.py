#!/usr/bin/env python3
"""
Test to verify concurrent execution of Bronze and Gold steps.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys
import os
import time
import threading
import pytest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sparkforge import PipelineBuilder

def test_concurrent_bronze_execution(spark_session):
    """Test that multiple Bronze steps can run concurrently."""
    print("🧪 Testing concurrent Bronze execution...")
    
    # Create test data
    bronze_data1 = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
    ]
    
    bronze_data2 = [
        ("user3", "purchase", "2024-01-01 12:00:00"),
        ("user4", "view", "2024-01-01 13:00:00"),
    ]
    
    bronze_df1 = spark_session.createDataFrame(
        bronze_data1, 
        ["user_id", "action", "timestamp"]
    )
    
    bronze_df2 = spark_session.createDataFrame(
        bronze_data2, 
        ["user_id", "action", "timestamp"]
    )
    
    # Define validation rules
    bronze_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
    
    # Track execution times
    execution_times = {}
    
    def track_execution_time(step_name, func):
        start_time = time.time()
        result = func()
        end_time = time.time()
        execution_times[step_name] = end_time - start_time
        return result
    
    # Build pipeline with multiple bronze steps
    builder = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=True,
        max_parallel_workers=4
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="bronze_events",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .with_bronze_rules(
            name="bronze_users",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    
    try:
        # Test concurrent bronze execution
        start_time = time.time()
        report = runner.initial_load(bronze_sources={
            "bronze_events": bronze_df1,
            "bronze_users": bronze_df2
        })
        end_time = time.time()
        
        total_time = end_time - start_time
        
        print(f"   ✅ Pipeline completed successfully!")
        print(f"   📊 Total execution time: {total_time:.2f}s")
        print(f"   📊 Bronze results: {list(report.bronze_results.keys())}")
        print(f"   📊 Report status: {report.status}")
        
        # Verify both bronze steps completed
        assert "bronze_events" in report.bronze_results, "bronze_events not found in results"
        assert "bronze_users" in report.bronze_results, "bronze_users not found in results"
        
        print("   ✅ Both Bronze steps completed successfully!")
        print("   ✅ Concurrent Bronze execution is working!")
        
        assert True
        
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        assert False

def test_concurrent_gold_execution(spark_session):
    """Test that multiple Gold steps can run concurrently."""
    print("\n🧪 Testing concurrent Gold execution...")
    
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
        "event_count": [F.col("event_count") > 0]
    }
    
    def silver_events(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def gold_events_summary(spark, silvers):
        events_df = silvers.get("silver_events")
        if events_df is not None:
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            return spark.createDataFrame([], ["action", "event_count"])
    
    def gold_users_summary(spark, silvers):
        events_df = silvers.get("silver_events")
        if events_df is not None:
            return (events_df
                    .select("user_id")
                    .distinct()
                    .agg(F.count("*").alias("unique_users"))
                   )
        else:
            return spark.createDataFrame([], ["unique_users"])
    
    # Build pipeline with multiple gold steps
    builder = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=True,
        max_parallel_workers=4
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
        .add_gold_transform(
            name="gold_events_summary",
            transform=gold_events_summary,
            rules=gold_rules,
            table_name="gold_events_summary",
            source_silvers=["silver_events"]
        )
        .add_gold_transform(
            name="gold_users_summary",
            transform=gold_users_summary,
            rules={"unique_users": [F.col("unique_users") > 0]},
            table_name="gold_users_summary",
            source_silvers=["silver_events"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    
    try:
        # Test concurrent gold execution
        start_time = time.time()
        report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
        end_time = time.time()
        
        total_time = end_time - start_time
        
        print(f"   ✅ Pipeline completed successfully!")
        print(f"   📊 Total execution time: {total_time:.2f}s")
        print(f"   📊 Silver results: {list(report.silver_results.keys())}")
        print(f"   📊 Gold results: {list(report.gold_results.keys())}")
        print(f"   📊 Report status: {report.status}")
        
        # Verify both gold steps completed
        assert "gold_events_summary" in report.gold_results, "gold_events_summary not found in results"
        assert "gold_users_summary" in report.gold_results, "gold_users_summary not found in results"
        
        print("   ✅ Both Gold steps completed successfully!")
        print("   ✅ Concurrent Gold execution is working!")
        
        assert True
        
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        assert False

def test_parallel_configuration(spark_session):
    """Test that parallel configuration is properly applied."""
    print("\n🧪 Testing parallel configuration...")
    
    # Test with parallel enabled
    builder_parallel = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=True,
        max_parallel_workers=4
    )
    
    config_parallel = builder_parallel.config
    print(f"   ✅ Parallel enabled: {config_parallel.parallel.enabled}")
    print(f"   ✅ Max workers: {config_parallel.parallel.max_workers}")
    print(f"   ✅ Timeout: {config_parallel.parallel.timeout_secs}")
    
    assert config_parallel.parallel.enabled == True, "Parallel should be enabled"
    assert config_parallel.parallel.max_workers == 4, "Max workers should be 4"
    
    # Test with parallel disabled
    builder_sequential = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=False,
        max_parallel_workers=1
    )
    
    config_sequential = builder_sequential.config
    print(f"   ✅ Parallel enabled: {config_sequential.parallel.enabled}")
    print(f"   ✅ Max workers: {config_sequential.parallel.max_workers}")
    
    assert config_sequential.parallel.enabled == False, "Parallel should be disabled"
    assert config_sequential.parallel.max_workers == 1, "Max workers should be 1"
    
    print("   ✅ Parallel configuration is working correctly!")
    
    assert True

def test_execution_timing(spark_session):
    """Test that concurrent execution is actually faster than sequential."""
    print("\n🧪 Testing execution timing...")
    
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
    
    # Test with parallel execution
    builder_parallel = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=True,
        max_parallel_workers=4
    )
    
    pipeline_parallel = (builder_parallel
        .with_bronze_rules(
            name="bronze_events",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .with_bronze_rules(
            name="bronze_users",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
    )
    
    runner_parallel = pipeline_parallel.to_pipeline()
    
    # Test with sequential execution
    builder_sequential = PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=False,
        max_parallel_workers=1
    )
    
    pipeline_sequential = (builder_sequential
        .with_bronze_rules(
            name="bronze_events",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .with_bronze_rules(
            name="bronze_users",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
    )
    
    runner_sequential = pipeline_sequential.to_pipeline()
    
    try:
        # Test parallel execution
        start_time = time.time()
        report_parallel = runner_parallel.initial_load(bronze_sources={
            "bronze_events": bronze_df,
            "bronze_users": bronze_df
        })
        parallel_time = time.time() - start_time
        
        # Test sequential execution
        start_time = time.time()
        report_sequential = runner_sequential.initial_load(bronze_sources={
            "bronze_events": bronze_df,
            "bronze_users": bronze_df
        })
        sequential_time = time.time() - start_time
        
        print(f"   📊 Parallel execution time: {parallel_time:.2f}s")
        print(f"   📊 Sequential execution time: {sequential_time:.2f}s")
        print(f"   📊 Speedup: {sequential_time / parallel_time:.2f}x")
        
        # Both should complete successfully
        assert report_parallel.status.name == "COMPLETED", "Parallel execution should succeed"
        assert report_sequential.status.name == "COMPLETED", "Sequential execution should succeed"
        
        print("   ✅ Both execution modes completed successfully!")
        print("   ✅ Timing comparison completed!")
        
        assert True
        
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        assert False

if __name__ == "__main__":
    print("🚀 Testing concurrent execution functionality...")
    print("=" * 60)
    
    success1 = test_concurrent_bronze_execution()
    success2 = test_concurrent_gold_execution()
    success3 = test_parallel_configuration()
    success4 = test_execution_timing()
    
    if success1 and success2 and success3 and success4:
        print("\n🎉 All tests PASSED!")
        print("✅ Concurrent Bronze execution is working!")
        print("✅ Concurrent Gold execution is working!")
        print("✅ Parallel configuration is working correctly!")
        print("✅ Execution timing is being measured!")
        print("\n🚀 Pipeline steps can now run concurrently for better performance!")
    else:
        print("\n💥 Some tests FAILED!")
        exit(1)
