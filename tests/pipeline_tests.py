# pipeline_tests.py
"""
Comprehensive tests for the Pipeline Builder functionality.
"""

import unittest
from unittest.mock import Mock, patch
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

from sparkforge import PipelineBuilder
from sparkforge.models import BronzeStep, SilverStep, GoldStep
from sparkforge.pipeline_builder import PipelineMode
from sparkforge.config import get_default_config, get_high_performance_config, get_conservative_config
from sparkforge.logger import PipelineLogger

# Initialize Spark session for testing
from pyspark.sql import SparkSession

# Create Spark session for testing
spark = SparkSession.builder \
    .appName("PipelineBuilderTests") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set log level to WARN to reduce noise
spark.sparkContext.setLogLevel("WARN")


# Test utilities
def test_case(name):
    """Decorator for test cases."""
    def decorator(func):
        func.__test_name__ = name
        return func
    return decorator


def run_test(test_func):
    """Run a single test and report results."""
    try:
        print(f"🧪 Running test: {test_func.__test_name__}")
        test_func()
        print(f"✅ PASSED: {test_func.__test_name__}")
        return True
    except Exception as e:
        print(f"❌ FAILED: {test_func.__test_name__} - {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def create_test_dataframe(spark, data, schema):
    """Create a test DataFrame with the given data and schema."""
    return spark.createDataFrame(data, schema)


# Basic Pipeline Tests
@test_case("Basic Bronze to Silver Pipeline")
def test_basic_bronze_to_silver():
    """Test basic Bronze to Silver pipeline execution."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define Silver transform
    def silver_transform(spark, bronze_df):
        print(f"Debug - silver_transform called with bronze_df type: {type(bronze_df)}")
        print(f"Debug - bronze_df count: {bronze_df.count()}")
        result = (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .filter(F.col("event_date").isNotNull())  # Filter out null dates
                .select("user_id", "action", "event_date")
               )
        print(f"Debug - silver result count: {result.count()}")
        return result
    
    # Build pipeline with lower validation thresholds and no Delta Lake
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        min_silver_rate=50.0,  # Lower silver validation threshold
        verbose=False
    )
    
    # Delta Lake is now properly configured - no mocking needed
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Debug output
    print(f"Debug - Report mode: {report.mode}")
    print(f"Debug - Report status: {report.status}")
    print(f"Debug - Silver results: {report.silver_results}")
    print(f"Debug - Metrics: {report.metrics}")
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.metrics.total_rows_written == 3
    
    print("✅ Basic Bronze to Silver pipeline test passed")


@test_case("Bronze to Silver to Gold Pipeline")
def test_bronze_silver_gold_pipeline():
    """Test complete Bronze to Silver to Gold pipeline."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define transforms
    def silver_transform(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def gold_transform(spark, silvers):
        # silvers is a dict of DataFrames, get the test_silver DataFrame
        print(f"Debug - gold_transform called with silvers type: {type(silvers)}")
        print(f"Debug - silvers keys: {list(silvers.keys()) if isinstance(silvers, dict) else 'Not a dict'}")
        if "test_silver" in silvers:
            events_df = silvers["test_silver"]
            print(f"Debug - events_df type: {type(events_df)}")
            if isinstance(events_df, dict):
                # If it's a dict, it's the result metadata, not the DataFrame
                # We need to read from the table
                table_name = events_df.get("table_fqn", "test_schema.test_silver")
                # Fix table name if it starts with a dot
                if table_name.startswith("."):
                    table_name = "test_schema" + table_name
                print(f"Debug - Reading from table: {table_name}")
                events_df = spark.table(table_name)
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            print(f"Debug - test_silver not found in silvers")
            return spark.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline with no Delta Lake
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        verbose=False
    )
    
    # Delta Lake is now properly configured - no mocking needed
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="test_gold",
            transform=gold_transform,
            rules=gold_rules,
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert "test_gold" in report.gold_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.gold_results["test_gold"]["skipped"] == False
    assert report.metrics.total_rows_written == 6  # 3 silver + 3 gold
    
    print("✅ Bronze to Silver to Gold pipeline test passed")


# Validation and Error Handling Tests
@test_case("Validation Failure Handling")
def test_validation_failure():
    """Test that validation failures are properly handled."""
    # Create test data with invalid records
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),  # Valid
        (None, "view", "2024-01-01 11:00:00"),      # Invalid: null user_id
        ("user3", None, "2024-01-01 12:00:00"),     # Invalid: null action
    ]
    
    bronze_df = spark.createDataFrame(
        bronze_data, 
        ["user_id", "action", "timestamp"]
    )
    
    # Define strict validation rules
    bronze_rules = {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
    
    # Build pipeline with high validation threshold
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        min_bronze_rate=99.0,  # Very high threshold
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
    )
    
    # Execute pipeline and expect validation failure
    runner = pipeline.to_pipeline()
    
    try:
        report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
        # If we get here, validation should have failed
        assert False, "Expected validation failure but pipeline succeeded"
    except ValueError as e:
        # Expected validation failure
        assert "validation" in str(e).lower()
        print("✅ Validation failure properly caught")
    
    print("✅ Validation failure handling test passed")


@test_case("Missing Bronze Source Error")
def test_missing_bronze_source():
    """Test error handling when bronze source is missing."""
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp"
        )
    )
    
    # Execute pipeline without providing bronze source
    runner = pipeline.to_pipeline()
    
    try:
        report = runner.initial_load(bronze_sources={})  # Empty sources
        assert False, "Expected error for missing bronze source"
    except ValueError as e:
        assert "missing bronze source" in str(e).lower()
        print("✅ Missing bronze source error properly caught")
    
    print("✅ Missing bronze source error handling test passed")


# Parallel Execution Tests
@test_case("Parallel Silver Execution")
def test_parallel_silver_execution():
    """Test that independent Silver steps run in parallel."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define independent Silver transforms
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
    
    # Build pipeline with parallel execution enabled and no Delta Lake
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        enable_parallel_silver=True,
        max_parallel_workers=2,
        min_silver_rate=50.0,  # Lower silver validation threshold
        verbose=False
    )
    
    # Delta Lake is now properly configured - no mocking needed
    
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
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Debug output
    print(f"Debug - Report mode: {report.mode}")
    print(f"Debug - Report status: {report.status}")
    print(f"Debug - Silver results: {report.silver_results}")
    print(f"Debug - Metrics: {report.metrics}")
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "silver_events" in report.silver_results
    assert "silver_users" in report.silver_results
    assert report.silver_results["silver_events"]["skipped"] == False
    assert report.silver_results["silver_users"]["skipped"] == False
    assert report.metrics.total_rows_written == 6  # 3 events + 3 users
    
    print("✅ Parallel Silver execution test passed")


# Configuration Tests
@test_case("Configuration Management")
def test_configuration_management():
    """Test configuration creation and validation."""
    # Test default configuration
    config = get_default_config("test_schema")
    assert config.schema == "test_schema"
    assert config.thresholds.bronze == 95.0
    assert config.parallel.enabled == True
    assert config.parallel.max_workers == 4
    
    # Test high-performance configuration
    perf_config = get_high_performance_config("test_schema")
    assert perf_config.parallel.max_workers == 8
    assert perf_config.verbose == False
    
    # Test conservative configuration
    conservative_config = get_conservative_config("test_schema")
    assert conservative_config.thresholds.bronze == 99.0
    assert conservative_config.parallel.enabled == False
    
    print("✅ Configuration management test passed")


# DataFrame Access and Type Safety Tests
@test_case("Gold Transform DataFrame Access")
def test_gold_transform_dataframe_access():
    """Test that Gold transforms receive actual DataFrames, not metadata dictionaries."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define transforms
    def silver_transform(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def gold_transform(spark, silvers):
        # This should receive actual DataFrames, not metadata dictionaries
        print(f"Debug - gold_transform called with silvers type: {type(silvers)}")
        print(f"Debug - silvers keys: {list(silvers.keys()) if isinstance(silvers, dict) else 'Not a dict'}")
        
        # Verify that silvers contains actual DataFrames
        for name, df in silvers.items():
            print(f"Debug - {name} type: {type(df)}")
            assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
        
        if "test_silver" in silvers:
            events_df = silvers["test_silver"]
            # This should work without any workarounds
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            return spark.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="test_gold",
            transform=gold_transform,
            rules=gold_rules,
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert "test_gold" in report.gold_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.gold_results["test_gold"]["skipped"] == False
    assert report.metrics.total_rows_written == 6  # 3 silver + 3 gold
    
    print("✅ Gold transform DataFrame access test passed")


@test_case("Silver Transform DataFrame Access")
def test_silver_transform_dataframe_access():
    """Test that Silver transforms receive actual DataFrames from Bronze."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define Silver transform that verifies DataFrame access
    def silver_transform(spark, bronze_df):
        # Verify that bronze_df is actually a DataFrame
        print(f"Debug - silver_transform called with bronze_df type: {type(bronze_df)}")
        assert hasattr(bronze_df, 'withColumn'), f"Expected DataFrame, got {type(bronze_df)}"
        assert hasattr(bronze_df, 'count'), f"Expected DataFrame, got {type(bronze_df)}"
        
        # This should work without any issues
        result = (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .filter(F.col("event_date").isNotNull())
                .select("user_id", "action", "event_date")
               )
        
        print(f"Debug - silver result type: {type(result)}")
        assert hasattr(result, 'withColumn'), f"Expected DataFrame result, got {type(result)}"
        return result
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.metrics.total_rows_written == 3
    
    print("✅ Silver transform DataFrame access test passed")


@test_case("Multiple Silver to Gold Dependencies")
def test_multiple_silver_to_gold_dependencies():
    """Test Gold transform with multiple Silver dependencies."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define transforms
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
        # Verify that silvers contains actual DataFrames
        print(f"Debug - gold_summary called with silvers type: {type(silvers)}")
        print(f"Debug - silvers keys: {list(silvers.keys()) if isinstance(silvers, dict) else 'Not a dict'}")
        
        for name, df in silvers.items():
            print(f"Debug - {name} type: {type(df)}")
            assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
        
        # This should work with actual DataFrames
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
            return spark.createDataFrame([], ["action", "total_events", "unique_users"])
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            source_silvers=["silver_events", "silver_users"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "silver_events" in report.silver_results
    assert "silver_users" in report.silver_results
    assert "gold_summary" in report.gold_results
    assert report.silver_results["silver_events"]["skipped"] == False
    assert report.silver_results["silver_users"]["skipped"] == False
    assert report.gold_results["gold_summary"]["skipped"] == False
    
    print("✅ Multiple Silver to Gold dependencies test passed")


@test_case("DataFrame Type Validation")
def test_dataframe_type_validation():
    """Test that all transform functions receive proper DataFrame types."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Track what types are received
    received_types = {}
    
    def silver_transform(spark, bronze_df):
        received_types['silver_bronze_df'] = type(bronze_df)
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def gold_transform(spark, silvers):
        received_types['gold_silvers'] = type(silvers)
        if isinstance(silvers, dict):
            for name, df in silvers.items():
                received_types[f'gold_silver_{name}'] = type(df)
        
        if "test_silver" in silvers:
            events_df = silvers["test_silver"]
            return (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            return spark.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="test_gold",
            transform=gold_transform,
            rules=gold_rules,
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify that correct types were received
    from pyspark.sql import DataFrame
    assert isinstance(received_types['silver_bronze_df'], DataFrame), f"Silver should receive DataFrame, got {received_types['silver_bronze_df']}"
    assert isinstance(received_types['gold_silvers'], dict), f"Gold should receive dict, got {received_types['gold_silvers']}"
    assert isinstance(received_types['gold_silver_test_silver'], DataFrame), f"Gold should receive DataFrame for silver, got {received_types['gold_silver_test_silver']}"
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert "test_gold" in report.gold_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.gold_results["test_gold"]["skipped"] == False
    
    print("✅ DataFrame type validation test passed")


@test_case("DataFrame Method Access Validation")
def test_dataframe_method_access_validation():
    """Test that DataFrame methods can be called on objects passed to transforms."""
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define transforms that use various DataFrame methods
    def silver_transform(spark, bronze_df):
        # Test various DataFrame methods that should work
        try:
            result = (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .filter(F.col("event_date").isNotNull())
                    .select("user_id", "action", "event_date")
                    .distinct()
                    .orderBy("user_id")
                   )
            # Test count method
            count = result.count()
            print(f"Debug - Silver result count: {count}")
            return result
        except AttributeError as e:
            raise AssertionError(f"DataFrame method failed on bronze_df: {e}")
    
    def gold_transform(spark, silvers):
        # Test various DataFrame methods on silvers dict
        try:
            if "test_silver" in silvers:
                events_df = silvers["test_silver"]
                
                # Test multiple DataFrame methods
                result = (events_df
                        .withColumn("rn", F.row_number().over(F.Window.partitionBy("action").orderBy("user_id")))
                        .filter(F.col("rn") == 1)
                        .groupBy("action")
                        .agg(F.count("*").alias("event_count"))
                        .orderBy("action")
                       )
                
                # Test count method
                count = result.count()
                print(f"Debug - Gold result count: {count}")
                return result
            else:
                return spark.createDataFrame([], ["action", "event_count"])
        except AttributeError as e:
            raise AssertionError(f"DataFrame method failed on silvers: {e}")
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="test_gold",
            transform=gold_transform,
            rules=gold_rules,
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert "test_gold" in report.gold_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.gold_results["test_gold"]["skipped"] == False
    
    print("✅ DataFrame method access validation test passed")


@test_case("Error Handling for Invalid DataFrame Access")
def test_error_handling_invalid_dataframe_access():
    """Test that proper errors are raised when trying to access DataFrame methods on non-DataFrames."""
    # This test simulates the original bug scenario
    from unittest.mock import Mock
    
    # Create a mock that simulates receiving a metadata dictionary instead of DataFrame
    mock_metadata = {
        "table_fqn": "test_schema.test_silver",
        "transform": {"input_rows": 3, "output_rows": 3},
        "validation": {"valid_rows": 3, "invalid_rows": 0},
        "write": {"rows_written": 3},
        "skipped": False
    }
    
    # Test that trying to call DataFrame methods on metadata raises AttributeError
    try:
        # This should fail with AttributeError
        mock_metadata.withColumn("test", F.lit("test"))
        assert False, "Expected AttributeError when calling withColumn on metadata dict"
    except AttributeError as e:
        assert "withColumn" in str(e), f"Expected withColumn error, got: {e}"
        print("✅ Proper error raised for invalid DataFrame access")
    
    # Test that the error message is helpful
    try:
        mock_metadata.count()
        assert False, "Expected AttributeError when calling count on metadata dict"
    except AttributeError as e:
        assert "count" in str(e), f"Expected count error, got: {e}"
        print("✅ Proper error raised for count method on metadata dict")
    
    print("✅ Error handling for invalid DataFrame access test passed")


@test_case("Regression Test for Original Bug")
def test_regression_original_bug():
    """Test that reproduces and prevents the original AttributeError bug."""
    # This test specifically reproduces the exact scenario that caused the original bug
    # where Gold transforms received metadata dictionaries instead of DataFrames
    
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Define transforms that use the exact same pattern as the original bug
    def silver_transform(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    def gold_transform(spark, silver_dfs):
        # This is the exact pattern that caused the original bug
        # The original code tried to call .withColumn() on silver_dfs['silver_main']
        # but silver_dfs was a metadata dictionary, not a DataFrame
        
        # Verify that silver_dfs contains actual DataFrames
        assert isinstance(silver_dfs, dict), f"Expected dict, got {type(silver_dfs)}"
        
        for name, df in silver_dfs.items():
            assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'filter'), f"Expected DataFrame for {name}, got {type(df)}"
            assert hasattr(df, 'select'), f"Expected DataFrame for {name}, got {type(df)}"
        
        # This is the exact code pattern that failed in the original bug
        if "silver_main" in silver_dfs:
            # This should work now - silver_dfs['silver_main'] should be a DataFrame
            return (silver_dfs['silver_main']
                    .withColumn("rn", F.row_number().over(F.Window.partitionBy("action").orderBy("user_id")))
                    .filter(F.col("rn") == 1)
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
        else:
            return spark.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="bronze_main",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="silver_main",
            source_bronze="bronze_main",
            transform=silver_transform,
            rules=silver_rules,
            table_name="silver_main",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="gold_main",
            transform=gold_transform,
            rules=gold_rules,
            table_name="gold_main",
            source_silvers=["silver_main"]
        )
    )
    
    # Execute pipeline - this should work without the original AttributeError
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"bronze_main": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "silver_main" in report.silver_results
    assert "gold_main" in report.gold_results
    assert report.silver_results["silver_main"]["skipped"] == False
    assert report.gold_results["gold_main"]["skipped"] == False
    assert report.metrics.total_rows_written == 6  # 3 silver + 3 gold
    
    print("✅ Regression test for original bug passed")


@test_case("Type Safety and Data Flow Validation")
def test_type_safety_and_data_flow():
    """Comprehensive test to validate type safety and data flow throughout the pipeline."""
    # This test ensures that the data flow and type annotations are correct
    # throughout the entire pipeline execution
    
    # Create test data
    bronze_data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    
    bronze_df = spark.createDataFrame(
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
    
    # Track data types at each stage
    data_types = {}
    
    def silver_transform(spark, bronze_df):
        # Validate input type
        from pyspark.sql import DataFrame
        assert isinstance(bronze_df, DataFrame), f"Bronze input should be DataFrame, got {type(bronze_df)}"
        data_types['silver_input'] = type(bronze_df)
        
        result = (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
        
        # Validate output type
        assert isinstance(result, DataFrame), f"Silver output should be DataFrame, got {type(result)}"
        data_types['silver_output'] = type(result)
        
        return result
    
    def gold_transform(spark, silvers):
        # Validate input type
        assert isinstance(silvers, dict), f"Gold input should be dict, got {type(silvers)}"
        data_types['gold_input'] = type(silvers)
        
        # Validate that silvers contains DataFrames
        for name, df in silvers.items():
            assert isinstance(df, DataFrame), f"Silver {name} should be DataFrame, got {type(df)}"
            data_types[f'gold_silver_{name}'] = type(df)
        
        if "test_silver" in silvers:
            events_df = silvers["test_silver"]
            result = (events_df
                    .groupBy("action")
                    .agg(F.count("*").alias("event_count"))
                    .orderBy("action")
                   )
            
            # Validate output type
            assert isinstance(result, DataFrame), f"Gold output should be DataFrame, got {type(result)}"
            data_types['gold_output'] = type(result)
            
            return result
        else:
            return spark.createDataFrame([], ["action", "event_count"])
    
    # Build pipeline
    builder = PipelineBuilder(
        spark=spark,
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
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
        .add_gold_transform(
            name="test_gold",
            transform=gold_transform,
            rules=gold_rules,
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Validate data types at each stage
    from pyspark.sql import DataFrame
    
    # Bronze to Silver
    assert isinstance(data_types['silver_input'], DataFrame), "Bronze to Silver input should be DataFrame"
    assert isinstance(data_types['silver_output'], DataFrame), "Bronze to Silver output should be DataFrame"
    
    # Silver to Gold
    assert isinstance(data_types['gold_input'], dict), "Gold input should be dict"
    assert isinstance(data_types['gold_silver_test_silver'], DataFrame), "Gold should receive DataFrame for silver"
    assert isinstance(data_types['gold_output'], DataFrame), "Gold output should be DataFrame"
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert "test_gold" in report.gold_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.gold_results["test_gold"]["skipped"] == False
    
    # Verify that silver_results contains metadata, not DataFrames
    silver_result = report.silver_results["test_silver"]
    assert isinstance(silver_result, dict), "Silver results should be metadata dict"
    assert "table_fqn" in silver_result, "Silver results should contain table_fqn"
    assert "transform" in silver_result, "Silver results should contain transform metadata"
    assert "validation" in silver_result, "Silver results should contain validation metadata"
    assert "write" in silver_result, "Silver results should contain write metadata"
    
    # Verify that gold_results contains metadata, not DataFrames
    gold_result = report.gold_results["test_gold"]
    assert isinstance(gold_result, dict), "Gold results should be metadata dict"
    assert "transform" in gold_result, "Gold results should contain transform metadata"
    assert "validation" in gold_result, "Gold results should contain validation metadata"
    assert "write" in gold_result, "Gold results should contain write metadata"
    
    print("✅ Type safety and data flow validation test passed")


# Edge Cases and Error Scenarios
@test_case("Empty DataFrame Handling")
def test_empty_dataframe_handling():
    """Test handling of empty DataFrames."""
    # Create empty test data with explicit schema
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    bronze_df = spark.createDataFrame([], schema)
    
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
    
    # Define Silver transform
    def silver_transform(spark, bronze_df):
        return (bronze_df
                .withColumn("event_date", F.to_date("timestamp"))
                .select("user_id", "action", "event_date")
               )
    
    # Build pipeline with lower validation threshold for empty data
    builder = PipelineBuilder(
        spark=spark,
        schema="test_schema",
        min_bronze_rate=0.0,  # Allow empty data
        min_silver_rate=0.0,  # Allow empty silver data
        verbose=False
    )
    
    pipeline = (builder
        .with_bronze_rules(
            name="test_bronze",
            rules=bronze_rules,
            incremental_col="timestamp"
        )
        .add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=silver_rules,
            table_name="test_silver",
            watermark_col="event_date"
        )
    )
    
    # Execute pipeline
    runner = pipeline.to_pipeline()
    report = runner.initial_load(bronze_sources={"test_bronze": bronze_df})
    
    # Verify results
    assert report.mode == PipelineMode.INITIAL
    assert "test_silver" in report.silver_results
    assert report.silver_results["test_silver"]["skipped"] == False
    assert report.metrics.total_rows_written == 0
    
    print("✅ Empty DataFrame handling test passed")


# Test Suite Runner
def run_all_tests():
    """Run all test cases and report results."""
    print("🚀 Starting Pipeline Builder Test Suite")
    print("=" * 50)
    
    # List all test functions
    test_functions = [
        test_basic_bronze_to_silver,
        test_bronze_silver_gold_pipeline,
        test_validation_failure,
        test_missing_bronze_source,
        test_parallel_silver_execution,
        test_configuration_management,
        test_gold_transform_dataframe_access,
        test_silver_transform_dataframe_access,
        test_multiple_silver_to_gold_dependencies,
        test_dataframe_type_validation,
        test_dataframe_method_access_validation,
        test_error_handling_invalid_dataframe_access,
        test_regression_original_bug,
        test_type_safety_and_data_flow,
        test_empty_dataframe_handling
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        if run_test(test_func):
            passed += 1
        else:
            failed += 1
        print()  # Add spacing between tests
    
    print("=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Pipeline Builder is working correctly.")
    else:
        print(f"❌ {failed} tests failed. Please check the errors above.")
    
    return passed, failed


# Cleanup function
def cleanup_test_tables():
    """Clean up test tables created during testing."""
    try:
        # List of test tables to clean up
        test_tables = [
            "test_schema.test_silver",
            "test_schema.test_gold", 
            "test_schema.silver_events",
            "test_schema.silver_users"
        ]
        
        for table in test_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table}")
                print(f"✅ Dropped table: {table}")
            except Exception as e:
                print(f"⚠️ Could not drop table {table}: {e}")
        
        print("✅ Test cleanup completed")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")


if __name__ == "__main__":
    # This will only run if the script is executed directly
    # In Databricks, you would run the individual test functions
    print("Pipeline Builder Test Suite")
    print("Run individual test functions or call run_all_tests() to execute all tests")
