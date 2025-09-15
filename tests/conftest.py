"""
Enhanced pytest configuration and shared fixtures for pipeline tests.

This module provides comprehensive test configuration, shared fixtures,
and utilities to support the entire test suite with better organization
and reduced duplication.
"""

import pytest
import sys
import os
import shutil
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import test helpers
from test_helpers import (
    TestDataGenerator, TestPipelineBuilder, TestAssertions, TestPerformance
)

def get_test_schema():
    """Get the test schema name."""
    return "test_schema"

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session with Delta Lake support for testing.
    
    This fixture creates a shared Spark session for all tests in the session,
    with Delta Lake support and optimized configuration for testing.
    """
    # Clean up any existing test data
    warehouse_dir = "/tmp/spark-warehouse"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)
    
    # Configure Spark with Delta Lake support
    try:
        from delta import configure_spark_with_delta_pip
        print("🔧 Configuring Spark with Delta Lake support for all tests")
        
        builder = SparkSession.builder \
            .appName("SparkForgeTests") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Configure Delta Lake with explicit version
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
    except Exception as e:
        print(f"⚠️ Delta Lake configuration failed: {e}")
        # Fall back to basic Spark if Delta Lake is not available
        print("🔧 Falling back to basic Spark configuration")
        builder = SparkSession.builder \
            .appName("SparkForgeTests") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Create test database
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")
    
    yield spark
    
    # Cleanup
    try:
        if hasattr(spark, 'sparkContext') and spark.sparkContext._jsc is not None:
            spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
    except Exception as e:
        print(f"Warning: Could not drop test_schema database: {e}")
    
    try:
        spark.stop()
    except Exception as e:
        print(f"Warning: Could not stop Spark session: {e}")
    
    # Clean up warehouse directory
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

@pytest.fixture(autouse=True, scope="function")
def cleanup_test_tables(spark_session):
    """Clean up test tables after each test."""
    yield
    # Cleanup after each test
    try:
        # Check if SparkContext is still valid
        if hasattr(spark_session, 'sparkContext') and spark_session.sparkContext._jsc is not None:
            # Drop any tables that might have been created
            tables = spark_session.sql("SHOW TABLES IN test_schema").collect()
            for table in tables:
                table_name = table.tableName
                spark_session.sql(f"DROP TABLE IF EXISTS test_schema.{table_name}")
    except Exception as e:
        # Ignore cleanup errors
        pass

@pytest.fixture
def sample_bronze_data(spark_session):
    """Create sample bronze data for testing."""
    data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    return spark_session.createDataFrame(
        data, 
        ["user_id", "action", "timestamp"]
    )


@pytest.fixture
def sample_bronze_rules():
    """Create sample bronze validation rules."""
    from pyspark.sql import functions as F
    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }

@pytest.fixture
def sample_silver_rules():
    """Create sample silver validation rules."""
    from pyspark.sql import functions as F
    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }

@pytest.fixture
def sample_gold_rules():
    """Create sample gold validation rules."""
    from pyspark.sql import functions as F
    return {
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }

@pytest.fixture
def pipeline_builder(spark_session):
    """Create a PipelineBuilder instance for testing."""
    from sparkforge import PipelineBuilder
    return PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=True,
        max_parallel_workers=4
    )

@pytest.fixture
def pipeline_builder_sequential(spark_session):
    """Create a PipelineBuilder instance with sequential execution for testing."""
    from sparkforge import PipelineBuilder
    return PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=False,
        max_parallel_workers=1
    )

# Enhanced fixtures using test helpers
@pytest.fixture
def test_data_generator():
    """Provide TestDataGenerator instance."""
    return TestDataGenerator()

@pytest.fixture
def test_pipeline_builder():
    """Provide TestPipelineBuilder instance."""
    return TestPipelineBuilder()

@pytest.fixture
def test_assertions():
    """Provide TestAssertions instance."""
    return TestAssertions()

@pytest.fixture
def test_performance():
    """Provide TestPerformance instance."""
    return TestPerformance()

# Enhanced data fixtures
@pytest.fixture
def small_events_data(spark_session):
    """Create small events dataset for fast tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=10)

@pytest.fixture
def medium_events_data(spark_session):
    """Create medium events dataset for integration tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=100)

@pytest.fixture
def large_events_data(spark_session):
    """Create large events dataset for performance tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=1000)

@pytest.fixture
def user_data(spark_session):
    """Create user profile data."""
    return TestDataGenerator.create_user_data(spark_session)

@pytest.fixture
def validation_rules():
    """Create standard validation rules."""
    return TestDataGenerator.create_validation_rules()

# Enhanced pipeline fixtures
@pytest.fixture
def simple_pipeline(spark_session):
    """Create a simple test pipeline."""
    return TestPipelineBuilder.create_simple_pipeline(spark_session)

@pytest.fixture
def complete_pipeline(spark_session, medium_events_data):
    """Create a complete Bronze → Silver → Gold pipeline."""
    return TestPipelineBuilder.create_bronze_silver_gold_pipeline(
        spark_session, 
        bronze_data=medium_events_data
    )

# Performance testing fixtures
@pytest.fixture
def performance_thresholds():
    """Define performance thresholds for tests."""
    return {
        "max_execution_time": 30.0,  # seconds
        "max_memory_usage": 1024,    # MB
        "min_throughput": 100        # records/second
    }

# Test configuration fixtures
@pytest.fixture
def test_config():
    """Provide test configuration."""
    return {
        "schema": "test_schema",
        "verbose": False,
        "enable_parallel_silver": True,
        "max_parallel_workers": 2,
        "min_bronze_rate": 95.0,
        "min_silver_rate": 98.0,
        "min_gold_rate": 99.0
    }