"""
Pytest configuration and shared fixtures for pipeline tests.
"""

import pytest
import sys
import os
from pyspark.sql import SparkSession

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing with Delta Lake support."""
    import shutil
    import os
    
    # Clean up any existing test data
    warehouse_dir = "/tmp/spark-warehouse"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)
    
    # Use basic Spark configuration for stability
    print("🔧 Using basic Spark configuration for test stability")
    builder = SparkSession.builder \
        .appName("PipelineBuilderTests") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Create test database first
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")
        # Continue anyway - some tests might not need the database
    
    # Skip Delta Lake verification for basic tests
    print("⚠️ Using basic Spark - Delta Lake tests are in separate test file")
    
    yield spark
    
    # Cleanup
    try:
        # Drop test database and tables
        spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
    except Exception as e:
        print(f"Warning: Could not drop test_schema database: {e}")
    
    spark.stop()
    
    # Clean up warehouse directory
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

@pytest.fixture(autouse=True)
def cleanup_test_tables(spark_session):
    """Clean up test tables after each test."""
    yield
    # Cleanup after each test
    try:
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
    from pipeline_builder import PipelineBuilder
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
    from pipeline_builder import PipelineBuilder
    return PipelineBuilder(
        spark=spark_session,
        schema="test_schema",
        verbose=False,
        enable_parallel_silver=False,
        max_parallel_workers=1
    )