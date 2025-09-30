"""
Enhanced pytest configuration and shared fixtures for pipeline tests.

This module provides comprehensive test configuration, shared fixtures,
and utilities to support the entire test suite with better organization
and reduced duplication.
"""

import os
import shutil
import sys
import time

import pytest
from pyspark.sql import SparkSession

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Add the tests directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import test helpers from system directory
try:
    from system.test_helpers import (
        TestAssertions,
        TestDataGenerator,
        TestPerformance,
        TestPipelineBuilder,
    )
except ImportError:
    # Fallback if test_helpers is not available
    class TestAssertions:
        pass

    class TestDataGenerator:
        pass

    class TestPerformance:
        pass

    class TestPipelineBuilder:
        pass


def get_test_schema():
    """Get the test schema name."""
    return "test_schema"


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a shared Spark session with Delta Lake support for testing.

    This fixture creates a shared Spark session for all tests in the session,
    with Delta Lake support and optimized configuration for testing.
    This is more efficient for most tests that don't need isolation.
    """
    # Clean up any existing test data
    warehouse_dir = f"/tmp/spark-warehouse-{os.getpid()}"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark with Delta Lake support
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        print("🔧 Configuring Spark with Delta Lake support for all tests")

        builder = (
            SparkSession.builder.appName(f"SparkForgeTests-{os.getpid()}")
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
        )

        # Configure Delta Lake with explicit version
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    except Exception as e:
        print(f"❌ Delta Lake configuration failed: {e}")
        print("💡 To fix this issue:")
        print("   1. Install Delta Lake: pip install delta-spark")
        print("   2. Or set SPARKFORGE_SKIP_DELTA=1 to skip Delta Lake tests")
        print("   3. Or set SPARKFORGE_BASIC_SPARK=1 to use basic Spark without Delta Lake")
        
        # Check if user explicitly wants to skip Delta Lake or use basic Spark
        skip_delta = os.environ.get("SPARKFORGE_SKIP_DELTA", "0") == "1"
        basic_spark = os.environ.get("SPARKFORGE_BASIC_SPARK", "0") == "1"
        
        if skip_delta or basic_spark:
            print("🔧 Using basic Spark configuration as requested")
            try:
                builder = (
                    SparkSession.builder.appName(f"SparkForgeTests-{os.getpid()}")
                    .master("local[1]")
                    .config("spark.sql.warehouse.dir", warehouse_dir)
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .config("spark.driver.memory", "1g")
                    .config("spark.executor.memory", "1g")
                )

                spark = builder.getOrCreate()
            except Exception as e2:
                print(f"❌ Failed to create basic Spark session: {e2}")
                raise
        else:
            # Fail fast with clear error message
            raise RuntimeError(
                f"Delta Lake configuration failed: {e}\n"
                "This is required for SparkForge tests. Please install Delta Lake or "
                "set environment variables to skip Delta Lake requirements."
            )

    # Ensure Spark session was created successfully
    if spark is None:
        raise RuntimeError("Failed to create Spark session")

    # Verify Spark context is properly initialized
    if not hasattr(spark, "sparkContext") or spark.sparkContext is None:
        raise RuntimeError("Spark context is not properly initialized")

    if not hasattr(spark.sparkContext, "_jsc") or spark.sparkContext._jsc is None:
        raise RuntimeError("Spark JVM context is not properly initialized")

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
        if (
            spark
            and hasattr(spark, "sparkContext")
            and spark.sparkContext._jsc is not None
        ):
            spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
    except Exception as e:
        print(f"Warning: Could not drop test_schema database: {e}")

    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Warning: Could not stop Spark session: {e}")

    # Clean up warehouse directory
    try:
        if os.path.exists(warehouse_dir):
            shutil.rmtree(warehouse_dir, ignore_errors=True)
    except Exception as e:
        print(f"Warning: Could not clean up warehouse directory: {e}")


@pytest.fixture(scope="function")
def isolated_spark_session():
    """
    Create an isolated Spark session for tests that need complete isolation.

    This fixture creates a new Spark session for each test function,
    with Delta Lake support and optimized configuration for testing.
    Use this for tests that modify global state or need complete isolation.
    """
    # Clean up any existing test data
    unique_id = int(time.time() * 1000000) % 1000000  # Microsecond timestamp
    warehouse_dir = f"/tmp/spark-warehouse-isolated-{os.getpid()}-{unique_id}"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark with Delta Lake support
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        print("🔧 Configuring isolated Spark with Delta Lake support")

        builder = (
            SparkSession.builder.appName(
                f"SparkForgeIsolatedTests-{os.getpid()}-{unique_id}"
            )
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
        )

        # Configure Delta Lake with explicit version
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    except Exception as e:
        print(f"❌ Delta Lake configuration failed for isolated session: {e}")
        print("💡 To fix this issue:")
        print("   1. Install Delta Lake: pip install delta-spark")
        print("   2. Or set SPARKFORGE_SKIP_DELTA=1 to skip Delta Lake tests")
        print("   3. Or set SPARKFORGE_BASIC_SPARK=1 to use basic Spark without Delta Lake")
        
        # Check if user explicitly wants to skip Delta Lake or use basic Spark
        skip_delta = os.environ.get("SPARKFORGE_SKIP_DELTA", "0") == "1"
        basic_spark = os.environ.get("SPARKFORGE_BASIC_SPARK", "0") == "1"
        
        if skip_delta or basic_spark:
            print("🔧 Using basic Spark configuration for isolated session as requested")
            try:
                builder = (
                    SparkSession.builder.appName(
                        f"SparkForgeIsolatedTests-{os.getpid()}-{unique_id}"
                    )
                    .master("local[1]")
                    .config("spark.sql.warehouse.dir", warehouse_dir)
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .config("spark.driver.memory", "512m")
                    .config("spark.executor.memory", "512m")
                )

                spark = builder.getOrCreate()
            except Exception as e2:
                print(f"❌ Failed to create basic isolated Spark session: {e2}")
                raise
        else:
            # Fail fast with clear error message
            raise RuntimeError(
                f"Delta Lake configuration failed for isolated session: {e}\n"
                "This is required for SparkForge tests. Please install Delta Lake or "
                "set environment variables to skip Delta Lake requirements."
            )

    # Ensure Spark session was created successfully
    if spark is None:
        raise RuntimeError("Failed to create isolated Spark session")

    # Verify Spark context is properly initialized
    if not hasattr(spark, "sparkContext") or spark.sparkContext is None:
        raise RuntimeError("Isolated Spark context is not properly initialized")

    if not hasattr(spark.sparkContext, "_jsc") or spark.sparkContext._jsc is None:
        raise RuntimeError("Isolated Spark JVM context is not properly initialized")

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # Create test database
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
        print("✅ Isolated test database created successfully")
    except Exception as e:
        print(f"❌ Could not create isolated test_schema database: {e}")

    yield spark

    # Cleanup
    try:
        if (
            spark
            and hasattr(spark, "sparkContext")
            and spark.sparkContext._jsc is not None
        ):
            spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
    except Exception as e:
        print(f"Warning: Could not drop isolated test_schema database: {e}")

    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Warning: Could not stop isolated Spark session: {e}")

    # Clean up warehouse directory
    try:
        if os.path.exists(warehouse_dir):
            shutil.rmtree(warehouse_dir, ignore_errors=True)
    except Exception as e:
        print(f"Warning: Could not clean up isolated warehouse directory: {e}")


@pytest.fixture(autouse=True, scope="function")
def cleanup_test_tables(spark_session):
    """Clean up test tables after each test."""
    yield
    # Cleanup after each test
    try:
        # Check if SparkContext is still valid
        if (
            hasattr(spark_session, "sparkContext")
            and spark_session.sparkContext._jsc is not None
        ):
            # Drop any tables that might have been created
            tables = spark_session.sql("SHOW TABLES IN test_schema").collect()
            for table in tables:
                table_name = table.tableName
                spark_session.sql(f"DROP TABLE IF EXISTS test_schema.{table_name}")
    except Exception:
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
    return spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])


@pytest.fixture
def sample_bronze_rules():
    """Create sample bronze validation rules."""
    from pyspark.sql import functions as F

    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
    }


@pytest.fixture
def sample_silver_rules():
    """Create sample silver validation rules."""
    from pyspark.sql import functions as F

    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()],
    }


@pytest.fixture
def sample_gold_rules():
    """Create sample gold validation rules."""
    from pyspark.sql import functions as F

    return {
        "action": [F.col("action").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()],
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
        max_parallel_workers=4,
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
        max_parallel_workers=1,
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
        spark_session, bronze_data=medium_events_data
    )


# Performance testing fixtures
@pytest.fixture
def performance_thresholds():
    """Define performance thresholds for tests."""
    return {
        "max_execution_time": 30.0,  # seconds
        "max_memory_usage": 1024,  # MB
        "min_throughput": 100,  # records/second
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
        "min_gold_rate": 99.0,
    }
