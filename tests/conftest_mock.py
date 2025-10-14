"""
Mock pytest configuration and shared fixtures for pipeline tests.

This module provides comprehensive test configuration, shared fixtures,
and utilities to support the entire test suite using mock_spark instead
of real Spark sessions.
"""

import os
import shutil
import sys
import time

import pytest
from mock_spark import MockSparkSession

from tests.mock_functions_wrapper import MockFunctions

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


def get_unique_test_schema():
    """Get a unique test schema name for isolated tests."""
    unique_id = int(time.time() * 1000000) % 1000000
    return f"test_schema_{unique_id}"


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a shared Mock Spark session for testing.

    This fixture creates a shared Mock Spark session for all tests in the session,
    providing a lightweight alternative to real Spark that doesn't require
    Java or Spark installation.
    """
    print("🔧 Creating Mock Spark session for all tests")

    # Create mock Spark session
    spark = MockSparkSession(f"SparkForgeTests-{os.getpid()}")

    # Create test database
    try:
        spark.catalog.createDatabase("test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")

    yield spark

    # Cleanup (mock sessions don't need explicit cleanup)
    print("🧹 Mock Spark session cleanup completed")


@pytest.fixture(scope="function")
def isolated_spark_session():
    """
    Create an isolated Mock Spark session for tests that need complete isolation.

    This fixture creates a new Mock Spark session for each test function.
    Use this for tests that modify global state or need complete isolation.
    """
    unique_id = int(time.time() * 1000000) % 1000000
    schema_name = f"test_schema_{unique_id}"

    print(f"🔧 Creating isolated Mock Spark session for {schema_name}")

    # Create mock Spark session
    spark = MockSparkSession(f"SparkForgeTests-{os.getpid()}-{unique_id}")

    # Create isolated test database
    try:
        spark.catalog.createDatabase(schema_name)
        print(f"✅ Isolated test database {schema_name} created successfully")
    except Exception as e:
        print(f"❌ Could not create isolated test database {schema_name}: {e}")

    yield spark

    # Cleanup
    print(f"🧹 Isolated Mock Spark session cleanup completed for {schema_name}")


@pytest.fixture(scope="function")
def mock_functions():
    """
    Create a Mock Functions instance for testing.

    This fixture provides mock PySpark functions for testing.
    """
    return MockFunctions()


@pytest.fixture(scope="function")
def test_data_generator():
    """
    Create a test data generator instance.

    This fixture provides utilities for generating test data.
    """
    return TestDataGenerator()


@pytest.fixture(scope="function")
def test_assertions():
    """
    Create a test assertions instance.

    This fixture provides custom assertion utilities.
    """
    return TestAssertions()


@pytest.fixture(scope="function")
def test_performance():
    """
    Create a test performance instance.

    This fixture provides performance testing utilities.
    """
    return TestPerformance()


@pytest.fixture(scope="function")
def test_pipeline_builder():
    """
    Create a test pipeline builder instance.

    This fixture provides pipeline building utilities.
    """
    return TestPipelineBuilder()


@pytest.fixture(scope="function")
def sample_dataframe(spark_session):
    """
    Create a sample DataFrame for testing.

    This fixture creates a sample DataFrame with common test data.
    """
    from mock_spark import (
        DoubleType,
        IntegerType,
        MockStructField,
        MockStructType,
        StringType,
    )

    schema = MockStructType(
        [
            MockStructField("user_id", StringType(), True),
            MockStructField("age", IntegerType(), True),
            MockStructField("score", DoubleType(), True),
            MockStructField("category", StringType(), True),
        ]
    )

    data = [
        ("user1", 25, 85.5, "A"),
        ("user2", 30, 92.0, "B"),
        ("user3", None, 78.5, "A"),
        ("user4", 35, None, "C"),
        ("user5", 28, 88.0, "B"),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def empty_dataframe(spark_session):
    """
    Create an empty DataFrame for testing.

    This fixture creates an empty DataFrame with a defined schema.
    """
    from mock_spark import MockStructField, MockStructType, StringType

    schema = MockStructType(
        [
            MockStructField("col1", StringType(), True),
            MockStructField("col2", StringType(), True),
        ]
    )

    return spark_session.createDataFrame([], schema)


@pytest.fixture(scope="function")
def test_warehouse_dir():
    """
    Create a temporary warehouse directory for testing.

    This fixture creates a temporary directory for warehouse operations.
    """
    warehouse_dir = f"/tmp/spark-warehouse-{os.getpid()}"
    os.makedirs(warehouse_dir, exist_ok=True)

    yield warehouse_dir

    # Cleanup
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)


# Test configuration
def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "system: marks tests as system tests")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file location."""
    for item in items:
        # Add markers based on file location
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "system" in str(item.fspath):
            item.add_marker(pytest.mark.system)
        else:
            item.add_marker(pytest.mark.unit)

        # Add slow marker for tests that take longer than 1 second
        if "performance" in str(item.fspath) or "load" in str(item.fspath):
            item.add_marker(pytest.mark.slow)
