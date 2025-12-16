"""
Integration test configuration and fixtures.

This module provides fixtures and configuration for integration tests,
which use real Spark sessions but mock external systems.
"""

import os

import pytest
from pyspark.sql import SparkSession

# Use engine-specific functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def integration_spark_session():
    """
    Create a real Spark session for integration tests.
    This is shared across all integration tests for efficiency.
    """
    # Clean up any existing test data
    import os
    import shutil

    warehouse_dir = f"/tmp/spark-warehouse-integration-{os.getpid()}"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark for integration tests
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName(f"SparkForgeIntegrationTests-{os.getpid()}")
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # Note: spark.sql.catalog.spark_catalog is set by configure_spark_with_delta_pip()
            # Don't set it here to avoid ClassNotFoundException if Delta Lake setup fails
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Verify and set Delta catalog if not already set
        try:
            current_catalog = spark.conf.get("spark.sql.catalog.spark_catalog", None)  # type: ignore[attr-defined]
            if current_catalog != "org.apache.spark.sql.delta.catalog.DeltaCatalog":
                spark.conf.set(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )  # type: ignore[attr-defined]
        except Exception:
            pass  # Ignore if we can't set it
    except Exception as e:
        print(f"⚠️ Delta Lake configuration failed: {e}")
        # Fall back to basic Spark
        builder = (
            SparkSession.builder.appName(f"SparkForgeIntegrationTests-{os.getpid()}")
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
        )
        spark = builder.getOrCreate()

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # Create test database
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
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


@pytest.fixture(autouse=True, scope="function")
def cleanup_integration_tables(integration_spark_session):
    """Reset global state before and after each integration test."""
    # Only reset global state - use unique table names for isolation
    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state,
        )

        reset_global_state()
        reset_execution_state()
    except Exception:
        pass

    yield

    # Only reset global state after test
    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state,
        )

        reset_global_state()
        reset_execution_state()
    except Exception:
        pass


@pytest.fixture(scope="function")
def unique_schema():
    """Provide a unique schema name for each integration test."""
    import time

    unique_id = int(time.time() * 1000000) % 1000000
    return f"test_schema_{unique_id}"


@pytest.fixture(scope="function")
def unique_table_name():
    """Provide a function to generate unique table names for each integration test."""
    import time

    def _get_unique_table(base_name: str) -> str:
        unique_id = int(time.time() * 1000000) % 1000000
        return f"{base_name}_{unique_id}"

    return _get_unique_table


@pytest.fixture
def sample_integration_data(integration_spark_session):
    """Create sample data for integration tests."""
    data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    return integration_spark_session.createDataFrame(
        data, ["user_id", "action", "timestamp"]
    )


@pytest.fixture
def sample_integration_rules():
    """Create sample validation rules for integration tests."""
    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
    }


# Mark all tests in this conftest as integration tests
pytestmark = pytest.mark.integration
