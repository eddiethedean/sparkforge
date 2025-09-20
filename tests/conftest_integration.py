"""
Integration test configuration and fixtures.

This module provides fixtures and configuration for integration tests,
which use real Spark sessions but mock external systems.
"""

import pytest
from pyspark.sql import SparkSession
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
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
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
    """Clean up test tables after each integration test."""
    yield
    # Cleanup after each test
    try:
        if (
            hasattr(integration_spark_session, "sparkContext")
            and integration_spark_session.sparkContext._jsc is not None
        ):
            # Drop any tables that might have been created
            tables = integration_spark_session.sql(
                "SHOW TABLES IN test_schema"
            ).collect()
            for table in tables:
                table_name = table.tableName
                integration_spark_session.sql(
                    f"DROP TABLE IF EXISTS test_schema.{table_name}"
                )
    except Exception:
        # Ignore cleanup errors
        pass


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
