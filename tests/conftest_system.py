"""
System test configuration and fixtures.

This module provides fixtures and configuration for system tests,
which use the full Spark environment with Delta Lake and real data.
"""

import os
import shutil
import time

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def system_spark_session():
    """
    Create a full Spark session with Delta Lake for system tests.
    This is shared across all system tests for efficiency.
    """
    # Clean up any existing test data
    unique_id = int(time.time() * 1000000) % 1000000
    warehouse_dir = f"/tmp/spark-warehouse-system-{os.getpid()}-{unique_id}"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark with Delta Lake support
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName(
                f"SparkForgeSystemTests-{os.getpid()}-{unique_id}"
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
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        print(f"⚠️ Delta Lake configuration failed: {e}")
        # Fall back to basic Spark
        builder = (
            SparkSession.builder.appName(
                f"SparkForgeSystemTests-{os.getpid()}-{unique_id}"
            )
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
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
def cleanup_system_tables(system_spark_session):
    """Clean up test tables after each system test."""
    yield
    # Cleanup after each test
    try:
        if (
            hasattr(system_spark_session, "sparkContext")
            and system_spark_session.sparkContext._jsc is not None
        ):
            # Drop any tables that might have been created
            tables = system_spark_session.sql("SHOW TABLES IN test_schema").collect()
            for table in tables:
                table_name = table.tableName
                system_spark_session.sql(
                    f"DROP TABLE IF EXISTS test_schema.{table_name}"
                )
    except Exception:
        # Ignore cleanup errors
        pass


@pytest.fixture
def large_test_data(system_spark_session):
    """Create large test dataset for system tests."""
    data = []
    for i in range(10000):
        data.append(
            (
                f"user{i}",
                "click" if i % 2 == 0 else "view",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 100,
            )
        )

    return system_spark_session.createDataFrame(
        data, ["user_id", "action", "timestamp", "value"]
    )


@pytest.fixture
def performance_test_data(system_spark_session):
    """Create performance test dataset for system tests."""
    data = []
    for i in range(100000):
        data.append(
            (
                f"user{i % 1000}",
                "click" if i % 3 == 0 else "view" if i % 3 == 1 else "purchase",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 1000,
                f"category{i % 10}",
            )
        )

    return system_spark_session.createDataFrame(
        data, ["user_id", "action", "timestamp", "value", "category"]
    )


@pytest.fixture
def delta_lake_test_data(system_spark_session):
    """Create test data specifically for Delta Lake system tests."""
    data = []
    for i in range(1000):
        data.append(
            (
                i,
                f"user{i}",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 100,
                f"status{i % 3}",
            )
        )

    return system_spark_session.createDataFrame(
        data, ["id", "user_id", "timestamp", "value", "status"]
    )


# Mark all tests in this conftest as system tests
pytestmark = pytest.mark.system
