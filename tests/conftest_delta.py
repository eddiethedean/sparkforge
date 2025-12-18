"""
Pytest configuration for Delta Lake specific tests.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def delta_spark_session():
    """Create a Spark session with Delta Lake support for testing."""
    import os
    import shutil

    # Clean up any existing test data
    warehouse_dir = "/tmp/spark-warehouse-delta"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark with Delta Lake support
    try:
        from delta import configure_spark_with_delta_pip

        print("🔧 Configuring Spark with Delta Lake support")

        # Get worker ID for concurrent testing isolation (pytest-xdist)
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
        builder = (
            SparkSession.builder.appName(f"pytest-spark-{worker_id}")
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    except Exception as e:
        print(f"⚠️ Delta Lake configuration failed: {e}")
        pytest.skip("Delta Lake not available")

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # Create test database first
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema_delta")
        print("✅ Delta Lake test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema_delta database: {e}")

    # Verify Delta Lake functionality
    try:
        print("🔍 Verifying Delta Lake functionality...")
        test_df = spark.createDataFrame([(1, "test")], ["id", "name"])
        test_table = "test_schema_delta.delta_verification"
        # Use prepare_delta_overwrite to handle Delta table overwrite properly
        from pipeline_builder.table_operations import prepare_delta_overwrite
        prepare_delta_overwrite(spark, test_table)
        test_df.write.format("delta").mode("overwrite").saveAsTable(test_table)

        # Test Delta Lake specific operations
        spark.sql(f"DESCRIBE HISTORY {test_table}")
        spark.sql(f"OPTIMIZE {test_table}")

        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        print("✅ Delta Lake verification successful!")

    except Exception as e:
        print(f"❌ Delta Lake verification failed: {e}")
        pytest.skip("Delta Lake verification failed")

    yield spark

    # Cleanup
    try:
        # Drop test database and tables
        spark.sql("DROP DATABASE IF EXISTS test_schema_delta CASCADE")
    except Exception as e:
        print(f"Warning: Could not drop test_schema_delta database: {e}")

    spark.stop()

    # Clean up warehouse directory
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)
