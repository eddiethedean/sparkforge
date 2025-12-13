"""
Enhanced pytest configuration and shared fixtures for pipeline tests.

This module provides comprehensive test configuration, shared fixtures,
and utilities to support the entire test suite with better organization
and reduced duplication.

Supports both mock_spark and real Spark environments via SPARK_MODE environment variable.
"""

import os
import shutil
import sys
import time

# Set PySpark Python environment variables early, before any Spark imports
# This ensures workers use the same Python version as the driver
# Use absolute path to ensure consistency
python_executable = os.path.abspath(sys.executable)
# Always set these to override any existing values and ensure consistency
os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

# Ensure project root and src directory are on sys.path before loading compatibility shims
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Load interpreter compatibility tweaks (e.g., typing.TypeAlias patch for Python 3.8)
import sitecustomize  # type: ignore  # noqa: E402,F401

import pytest

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


@pytest.fixture(autouse=True, scope="function")
def reset_global_state():
    """Reset global state before and after each test to prevent pollution."""
    # Reset before test
    try:
        from pipeline_builder.logging import reset_global_logger

        reset_global_logger()
    except Exception:
        pass

    # Clear any cached Spark modules
    import sys

    [k for k in sys.modules.keys() if "pyspark" in k.lower() and "_jvm" not in k]
    # Don't remove modules, just ensure SparkContext is clean
    try:
        from pipeline_builder.compat import compat_name

        if compat_name() == "pyspark":
            # Use compatibility layer

            # SparkContext is accessed via SparkSession
            SparkContext = None  # Not needed for mock-spark

            if SparkContext._active_spark_context is not None:
                # Don't stop it as other tests might need it
                pass
    except Exception:
        pass

    yield

    # Reset after test
    try:
        from pipeline_builder.logging import reset_global_logger

        reset_global_logger()
    except Exception:
        pass


def get_test_schema():
    """Get the test schema name."""
    return "test_schema"


def get_unique_test_schema():
    """Get a unique test schema name for isolated tests."""
    unique_id = int(time.time() * 1000000) % 1000000
    return f"test_schema_{unique_id}"


def _create_mock_spark_session():
    """Create a mock Spark session."""
    from mock_spark import SparkSession

    print("🔧 Creating Mock Spark session for all tests")

    # Create mock Spark session
    spark = SparkSession(f"SparkForgeTests-{os.getpid()}")

    # Create test database using SQL (works for both mock-spark and PySpark)
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")

    return spark


def _create_real_spark_session():
    """Create a real Spark session with Delta Lake support."""
    from pyspark.sql import SparkSession

    # Set Python version for PySpark to match current interpreter
    # This prevents Python version mismatch between driver and workers
    # Use the python_executable from module level (set early)
    # Force update to ensure it's set before Spark initialization
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    print(f"🔧 Using Python at: {python_executable}")
    print(f"🔧 PYSPARK_PYTHON={os.environ.get('PYSPARK_PYTHON')}")
    print(f"🔧 PYSPARK_DRIVER_PYTHON={os.environ.get('PYSPARK_DRIVER_PYTHON')}")

    # Set Java environment
    java_home = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/java11")
    if not os.path.exists(java_home):
        # Try alternative Java paths
        for alt_path in [
            "/opt/homebrew/opt/openjdk@11",
            "/usr/lib/jvm/java-11-openjdk",
        ]:
            if os.path.exists(alt_path):
                java_home = alt_path
                break

    os.environ["JAVA_HOME"] = java_home
    print(f"🔧 Using Java at: {java_home}")

    # Clean up any existing test data
    warehouse_dir = f"/tmp/spark-warehouse-{os.getpid()}"
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    # Configure Spark with Delta Lake support
    spark = None
    try:
        # Import delta-spark module - handle namespace package issues
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError:
            # Try alternative import path
            import delta.pip_utils as pip_utils

            configure_spark_with_delta_pip = pip_utils.configure_spark_with_delta_pip

        print("🔧 Configuring real Spark with Delta Lake support for all tests")

        # Build Spark session builder with basic config
        # Note: Do NOT set spark.sql.extensions or spark.sql.catalog here
        # configure_spark_with_delta_pip will handle Delta Lake configuration
        builder = (
            SparkSession.builder.appName(f"SparkForgeTests-{os.getpid()}")
            .master("local[*]")  # Use all available cores for parallel execution
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
        )

        # Configure Delta Lake - configure_spark_with_delta_pip handles extensions and catalog
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Verify Delta Lake configuration was applied
        try:
            # Check if extensions include Delta
            extensions = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
            if "io.delta.sql.DeltaSparkSessionExtension" not in extensions:
                print("⚠️ Warning: Delta extensions not automatically set, setting manually")
                existing_extensions = extensions.split(",") if extensions else []
                if "io.delta.sql.DeltaSparkSessionExtension" not in existing_extensions:
                    new_extensions = ",".join(existing_extensions + ["io.delta.sql.DeltaSparkSessionExtension"])
                    spark.conf.set("spark.sql.extensions", new_extensions)  # type: ignore[attr-defined]
                else:
                    print("   Delta extensions already present")
            
            # Check if catalog is set to Delta
            current_catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]
            if "DeltaCatalog" not in current_catalog:
                print("⚠️ Warning: Delta catalog not automatically set, attempting to set manually")
                try:
                    spark.conf.set(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )  # type: ignore[attr-defined]
                    print("   Delta catalog set successfully")
                except Exception as catalog_error:
                    print(f"   Could not set Delta catalog: {catalog_error}")
                    print("   This may cause issues with Delta table operations")
            else:
                print("✅ Delta catalog configured correctly")
            
            print("✅ Delta Lake configuration completed")
        except Exception as verify_error:
            print(f"⚠️ Delta Lake verification issue (non-fatal): {verify_error}")

    except Exception as e:
        import traceback
        print(f"❌ Delta Lake configuration failed: {e}")
        print("💡 Error details:")
        traceback.print_exc()
        print("\n💡 To fix this issue:")
        print("   1. Install Delta Lake: pip install delta-spark")
        print("   2. Ensure PySpark is installed: pip install pyspark>=3.5.0")
        print("   3. Check Java version: java -version (should be Java 8, 11, or 17)")
        print("   4. Set JAVA_HOME environment variable if needed")
        print("   5. Or set SPARKFORGE_SKIP_DELTA=1 to skip Delta Lake tests")
        print("   6. Or set SPARKFORGE_BASIC_SPARK=1 to use basic Spark without Delta Lake")

        # Check if user explicitly wants to skip Delta Lake or use basic Spark
        skip_delta = os.environ.get("SPARKFORGE_SKIP_DELTA", "0") == "1"
        basic_spark = os.environ.get("SPARKFORGE_BASIC_SPARK", "0") == "1"

        if skip_delta or basic_spark:
            print("🔧 Using basic Spark configuration as requested")
            try:
                builder = (
                    SparkSession.builder.appName(f"SparkForgeTests-{os.getpid()}")
                    .master("local[*]")
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
            # Default to basic Spark if Delta Lake is not available
            # This allows tests to run without Delta Lake (logging will use parquet format)
            print("🔧 Delta Lake not available, using basic Spark configuration")
            try:
                builder = (
                    SparkSession.builder.appName(f"SparkForgeTests-{os.getpid()}")
                    .master("local[*]")
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

    return spark


@pytest.fixture(scope="function")
def spark_session():
    """
    Create a Spark session for testing (function-scoped for test isolation).

    This fixture creates either a mock Spark session or a real Spark session
    based on the SPARK_MODE environment variable:
    - SPARK_MODE=mock (default): Uses mock_spark
    - SPARK_MODE=real: Uses real Spark with Delta Lake
    
    This is the primary fixture for all tests - it automatically provides
    the correct session type based on SPARK_MODE.
    """
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        spark = _create_real_spark_session()
    else:
        spark = _create_mock_spark_session()

    yield spark

    # Cleanup
    try:
        if spark_mode == "real":
            # Real Spark cleanup
            if (
                spark
                and hasattr(spark, "sparkContext")
                and spark.sparkContext._jsc is not None
            ):
                # Clear all cached tables and temp views
                spark.catalog.clearCache()

                # Drop all tables in test schema
                try:
                    tables = spark.catalog.listTables("test_schema")
                    for table in tables:
                        spark.sql(f"DROP TABLE IF EXISTS test_schema.{table.name}")
                except Exception:
                    pass  # Ignore errors when dropping tables

                # Drop test schema
                spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")

            if spark:
                spark.stop()
        else:
            # Mock Spark cleanup - use SQL to drop schemas
            if spark:
                try:
                    # Get list of databases/schemas
                    databases = [db.name for db in spark.catalog.listDatabases()]
                    for schema_name in databases:
                        if schema_name not in ["default", "information_schema"]:
                            try:
                                spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                            except Exception:
                                pass
                except Exception:
                    pass

            # Stop the session
            if spark and hasattr(spark, "stop"):
                spark.stop()

            print("🧹 Mock Spark session cleanup completed")
    except Exception as e:
        print(f"Warning: Could not clean up test database: {e}")


@pytest.fixture(scope="function")
def isolated_spark_session():
    """
    Create an isolated Spark session for tests that need complete isolation.

    This fixture creates a new Spark session for each test function.
    """
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        # For real Spark, create a new session
        unique_id = int(time.time() * 1000000) % 1000000
        schema_name = f"test_schema_{unique_id}"

        print(f"🔧 Creating isolated real Spark session for {schema_name}")

        # Create the spark session using the main fixture
        from conftest import spark_session_fixture

        spark = spark_session_fixture()

        # Create isolated test database
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
            print(f"✅ Isolated test database {schema_name} created successfully")
        except Exception as e:
            print(f"❌ Could not create isolated test database {schema_name}: {e}")

        yield spark

        # Cleanup
        try:
            spark.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except Exception:
            pass
    else:
        # For mock Spark, create a new session
        unique_id = int(time.time() * 1000000) % 1000000
        schema_name = f"test_schema_{unique_id}"

        print(f"🔧 Creating isolated Mock Spark session for {schema_name}")

        from mock_spark import SparkSession

        spark = SparkSession(f"SparkForgeTests-{os.getpid()}-{unique_id}")

        # Create isolated test database using SQL (works for both mock-spark and PySpark)
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            print(f"✅ Isolated test database {schema_name} created successfully")
        except Exception as e:
            print(f"❌ Could not create isolated test database {schema_name}: {e}")

        yield spark

        # Cleanup
        print(f"🧹 Isolated Mock Spark session cleanup completed for {schema_name}")


@pytest.fixture(scope="function")
def mock_spark_session(spark_session):
    """
    Create a Spark session for testing (interchangeable with spark_session).

    This fixture is an alias for spark_session - it automatically provides
    the correct session type based on SPARK_MODE:
    - SPARK_MODE=mock (default): Returns mock-spark session
    - SPARK_MODE=real: Returns real PySpark session
    
    Use this fixture when you want to make it clear you're using a session
    that works in both modes. For maximum compatibility, prefer spark_session.
    
    This makes tests work seamlessly in both modes without code changes.
    """
    # Simply return the spark_session fixture which already handles mode switching
    # The spark_session fixture handles all cleanup automatically
    return spark_session


@pytest.fixture(scope="function")
def mock_functions():
    """
    Create a Mock Functions instance for testing.

    This fixture provides mock PySpark functions for testing.
    Only available when using mock Spark mode.
    """
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        # For real Spark, return None or skip this fixture
        pytest.skip("Mock functions not available in real Spark mode")

    from mock_spark import Functions

    return Functions()


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
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
                StructField("category", StringType(), True),
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
    else:
        from mock_spark import (
            DoubleType,
            IntegerType,
            StructField,
            StructType,
            StringType,
        )

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
                StructField("category", StringType(), True),
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
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        return spark_session.createDataFrame([], schema)
    else:
        from mock_spark import StructField, StructType, StringType

        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        return spark_session.createDataFrame([], schema)


@pytest.fixture(scope="function")
def large_dataset():
    """
    Create a large dataset for testing.

    This fixture creates a list of dictionaries representing a large dataset
    for testing pipeline performance and data handling.
    """
    # Create 1000 rows of test data
    return [
        {
            "id": i,
            "name": f"name_{i}",
            "value": float(i * 1.5),
            "category": f"category_{i % 10}",
        }
        for i in range(1, 1001)
    ]


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


@pytest.fixture(scope="function")
def test_config():
    """
    Provide test configuration for pipeline tests.

    Returns a PipelineConfig object for testing.
    """
    from pipeline_builder.models import (
        ParallelConfig,
        PipelineConfig,
        ValidationThresholds,
    )

    return PipelineConfig(
        schema="test_schema",
        thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
        parallel=ParallelConfig(enabled=False, max_workers=1),
    )


@pytest.fixture(scope="function", autouse=True)
def mock_pyspark_functions():
    """
    Automatically mock PySpark functions when in mock mode.

    This fixture replaces PySpark functions with mock functions
    when SPARK_MODE=mock to prevent JVM-related errors.
    """
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "mock":
        import sys

        from mock_spark import functions as mock_functions

        # Store original module
        original_pyspark_functions = sys.modules.get("pyspark.sql.functions")

        # Replace with mock functions
        sys.modules["pyspark.sql.functions"] = mock_functions

        yield

        # Restore original module
        if original_pyspark_functions:
            sys.modules["pyspark.sql.functions"] = original_pyspark_functions
        else:
            # Remove the mock if it wasn't there originally
            if "pyspark.sql.functions" in sys.modules:
                del sys.modules["pyspark.sql.functions"]
    else:
        yield


# Test configuration
def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "system: marks tests as system tests")
    config.addinivalue_line(
        "markers", "mock_only: marks tests that only work with mock Spark"
    )
    config.addinivalue_line(
        "markers", "real_spark_only: marks tests that only work with real Spark"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file location and environment."""
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

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

        # Skip tests based on Spark mode
        if spark_mode == "real" and "mock_only" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="Test requires mock Spark mode"))
        elif spark_mode == "mock" and "real_spark_only" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="Test requires real Spark mode"))
