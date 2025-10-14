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

import pytest

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


@pytest.fixture(autouse=True, scope="function")
def reset_global_state():
    """Reset global state before and after each test to prevent pollution."""
    # Reset before test
    try:
        from sparkforge.logging import reset_global_logger
        reset_global_logger()
    except Exception:
        pass

    # Clear any cached Spark modules
    import sys
    [k for k in sys.modules.keys() if 'pyspark' in k.lower() and '_jvm' not in k]
    # Don't remove modules, just ensure SparkContext is clean
    try:
        from sparkforge.compat import compat_name
        if compat_name() == "pyspark":
            from pyspark import SparkContext
            if SparkContext._active_spark_context is not None:
                # Don't stop it as other tests might need it
                pass
    except Exception:
        pass

    yield

    # Reset after test
    try:
        from sparkforge.logging import reset_global_logger
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
    from mock_spark import MockSparkSession

    print("🔧 Creating Mock Spark session for all tests")

    # Create mock Spark session
    spark = MockSparkSession(f"SparkForgeTests-{os.getpid()}")

    # Monkey-patch createDataFrame to handle tuples when schema is provided
    original_createDataFrame = spark.createDataFrame

    def createDataFrame_wrapper(data, schema=None, **kwargs):
        """Wrapper to convert tuples to dicts when schema is provided."""
        if schema is not None and data and isinstance(data, list) and len(data) > 0:
            # Check if data contains tuples
            if isinstance(data[0], tuple):
                # Get column names from schema
                if hasattr(schema, "fieldNames"):
                    # Real PySpark StructType
                    column_names = schema.fieldNames()
                elif hasattr(schema, "names"):
                    # Mock StructType might have names attribute
                    column_names = schema.names
                elif hasattr(schema, "fields"):
                    # Extract names from fields
                    column_names = [field.name for field in schema.fields]
                elif isinstance(schema, list):
                    # Schema is a list of column names (string schema)
                    column_names = schema
                else:
                    # Can't determine column names, pass through
                    if schema is None:
                        return original_createDataFrame(data)
                    else:
                        return original_createDataFrame(data, schema)

                # Convert tuples to dictionaries
                data = [dict(zip(column_names, row)) for row in data]

        if schema is None:
            return original_createDataFrame(data)
        else:
            return original_createDataFrame(data, schema)

    spark.createDataFrame = createDataFrame_wrapper

    # Create test database
    try:
        spark.catalog.createDatabase("test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")

    return spark


def _create_real_spark_session():
    """Create a real Spark session with Delta Lake support."""
    from pyspark.sql import SparkSession

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
        from delta import configure_spark_with_delta_pip

        print("🔧 Configuring real Spark with Delta Lake support for all tests")

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
        print(
            "   3. Or set SPARKFORGE_BASIC_SPARK=1 to use basic Spark without Delta Lake"
        )

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
            ) from e

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
            # Mock Spark cleanup
            if spark and hasattr(spark, "storage"):
                # Clear all schemas except system ones
                schemas = (
                    list(spark.storage.schemas.keys())
                    if hasattr(spark.storage, "schemas")
                    else []
                )
                for schema_name in schemas:
                    if schema_name not in ["default", "information_schema"]:
                        try:
                            spark.storage.drop_schema(schema_name, cascade=True)
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

        from mock_spark import MockSparkSession

        spark = MockSparkSession(f"SparkForgeTests-{os.getpid()}-{unique_id}")

        # Monkey-patch createDataFrame to handle tuples when schema is provided
        original_createDataFrame = spark.createDataFrame

        def createDataFrame_wrapper(data, schema=None, **kwargs):
            """Wrapper to convert tuples to dicts when schema is provided."""
            if schema is not None and data and isinstance(data, list) and len(data) > 0:
                # Check if data contains tuples
                if isinstance(data[0], tuple):
                    # Get column names from schema
                    if hasattr(schema, "fieldNames"):
                        column_names = schema.fieldNames()
                    elif hasattr(schema, "names"):
                        column_names = schema.names
                    elif hasattr(schema, "fields"):
                        column_names = [field.name for field in schema.fields]
                    elif isinstance(schema, list):
                        column_names = schema
                    else:
                        if schema is None:
                            return original_createDataFrame(data)
                        else:
                            return original_createDataFrame(data, schema)

                    # Convert tuples to dictionaries
                    data = [dict(zip(column_names, row)) for row in data]

            if schema is None:
                return original_createDataFrame(data)
            else:
                return original_createDataFrame(data, schema)

        spark.createDataFrame = createDataFrame_wrapper

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
def mock_spark_session():
    """
    Create a mock Spark session for testing.

    This fixture provides a mock Spark session for individual test functions.
    Only available when using mock Spark mode.
    """
    # Set mock as default if SPARK_MODE is not explicitly set
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        # For real Spark, return None or skip this fixture
        pytest.skip("Mock Spark session not available in real Spark mode")

    from mock_spark import MockSparkSession

    spark = MockSparkSession(f"TestApp-{os.getpid()}")

    # Monkey-patch createDataFrame to handle tuples when schema is provided
    original_createDataFrame = spark.createDataFrame

    def createDataFrame_wrapper(data, schema=None, **kwargs):
        """Wrapper to convert tuples to dicts when schema is provided."""
        if schema is not None and data and isinstance(data, list) and len(data) > 0:
            # Check if data contains tuples
            if isinstance(data[0], tuple):
                # Get column names from schema
                if hasattr(schema, "fieldNames"):
                    column_names = schema.fieldNames()
                elif hasattr(schema, "names"):
                    column_names = schema.names
                elif hasattr(schema, "fields"):
                    column_names = [field.name for field in schema.fields]
                elif isinstance(schema, list):
                    column_names = schema
                else:
                    if schema is None:
                        return original_createDataFrame(data)
                    else:
                        return original_createDataFrame(data, schema)

                # Convert tuples to dictionaries
                data = [dict(zip(column_names, row)) for row in data]

        if schema is None:
            return original_createDataFrame(data)
        else:
            return original_createDataFrame(data, schema)

    spark.createDataFrame = createDataFrame_wrapper

    yield spark

    # Cleanup: Aggressively clear all mock-spark state
    try:
        if hasattr(spark, "storage") and spark.storage is not None:
            # Clear all non-system schemas
            try:
                schemas_to_drop = []
                if hasattr(spark.storage, "schemas"):
                    for schema_name in list(spark.storage.schemas.keys()):
                        if schema_name not in ["default", "information_schema", "main"]:
                            schemas_to_drop.append(schema_name)

                for schema_name in schemas_to_drop:
                    try:
                        spark.storage.drop_schema(schema_name, cascade=True)
                    except Exception:
                        pass

                # Also try to clear tables from default schema
                try:
                    if "default" in spark.storage.schemas:
                        default_schema = spark.storage.schemas["default"]
                        if hasattr(default_schema, "tables"):
                            tables_to_drop = list(default_schema.tables.keys())
                            for table_name in tables_to_drop:
                                try:
                                    spark.storage.drop_table("default", table_name)
                                except Exception:
                                    pass
                except Exception:
                    pass
            except Exception:
                pass

        # Clear catalog cache
        if hasattr(spark, "catalog"):
            try:
                spark.catalog.clearCache()
            except Exception:
                pass

        # Stop the session
        if hasattr(spark, "stop"):
            try:
                spark.stop()
            except Exception:
                pass
    except Exception:
        pass


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

    from mock_spark import MockFunctions

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
        from mock_spark import MockStructField, MockStructType, StringType

        schema = MockStructType(
            [
                MockStructField("col1", StringType(), True),
                MockStructField("col2", StringType(), True),
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
    from sparkforge.models import ParallelConfig, PipelineConfig, ValidationThresholds

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
