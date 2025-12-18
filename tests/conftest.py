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

# CRITICAL: Ensure Delta jars are always on classpath for PySpark workers (including xdist)
# This MUST be set before any PySpark imports to propagate to child workers in pytest-xdist.
# Worker processes in pytest-xdist will inherit this environment variable, ensuring
# Delta Lake JARs are available in all worker processes.
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages io.delta:delta-spark_2.12:3.0.0 pyspark-shell"
    )
    print(f"🔧 Set PYSPARK_SUBMIT_ARGS for Delta Lake: {os.environ['PYSPARK_SUBMIT_ARGS']}")
else:
    # Log if already set to help debug worker process issues
    print(f"🔧 PYSPARK_SUBMIT_ARGS already set: {os.environ.get('PYSPARK_SUBMIT_ARGS', 'NOT SET')}")

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

# Configure engine early based on SPARK_MODE
from pipeline_builder.engine_config import configure_engine

spark_mode_env = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode_env == "mock":
    from sparkless import functions as mock_functions  # type: ignore[import]
    from sparkless import spark_types as mock_types  # type: ignore[import]
    from sparkless import AnalysisException as MockAnalysisException  # type: ignore[import]
    from sparkless import Window as MockWindow  # type: ignore[import]
    from sparkless.functions import desc as mock_desc  # type: ignore[import]
    from sparkless import DataFrame as MockDataFrame  # type: ignore[import]
    from sparkless import SparkSession as MockSparkSession  # type: ignore[import]
    from sparkless import Column as MockColumn  # type: ignore[import]

    configure_engine(
        functions=mock_functions,
        types=mock_types,
        analysis_exception=MockAnalysisException,
        window=MockWindow,
        desc=mock_desc,
        engine_name="mock",
        dataframe_cls=MockDataFrame,
        spark_session_cls=MockSparkSession,
        column_cls=MockColumn,
    )
else:
    from pyspark.sql import functions as pyspark_functions
    from pyspark.sql import types as pyspark_types
    from pyspark.sql.functions import desc as pyspark_desc
    from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
    from pyspark.sql.window import Window as PySparkWindow
    from pyspark.sql import DataFrame as PySparkDataFrame
    from pyspark.sql import SparkSession as PySparkSparkSession
    from pyspark.sql import Column as PySparkColumn

    configure_engine(
        functions=pyspark_functions,
        types=pyspark_types,
        analysis_exception=PySparkAnalysisException,
        window=PySparkWindow,
        desc=pyspark_desc,
        engine_name="pyspark",
        dataframe_cls=PySparkDataFrame,
        spark_session_cls=PySparkSparkSession,
        column_cls=PySparkColumn,
    )

# Add the tests directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import test helpers from system directory
try:
    from system.system_test_helpers import (
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

    # Reset execution state and global caches
    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state as reset_globals,
        )

        reset_globals()
        reset_execution_state()
    except Exception:
        pass

    # Clear any cached Spark modules
    import sys

    [k for k in sys.modules.keys() if "pyspark" in k.lower() and "_jvm" not in k]

    yield  # Run the test

    # Reset after test - clear engine state and global caches
    try:
        from tests.test_helpers.isolation import reset_engine_state

        reset_engine_state()
    except Exception:
        pass

    try:
        from pipeline_builder.logging import reset_global_logger

        reset_global_logger()
    except Exception:
        pass

    # Reset execution state and global caches after test
    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state as reset_globals,
        )

        reset_globals()
        reset_execution_state()
    except Exception:
        pass

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


@pytest.fixture(scope="function", autouse=True)
def cleanup_before_test():
    """Reset global state before and after each test."""
    # Only reset global state - no table cleanup needed since we use unique names
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


@pytest.fixture
def fully_isolated_test(spark_session):
    """
    Comprehensive isolation fixture that combines all isolation mechanisms.
    
    Provides:
    - Unique Spark session (from spark_session fixture)
    - Isolated engine state (thread-local)
    - Isolated environment variables (thread-local)
    - Unique schemas and warehouse directories
    
    Usage:
        def test_something(fully_isolated_test):
            # Test has complete isolation
            pass
    """
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from test_helpers.isolation import (
        ThreadLocalEnvVar,
        get_unique_schema,
        reset_engine_state,
    )
    
    # Isolate environment variables
    env_vars = {}
    for var_name in ["SPARKFORGE_ENGINE"]:
        env_var = ThreadLocalEnvVar(var_name)
        env_vars[var_name] = env_var
    
    # Reset engine state at start
    reset_engine_state()
    
    yield {
        'spark': spark_session,
        'get_unique_schema': get_unique_schema,
        'env_vars': env_vars,
    }
    
    # Cleanup: reset engine state
    reset_engine_state()


@pytest.fixture(scope="function")
def unique_schema():
    """Provide a unique schema name for each test."""
    return get_unique_test_schema()


@pytest.fixture(scope="function")
def unique_name():
    """Generate unique schema/table names per test."""
    unique_id = int(time.time() * 1_000_000) % 1_000_000
    base = f"t{os.getpid()}_{unique_id}"

    def _make(kind: str, name: str) -> str:
        return f"{kind}_{name}_{base}"

    return _make


@pytest.fixture(scope="function")
def unique_table_name():
    """Provide a function to generate unique table names for each test."""
    import time

    def _get_unique_table(base_name: str) -> str:
        unique_id = int(time.time() * 1000000) % 1000000
        return f"{base_name}_{unique_id}"

    return _get_unique_table


def get_test_schema():
    """Get the test schema name."""
    return "test_schema"


def get_unique_test_schema():
    """Get a unique test schema name for isolated tests."""
    unique_id = int(time.time() * 1000000) % 1000000
    return f"test_schema_{unique_id}"


def get_unique_table_name(base_name: str) -> str:
    """Generate a unique table name by appending a timestamp-based ID."""
    import time

    unique_id = int(time.time() * 1000000) % 1000000
    return f"{base_name}_{unique_id}"


def _log_session_configs(spark, context: str = ""):
    """
    Log current session configs and identity for debugging Delta Lake configuration issues.
    
    Args:
        spark: SparkSession to check
        context: Context string to include in log message
    """
    try:
        import os
        from pyspark.sql import SparkSession as PySparkSparkSession
        
        # Get session identity
        session_id = id(spark)
        session_pid = os.getpid()
        
        # Try to get JVM session ID if available
        jvm_session_id = None
        try:
            if hasattr(spark, "_jsparkSession"):
                jvm_session_id = str(id(spark._jsparkSession))
        except Exception:
            pass
        
        # Get active session for comparison
        active_session_id = None
        active_session_match = False
        try:
            active_session = PySparkSparkSession.getActiveSession()
            if active_session:
                active_session_id = id(active_session)
                active_session_match = (active_session_id == session_id)
        except Exception:
            pass
        
        # Get configs
        ext = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        cat = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]
        
        # Try to check JVM-level configuration
        jvm_extensions = None
        jvm_catalog = None
        try:
            if hasattr(spark, "_jsparkSession"):
                jspark_session = spark._jsparkSession
                # Try to get config from JVM session
                try:
                    jvm_extensions = jspark_session.conf().get("spark.sql.extensions", "")
                except Exception:
                    pass
                try:
                    jvm_catalog = jspark_session.conf().get("spark.sql.catalog.spark_catalog", "")
                except Exception:
                    pass
        except Exception:
            pass
        
        # Log comprehensive session state
        print(f"🔍 {context} - Session State:")
        print(f"   PID: {session_pid}")
        print(f"   Session ID (Python): {session_id}")
        if jvm_session_id:
            print(f"   Session ID (JVM): {jvm_session_id}")
        if active_session_id is not None:
            print(f"   Active Session ID: {active_session_id}")
            print(f"   Matches Active: {active_session_match}")
        print(f"   Extensions (Python): '{ext}'")
        print(f"   Catalog (Python): '{cat}'")
        if jvm_extensions is not None:
            print(f"   Extensions (JVM): '{jvm_extensions}'")
        if jvm_catalog is not None:
            print(f"   Catalog (JVM): '{jvm_catalog}'")
        
        # Check if Delta is actually configured
        delta_configured = (
            "DeltaSparkSessionExtension" in ext and "DeltaCatalog" in cat
        )
        if not delta_configured:
            print(f"⚠️ {context} - Delta Lake NOT properly configured!")
        else:
            print(f"✅ {context} - Delta Lake configuration verified (Python level)")
            
        # Check JVM level if available
        if jvm_extensions is not None or jvm_catalog is not None:
            jvm_delta_configured = (
                jvm_extensions and "DeltaSparkSessionExtension" in jvm_extensions
                and jvm_catalog and "DeltaCatalog" in jvm_catalog
            )
            if jvm_delta_configured:
                print(f"✅ {context} - Delta Lake configuration verified (JVM level)")
            else:
                print(f"⚠️ {context} - Delta Lake NOT properly configured at JVM level!")
                print(f"   This may explain why Delta operations fail despite Python configs being set")
        
        return delta_configured
    except Exception as e:
        import traceback
        print(f"⚠️ {context} - Could not read session configs: {e}")
        traceback.print_exc()
        return False


def _create_mock_spark_session():
    """Create a mock Spark session."""
    from sparkless import SparkSession, functions as mock_functions  # type: ignore[import]
    from sparkless import spark_types as mock_types  # type: ignore[import]
    from sparkless import AnalysisException as MockAnalysisException  # type: ignore[import]
    from sparkless import Window as MockWindow  # type: ignore[import]
    from sparkless.functions import desc as mock_desc  # type: ignore[import]
    from pipeline_builder.engine import configure_engine

    print("🔧 Creating Mock Spark session for all tests")

    # Create mock Spark session
    spark = SparkSession(f"SparkForgeTests-{os.getpid()}")

    # Create test database using SQL (works for both mock-spark and PySpark)
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        print("✅ Test database created successfully")
    except Exception as e:
        print(f"❌ Could not create test_schema database: {e}")

    # Configure pipeline_builder engine for sparkless
    configure_engine(
        functions=mock_functions,
        types=mock_types,
        analysis_exception=MockAnalysisException,
        window=MockWindow,
        desc=mock_desc,
    )

    return spark


def _create_real_spark_session():
    """Create a real Spark session with Delta Lake support."""
    from pyspark.sql import SparkSession, functions as pyspark_functions
    from pyspark.sql import types as pyspark_types
    from pyspark.sql.functions import desc as pyspark_desc
    from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
    from pyspark.sql.window import Window as PySparkWindow
    from pipeline_builder.engine import configure_engine

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

        # CRITICAL: Stop any existing Spark session to ensure getOrCreate() doesn't reuse it
        # getOrCreate() will reuse an existing session if one exists, even if it doesn't have Delta config
        # We must stop it first to force creation of a new session with our Delta config
        try:
            existing_spark = SparkSession.getActiveSession()
            if existing_spark:
                print("🔧 Stopping existing Spark session before creating new one (prevent getOrCreate() reuse)")
                existing_spark.stop()
                # Also clear internal cache to be absolutely sure
                if hasattr(SparkSession, "_instantiatedContext"):
                    SparkSession._instantiatedContext = None  # type: ignore[attr-defined]
        except Exception:
            pass  # Ignore errors when stopping sessions

        # Build Spark session builder - match conftest_delta.py pattern exactly
        # Set Delta configs in builder BEFORE calling configure_spark_with_delta_pip
        # Get worker ID for concurrent testing isolation (pytest-xdist)
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
        unique_app_name = f"pytest-spark-{worker_id}"
        builder = (
            SparkSession.builder.appName(unique_app_name)
            .master("local[1]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        # Use configure_spark_with_delta_pip for JAR management
        # Match conftest_delta.py: use getOrCreate() but ensure no existing session
        print(f"🔍 Creating Spark session with app name: {unique_app_name}")
        
        # Check if there's an active session before calling getOrCreate()
        active_before = SparkSession.getActiveSession()
        if active_before:
            print(f"⚠️ WARNING: Active session exists before getOrCreate(): {id(active_before)}")
            _log_session_configs(active_before, "Active session BEFORE getOrCreate()")
        
        configured_builder = configure_spark_with_delta_pip(builder)
        
        # Use getOrCreate() - .create() only works with Spark Connect (remote mode)
        # We've already stopped any existing session, so getOrCreate() should create a new one
        print(f"🔍 Calling getOrCreate() (after stopping existing session)...")
        spark = configured_builder.getOrCreate()
        print(f"✅ getOrCreate() returned session: {id(spark)}")

        # Verify configuration is correct
        actual_ext = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        actual_cat = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]
        print(f"🔍 Delta config after session creation - Extensions: '{actual_ext}', Catalog: '{actual_cat}'")
        
        # If configs aren't set, something went wrong
        if "DeltaSparkSessionExtension" not in actual_ext or "DeltaCatalog" not in actual_cat:
            raise RuntimeError(
                f"Delta Lake not properly configured. Extensions: '{actual_ext}', Catalog: '{actual_cat}'. "
                f"This should not happen if configs are set in builder before .create()."
            )

        print("✅ Delta Lake configuration completed and verified")
        
        # Log session identity after creation
        print(f"🔍 _create_real_spark_session - Session created:")
        print(f"   Session ID (Python): {id(spark)}")
        try:
            if hasattr(spark, "_jsparkSession"):
                print(f"   Session ID (JVM): {id(spark._jsparkSession)}")
        except Exception:
            pass
        _log_session_configs(spark, "_create_real_spark_session (after creation)")
        
        # Set log level to WARN to reduce noise (matching conftest_delta.py pattern)
        spark.sparkContext.setLogLevel("WARN")
        
        # Clear catalog cache at start to ensure clean state
        # This prevents stale table metadata from causing conflicts in parallel tests
        try:
            spark.catalog.clearCache()
        except Exception:
            pass  # Ignore cache clearing errors

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
        print(
            "   6. Or set SPARKFORGE_BASIC_SPARK=1 to use basic Spark without Delta Lake"
        )

        # Check if user explicitly wants to skip Delta Lake or use basic Spark
        skip_delta = os.environ.get("SPARKFORGE_SKIP_DELTA", "0") == "1"
        basic_spark = os.environ.get("SPARKFORGE_BASIC_SPARK", "0") == "1"

        if skip_delta or basic_spark:
            print("🔧 Using basic Spark configuration as requested")
            try:
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
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .config("spark.driver.memory", "1g")
                    .config("spark.executor.memory", "1g")
                )

                spark = builder.getOrCreate()
                # Clear catalog cache at start to ensure clean state
                try:
                    spark.catalog.clearCache()
                except Exception:
                    pass  # Ignore cache clearing errors
            except Exception as e2:
                print(f"❌ Failed to create basic Spark session: {e2}")
                raise
        else:
            # Default to basic Spark if Delta Lake is not available
            # This allows tests to run without Delta Lake (logging will use parquet format)
            print("🔧 Delta Lake not available, using basic Spark configuration")
            try:
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
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .config("spark.driver.memory", "1g")
                    .config("spark.executor.memory", "1g")
                )

                spark = builder.getOrCreate()
                # Clear catalog cache at start to ensure clean state
                try:
                    spark.catalog.clearCache()
                except Exception:
                    pass  # Ignore cache clearing errors
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

    # Configure pipeline_builder engine for PySpark
    configure_engine(
        functions=pyspark_functions,
        types=pyspark_types,
        analysis_exception=PySparkAnalysisException,
        window=PySparkWindow,
        desc=pyspark_desc,
    )

    return spark


@pytest.fixture
def isolate_engine_config():
    """
    Context manager fixture to isolate engine configuration changes.
    
    Saves the current engine state, allows modification within the test,
    and restores it after the test completes.
    
    Usage:
        def test_something(isolate_engine_config):
            with isolate_engine_config():
                # Modify engine configuration
                configure_engine(...)
                # Test code
                pass
            # Engine state automatically restored
    """
    from contextlib import contextmanager
    from pipeline_builder.engine_config import get_engine, configure_engine
    
    @contextmanager
    def _isolate():
        # Save current engine state
        try:
            current_engine = get_engine()
            saved_config = {
                'functions': current_engine.functions,
                'types': current_engine.types,
                'analysis_exception': current_engine.analysis_exception,
                'window': current_engine.window,
                'desc': current_engine.desc,
                'engine_name': current_engine.engine_name,
                'dataframe_cls': current_engine.dataframe_cls,
                'spark_session_cls': current_engine.spark_session_cls,
                'column_cls': current_engine.column_cls,
            }
        except Exception:
            saved_config = None
        
        try:
            yield
        finally:
            # Restore saved engine state
            if saved_config is not None:
                try:
                    configure_engine(**saved_config)
                except Exception:
                    pass
    
    return _isolate


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
        print("🔧 spark_session fixture: Creating real Spark session...")
        print(f"🔧 spark_session fixture: PID={os.getpid()}")
        spark = _create_real_spark_session()
        
        # Log session identity before yielding
        print(f"🔧 spark_session fixture: Session ID (Python)={id(spark)}")
        try:
            if hasattr(spark, "_jsparkSession"):
                print(f"🔧 spark_session fixture: Session ID (JVM)={id(spark._jsparkSession)}")
        except Exception:
            pass
        
        # Verify Delta configs are still set before yielding to tests
        # This ensures configs haven't been lost between creation and test execution
        print("🔧 spark_session fixture: Verifying Delta configs before yielding...")
        _log_session_configs(spark, "spark_session fixture (BEFORE YIELD)")
        ext = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        cat = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]
        if "DeltaSparkSessionExtension" not in ext or "DeltaCatalog" not in cat:
            # Use our debug logging helper
            _log_session_configs(spark, "spark_session fixture (BEFORE YIELD - CONFIGS MISSING)")
            raise RuntimeError(
                f"Delta configs lost before test execution! Extensions: '{ext}', Catalog: '{cat}'. "
                f"This indicates the session configuration was lost or overridden."
            )
        print(f"✅ spark_session fixture: Verified Delta configs - ready to yield")
    else:
        spark = _create_mock_spark_session()

    # Attach storage wrapper to SparkSession for all tests
    try:
        from tests.builder_tests.storage_wrapper import StorageWrapper

        spark.storage = StorageWrapper(spark)  # type: ignore[attr-defined]
    except ImportError:
        # If storage wrapper is not available, skip attaching it
        # This allows tests to work even if builder_tests is not available
        pass

    yield spark

    # Cleanup using comprehensive isolation helpers
    try:
        # Import isolation helpers
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state,
        )

        # Reset global caches first (before Spark cleanup)
        reset_global_state()
        reset_execution_state()

        if spark_mode == "real":
            # Real Spark cleanup - Stop session to maintain test isolation with function scope
            if spark:
                try:
                    # Clear cache first
                    if (
                        hasattr(spark, "sparkContext")
                        and spark.sparkContext._jsc is not None
                    ):
                        try:
                            spark.catalog.clearCache()
                        except Exception:
                            pass
                    # Stop the session to ensure clean state for next test
                    print(f"🔧 spark_session fixture: Stopping session after test (function scope isolation)")
                    spark.stop()
                    # Clear internal cache to prevent getOrCreate() from reusing this session
                    try:
                        if hasattr(SparkSession, "_instantiatedContext"):
                            SparkSession._instantiatedContext = None  # type: ignore[attr-defined]
                    except Exception:
                        pass
                except Exception as e:
                    print(f"⚠️ Error stopping Spark session: {e}")
        else:
            # Mock Spark cleanup - minimal since we use unique table names
            if spark:
                try:
                    spark.catalog.clearCache()
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

        # Create the spark session using the helper function
        spark = _create_real_spark_session()

        # Create isolated test database
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
            print(f"✅ Isolated test database {schema_name} created successfully")
        except Exception as e:
            print(f"❌ Could not create isolated test database {schema_name}: {e}")

        # Attach storage wrapper
        try:
            from tests.builder_tests.storage_wrapper import StorageWrapper

            spark.storage = StorageWrapper(spark)  # type: ignore[attr-defined]
        except ImportError:
            pass

        yield spark

        # Minimal cleanup - unique schema names ensure isolation
        try:
            if spark:
                spark.catalog.clearCache()
        except Exception:
            pass
    else:
        # For mock Spark, create a new session using sparkless
        unique_id = int(time.time() * 1000000) % 1000000
        schema_name = f"test_schema_{unique_id}"

        print(f"🔧 Creating isolated Mock Spark session for {schema_name}")

        from sparkless import SparkSession  # type: ignore[import]

        spark = SparkSession(f"SparkForgeTests-{os.getpid()}-{unique_id}")

        # Create isolated test database using SQL
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            print(f"✅ Isolated test database {schema_name} created successfully")
        except Exception as e:
            print(f"❌ Could not create isolated test database {schema_name}: {e}")

        # Attach storage wrapper
        try:
            from tests.builder_tests.storage_wrapper import StorageWrapper

            spark.storage = StorageWrapper(spark)  # type: ignore[attr-defined]
        except ImportError:
            pass

        yield spark

        # Minimal cleanup - unique schema names ensure isolation
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
    import os
    print(f"🔧 mock_spark_session fixture: Executing (alias for spark_session)")
    print(f"🔧 mock_spark_session fixture: PID={os.getpid()}")
    print(f"🔧 mock_spark_session fixture: Session ID (Python)={id(spark_session)}")
    try:
        if hasattr(spark_session, "_jsparkSession"):
            print(f"🔧 mock_spark_session fixture: Session ID (JVM)={id(spark_session._jsparkSession)}")
    except Exception:
        pass
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

    from sparkless import Functions  # type: ignore[import]

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
        from sparkless.spark_types import (  # type: ignore[import]
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
        from sparkless.spark_types import (  # type: ignore[import]
            StructField,
            StructType,
            StringType,
        )

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

        from sparkless import functions as mock_functions  # type: ignore[import]

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
    config.addinivalue_line(
        "markers", "sequential: marks tests that must run sequentially (not in parallel) to avoid race conditions"
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
