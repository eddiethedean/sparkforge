"""
Pytest configuration using sparkless.testing framework.

This module provides test configuration using the sparkless.testing plugin
which handles Spark session management, imports, and mode switching automatically.

The sparkless.testing plugin provides:
- `spark` fixture: Main Spark session (works in both sparkless and pyspark modes)
- `spark_imports` fixture: Unified access to F, types, Window, etc.
- `spark_mode` fixture: Returns current Mode enum (Mode.SPARKLESS or Mode.PYSPARK)
- `isolated_session` fixture: Fresh session for isolation
- `table_prefix` fixture: Unique prefix for test isolation
- `@pytest.mark.sparkless_only`: Skip test in pyspark mode
- `@pytest.mark.pyspark_only`: Skip test in sparkless mode
"""

import os
import shutil
import sys
import time
import tempfile

import pytest

# Configure Delta Lake packages for PySpark before any Spark session is created
# This ensures all pytest-xdist workers have Delta Lake available
_mode = os.environ.get("SPARKLESS_TEST_MODE", "sparkless").lower()
if _mode == "pyspark":
    _pyspark_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "io.delta" not in _pyspark_args:
        # Add Delta Lake packages to PYSPARK_SUBMIT_ARGS
        _delta_packages = "io.delta:delta-spark_2.12:3.0.0"
        if _pyspark_args:
            if "--packages" in _pyspark_args:
                # Append to existing packages
                _pyspark_args = _pyspark_args.replace(
                    "--packages ",
                    f"--packages {_delta_packages},"
                )
            else:
                _pyspark_args = f"--packages {_delta_packages} {_pyspark_args}"
        else:
            _pyspark_args = f"--packages {_delta_packages} pyspark-shell"
        os.environ["PYSPARK_SUBMIT_ARGS"] = _pyspark_args

# Register sparkless.testing plugin - provides spark, spark_imports, spark_mode fixtures
pytest_plugins = ["sparkless.testing"]

# Override spark fixture to enable Delta Lake in PySpark mode
@pytest.fixture
def spark(request):
    """SparkSession fixture with Delta Lake enabled for PySpark mode."""
    from sparkless.testing import get_mode, Mode, create_session
    
    mode = get_mode()
    test_name = "test_app"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        test_name = f"test_{request.node.name[:50]}"
    
    # For parity, enable Delta support in both modes.
    # (In sparkless this is typically a no-op, but it keeps session setup aligned.)
    enable_delta = True
    
    session = create_session(app_name=test_name, mode=mode, enable_delta=enable_delta)

    # Align core Spark SQL settings across sparkless and pyspark.
    # Many tests write Delta tables via saveAsTable(..., format="delta"), which requires
    # spark.sql.warehouse.dir to be set (even in sparkless).
    warehouse_dir = tempfile.mkdtemp(prefix=f"spark-warehouse-{os.getpid()}-")
    try:
        # Prefer SparkSession.conf API when present.
        if hasattr(session, "conf") and hasattr(session.conf, "set"):
            session.conf.set("spark.sql.warehouse.dir", warehouse_dir)
        else:
            # Fallback: some engines expose a SQL interface only.
            if hasattr(session, "sql"):
                session.sql(f"SET spark.sql.warehouse.dir={warehouse_dir}")
    except Exception:
        # If the underlying engine doesn't support setting conf, tests will surface it.
        pass

    # Some tests call `saveAsTable` directly and expect `test_schema` to exist.
    # In PySpark mode this schema/database is not guaranteed to be present, so
    # create it here for consistent behavior.
    try:
        if mode == Mode.PYSPARK:
            if hasattr(session, "sql"):
                session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    except Exception:
        pass
    yield session
    
    # Cleanup - don't stop PySpark session (reused across tests)
    if mode == Mode.SPARKLESS:
        try:
            session.stop()
        except Exception:
            pass
    try:
        shutil.rmtree(warehouse_dir, ignore_errors=True)
    except Exception:
        pass


# Ensure project root and src directory are on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Add tests directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load interpreter compatibility tweaks
try:
    import sitecustomize  # type: ignore  # noqa: E402,F401
except ImportError:
    pass


# Configure default engine early (before test modules are collected)
# This allows test modules to import from pipeline_builder.compat at module level
def _configure_default_engine():
    """Configure default sparkless engine early for module-level imports."""
    mode_str = os.environ.get("SPARKLESS_TEST_MODE", "sparkless").lower()
    from pipeline_builder.engine_config import configure_engine

    if mode_str == "pyspark":
        try:
            from pyspark.sql import functions as pyspark_functions
            from pyspark.sql import types as pyspark_types
            from pyspark.sql.functions import desc as pyspark_desc
            from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
            from pyspark.sql.window import Window as PySparkWindow
            from pyspark.sql import DataFrame as PySparkDataFrame
            from pyspark.sql import SparkSession as PySparkSparkSession
            from pyspark.sql import Column as PySparkColumn

            # In PySpark mode, sparkless.testing often wraps the real SparkSession/DF
            # (e.g. for JDBC simulation). Bind compat `DataFrame` early to the wrapper
            # type so `isinstance(..., DataFrame)` works for module-level imports.
            dataframe_cls = PySparkDataFrame
            spark_session_cls = PySparkSparkSession
            try:
                from sparkless.testing.jdbc import (  # type: ignore[import-not-found]
                    JdbcDataFrameWrapper,
                    JdbcSessionWrapper,
                )

                # PySpark-mode tests may receive `JdbcDataFrameWrapper` instances
                # from `sparkless.testing`, but some unit tests also expect that
                # `Mock(spec=compat.DataFrame)` exposes PySpark DataFrame APIs
                # like `.limit()`. We solve this by using a compat proxy type that:
                # - subclasses PySpark's DataFrame (API surface for mocks)
                # - customizes `isinstance` checks to also accept JdbcDataFrameWrapper
                class _JdbcDataFrameCompatMeta(type):
                    def __instancecheck__(cls, instance):  # type: ignore[override]
                        return isinstance(
                            instance,
                            (PySparkDataFrame, JdbcDataFrameWrapper),
                        )

                class JdbcCompatibleDataFrame(
                    PySparkDataFrame, metaclass=_JdbcDataFrameCompatMeta
                ):
                    __module__ = "pyspark.sql.dataframe"

                dataframe_cls = JdbcCompatibleDataFrame
                spark_session_cls = PySparkSparkSession
            except Exception:
                pass

            configure_engine(
                functions=pyspark_functions,
                types=pyspark_types,
                analysis_exception=PySparkAnalysisException,
                window=PySparkWindow,
                desc=pyspark_desc,
                engine_name="pyspark",
                dataframe_cls=dataframe_cls,
                spark_session_cls=spark_session_cls,
                column_cls=PySparkColumn,
            )
            return
        except ImportError:
            pass

    # Default to sparkless
    from sparkless.sql import functions as mock_functions
    from sparkless import spark_types as mock_types
    from sparkless.sql.utils import AnalysisException as MockAnalysisException
    from sparkless import Window as MockWindow
    from sparkless import DataFrame as MockDataFrame
    from sparkless import SparkSession as MockSparkSession
    from sparkless import Column as MockColumn

    configure_engine(
        functions=mock_functions,
        types=mock_types,
        analysis_exception=MockAnalysisException,
        window=MockWindow,
        desc=mock_functions.desc,
        engine_name="mock",
        dataframe_cls=MockDataFrame,
        spark_session_cls=MockSparkSession,
        column_cls=MockColumn,
    )


# Configure engine immediately at import time
_configure_default_engine()


# Configure engine based on spark mode when tests start
def _configure_engine_for_mode(spark_mode):
    """Configure pipeline_builder engine based on current spark mode."""
    from sparkless.testing import Mode
    from pipeline_builder.engine_config import configure_engine

    if spark_mode == Mode.SPARKLESS:
        from sparkless.sql import functions as mock_functions
        from sparkless import spark_types as mock_types
        from sparkless.sql.utils import AnalysisException as MockAnalysisException
        from sparkless import Window as MockWindow
        from sparkless import DataFrame as MockDataFrame
        from sparkless import SparkSession as MockSparkSession
        from sparkless import Column as MockColumn

        configure_engine(
            functions=mock_functions,
            types=mock_types,
            analysis_exception=MockAnalysisException,
            window=MockWindow,
            desc=mock_functions.desc,
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

        # In PySpark mode, sparkless.testing typically wraps the real SparkSession
        # and DataFrame for JDBC-like compatibility. For test `isinstance` checks,
        # tests still expect real PySpark types for compat bindings.
        dataframe_cls = PySparkDataFrame
        spark_session_cls = PySparkSparkSession
        try:
            from sparkless.testing.jdbc import (  # type: ignore[import-not-found]
                JdbcDataFrameWrapper,
                JdbcSessionWrapper,
            )

            class _JdbcDataFrameCompatMeta(type):
                def __instancecheck__(cls, instance):  # type: ignore[override]
                    return isinstance(
                        instance,
                        (PySparkDataFrame, JdbcDataFrameWrapper),
                    )

            class JdbcCompatibleDataFrame(
                PySparkDataFrame, metaclass=_JdbcDataFrameCompatMeta
            ):
                __module__ = "pyspark.sql.dataframe"

            dataframe_cls = JdbcCompatibleDataFrame
            spark_session_cls = PySparkSparkSession
        except Exception:
            pass

        configure_engine(
            functions=pyspark_functions,
            types=pyspark_types,
            analysis_exception=PySparkAnalysisException,
            window=PySparkWindow,
            desc=pyspark_desc,
            engine_name="pyspark",
            dataframe_cls=dataframe_cls,
            spark_session_cls=spark_session_cls,
            column_cls=PySparkColumn,
        )


@pytest.fixture(autouse=True)
def configure_engine_for_test(spark_mode):
    """Auto-configure pipeline_builder engine based on current test mode."""
    _configure_engine_for_mode(spark_mode)
    yield


@pytest.fixture(autouse=True, scope="function")
def reset_global_state():
    """Reset global state before and after each test to prevent pollution."""
    try:
        from pipeline_builder.logging import reset_global_logger
        reset_global_logger()
    except Exception:
        pass

    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state as reset_globals,
        )
        reset_globals()
        reset_execution_state()
    except Exception:
        pass

    yield

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

    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state as reset_globals,
        )
        reset_globals()
        reset_execution_state()
    except Exception:
        pass


# Alias fixtures for backward compatibility during migration
@pytest.fixture(scope="function")
def spark_session(spark):
    """Backward compatibility alias: use `spark` fixture instead."""
    return spark


@pytest.fixture(scope="function")
def mock_spark_session(spark):
    """Backward compatibility alias: use `spark` fixture instead."""
    return spark


@pytest.fixture(scope="function")
def base_spark_session(spark):
    """Backward compatibility alias: use `spark` fixture instead."""
    return spark


@pytest.fixture(scope="function")
def isolated_spark_session(isolated_session):
    """Backward compatibility alias: use `isolated_session` fixture instead."""
    return isolated_session


@pytest.fixture(scope="function")
def unique_schema(table_prefix):
    """Unique schema name for test isolation."""
    return f"test_{table_prefix}"


@pytest.fixture(scope="function")
def unique_name(table_prefix):
    """Generate unique schema/table names per test."""
    def _make(kind: str, name: str) -> str:
        return f"{kind}_{name}_{table_prefix}"
    return _make


@pytest.fixture(scope="function")
def unique_table_name(table_prefix):
    """Provide a function to generate unique table names for each test."""
    def _get_unique_table(base_name: str) -> str:
        return f"{base_name}_{table_prefix}"
    return _get_unique_table


@pytest.fixture(scope="function")
def sample_dataframe(spark, spark_imports):
    """Create a sample DataFrame for testing."""
    StructType = spark_imports.StructType
    StructField = spark_imports.StructField
    StringType = spark_imports.StringType
    IntegerType = spark_imports.IntegerType
    DoubleType = spark_imports.DoubleType

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", DoubleType(), True),
        StructField("category", StringType(), True),
    ])

    data = [
        ("user1", 25, 85.5, "A"),
        ("user2", 30, 92.0, "B"),
        ("user3", None, 78.5, "A"),
        ("user4", 35, None, "C"),
        ("user5", 28, 88.0, "B"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def empty_dataframe(spark, spark_imports):
    """Create an empty DataFrame for testing."""
    StructType = spark_imports.StructType
    StructField = spark_imports.StructField
    StringType = spark_imports.StringType

    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
    ])

    return spark.createDataFrame([], schema)


@pytest.fixture(scope="function")
def large_dataset():
    """Create a large dataset for testing."""
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
    """Create a temporary warehouse directory for testing."""
    warehouse_dir = f"/tmp/spark-warehouse-{os.getpid()}"
    os.makedirs(warehouse_dir, exist_ok=True)

    yield warehouse_dir

    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def test_config():
    """Provide test configuration for pipeline tests."""
    from pipeline_builder.models import PipelineConfig, ValidationThresholds

    return PipelineConfig(
        schema="test_schema",
        thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
    )


@pytest.fixture
def fully_isolated_test(spark, table_prefix):
    """Comprehensive isolation fixture that combines all isolation mechanisms."""
    from test_helpers.isolation import (
        ThreadLocalEnvVar,
        get_unique_schema,
        reset_engine_state,
    )

    env_vars = {}
    for var_name in ["SPARKFORGE_ENGINE"]:
        env_var = ThreadLocalEnvVar(var_name)
        env_vars[var_name] = env_var

    reset_engine_state()

    yield {
        "spark": spark,
        "get_unique_schema": get_unique_schema,
        "env_vars": env_vars,
    }

    reset_engine_state()


@pytest.fixture
def isolate_engine_config():
    """Context manager fixture to isolate engine configuration changes."""
    from contextlib import contextmanager
    from pipeline_builder.engine_config import get_engine, configure_engine

    @contextmanager
    def _isolate():
        try:
            current_engine = get_engine()
            saved_config = {
                "functions": current_engine.functions,
                "types": current_engine.types,
                "analysis_exception": current_engine.analysis_exception,
                "window": current_engine.window,
                "desc": current_engine.desc,
                "engine_name": current_engine.engine_name,
                "dataframe_cls": current_engine.dataframe_cls,
                "spark_session_cls": current_engine.spark_session_cls,
                "column_cls": current_engine.column_cls,
            }
        except Exception:
            saved_config = None

        try:
            yield
        finally:
            if saved_config is not None:
                try:
                    configure_engine(**saved_config)
                except Exception:
                    pass

    return _isolate


# Import test helpers (optional - may not be available in all test setups)
# These imports may fail if engine is not configured yet, so use fallback classes
try:
    from system.system_test_helpers import (
        TestAssertions,
        TestDataGenerator,
        TestPerformance,
        TestPipelineBuilder,
    )
except (ImportError, RuntimeError):
    class TestAssertions:
        pass

    class TestDataGenerator:
        pass

    class TestPerformance:
        pass

    class TestPipelineBuilder:
        pass


@pytest.fixture(scope="function")
def test_data_generator():
    """Create a test data generator instance."""
    return TestDataGenerator()


@pytest.fixture(scope="function")
def test_assertions():
    """Create a test assertions instance."""
    return TestAssertions()


@pytest.fixture(scope="function")
def test_performance():
    """Create a test performance instance."""
    return TestPerformance()


@pytest.fixture(scope="function")
def test_pipeline_builder():
    """Create a test pipeline builder instance."""
    return TestPipelineBuilder()


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
        "markers",
        "sequential: marks tests that must run sequentially (not in parallel)",
    )
    # Legacy markers - mapped to sparkless.testing markers
    config.addinivalue_line(
        "markers", "mock_only: DEPRECATED - use @pytest.mark.sparkless_only instead"
    )
    config.addinivalue_line(
        "markers", "real_spark_only: DEPRECATED - use @pytest.mark.pyspark_only instead"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file location."""
    from sparkless.testing import Mode

    # Get current mode from environment
    mode_str = os.environ.get("SPARKLESS_TEST_MODE", "sparkless").lower()
    is_pyspark = mode_str == "pyspark"

    for item in items:
        # Add markers based on file location
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "system" in str(item.fspath):
            item.add_marker(pytest.mark.system)
        else:
            item.add_marker(pytest.mark.unit)

        if "performance" in str(item.fspath) or "load" in str(item.fspath):
            item.add_marker(pytest.mark.slow)

        # Handle legacy markers by mapping to new behavior
        if "mock_only" in item.keywords and is_pyspark:
            item.add_marker(pytest.mark.skip(reason="Test requires sparkless mode (legacy mock_only marker)"))
        elif "real_spark_only" in item.keywords and not is_pyspark:
            item.add_marker(pytest.mark.skip(reason="Test requires pyspark mode (legacy real_spark_only marker)"))
