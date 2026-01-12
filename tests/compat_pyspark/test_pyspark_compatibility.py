"""
PySpark compatibility tests for sparkforge.

These tests verify that sparkforge works correctly with real PySpark.
They require PySpark to be installed and will be skipped if not available.

Run with: pytest tests/compat_pyspark/ -v
"""

import os

import pytest

# Mark all tests in this module as requiring PySpark
pytestmark = pytest.mark.pyspark_compat

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark compatibility tests require SPARK_MODE=real"
    )

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


@pytest.fixture(scope="module")
def pyspark_available():
    """Check if PySpark is available."""
    try:
        import importlib.util

        if importlib.util.find_spec("pyspark") is None:
            pytest.skip(
                "PySpark not installed. Install with: pip install sparkforge[compat-test]"
            )
        return True
    except ImportError:
        pytest.skip(
            "PySpark not installed. Install with: pip install sparkforge[compat-test]"
        )


@pytest.fixture(scope="function")
def setup_pyspark_engine():
    """Set up PySpark as the engine for these tests."""
    from pipeline_builder.engine_config import configure_engine, get_engine
    
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
    
    # Configure PySpark engine
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
    
    yield
    
    # Restore saved engine state
    if saved_config is not None:
        try:
            configure_engine(**saved_config)
        except Exception:
            pass


class TestPySparkCompatibility:
    """Test suite for PySpark compatibility."""

    @pytest.mark.sequential
    def test_pyspark_engine_detection(self, pyspark_available, setup_pyspark_engine):
        """Test that PySpark engine is detected correctly."""
        from pipeline_builder.compat import compat_name, is_mock_spark

        assert compat_name() == "pyspark"
        assert not is_mock_spark()

    def test_pyspark_imports(self, pyspark_available, setup_pyspark_engine):
        """Test that PySpark imports work through compat layer."""
        from pipeline_builder.compat import Column, DataFrame, SparkSession

        # Verify these are PySpark types
        assert "pyspark" in str(DataFrame)
        assert "pyspark" in str(SparkSession)
        assert "pyspark" in str(Column)

    def test_pyspark_dataframe_operations(
        self, pyspark_available, setup_pyspark_engine
    ):
        """Test basic DataFrame operations with PySpark."""
        from pyspark.sql import SparkSession

        from pipeline_builder.compat import F

        spark = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        # Create a simple DataFrame
        data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        df = spark.createDataFrame(data, ["id", "name"])

        # Test filtering
        filtered = df.filter(F.col("id") > 1)
        assert filtered.count() == 2

        # Test selection
        selected = df.select("name")
        assert selected.count() == 3

        spark.stop()

    def test_pyspark_pipeline_building(self, pyspark_available, setup_pyspark_engine):
        """Test that PipelineBuilder works with PySpark."""
        from pyspark.sql import SparkSession

        from pipeline_builder import PipelineBuilder
        from pipeline_builder.compat import F

        spark = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        # Build pipeline with unique schema for concurrent testing isolation
        unique_schema = get_unique_schema("test")
        builder = PipelineBuilder(spark=spark, schema=unique_schema)
        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col=None,
        )

        pipeline = builder.to_pipeline()
        assert pipeline is not None

        spark.stop()

    def test_pyspark_validation(self, pyspark_available, setup_pyspark_engine):
        """Test validation with PySpark."""
        from pyspark.sql import SparkSession

        from pipeline_builder.compat import types
        from pipeline_builder.validation import validate_dataframe_schema

        spark = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        # Create DataFrame with schema
        schema = types.StructType(
            [
                types.StructField("id", types.IntegerType(), False),
                types.StructField("name", types.StringType(), True),
            ]
        )
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, schema)

        # Validate schema
        result = validate_dataframe_schema(df, ["id", "name"])
        assert result

        spark.stop()

    def test_pyspark_delta_lake_operations(
        self, pyspark_available, setup_pyspark_engine
    ):
        """Test Delta Lake operations with PySpark (if available)."""
        import importlib.util

        try:
            if importlib.util.find_spec("delta") is None:
                pytest.skip(
                    "Delta Lake not installed. Install with: pip install sparkforge[compat-test]"
                )
        except (ValueError, ImportError):
            pytest.skip("Delta Lake not available")

        from pyspark.sql import SparkSession

        # Configure Spark with Delta Lake (use helper to ensure Delta jars are added)
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError:
            import delta.pip_utils as pip_utils  # type: ignore

            configure_spark_with_delta_pip = pip_utils.configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("DeltaTest")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Create test table
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        table_name = "test_delta_table"

        # Write as Delta table (use append + overwriteSchema to avoid truncate limits)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # type: ignore[attr-defined]
        (
            df.write.format("delta")
            .mode("append")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

        # Read back
        result = spark.table(table_name)
        assert result.count() == 2

        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

    def test_pyspark_error_handling(self, pyspark_available, setup_pyspark_engine):
        """Test error handling with PySpark."""
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        # Create DataFrame with null values
        data = [(1, "Alice"), (None, "Bob"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])

        # Try validation that should identify invalid rows
        from pipeline_builder.compat import F
        from pipeline_builder.validation import apply_column_rules

        rules = {
            "id": [F.col("id").isNotNull()],
            "name": [F.col("name").isNotNull()],
        }

        # This should return valid and invalid DataFrames with statistics
        valid_df, invalid_df, stats = apply_column_rules(
            df, rules, stage="test", step="test"
        )

        # Check that we found invalid rows
        assert invalid_df.count() > 0
        assert stats.invalid_rows > 0

        spark.stop()

    def test_pyspark_performance_monitoring(
        self, pyspark_available, setup_pyspark_engine
    ):
        """Test performance monitoring with PySpark."""
        from pyspark.sql import SparkSession

        from pipeline_builder.performance import time_operation

        spark = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        @time_operation("test_operation")
        def test_func():
            data = [(i, f"name{i}") for i in range(1000)]
            df = spark.createDataFrame(data, ["id", "name"])
            return df.count()

        result = test_func()
        assert result == 1000

        spark.stop()

    def test_pyspark_table_operations(self, pyspark_available, setup_pyspark_engine):
        """Test table operations with PySpark."""
        import importlib.util

        # Check if Delta Lake is available
        try:
            if importlib.util.find_spec("delta") is None:
                pytest.skip(
                    "Delta Lake not installed. Install with: pip install sparkforge[compat-test]"
                )
        except (ValueError, ImportError):
            pytest.skip("Delta Lake not available")

        from pyspark.sql import SparkSession

        from pipeline_builder.table_operations import (
            read_table,
            table_exists,
            write_overwrite_table,
        )

        # Configure Spark with Delta Lake (use helper to ensure Delta jars are added)
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError:
            import delta.pip_utils as pip_utils  # type: ignore

            configure_spark_with_delta_pip = pip_utils.configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Use fully qualified table name (schema.table) as required by write_overwrite_table
        table_name = "default.test_table"

        # Clean up any existing table and its data - use prepare_delta_overwrite to ensure Delta tables are properly dropped
        from pipeline_builder.table_operations import prepare_delta_overwrite
        
        try:
            prepare_delta_overwrite(spark, table_name)
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        try:
            import os
            import shutil

            # Clean up warehouse directory for the table
            warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse", "test_table")
            if os.path.exists(warehouse_dir):
                shutil.rmtree(warehouse_dir, ignore_errors=True)
        except Exception:
            pass

        # Create test data
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        # Write table using fully qualified name (prepare_delta_overwrite is called internally by write_overwrite_table)
        write_overwrite_table(df, table_name)

        # Check if exists
        assert table_exists(spark, table_name)

        # Read back
        result = read_table(spark, table_name)
        assert result.count() == 2

        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()


class TestPySparkEngineSwitching:
    """Test engine switching functionality."""

    @pytest.mark.sequential
    def test_switch_to_pyspark(self, pyspark_available, setup_pyspark_engine):
        """Test switching to PySpark engine."""
        from pipeline_builder.compat import compat_name, is_mock_spark

        assert compat_name() == "pyspark"
        assert not is_mock_spark()

    def test_switch_to_mock(self, pyspark_available):
        """Test switching to mock engine."""
        # Skip this test when running in pyspark mode as it requires mock-spark
        pytest.skip("Mock engine switching test skipped in pyspark mode")

    @pytest.mark.sequential
    def test_auto_detection(self, pyspark_available, setup_pyspark_engine):
        """Test auto-detection of engine."""
        from pipeline_builder.compat import compat_name

        # When SPARK_MODE=real, engine should already be configured as pyspark
        # The setup_pyspark_engine fixture ensures it's configured correctly
        assert compat_name() == "pyspark"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
