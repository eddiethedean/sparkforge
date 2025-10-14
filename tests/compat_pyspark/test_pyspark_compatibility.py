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


@pytest.fixture(scope="module")
def pyspark_available():
    """Check if PySpark is available."""
    try:
        import importlib.util

        if importlib.util.find_spec("pyspark") is None:
            pytest.skip("PySpark not installed. Install with: pip install sparkforge[compat-test]")
        return True
    except ImportError:
        pytest.skip("PySpark not installed. Install with: pip install sparkforge[compat-test]")


@pytest.fixture(scope="module")
def setup_pyspark_engine():
    """Set up PySpark as the engine for these tests."""
    # Force PySpark engine
    os.environ["SPARKFORGE_ENGINE"] = "pyspark"
    yield
    # Clean up
    if "SPARKFORGE_ENGINE" in os.environ:
        del os.environ["SPARKFORGE_ENGINE"]


class TestPySparkCompatibility:
    """Test suite for PySpark compatibility."""

    def test_pyspark_engine_detection(self, pyspark_available, setup_pyspark_engine):
        """Test that PySpark engine is detected correctly."""
        from sparkforge.compat import compat_name, is_mock_spark

        assert compat_name() == "pyspark"
        assert not is_mock_spark()

    def test_pyspark_imports(self, pyspark_available, setup_pyspark_engine):
        """Test that PySpark imports work through compat layer."""
        from sparkforge.compat import Column, DataFrame, SparkSession

        # Verify these are PySpark types
        assert "pyspark" in str(DataFrame)
        assert "pyspark" in str(SparkSession)
        assert "pyspark" in str(Column)

    def test_pyspark_dataframe_operations(self, pyspark_available, setup_pyspark_engine):
        """Test basic DataFrame operations with PySpark."""
        from pyspark.sql import SparkSession

        from sparkforge.compat import F

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

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

        from sparkforge import PipelineBuilder
        from sparkforge.compat import F

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

        # Build pipeline
        builder = PipelineBuilder(spark=spark, schema="test_schema")
        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col=None
        )

        pipeline = builder.to_pipeline()
        assert pipeline is not None

        spark.stop()

    def test_pyspark_validation(self, pyspark_available, setup_pyspark_engine):
        """Test validation with PySpark."""
        from pyspark.sql import SparkSession

        from sparkforge.compat import types
        from sparkforge.validation import validate_dataframe_schema

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

        # Create DataFrame with schema
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("name", types.StringType(), True),
        ])
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, schema)

        # Validate schema
        result = validate_dataframe_schema(df, ["id", "name"])
        assert result

        spark.stop()

    def test_pyspark_delta_lake_operations(self, pyspark_available, setup_pyspark_engine):
        """Test Delta Lake operations with PySpark (if available)."""
        import importlib.util

        try:
            if importlib.util.find_spec("delta") is None:
                pytest.skip("Delta Lake not installed. Install with: pip install sparkforge[compat-test]")
        except (ValueError, ImportError):
            pytest.skip("Delta Lake not available")

        from pyspark.sql import SparkSession


        # Configure Spark with Delta Lake
        spark = SparkSession.builder \
            .appName("DeltaTest") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        # Create test table
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        table_name = "test_delta_table"

        # Write as Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        # Read back
        result = spark.table(table_name)
        assert result.count() == 2

        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

    def test_pyspark_error_handling(self, pyspark_available, setup_pyspark_engine):
        """Test error handling with PySpark."""
        from pyspark.sql import SparkSession

        from sparkforge.errors import ValidationError

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

        # Create DataFrame with null values
        data = [(1, "Alice"), (None, "Bob"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])

        # Try validation that should identify invalid rows
        from sparkforge.compat import F
        from sparkforge.validation import apply_column_rules

        rules = {
            "id": [F.col("id").isNotNull()],
            "name": [F.col("name").isNotNull()],
        }

        # This should return valid and invalid DataFrames with statistics
        valid_df, invalid_df, stats = apply_column_rules(df, rules, stage="test", step="test")
        
        # Check that we found invalid rows
        assert invalid_df.count() > 0
        assert stats.invalid_rows > 0

        spark.stop()

    def test_pyspark_performance_monitoring(self, pyspark_available, setup_pyspark_engine):
        """Test performance monitoring with PySpark."""
        from pyspark.sql import SparkSession

        from sparkforge.performance import time_operation

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

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
        from pyspark.sql import SparkSession

        from sparkforge.table_operations import (
            read_table,
            table_exists,
            write_overwrite_table,
        )

        spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()

        table_name = "test_table"
        
        # Clean up any existing table and its data
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        try:
            import shutil
            import os
            warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse", table_name)
            if os.path.exists(warehouse_dir):
                shutil.rmtree(warehouse_dir, ignore_errors=True)
        except Exception:
            pass

        # Create test data
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        # Write table
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

    def test_switch_to_pyspark(self, pyspark_available):
        """Test switching to PySpark engine."""
        os.environ["SPARKFORGE_ENGINE"] = "pyspark"

        # Import after setting env var
        from sparkforge.compat import compat_name, is_mock_spark

        assert compat_name() == "pyspark"
        assert not is_mock_spark()

    def test_switch_to_mock(self, pyspark_available):
        """Test switching to mock engine."""
        # Skip this test when running in pyspark mode as it requires mock-spark
        pytest.skip("Mock engine switching test skipped in pyspark mode")

    def test_auto_detection(self, pyspark_available):
        """Test auto-detection of engine."""
        if "SPARKFORGE_ENGINE" in os.environ:
            del os.environ["SPARKFORGE_ENGINE"]

        # Import after clearing env var
        from sparkforge.compat import compat_name

        # Should detect PySpark since it's available
        assert compat_name() == "pyspark"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

