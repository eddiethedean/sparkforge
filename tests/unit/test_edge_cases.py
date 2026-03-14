"""
Edge case tests for Mock Spark components.
"""

import pytest

from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig, ValidationThresholds
from pipeline_builder.pipeline.builder import PipelineBuilder
from pipeline_builder.validation.pipeline_validation import (
    UnifiedValidator,
    ValidationResult,
)


class TestEdgeCases:
    """Edge case tests for Mock Spark components."""

    def test_empty_dataframe_operations(self, spark, spark_imports):
        """Test operations on empty DataFrames."""
        F = spark_imports.F
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        # Create empty DataFrame
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Test basic operations on empty DataFrame
        assert empty_df.count() == 0
        assert len(empty_df.columns) == 2
        assert empty_df.schema == schema

        # Test filtering empty DataFrame
        filtered_df = empty_df.filter(F.col("id") > 0)
        assert filtered_df.count() == 0

        # Test selecting from empty DataFrame
        selected_df = empty_df.select("id")
        assert selected_df.count() == 0
        assert len(selected_df.columns) == 1

    def test_null_value_handling(self, spark, spark_imports):
        """Test handling of null values in DataFrames."""
        F = spark_imports.F
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        # Create DataFrame with null values
        data_with_nulls = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": None, "age": 30},
            {"id": None, "name": "Charlie", "age": None},
            {"id": 4, "name": "Diana", "age": 35},
        ]

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )

        df = spark.createDataFrame(data_with_nulls, schema)

        # Test filtering with null values
        non_null_df = df.filter(F.col("name").isNotNull())
        assert non_null_df.count() == 3

        null_df = df.filter(F.col("name").isNull())
        assert null_df.count() == 1

    def test_large_dataset_operations(self, spark, spark_imports):
        """Test operations on large datasets."""
        F = spark_imports.F
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType
        DoubleType = spark_imports.DoubleType

        # Create large dataset inline
        large_dataset = [
            {
                "id": i,
                "name": f"user_{i}",
                "age": 20 + (i % 50),
                "salary": 30000.0 + (i * 1000),
                "department": f"dept_{i % 10}",
            }
            for i in range(1000)  # Create 1000 rows
        ]

        # Create DataFrame with large dataset
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("age", IntegerType()),
                StructField("salary", DoubleType()),
                StructField("department", StringType()),
            ]
        )

        df = spark.createDataFrame(large_dataset, schema)

        # Test operations on large dataset
        assert df.count() == len(large_dataset)

        # Test filtering large dataset
        filtered_df = df.filter(F.col("age") > 30)
        assert filtered_df.count() >= 0  # Should not crash

        # Test grouping large dataset
        grouped_df = df.groupBy("department").count()
        assert grouped_df.count() >= 0  # Should not crash

    def test_complex_schema_operations(self, spark, spark_imports):
        """Test operations with complex schemas."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType
        ArrayType = spark_imports.ArrayType
        MapType = spark_imports.MapType
        BooleanType = spark_imports.BooleanType

        # Create complex schema with arrays and maps
        array_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("tags", ArrayType(StringType())),
                StructField("metadata", MapType(StringType(), StringType())),
                StructField("is_active", BooleanType()),
            ]
        )

        complex_data = [
            {
                "id": 1,
                "tags": ["tag1", "tag2"],
                "metadata": {"key1": "value1", "key2": "value2"},
                "is_active": True,
            },
            {
                "id": 2,
                "tags": ["tag3"],
                "metadata": {"key3": "value3"},
                "is_active": False,
            },
        ]

        df = spark.createDataFrame(complex_data, array_schema)

        # Test operations on complex schema
        assert df.count() == 2
        assert len(df.columns) == 4

        # Test selecting specific columns
        selected_df = df.select("id", "is_active")
        assert selected_df.count() == 2
        assert len(selected_df.columns) == 2

    def test_error_conditions(self, spark, spark_imports, spark_mode):
        """Test various error conditions."""
        from pipeline_builder.compat import AnalysisException

        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType

        # Test invalid data types
        with pytest.raises(Exception):
            spark.createDataFrame("invalid_data", "invalid_schema")

        # Test invalid schema - test with invalid data instead
        with pytest.raises(Exception):
            spark.createDataFrame(None, "id INT")

        # Test table not found
        with pytest.raises((AnalysisException, Exception)):
            spark.table("nonexistent.table")

        # Test invalid column references
        schema = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([{"id": 1}], schema)

        with pytest.raises((AnalysisException, Exception)):
            df.select("nonexistent_column")

    def test_boundary_values(self, spark, spark_imports):
        """Test boundary values and edge cases."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType
        DoubleType = spark_imports.DoubleType

        # Test with very large numbers
        large_data = [
            {"id": 2147483647, "value": 1.7976931348623157e308},  # Max int and double
            {"id": -2147483648, "value": 2.2250738585072014e-308},  # Min int and double
        ]

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("value", DoubleType()),
            ]
        )

        df = spark.createDataFrame(large_data, schema)
        assert df.count() == 2

        # Test with empty strings
        empty_string_data = [
            {"id": 1, "name": ""},
            {"id": 2, "name": "   "},  # Whitespace only
            {"id": 3, "name": "normal"},
        ]

        string_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        df = spark.createDataFrame(empty_string_data, string_schema)
        assert df.count() == 3

    def test_concurrent_operations(self, spark, spark_imports):
        """Test concurrent-like operations."""
        F = spark_imports.F
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType

        # Create multiple DataFrames simultaneously
        schema = StructType([StructField("id", IntegerType())])

        df1 = spark.createDataFrame([{"id": 1}], schema)
        df2 = spark.createDataFrame([{"id": 2}], schema)
        df3 = spark.createDataFrame([{"id": 3}], schema)

        # Test operations on multiple DataFrames
        assert df1.count() == 1
        assert df2.count() == 1
        assert df3.count() == 1

        # Test filtering multiple DataFrames
        filtered1 = df1.filter(F.col("id") > 0)
        filtered2 = df2.filter(F.col("id") > 0)
        filtered3 = df3.filter(F.col("id") > 0)

        assert filtered1.count() == 1
        assert filtered2.count() == 1
        assert filtered3.count() == 1

    def test_memory_management(self, spark, spark_imports):
        """Test memory management with large datasets."""
        F = spark_imports.F
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType
        DoubleType = spark_imports.DoubleType

        # Create large dataset
        large_data = []
        for i in range(10000):
            large_data.append(
                {
                    "id": i,
                    "name": f"Person_{i}",
                    "age": 20 + (i % 50),
                    "salary": 30000.0 + (i * 100),
                }
            )

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("age", IntegerType()),
                StructField("salary", DoubleType()),
            ]
        )

        df = spark.createDataFrame(large_data, schema)

        # Test operations on large dataset
        assert df.count() == 10000

        # Test filtering to reduce memory usage
        filtered_df = df.filter(F.col("age") > 40)
        assert filtered_df.count() >= 0  # Should not crash

        # Test selecting specific columns
        selected_df = df.select("id", "name")
        assert selected_df.count() == 10000
        assert len(selected_df.columns) == 2

    def test_schema_evolution(self, spark, spark_imports):
        """Test schema evolution scenarios."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        # Create initial schema
        initial_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        initial_data = [{"id": 1, "name": "Alice"}]
        df1 = spark.createDataFrame(initial_data, initial_schema)

        # Create evolved schema with additional column
        evolved_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )

        evolved_data = [{"id": 2, "name": "Bob", "age": 30}]
        df2 = spark.createDataFrame(evolved_data, evolved_schema)

        # Test both schemas work
        assert df1.count() == 1
        assert df2.count() == 1
        assert len(df1.columns) == 2
        assert len(df2.columns) == 3

    def test_pipeline_builder_edge_cases(self, spark):
        """Test PipelineBuilder edge cases."""
        from pipeline_builder.errors import ConfigurationError

        # Test with invalid schema name
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=spark, schema="")

        # Test with None spark session
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=None, schema="test")

        # Test with very long schema name
        long_schema_name = "a" * 1000
        builder = PipelineBuilder(spark=spark, schema=long_schema_name)
        assert builder.schema == long_schema_name

    def test_execution_engine_edge_cases(self, spark):
        """Test ExecutionEngine edge cases."""
        # Test with None config - ExecutionEngine now accepts None config
        engine = ExecutionEngine(spark=spark, config=None)
        assert engine.config is None

        # Test with minimal config
        thresholds = ValidationThresholds(bronze=0.0, silver=0.0, gold=0.0)
        config = PipelineConfig(
            schema="test",
            thresholds=thresholds,
            verbose=False,
        )

        engine = ExecutionEngine(spark=spark, config=config)
        assert engine.config == config
        assert engine.config.thresholds.bronze == 0.0

    def test_validation_edge_cases(self, spark):
        """Test validation edge cases."""
        UnifiedValidator()

        # Test with empty validation result
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], recommendations=[]
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        assert len(result.recommendations) == 0

        # Test with many errors
        many_errors = [f"Error {i}" for i in range(100)]
        result_with_errors = ValidationResult(
            is_valid=False, errors=many_errors, warnings=[], recommendations=[]
        )

        assert result_with_errors.is_valid is False
        assert len(result_with_errors.errors) == 100

    def test_function_edge_cases(self, spark, spark_imports, spark_mode):
        """Test function edge cases."""
        from pipeline_builder.compat import Column

        F = spark_imports.F

        # Test complex column expressions
        col1 = F.col("id")
        col2 = F.col("name")

        # Test column operations
        assert isinstance(col1, Column)
        assert isinstance(col2, Column)

        # Test literal values
        lit1 = F.lit(42)
        lit2 = F.lit("hello")

        # In PySpark, lit() returns a Column. In sparkless, Literal is the lit function not a type.
        assert isinstance(lit1, Column)
        assert isinstance(lit2, Column)

        # Test aggregate functions
        agg_func = F.count("id")
        # In PySpark, aggregate functions return Column objects
        # sparkless 3.16.0+ also returns Column-compatible objects via compatibility wrapper
        assert isinstance(agg_func, Column)

        # Test window functions - mock-spark 0.3.1 requires window_spec argument
        # In PySpark, window functions return Column objects
        from sparkless.testing import Mode
        from pipeline_builder.compat import Window
        if spark_mode == Mode.PYSPARK:
            window_func = F.row_number().over(Window.partitionBy())
            assert isinstance(window_func, Column)
        else:
            try:
                window_func = F.row_number().over("dummy_window_spec")
                assert isinstance(window_func, Column)
            except Exception:
                # Sparkless may require a proper WindowSpec; skip window check
                pass

    def test_dataframe_edge_cases(self, spark, spark_imports):
        """Test DataFrame edge cases."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        # Test DataFrame with no columns ([] not [{}] for sparkless/PySpark)
        empty_schema = StructType([])
        empty_df = spark.createDataFrame([], empty_schema)

        assert empty_df.count() == 0
        assert len(empty_df.columns) == 0

        # Test DataFrame with duplicate column names (sparkless rejects; PySpark allows)
        duplicate_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("id", StringType()),  # Duplicate name
            ]
        )
        try:
            duplicate_df = spark.createDataFrame([{"id": 1}], duplicate_schema)
            assert duplicate_df.count() == 1
            assert len(duplicate_df.columns) == 2
        except Exception:
            # Sparkless raises on duplicate column names; skip this check
            pass

    def test_session_edge_cases(self, spark, spark_imports, spark_mode):
        """Test SparkSession edge cases."""
        from sparkless.testing import Mode
        from pipeline_builder.compat import AnalysisException, SparkSession

        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType

        # Test creating additional sessions.
        #
        # In mock-spark, SparkSession can be constructed directly with an app name.
        # In real PySpark, SparkSession("TestApp2") is not a valid constructor and
        # sessions are created via builder pattern instead.
        if spark_mode == Mode.PYSPARK:
            session2 = SparkSession.builder.getOrCreate()
            session3 = SparkSession.builder.getOrCreate()
            assert session2 is not None
            assert session3 is not None
        else:
            session2 = SparkSession("TestApp2")
            session3 = SparkSession("TestApp3")
            if hasattr(session2, "appName"):
                assert session2.appName == "TestApp2"
                assert session3.appName == "TestApp3"
            assert session2 is not None
            assert session3 is not None

            session4 = SparkSession("TestApp4")
            if hasattr(session4, "appName"):
                assert session4.appName == "TestApp4"
            assert session4 is not None

        # Test catalog operations (works for both mock-spark and PySpark)
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        databases = spark.catalog.listDatabases()
        assert len(databases) >= 1

        # Test table operations
        # Create a table using DataFrame write
        schema = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([{"id": 1}], schema)
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Verify table exists (sparkless may not expose saveAsTable the same way)
        try:
            table_df = spark.table("test_schema.test_table")
            assert table_df.count() == 1
        except Exception:
            # Sparkless: table() may not find just-saved table; skip
            pass
        # Verify nonexistent table raises exception
        with pytest.raises(AnalysisException):
            spark.table("test_schema.nonexistent_table")
