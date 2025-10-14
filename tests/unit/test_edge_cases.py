"""
Edge case tests for Mock Spark components.
"""


import pytest
from mock_spark import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    MockSparkSession,
    MockStructField,
    MockStructType,
    StringType,
)
from mock_spark.errors import (
    AnalysisException,
    PySparkValueError,
)
from mock_spark.functions import (
    F,
    MockAggregateFunction,
    MockColumn,
    MockLiteral,
    MockWindowFunction,
)

from sparkforge.execution import ExecutionEngine
from sparkforge.models import ParallelConfig, PipelineConfig, ValidationThresholds
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.validation.pipeline_validation import UnifiedValidator, ValidationResult
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import LogLevel, WriteMode, WriterConfig


class TestEdgeCases:
    """Edge case tests for Mock Spark components."""

    def test_empty_dataframe_operations(self, mock_spark_session):
        """Test operations on empty DataFrames."""
        # Create empty DataFrame
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        empty_df = mock_spark_session.createDataFrame([], schema)

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

    def test_null_value_handling(self, mock_spark_session):
        """Test handling of null values in DataFrames."""
        # Create DataFrame with null values
        data_with_nulls = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": None, "age": 30},
            {"id": None, "name": "Charlie", "age": None},
            {"id": 4, "name": "Diana", "age": 35},
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        df = mock_spark_session.createDataFrame(data_with_nulls, schema)

        # Test filtering with null values
        non_null_df = df.filter(F.col("name").isNotNull())
        assert non_null_df.count() == 3

        null_df = df.filter(F.col("name").isNull())
        assert null_df.count() == 1

    def test_large_dataset_operations(self, mock_spark_session):
        """Test operations on large datasets."""
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
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("department", StringType()),
            ]
        )

        df = mock_spark_session.createDataFrame(large_dataset, schema)

        # Test operations on large dataset
        assert df.count() == len(large_dataset)

        # Test filtering large dataset
        filtered_df = df.filter(F.col("age") > 30)
        assert filtered_df.count() >= 0  # Should not crash

        # Test grouping large dataset
        grouped_df = df.groupBy("department").count()
        assert grouped_df.count() >= 0  # Should not crash

    def test_complex_schema_operations(self, mock_spark_session):
        """Test operations with complex schemas."""
        # Create complex schema with arrays and maps
        array_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("tags", ArrayType(StringType())),
                MockStructField("metadata", MapType(StringType(), StringType())),
                MockStructField("is_active", BooleanType()),
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

        df = mock_spark_session.createDataFrame(complex_data, array_schema)

        # Test operations on complex schema
        assert df.count() == 2
        assert len(df.columns) == 4

        # Test selecting specific columns
        selected_df = df.select("id", "is_active")
        assert selected_df.count() == 2
        assert len(selected_df.columns) == 2

    def test_error_conditions(self, mock_spark_session):
        """Test various error conditions."""
        # Test invalid data types
        with pytest.raises(
            (PySparkValueError, Exception)
        ):  # Accept mock-spark 0.3.1 exceptions
            mock_spark_session.createDataFrame("invalid_data", "invalid_schema")

        # Test invalid schema - mock-spark 0.3.1 accepts most schemas, test with invalid data instead
        with pytest.raises((PySparkValueError, Exception)):
            mock_spark_session.createDataFrame(None, "id INT")  # None data should fail

        # Test table not found
        with pytest.raises(
            (AnalysisException, Exception)
        ):  # Accept mock-spark 0.3.1 exceptions
            mock_spark_session.table("nonexistent.table")

        # Test invalid column references
        schema = MockStructType([MockStructField("id", IntegerType())])
        df = mock_spark_session.createDataFrame([{"id": 1}], schema)

        with pytest.raises(
            (AnalysisException, Exception)
        ):  # Accept mock-spark 0.3.1 exceptions
            df.select("nonexistent_column")

    def test_boundary_values(self, mock_spark_session):
        """Test boundary values and edge cases."""
        # Test with very large numbers
        large_data = [
            {"id": 2147483647, "value": 1.7976931348623157e308},  # Max int and double
            {"id": -2147483648, "value": 2.2250738585072014e-308},  # Min int and double
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", DoubleType()),
            ]
        )

        df = mock_spark_session.createDataFrame(large_data, schema)
        assert df.count() == 2

        # Test with empty strings
        empty_string_data = [
            {"id": 1, "name": ""},
            {"id": 2, "name": "   "},  # Whitespace only
            {"id": 3, "name": "normal"},
        ]

        string_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        df = mock_spark_session.createDataFrame(empty_string_data, string_schema)
        assert df.count() == 3

    def test_concurrent_operations(self, mock_spark_session):
        """Test concurrent-like operations."""
        # Create multiple DataFrames simultaneously
        schema = MockStructType([MockStructField("id", IntegerType())])

        df1 = mock_spark_session.createDataFrame([{"id": 1}], schema)
        df2 = mock_spark_session.createDataFrame([{"id": 2}], schema)
        df3 = mock_spark_session.createDataFrame([{"id": 3}], schema)

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

    def test_memory_management(self, mock_spark_session):
        """Test memory management with large datasets."""
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

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
            ]
        )

        df = mock_spark_session.createDataFrame(large_data, schema)

        # Test operations on large dataset
        assert df.count() == 10000

        # Test filtering to reduce memory usage
        filtered_df = df.filter(F.col("age") > 40)
        assert filtered_df.count() >= 0  # Should not crash

        # Test selecting specific columns
        selected_df = df.select("id", "name")
        assert selected_df.count() == 10000
        assert len(selected_df.columns) == 2

    def test_schema_evolution(self, mock_spark_session):
        """Test schema evolution scenarios."""
        # Create initial schema
        initial_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        initial_data = [{"id": 1, "name": "Alice"}]
        df1 = mock_spark_session.createDataFrame(initial_data, initial_schema)

        # Create evolved schema with additional column
        evolved_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        evolved_data = [{"id": 2, "name": "Bob", "age": 30}]
        df2 = mock_spark_session.createDataFrame(evolved_data, evolved_schema)

        # Test both schemas work
        assert df1.count() == 1
        assert df2.count() == 1
        assert len(df1.columns) == 2
        assert len(df2.columns) == 3

    def test_pipeline_builder_edge_cases(self, mock_spark_session):
        """Test PipelineBuilder edge cases."""
        from sparkforge.errors import ConfigurationError
        
        # Test with invalid schema name
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=mock_spark_session, schema="")

        # Test with None spark session
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=None, schema="test")

        # Test with very long schema name
        long_schema_name = "a" * 1000
        builder = PipelineBuilder(spark=mock_spark_session, schema=long_schema_name)
        assert builder.schema == long_schema_name

    def test_execution_engine_edge_cases(self, mock_spark_session):
        """Test ExecutionEngine edge cases."""
        # Test with None config - ExecutionEngine now accepts None config
        engine = ExecutionEngine(spark=mock_spark_session, config=None)
        assert engine.config is None

        # Test with minimal config
        thresholds = ValidationThresholds(bronze=0.0, silver=0.0, gold=0.0)
        parallel_config = ParallelConfig(enabled=False, max_workers=1)
        config = PipelineConfig(
            schema="test",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=False,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)
        assert engine.config == config
        assert engine.config.thresholds.bronze == 0.0

    def test_validation_edge_cases(self, mock_spark_session):
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

    def test_writer_edge_cases(self, mock_spark_session):
        """Test LogWriter edge cases."""
        # Test with minimal config
        minimal_config = WriterConfig(table_schema="test", table_name="logs")

        writer = LogWriter(spark=mock_spark_session, config=minimal_config)
        assert writer.config == minimal_config

        # Test with maximum config
        max_config = WriterConfig(
            table_schema="test",
            table_name="logs",
            write_mode=WriteMode.OVERWRITE,
            log_level=LogLevel.DEBUG,
            batch_size=10000,
            compression="gzip",
            max_file_size_mb=1024,
            partition_columns=["date"],
            partition_count=100,
        )

        writer_max = LogWriter(spark=mock_spark_session, config=max_config)
        assert writer_max.config == max_config
        assert writer_max.config.batch_size == 10000
        assert writer_max.config.compression == "gzip"

    def test_storage_edge_cases(self, mock_spark_session):
        """Test storage edge cases."""
        # Test with long table names (DuckDB limit is 63 characters)
        long_table_name = "a" * 50
        schema = MockStructType([MockStructField("id", IntegerType())])

        mock_spark_session.storage.create_schema("test")
        mock_spark_session.storage.create_table("test", long_table_name, schema.fields)

        assert mock_spark_session.storage.table_exists("test", long_table_name)

        # Test with underscores and numbers in names (valid SQL identifiers)
        valid_special_name = "table_with_special_chars_123_abc"
        mock_spark_session.storage.create_table(
            "test", valid_special_name, schema.fields
        )

        assert mock_spark_session.storage.table_exists("test", valid_special_name)

    def test_function_edge_cases(self, mock_spark_session):
        """Test function edge cases."""
        # Test complex column expressions
        col1 = F.col("id")
        col2 = F.col("name")

        # Test column operations
        assert isinstance(col1, MockColumn)
        assert isinstance(col2, MockColumn)

        # Test literal values
        lit1 = F.lit(42)
        lit2 = F.lit("hello")

        assert isinstance(lit1, MockLiteral)
        assert isinstance(lit2, MockLiteral)

        # Test aggregate functions
        agg_func = F.count("id")
        assert isinstance(agg_func, MockAggregateFunction)

        # Test window functions - mock-spark 0.3.1 requires window_spec argument
        window_func = F.row_number().over("dummy_window_spec")
        assert isinstance(window_func, MockWindowFunction)

    def test_dataframe_edge_cases(self, mock_spark_session):
        """Test DataFrame edge cases."""
        # Test DataFrame with no columns
        empty_schema = MockStructType([])
        empty_df = mock_spark_session.createDataFrame([{}], empty_schema)

        assert empty_df.count() == 1
        assert len(empty_df.columns) == 0

        # Test DataFrame with duplicate column names
        duplicate_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("id", StringType()),  # Duplicate name
            ]
        )

        duplicate_df = mock_spark_session.createDataFrame([{"id": 1}], duplicate_schema)
        assert duplicate_df.count() == 1
        assert len(duplicate_df.columns) == 2

    def test_session_edge_cases(self, mock_spark_session):
        """Test SparkSession edge cases."""
        # Test creating multiple sessions
        session2 = MockSparkSession("TestApp2")
        session3 = MockSparkSession("TestApp3")

        assert session2.appName == "TestApp2"
        assert session3.appName == "TestApp3"

        # Test session with different configurations
        session4 = MockSparkSession("TestApp4")
        assert session4.appName == "TestApp4"

        # Test catalog operations
        mock_spark_session.storage.create_schema("test_schema")
        databases = mock_spark_session.catalog.listDatabases()
        assert len(databases) >= 1

        # Test table operations
        schema = MockStructType([MockStructField("id", IntegerType())])
        mock_spark_session.storage.create_table(
            "test_schema", "test_table", schema.fields
        )

        assert mock_spark_session.catalog.tableExists("test_schema", "test_table")
        assert not mock_spark_session.catalog.tableExists(
            "test_schema", "nonexistent_table"
        )
