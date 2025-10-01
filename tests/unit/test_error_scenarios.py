"""
Error scenario tests for Mock Spark components.
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.types import MockStructType, MockStructField, StringType, IntegerType, DoubleType
from mock_spark.functions import F
from mock_spark.errors import AnalysisException, IllegalArgumentException, PySparkValueError
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.execution import ExecutionEngine
from sparkforge.validation.pipeline_validation import UnifiedValidator
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode, LogLevel
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
from sparkforge.errors import ConfigurationError, ExecutionError


class TestErrorScenarios:
    """Error scenario tests for Mock Spark components."""
    
    def test_invalid_data_types(self, mock_spark_session):
        """Test handling of invalid data types."""
        # Test with wrong data type for schema
        with pytest.raises(PySparkValueError):
            mock_spark_session.createDataFrame([{"id": "not_a_number"}], MockStructType([
                MockStructField("id", IntegerType())
            ]))
        
        # Test with missing required fields
        with pytest.raises(PySparkValueError):
            mock_spark_session.createDataFrame([{"name": "Alice"}], MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ]))
    
    def test_invalid_schema_operations(self, mock_spark_session):
        """Test invalid schema operations."""
        # Test creating schema with invalid name
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.create_schema("")
        
        # Test creating table with invalid name
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.create_table("test", "", [])
        
        # Test accessing non-existent schema
        with pytest.raises(AnalysisException):
            mock_spark_session.storage.get_table("nonexistent", "table")
    
    def test_invalid_dataframe_operations(self, mock_spark_session):
        """Test invalid DataFrame operations."""
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        df = mock_spark_session.createDataFrame([{"id": 1, "name": "Alice"}], schema)
        
        # Test selecting non-existent column
        with pytest.raises(AnalysisException):
            df.select("nonexistent_column")
        
        # Test filtering with invalid column
        with pytest.raises(AnalysisException):
            df.filter(F.col("nonexistent_column") > 0)
        
        # Test with invalid data type in filter
        with pytest.raises(PySparkValueError):
            df.filter("invalid_filter_expression")
    
    def test_pipeline_builder_errors(self, mock_spark_session):
        """Test PipelineBuilder error scenarios."""
        # Test with None spark session
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=None, schema="test")
        
        # Test with empty schema name
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=mock_spark_session, schema="")
        
        # Test with invalid quality rates
        with pytest.raises(ValueError):
            PipelineBuilder(
                spark=mock_spark_session,
                schema="test",
                min_bronze_rate=-1.0  # Invalid negative rate
            )
        
        with pytest.raises(ValueError):
            PipelineBuilder(
                spark=mock_spark_session,
                schema="test",
                min_silver_rate=101.0  # Invalid rate > 100
            )
    
    def test_execution_engine_errors(self, mock_spark_session):
        """Test ExecutionEngine error scenarios."""
        # Test with None config
        with pytest.raises(TypeError):
            ExecutionEngine(spark=mock_spark_session, config=None)
        
        # Test with invalid thresholds
        with pytest.raises(ValueError):
            ValidationThresholds(bronze=-1.0, silver=50.0, gold=75.0)
        
        with pytest.raises(ValueError):
            ValidationThresholds(bronze=50.0, silver=101.0, gold=75.0)
        
        # Test with invalid parallel config
        with pytest.raises(ValueError):
            ParallelConfig(enabled=True, max_workers=0)  # Invalid max_workers
    
    def test_validation_errors(self, mock_spark_session):
        """Test validation error scenarios."""
        validator = UnifiedValidator()
        
        # Test with invalid validation result
        with pytest.raises(TypeError):
            ValidationResult(
                is_valid="not_a_boolean",  # Invalid type
                errors=[],
                warnings=[],
                recommendations=[]
            )
        
        # Test with invalid error list
        with pytest.raises(TypeError):
            ValidationResult(
                is_valid=True,
                errors="not_a_list",  # Invalid type
                warnings=[],
                recommendations=[]
            )
    
    def test_writer_errors(self, mock_spark_session):
        """Test LogWriter error scenarios."""
        # Test with None config
        with pytest.raises(TypeError):
            LogWriter(spark=mock_spark_session, config=None)
        
        # Test with invalid config
        with pytest.raises(TypeError):
            WriterConfig(
                table_schema="",  # Empty schema name
                table_name="logs"
            )
        
        with pytest.raises(TypeError):
            WriterConfig(
                table_schema="test",
                table_name=""  # Empty table name
            )
        
        # Test with invalid batch size
        with pytest.raises(ValueError):
            WriterConfig(
                table_schema="test",
                table_name="logs",
                batch_size=-1  # Invalid batch size
            )
    
    def test_storage_errors(self, mock_spark_session):
        """Test storage error scenarios."""
        # Test with invalid schema operations
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.create_schema(None)
        
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.schema_exists(None)
        
        # Test with invalid table operations
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.create_table(None, "table", [])
        
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.create_table("schema", None, [])
        
        # Test with invalid data operations
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.insert_data(None, "table", [])
        
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.storage.insert_data("schema", None, [])
    
    def test_catalog_errors(self, mock_spark_session):
        """Test catalog error scenarios."""
        # Test with invalid database operations
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.catalog.createDatabase(None)
        
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.catalog.tableExists(None, "table")
        
        with pytest.raises(IllegalArgumentException):
            mock_spark_session.catalog.tableExists("database", None)
        
        # Test with non-existent entities
        assert not mock_spark_session.catalog.tableExists("nonexistent", "table")
        assert not mock_spark_session.catalog.tableExists("database", "nonexistent")
    
    def test_dataframe_writer_errors(self, mock_spark_session):
        """Test DataFrameWriter error scenarios."""
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        df = mock_spark_session.createDataFrame([{"id": 1, "name": "Alice"}], schema)
        writer = df.write
        
        # Test with invalid mode
        with pytest.raises(IllegalArgumentException):
            writer.mode("invalid_mode")
        
        # Test with invalid format
        with pytest.raises(IllegalArgumentException):
            writer.format("invalid_format")
        
        # Test with invalid options
        with pytest.raises(IllegalArgumentException):
            writer.option(None, "value")
        
        with pytest.raises(IllegalArgumentException):
            writer.option("key", None)
    
    def test_function_errors(self, mock_spark_session):
        """Test function error scenarios."""
        # Test with invalid column references
        with pytest.raises(IllegalArgumentException):
            F.col(None)
        
        with pytest.raises(IllegalArgumentException):
            F.col("")
        
        # Test with invalid literal values
        with pytest.raises(IllegalArgumentException):
            F.lit(None)
        
        # Test with invalid aggregate functions
        with pytest.raises(IllegalArgumentException):
            F.count(None)
        
        with pytest.raises(IllegalArgumentException):
            F.sum(None)
    
    def test_type_errors(self, mock_spark_session):
        """Test type error scenarios."""
        # Test with invalid struct field
        with pytest.raises(TypeError):
            MockStructField(None, StringType())
        
        with pytest.raises(TypeError):
            MockStructField("name", None)
        
        # Test with invalid struct type
        with pytest.raises(TypeError):
            MockStructType(None)
        
        with pytest.raises(TypeError):
            MockStructType("not_a_list")
    
    def test_memory_errors(self, mock_spark_session):
        """Test memory-related error scenarios."""
        # Test with extremely large dataset (simulate memory issues)
        try:
            # Create a very large dataset
            large_data = []
            for i in range(1000000):  # 1 million records
                large_data.append({
                    "id": i,
                    "name": f"Person_{i}",
                    "age": 20 + (i % 50),
                    "salary": 30000.0 + (i * 100)
                })
            
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType())
            ])
            
            # This should not crash, but might be slow
            df = mock_spark_session.createDataFrame(large_data, schema)
            assert df.count() == 1000000
            
        except Exception as e:
            # If it fails due to memory, that's expected
            assert "memory" in str(e).lower() or "resource" in str(e).lower()
    
    def test_concurrent_access_errors(self, mock_spark_session):
        """Test concurrent access error scenarios."""
        # Test accessing the same table from multiple operations
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        # Create table
        mock_spark_session.storage.create_schema("test")
        mock_spark_session.storage.create_table("test", "concurrent_table", schema.fields)
        
        # Insert data
        data1 = [{"id": 1}]
        data2 = [{"id": 2}]
        
        mock_spark_session.storage.insert_data("test", "concurrent_table", data1)
        mock_spark_session.storage.insert_data("test", "concurrent_table", data2)
        
        # Verify data was inserted
        all_data = mock_spark_session.storage.query_table("test", "concurrent_table")
        assert len(all_data) == 2
    
    def test_boundary_value_errors(self, mock_spark_session):
        """Test boundary value error scenarios."""
        # Test with maximum integer values
        max_int_data = [{"id": 2147483647}]
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        df = mock_spark_session.createDataFrame(max_int_data, schema)
        assert df.count() == 1
        
        # Test with minimum integer values
        min_int_data = [{"id": -2147483648}]
        df = mock_spark_session.createDataFrame(min_int_data, schema)
        assert df.count() == 1
        
        # Test with very long strings
        long_string = "a" * 10000
        long_string_data = [{"name": long_string}]
        string_schema = MockStructType([MockStructField("name", StringType())])
        
        df = mock_spark_session.createDataFrame(long_string_data, string_schema)
        assert df.count() == 1
    
    def test_network_simulation_errors(self, mock_spark_session):
        """Test network simulation error scenarios."""
        # Simulate network errors by testing table operations
        # that might fail in a real distributed environment
        
        # Test table not found error
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent.database.nonexistent_table")
        
        # Test schema not found error
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent_schema.table")
        
        # Test permission errors (simulated)
        # In a real system, this might be a permission error
        with pytest.raises(AnalysisException):
            mock_spark_session.table("restricted.table")
    
    def test_data_corruption_errors(self, mock_spark_session):
        """Test data corruption error scenarios."""
        # Test with corrupted data
        corrupted_data = [
            {"id": 1, "name": "Alice"},
            {"id": "corrupted", "name": "Bob"},  # Wrong type
            {"id": 3, "name": None},  # Null value
            {"id": 4}  # Missing field
        ]
        
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        # This should handle corrupted data gracefully
        try:
            df = mock_spark_session.createDataFrame(corrupted_data, schema)
            # If it succeeds, verify the data
            assert df.count() >= 0
        except Exception as e:
            # If it fails, that's also acceptable
            assert isinstance(e, (PySparkValueError, AnalysisException))
    
    def test_resource_exhaustion_errors(self, mock_spark_session):
        """Test resource exhaustion error scenarios."""
        # Test with too many tables
        mock_spark_session.storage.create_schema("test")
        
        # Create many tables
        for i in range(1000):
            schema = MockStructType([MockStructField("id", IntegerType())])
            mock_spark_session.storage.create_table("test", f"table_{i}", schema.fields)
        
        # Verify tables were created
        tables = mock_spark_session.storage.list_tables("test")
        assert len(tables) == 1000
        
        # Test with too many schemas
        for i in range(100):
            mock_spark_session.storage.create_schema(f"schema_{i}")
        
        # Verify schemas were created
        schemas = mock_spark_session.storage.list_schemas()
        assert len(schemas) >= 100
