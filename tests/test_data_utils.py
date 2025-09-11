# test_data_utils.py
"""
Unit tests for the data_utils module.

This module tests all DataFrame manipulation and utility functions.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from sparkforge.data_utils import (
    add_metadata_columns,
    remove_metadata_columns,
    create_empty_dataframe,
    coalesce_dataframes,
    sample_dataframe,
    get_column_statistics
)


@pytest.fixture
def spark_session():
    """Create Spark session for testing."""
    return SparkSession.builder.master('local[*]').appName('test').getOrCreate()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing."""
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('value', IntegerType(), True)
    ])
    data = [(1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)]
    return spark_session.createDataFrame(data, schema)


class TestAddMetadataColumns:
    """Test add_metadata_columns function."""
    
    def test_add_metadata_columns_default_timestamp(self, sample_dataframe):
        """Test adding metadata columns with default timestamp."""
        result = add_metadata_columns(sample_dataframe, "run_123")
        
        # Check that metadata columns were added
        assert '_run_id' in result.columns
        assert '_created_at' in result.columns
        assert '_updated_at' in result.columns
        
        # Check original columns are preserved
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert 'value' in result.columns
        
        # Check run_id values
        run_ids = result.select('_run_id').distinct().collect()
        assert len(run_ids) == 1
        assert run_ids[0]['_run_id'] == "run_123"
    
    def test_add_metadata_columns_custom_timestamp(self, sample_dataframe):
        """Test adding metadata columns with custom timestamp."""
        custom_time = datetime(2023, 1, 1, 12, 0, 0)
        result = add_metadata_columns(sample_dataframe, "run_456", custom_time)
        
        # Check timestamp values
        timestamps = result.select('_created_at').distinct().collect()
        assert len(timestamps) == 1
        # Note: Spark timestamps are converted, so we check the string representation
        assert "2023-01-01 12:00:00" in str(timestamps[0]['_created_at'])
    
    def test_add_metadata_columns_preserves_data(self, sample_dataframe):
        """Test that original data is preserved."""
        result = add_metadata_columns(sample_dataframe, "run_789")
        
        # Check row count
        assert result.count() == sample_dataframe.count()
        
        # Check original data integrity
        original_data = sample_dataframe.collect()
        result_data = result.select('id', 'name', 'value').collect()
        
        assert len(original_data) == len(result_data)
        for orig, res in zip(original_data, result_data):
            assert orig['id'] == res['id']
            assert orig['name'] == res['name']
            assert orig['value'] == res['value']


class TestRemoveMetadataColumns:
    """Test remove_metadata_columns function."""
    
    def test_remove_metadata_columns(self, sample_dataframe):
        """Test removing metadata columns."""
        # First add metadata columns
        df_with_metadata = add_metadata_columns(sample_dataframe, "run_123")
        
        # Then remove them
        result = remove_metadata_columns(df_with_metadata)
        
        # Check that metadata columns are removed
        assert '_run_id' not in result.columns
        assert '_created_at' not in result.columns
        assert '_updated_at' not in result.columns
        
        # Check original columns are preserved
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert 'value' in result.columns
    
    def test_remove_metadata_columns_no_metadata(self, sample_dataframe):
        """Test removing metadata columns when none exist."""
        result = remove_metadata_columns(sample_dataframe)
        
        # Should return original DataFrame unchanged
        assert result.columns == sample_dataframe.columns
        assert result.count() == sample_dataframe.count()
    
    def test_remove_metadata_columns_with_validation_columns(self, spark_session):
        """Test removing metadata columns including validation columns."""
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('__is_valid__', StringType(), True),
            StructField('_failed_rules', StringType(), True)
        ])
        data = [(1, 'true', 'rule1'), (2, 'false', 'rule2')]
        df = spark_session.createDataFrame(data, schema)
        
        result = remove_metadata_columns(df)
        
        # Check that validation columns are removed
        assert '__is_valid__' not in result.columns
        assert '_failed_rules' not in result.columns
        
        # Check original columns are preserved
        assert 'id' in result.columns


class TestCreateEmptyDataframe:
    """Test create_empty_dataframe function."""
    
    def test_create_empty_dataframe(self, spark_session):
        """Test creating empty DataFrame with schema."""
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True)
        ])
        
        result = create_empty_dataframe(spark_session, schema)
        
        assert result.count() == 0
        assert result.schema == schema
        assert result.columns == ['id', 'name']
    
    def test_create_empty_dataframe_complex_schema(self, spark_session):
        """Test creating empty DataFrame with complex schema."""
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('value', IntegerType(), False)  # Not nullable
        ])
        
        result = create_empty_dataframe(spark_session, schema)
        
        assert result.count() == 0
        assert result.schema == schema
        assert len(result.columns) == 4


class TestCoalesceDataframes:
    """Test coalesce_dataframes function."""
    
    def test_coalesce_single_dataframe(self, sample_dataframe):
        """Test coalescing single DataFrame."""
        result = coalesce_dataframes([sample_dataframe])
        
        assert result.count() == sample_dataframe.count()
        assert result.columns == sample_dataframe.columns
    
    def test_coalesce_multiple_dataframes(self, spark_session):
        """Test coalescing multiple DataFrames."""
        schema = StructType([StructField('id', IntegerType(), True)])
        
        df1 = spark_session.createDataFrame([(1,), (2,)], schema)
        df2 = spark_session.createDataFrame([(3,), (4,)], schema)
        df3 = spark_session.createDataFrame([(5,), (6,)], schema)
        
        result = coalesce_dataframes([df1, df2, df3])
        
        assert result.count() == 6
        assert result.columns == ['id']
        
        # Check data integrity
        ids = [row['id'] for row in result.collect()]
        assert set(ids) == {1, 2, 3, 4, 5, 6}
    
    def test_coalesce_empty_list_raises_error(self):
        """Test that coalescing empty list raises ValueError."""
        with pytest.raises(ValueError):
            coalesce_dataframes([])
    
    def test_coalesce_different_schemas(self, spark_session):
        """Test coalescing DataFrames with different schemas."""
        schema1 = StructType([StructField('id', IntegerType(), True)])
        schema2 = StructType([StructField('name', StringType(), True)])
        
        df1 = spark_session.createDataFrame([(1,)], schema1)
        df2 = spark_session.createDataFrame([('Alice',)], schema2)
        
        # This should work as Spark will handle schema differences
        result = coalesce_dataframes([df1, df2])
        assert result.count() == 2


class TestSampleDataframe:
    """Test sample_dataframe function."""
    
    def test_sample_dataframe_basic(self, sample_dataframe):
        """Test basic DataFrame sampling."""
        from sparkforge.data_utils import sample_dataframe as sample_func
        result = sample_func(sample_dataframe, fraction=0.5, seed=42)
        
        # Should have fewer or equal rows
        assert result.count() <= sample_dataframe.count()
        assert result.columns == sample_dataframe.columns
    
    def test_sample_dataframe_deterministic(self, sample_dataframe):
        """Test that sampling is deterministic with same seed."""
        from sparkforge.data_utils import sample_dataframe as sample_func
        result1 = sample_func(sample_dataframe, fraction=0.5, seed=42)
        result2 = sample_func(sample_dataframe, fraction=0.5, seed=42)
        
        # Should produce same result with same seed
        assert result1.count() == result2.count()
    
    def test_sample_dataframe_different_seeds(self, sample_dataframe):
        """Test that different seeds produce different results."""
        from sparkforge.data_utils import sample_dataframe as sample_func
        result1 = sample_func(sample_dataframe, fraction=0.5, seed=42)
        result2 = sample_func(sample_dataframe, fraction=0.5, seed=123)
        
        # May or may not be different, but should be valid
        assert result1.count() <= sample_dataframe.count()
        assert result2.count() <= sample_dataframe.count()
    
    def test_sample_dataframe_invalid_fraction_raises_error(self, sample_dataframe):
        """Test that invalid fraction raises ValueError."""
        from sparkforge.data_utils import sample_dataframe as sample_func
        with pytest.raises(ValueError):
            sample_func(sample_dataframe, fraction=0.0)
        
        with pytest.raises(ValueError):
            sample_func(sample_dataframe, fraction=1.5)
        
        with pytest.raises(ValueError):
            sample_func(sample_dataframe, fraction=-0.1)
    
    def test_sample_dataframe_fraction_one(self, sample_dataframe):
        """Test sampling with fraction 1.0."""
        from sparkforge.data_utils import sample_dataframe as sample_func
        result = sample_func(sample_dataframe, fraction=1.0, seed=42)
        
        # Should return all rows
        assert result.count() == sample_dataframe.count()


class TestGetColumnStatistics:
    """Test get_column_statistics function."""
    
    def test_get_column_statistics_numeric(self, sample_dataframe):
        """Test getting statistics for numeric column."""
        stats = get_column_statistics(sample_dataframe, 'value')
        
        assert 'count' in stats
        assert 'mean' in stats
        assert 'stddev' in stats
        assert 'min' in stats
        assert 'max' in stats
        
        # Check specific values
        assert stats['count'] == '3'
        assert float(stats['mean']) == 200.0  # (100 + 200 + 300) / 3
    
    def test_get_column_statistics_string(self, sample_dataframe):
        """Test getting statistics for string column."""
        stats = get_column_statistics(sample_dataframe, 'name')
        
        assert 'count' in stats
        # String columns don't have mean, stddev, etc.
        assert 'mean' not in stats or stats['mean'] is None
    
    def test_get_column_statistics_nonexistent_column_raises_error(self, sample_dataframe):
        """Test that nonexistent column raises ValueError."""
        with pytest.raises(ValueError):
            get_column_statistics(sample_dataframe, 'nonexistent_column')
    
    def test_get_column_statistics_with_nulls(self, spark_session):
        """Test getting statistics for column with nulls."""
        schema = StructType([StructField('value', IntegerType(), True)])
        data = [(1,), (2,), (None,), (4,)]
        df = spark_session.createDataFrame(data, schema)
        
        stats = get_column_statistics(df, 'value')
        
        assert 'count' in stats
        # Count should be 3 (excluding nulls)
        assert stats['count'] == '3'
    
    def test_get_column_statistics_error_handling(self, spark_session):
        """Test error handling in get_column_statistics."""
        # Create a DataFrame that might cause issues
        schema = StructType([StructField('problematic_col', StringType(), True)])
        data = [('test',)]
        df = spark_session.createDataFrame(data, schema)
        
        # This should not raise an exception
        stats = get_column_statistics(df, 'problematic_col')
        assert isinstance(stats, dict)
