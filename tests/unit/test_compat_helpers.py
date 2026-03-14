"""
Unit tests for compatibility helpers.

Tests the centralized helper functions that abstract over PySpark and mock-spark differences.
"""

import pytest

from pipeline_builder.compat import is_mock_spark
from pipeline_builder.compat_helpers import (
    create_dataframe_compat,
    create_test_dataframe,
    detect_spark_type,
    is_dataframe_like,
)


class TestCreateDataframeCompat:
    """Test create_dataframe_compat function."""

    def test_create_dataframe_with_dict_data(self, spark):
        """Test creating DataFrame with dict data."""
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_dataframe_compat(spark, data)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_dataframe_with_tuple_data(self, spark):
        """Test creating DataFrame with tuple data (converts to dict for mock-spark)."""
        data = [(1, "test1"), (2, "test2")]
        schema = ["id", "name"]
        df = create_dataframe_compat(spark, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        # Verify columns exist
        assert "id" in df.columns
        assert "name" in df.columns

    def test_create_dataframe_with_structtype_schema(self, spark, spark_imports):
        """Test creating DataFrame with StructType schema."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_dataframe_compat(spark, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_dataframe_with_tuple_and_structtype(self, spark, spark_imports):
        """Test creating DataFrame with tuple data and StructType schema."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [(1, "test1"), (2, "test2")]
        df = create_dataframe_compat(spark, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns


class TestIsDataframeLike:
    """Test is_dataframe_like function."""

    def test_pyspark_dataframe(self, spark):
        """Test with PySpark DataFrame."""
        data = [{"id": 1, "name": "test"}]
        df = spark.createDataFrame(data)
        assert is_dataframe_like(df) is True

    def test_mock_spark_dataframe(self, spark):
        """Test with mock-spark DataFrame."""
        data = [{"id": 1, "name": "test"}]
        df = spark.createDataFrame(data)
        assert is_dataframe_like(df) is True

    def test_non_dataframe_object(self):
        """Test with non-DataFrame object."""
        assert is_dataframe_like({"not": "a dataframe"}) is False
        assert is_dataframe_like([1, 2, 3]) is False
        assert is_dataframe_like("string") is False
        assert is_dataframe_like(None) is False

    def test_object_with_some_methods(self):
        """Test with object that has some but not all DataFrame methods."""

        class PartialDataFrame:
            def count(self):
                return 1

            def columns(self):
                return ["col1"]

        assert is_dataframe_like(PartialDataFrame()) is False  # Missing filter method


class TestDetectSparkType:
    """Test detect_spark_type function."""

    def test_detect_mock_spark(self, spark, spark_mode):
        """Test detecting mock-spark session."""
        from sparkless.testing import Mode
        if spark_mode == Mode.SPARKLESS:
            spark_type = detect_spark_type(spark)
            assert spark_type == "mock"

    def test_detect_pyspark(self, spark, spark_mode):
        """Test detecting PySpark session."""
        from sparkless.testing import Mode
        if spark_mode == Mode.SPARKLESS:
            pytest.skip("PySpark detection test requires real PySpark")
        spark_type = detect_spark_type(spark)
        assert spark_type == "pyspark"


class TestCreateTestDataframe:
    """Test create_test_dataframe function."""

    def test_create_test_dataframe_with_dict(self, spark):
        """Test creating test DataFrame with dict data."""
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_test_dataframe(spark, data)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_test_dataframe_with_schema(self, spark, spark_imports):
        """Test creating test DataFrame with schema."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        IntegerType = spark_imports.IntegerType
        StringType = spark_imports.StringType

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_test_dataframe(spark, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_create_test_dataframe_with_tuples(self, spark):
        """Test creating test DataFrame with tuple data."""
        data = [(1, "test1"), (2, "test2")]
        schema = ["id", "name"]
        df = create_test_dataframe(spark, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns
