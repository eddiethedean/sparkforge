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

    def test_create_dataframe_with_dict_data(self, spark_session):
        """Test creating DataFrame with dict data."""
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_dataframe_compat(spark_session, data)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_dataframe_with_tuple_data(self, spark_session):
        """Test creating DataFrame with tuple data (converts to dict for mock-spark)."""
        data = [(1, "test1"), (2, "test2")]
        schema = ["id", "name"]
        df = create_dataframe_compat(spark_session, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        # Verify columns exist
        assert "id" in df.columns
        assert "name" in df.columns

    def test_create_dataframe_with_structtype_schema(self, spark_session):
        """Test creating DataFrame with StructType schema."""
        if is_mock_spark():
            from mock_spark.spark_types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )
        else:
            from pyspark.sql.types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_dataframe_compat(spark_session, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_dataframe_with_tuple_and_structtype(self, spark_session):
        """Test creating DataFrame with tuple data and StructType schema."""
        if is_mock_spark():
            from mock_spark.spark_types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )
        else:
            from pyspark.sql.types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [(1, "test1"), (2, "test2")]
        df = create_dataframe_compat(spark_session, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns


class TestIsDataframeLike:
    """Test is_dataframe_like function."""

    def test_pyspark_dataframe(self, spark_session):
        """Test with PySpark DataFrame."""
        data = [{"id": 1, "name": "test"}]
        df = spark_session.createDataFrame(data)
        assert is_dataframe_like(df) is True

    def test_mock_spark_dataframe(self, spark_session):
        """Test with mock-spark DataFrame."""
        data = [{"id": 1, "name": "test"}]
        df = spark_session.createDataFrame(data)
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

    def test_detect_mock_spark(self, spark_session):
        """Test detecting mock-spark session."""
        if is_mock_spark():
            spark_type = detect_spark_type(spark_session)
            assert spark_type == "mock"

    def test_detect_pyspark(self, spark_session):
        """Test detecting PySpark session."""
        # Skip if using mock-spark (this test only works with real PySpark)
        if is_mock_spark():
            pytest.skip("PySpark detection test requires real PySpark")
        # Only run if we have PySpark
        spark_type = detect_spark_type(spark_session)
        # When using mock-spark, this will be 'mock', so skip
        if spark_type == "mock":
            pytest.skip("PySpark detection test requires real PySpark")
        assert spark_type == "pyspark"


class TestCreateTestDataframe:
    """Test create_test_dataframe function."""

    def test_create_test_dataframe_with_dict(self, spark_session):
        """Test creating test DataFrame with dict data."""
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_test_dataframe(spark_session, data)
        assert is_dataframe_like(df)
        assert df.count() == 2

    def test_create_test_dataframe_with_schema(self, spark_session):
        """Test creating test DataFrame with schema."""
        if is_mock_spark():
            from mock_spark.spark_types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )
        else:
            from pyspark.sql.types import (
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        df = create_test_dataframe(spark_session, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_create_test_dataframe_with_tuples(self, spark_session):
        """Test creating test DataFrame with tuple data."""
        data = [(1, "test1"), (2, "test2")]
        schema = ["id", "name"]
        df = create_test_dataframe(spark_session, data, schema)
        assert is_dataframe_like(df)
        assert df.count() == 2
        assert "id" in df.columns
        assert "name" in df.columns
