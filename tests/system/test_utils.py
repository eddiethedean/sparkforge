#!/usr/bin/env python3
"""
Comprehensive tests for the utils module using real Spark operations.

This module tests all utility functions, validation, table operations, and reporting
with actual Spark DataFrames and Delta Lake operations.
"""

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# add_metadata_columns and remove_metadata_columns functions removed - not needed for simplified system
from sparkforge.models import StageStats
from sparkforge.reporting import create_validation_dict, create_write_dict

# Import the actual functions we're testing
from sparkforge.validation import (
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    get_dataframe_info,
    validate_dataframe_schema,
)


class TestDataValidation:
    """Test data validation utility functions with real Spark operations."""

    @pytest.fixture
    def sample_dataframe(self, spark_session):
        """Create a sample DataFrame for testing."""
        data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
            ("user3", "purchase", "2024-01-01 12:00:00"),
            ("user4", "click", "2024-01-01 13:00:00"),
            ("user5", "view", "2024-01-01 14:00:00"),
        ]
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    @pytest.mark.spark
    def test_and_all_rules(self, sample_dataframe):
        """Test rule combination with real Spark operations."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
        }

        result = and_all_rules(rules)
        assert result is not None

        # Test that the result can be used in a real DataFrame operation
        test_df = sample_dataframe.withColumn("is_valid", result)
        assert test_df.count() == 5

    @pytest.mark.spark
    def test_and_all_rules_empty(self, sample_dataframe):
        """Test rule combination with empty rules."""
        result = and_all_rules({})
        assert result is True  # Should return True for empty rules

        # Test that the result is a boolean (not a Column)
        assert isinstance(result, bool)

    @pytest.mark.spark
    def test_apply_column_rules(self, sample_dataframe):
        """Test column rule application with real Spark operations."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, rules, "bronze", "test_step", filter_columns_by_rules=True
        )

        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert isinstance(stats, StageStats)
        assert stats.stage == "bronze"
        assert stats.step == "test_step"
        assert stats.total_rows == 5
        assert stats.valid_rows + stats.invalid_rows == 5

    @pytest.mark.spark
    def test_apply_column_rules_none_rules(self, sample_dataframe):
        """Test column rule application with None rules."""
        from sparkforge.errors import ValidationError

        with pytest.raises(ValidationError):
            apply_column_rules(
                sample_dataframe,
                None,
                "bronze",
                "test_step",
                filter_columns_by_rules=True,
            )

    @pytest.mark.spark
    def test_assess_data_quality(self, sample_dataframe):
        """Test data quality assessment with real Spark operations."""
        quality = assess_data_quality(sample_dataframe)

        assert quality["total_rows"] == 5
        assert "quality_rate" in quality
        assert "valid_rows" in quality
        assert "invalid_rows" in quality
        assert "is_empty" in quality
        assert quality["quality_rate"] >= 0.0
        assert quality["quality_rate"] <= 100.0

    @pytest.mark.spark
    def test_get_dataframe_info(self, sample_dataframe):
        """Test DataFrame info extraction with real Spark operations."""
        info = get_dataframe_info(sample_dataframe)

        assert info["row_count"] == 5
        assert info["column_count"] == 3
        assert info["columns"] == ["user_id", "action", "timestamp"]
        assert not info["is_empty"]

    @pytest.mark.spark
    def test_validate_dataframe_schema(self, sample_dataframe):
        """Test DataFrame schema validation with real Spark operations."""
        # Test valid schema
        assert validate_dataframe_schema(
            sample_dataframe, ["user_id", "action", "timestamp"]
        )

        # Test missing columns
        assert not validate_dataframe_schema(
            sample_dataframe, ["user_id", "missing_col"]
        )

        # Test partial match
        assert validate_dataframe_schema(sample_dataframe, ["user_id", "action"])


class TestDataTransformationUtilities:
    """Test data transformation utility functions with real Spark operations."""

    @pytest.fixture
    def sample_dataframe(self, spark_session):
        """Create a sample DataFrame for testing."""
        data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
            ("user3", "purchase", "2024-01-01 12:00:00"),
        ]
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    @pytest.mark.spark
    def test_basic_dataframe_operations(self, sample_dataframe):
        """Test basic DataFrame operations (metadata functions removed in simplified system)."""
        # Test basic DataFrame operations
        result = sample_dataframe.withColumn("_test_column", F.lit("test_value"))

        # Check that the result has the expected columns
        columns = result.columns
        assert "_test_column" in columns
        assert "user_id" in columns
        assert "action" in columns
        assert "timestamp" in columns

        # Verify the data is still there
        assert result.count() == 3

    @pytest.mark.spark
    def test_dataframe_filtering(self, sample_dataframe):
        """Test DataFrame filtering operations."""
        # Test filtering
        result = sample_dataframe.filter(F.col("user_id").isNotNull())

        # Check that original columns are still there
        columns = result.columns
        assert "user_id" in columns
        assert "action" in columns
        assert "timestamp" in columns

        # Verify the data is still there
        assert result.count() == 3


class TestFactoryFunctions:
    """Test factory functions with real data."""

    @pytest.mark.spark
    def test_create_validation_dict(self, spark_session):
        """Test validation dictionary creation with real data."""
        stats = StageStats(
            stage="bronze",
            step="test_bronze",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=10.5,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        start_time = datetime.now()
        end_time = datetime.now()
        result = create_validation_dict(stats, start_at=start_time, end_at=end_time)

        assert result["validation_rate"] == 95.0
        assert result["total_rows"] == 100
        assert result["valid_rows"] == 95
        assert result["invalid_rows"] == 5
        assert result["start_at"] == start_time
        assert result["end_at"] == end_time

    @pytest.mark.spark
    def test_create_write_dict(self, spark_session):
        """Test write dictionary creation with real data."""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 5, 0)

        result = create_write_dict(
            mode="overwrite",
            rows=95,
            duration_secs=300.0,
            table_fqn="test_table",
            skipped=False,
            start_at=start_time,
            end_at=end_time,
        )

        assert result["mode"] == "overwrite"
        assert result["rows_written"] == 95
        assert result["duration_secs"] == 300.0
        assert result["table_fqn"] == "test_table"
        assert not result["skipped"]
        assert result["start_at"] == start_time
        assert result["end_at"] == end_time


class TestPerformanceWithRealData:
    """Test performance with real Spark operations and larger datasets."""

    @pytest.mark.spark
    def test_large_dataset_validation(self, spark_session):
        """Test validation with a larger dataset."""
        # Create a larger dataset
        data = []
        for i in range(1000):
            data.append((f"user{i}", "click", f"2024-01-01 {10 + i % 14:02d}:00:00"))

        df = spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])

        # Test validation rules
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            df, rules, "bronze", "test_step", filter_columns_by_rules=True
        )

        assert stats.total_rows == 1000
        assert stats.valid_rows + stats.invalid_rows == 1000
        assert stats.validation_rate >= 0.0
        assert stats.validation_rate <= 100.0

    @pytest.mark.spark
    def test_complex_transformations(self, spark_session):
        """Test complex transformations with real Spark operations."""
        # Create test data
        data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user1", "view", "2024-01-01 11:00:00"),
            ("user2", "click", "2024-01-01 12:00:00"),
            ("user2", "purchase", "2024-01-01 13:00:00"),
        ]
        df = spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])

        # Test complex transformation
        result = (
            df.withColumn("event_date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .groupBy("user_id", "event_date")
            .agg(F.count("action").alias("event_count"))
            .orderBy("user_id", "event_date")
        )

        assert result.count() == 2  # 2 users, 1 date each
        assert "event_count" in result.columns
        assert "event_date" in result.columns
