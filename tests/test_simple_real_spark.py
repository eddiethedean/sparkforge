#!/usr/bin/env python3
"""
Simple tests demonstrating real Spark operations without mocks.

This module shows how to test core functionality using real Spark DataFrames
and operations instead of mocks.
"""


import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Import the actual functions we're testing
from sparkforge.validation import (
    and_all_rules,
    assess_data_quality,
    get_dataframe_info,
    validate_dataframe_schema,
)

# add_metadata_columns function removed - not needed for simplified system


class TestRealSparkOperations:
    """Test core functionality with real Spark operations."""

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
    def test_real_spark_dataframe_operations(self, sample_dataframe):
        """Test basic Spark DataFrame operations with real data."""
        # Test basic operations
        assert sample_dataframe.count() == 5
        assert len(sample_dataframe.columns) == 3

        # Test filtering
        click_events = sample_dataframe.filter(F.col("action") == "click")
        assert click_events.count() == 2

        # Test grouping
        action_counts = sample_dataframe.groupBy("action").count()
        assert action_counts.count() == 3

        # Test aggregation
        total_events = sample_dataframe.count()
        assert total_events == 5

    @pytest.mark.spark
    def test_real_spark_transformations(self, sample_dataframe):
        """Test Spark transformations with real data."""
        # Test adding columns
        df_with_date = sample_dataframe.withColumn("event_date", F.to_date("timestamp"))
        assert "event_date" in df_with_date.columns

        # Test window functions
        from pyspark.sql.window import Window

        window = Window.partitionBy("action").orderBy("timestamp")
        df_with_rank = df_with_date.withColumn("rank", F.row_number().over(window))
        assert "rank" in df_with_rank.columns

        # Test complex transformations
        df_processed = (
            sample_dataframe.withColumn("event_date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .groupBy("user_id", "event_date")
            .agg(F.count("action").alias("event_count"))
            .orderBy("user_id", "event_date")
        )

        assert df_processed.count() == 5  # 5 users, 1 date each
        assert "event_count" in df_processed.columns

    @pytest.mark.spark
    def test_real_spark_validation_rules(self, sample_dataframe):
        """Test validation rules with real Spark operations."""
        # Test rule combination
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
        }

        result = and_all_rules(rules)
        assert result is not None

        # Test that the result can be used in a real DataFrame operation
        test_df = sample_dataframe.withColumn("is_valid", result)
        assert test_df.count() == 5
        assert "is_valid" in test_df.columns

    @pytest.mark.spark
    def test_real_spark_data_quality(self, sample_dataframe):
        """Test data quality assessment with real Spark operations."""
        # Test data quality assessment
        quality = assess_data_quality(sample_dataframe)

        assert quality["total_rows"] == 5
        assert "quality_rate" in quality
        assert "valid_rows" in quality
        assert "invalid_rows" in quality
        assert "is_empty" in quality
        assert quality["quality_rate"] >= 0.0
        assert quality["quality_rate"] <= 100.0

    @pytest.mark.spark
    def test_real_spark_metadata_operations(self, sample_dataframe):
        """Test metadata operations with real Spark operations."""
        # Test basic DataFrame operations (metadata functions removed in simplified system)
        result = sample_dataframe.withColumn("_test_column", F.lit("test_value"))

        # Check that the result has the expected columns
        columns = result.columns
        assert "_test_column" in columns
        assert "user_id" in columns
        assert "action" in columns
        assert "timestamp" in columns

        # Verify the data is still there
        assert result.count() == 5

    @pytest.mark.spark
    def test_real_spark_performance(self, spark_session):
        """Test performance with larger datasets using real Spark operations."""
        # Create a larger dataset
        data = []
        for i in range(1000):
            data.append((f"user{i}", "click", f"2024-01-01 {10 + i % 14:02d}:00:00"))

        df = spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])

        # Test performance with larger dataset
        assert df.count() == 1000

        # Test complex operations on larger dataset
        result = (
            df.withColumn("event_date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .groupBy("user_id", "event_date")
            .agg(F.count("action").alias("event_count"))
            .orderBy("user_id", "event_date")
        )

        assert result.count() == 1000  # 1000 users, 1 date each
        assert "event_count" in result.columns

    @pytest.mark.spark
    def test_real_spark_error_handling(self, sample_dataframe):
        """Test error handling with real Spark operations."""
        # Test with invalid operations
        try:
            # This should work
            result = sample_dataframe.filter(F.col("action") == "click")
            assert result.count() == 2
        except Exception as e:
            pytest.fail(f"Valid operation failed: {e}")

        # Test with null handling
        df_with_nulls = sample_dataframe.withColumn("test_null", F.lit(None))
        assert "test_null" in df_with_nulls.columns

        # Test null filtering
        non_null_df = df_with_nulls.filter(F.col("user_id").isNotNull())
        assert non_null_df.count() == 5

    @pytest.mark.spark
    def test_real_spark_schema_operations(self, sample_dataframe):
        """Test schema operations with real Spark operations."""
        # Test schema validation
        assert validate_dataframe_schema(
            sample_dataframe, ["user_id", "action", "timestamp"]
        )
        assert not validate_dataframe_schema(
            sample_dataframe, ["user_id", "missing_col"]
        )

        # Test schema information
        info = get_dataframe_info(sample_dataframe)
        assert info["row_count"] == 5
        assert info["column_count"] == 3
        assert info["columns"] == ["user_id", "action", "timestamp"]
        assert not info["is_empty"]

    @pytest.mark.spark
    def test_real_spark_joins(self, spark_session):
        """Test join operations with real Spark DataFrames."""
        # Create two DataFrames
        users_data = [
            ("user1", "Alice"),
            ("user2", "Bob"),
            ("user3", "Charlie"),
        ]
        users_df = spark_session.createDataFrame(users_data, ["user_id", "name"])

        events_data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
            ("user1", "purchase", "2024-01-01 12:00:00"),
        ]
        events_df = spark_session.createDataFrame(
            events_data, ["user_id", "action", "timestamp"]
        )

        # Test inner join
        joined_df = users_df.join(events_df, "user_id", "inner")
        assert joined_df.count() == 3
        assert "name" in joined_df.columns
        assert "action" in joined_df.columns

        # Test left join
        left_joined_df = users_df.join(events_df, "user_id", "left")
        assert (
            left_joined_df.count() == 4
        )  # All users, some with events (user1 has 2 events)

        # Test aggregation after join
        user_activity = (
            joined_df.groupBy("name")
            .agg(F.count("action").alias("event_count"))
            .orderBy("name")
        )

        assert user_activity.count() == 2  # Alice and Bob have events
        assert "event_count" in user_activity.columns
