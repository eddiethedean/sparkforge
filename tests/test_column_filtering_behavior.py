"""
Tests for column filtering behavior in validation.

This module tests the filter_columns_by_rules parameter in apply_column_rules
to ensure proper column filtering behavior.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge.errors.data import ValidationError
from sparkforge.validation import apply_column_rules


class TestColumnFilteringBehavior:
    """Test column filtering behavior in validation."""

    @pytest.fixture
    def sample_dataframe_with_extra_columns(self, spark_session):
        """Create a DataFrame with extra columns that don't have validation rules."""
        data = [
            ("user1", "click", 100, "mobile", "premium"),
            ("user2", "view", 200, "desktop", "basic"),
            ("user3", "purchase", 50, "tablet", "standard"),
            ("user4", "click", 75, "mobile", "premium"),
        ]

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("device_type", StringType(), True),  # No rules
                StructField("user_tier", StringType(), True),  # No rules
            ]
        )

        return spark_session.createDataFrame(data, schema)

    def test_filter_columns_by_rules_true_default(
        self, sample_dataframe_with_extra_columns
    ):
        """Test default behavior (filter_columns_by_rules=True)."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=True,
        )

        # Should only have columns with rules
        expected_columns = set(rules.keys())
        assert set(valid_df.columns) == expected_columns
        # Invalid DataFrame includes _failed_rules column
        expected_invalid_columns = expected_columns | {"_failed_rules"}
        assert set(invalid_df.columns) == expected_invalid_columns

        # Should have all rows as valid
        assert stats.total_rows == 4
        assert stats.valid_rows == 4
        assert stats.invalid_rows == 0
        assert stats.validation_rate == 100.0

    def test_filter_columns_by_rules_false(self, sample_dataframe_with_extra_columns):
        """Test preserving all columns (filter_columns_by_rules=False)."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=False,
        )

        # Should preserve all original columns
        original_columns = set(sample_dataframe_with_extra_columns.columns)
        assert set(valid_df.columns) == original_columns
        # Invalid DataFrame includes _failed_rules column
        expected_invalid_columns = original_columns | {"_failed_rules"}
        assert set(invalid_df.columns) == expected_invalid_columns

        # Should have all rows as valid
        assert stats.total_rows == 4
        assert stats.valid_rows == 4
        assert stats.invalid_rows == 0
        assert stats.validation_rate == 100.0

    def test_filter_columns_by_rules_default_behavior(
        self, sample_dataframe_with_extra_columns
    ):
        """Test that default behavior is filter_columns_by_rules=True."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0],
        }

        # Test default behavior (no parameter specified)
        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
        )

        # Should only have columns with rules (default behavior)
        expected_columns = set(rules.keys())
        assert set(valid_df.columns) == expected_columns
        # Invalid DataFrame includes _failed_rules column
        expected_invalid_columns = expected_columns | {"_failed_rules"}
        assert set(invalid_df.columns) == expected_invalid_columns

    def test_filter_columns_with_validation_failures(
        self, sample_dataframe_with_extra_columns
    ):
        """Test column filtering with validation failures."""
        # Create rules that will cause some rows to fail validation
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [
                F.col("event_type").isin(["click", "view"])
            ],  # "purchase" will fail
            "value": [F.col("value") > 100],  # Some values will fail
        }

        # Test with filtering enabled
        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=True,
        )

        # Should only have columns with rules
        expected_columns = set(rules.keys())
        assert set(valid_df.columns) == expected_columns
        # Invalid DataFrame includes _failed_rules column
        expected_invalid_columns = expected_columns | {"_failed_rules"}
        assert set(invalid_df.columns) == expected_invalid_columns

        # Should have some validation failures
        assert stats.total_rows == 4
        assert stats.valid_rows < 4
        assert stats.invalid_rows > 0
        assert stats.validation_rate < 100.0

    def test_filter_columns_with_validation_failures_preserve_all(
        self, sample_dataframe_with_extra_columns
    ):
        """Test preserving all columns with validation failures."""
        # Create rules that will cause some rows to fail validation
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [
                F.col("event_type").isin(["click", "view"])
            ],  # "purchase" will fail
            "value": [F.col("value") > 100],  # Some values will fail
        }

        # Test with filtering disabled
        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=False,
        )

        # Should preserve all original columns
        original_columns = set(sample_dataframe_with_extra_columns.columns)
        assert set(valid_df.columns) == original_columns
        # Invalid DataFrame includes _failed_rules column
        expected_invalid_columns = original_columns | {"_failed_rules"}
        assert set(invalid_df.columns) == expected_invalid_columns

        # Should have some validation failures
        assert stats.total_rows == 4
        assert stats.valid_rows < 4
        assert stats.invalid_rows > 0
        assert stats.validation_rate < 100.0

    def test_filter_columns_with_empty_rules(self, sample_dataframe_with_extra_columns):
        """Test column filtering with empty rules."""
        rules = {}

        # Test with filtering enabled
        valid_df, invalid_df, stats = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=True,
        )

        # With empty rules, should preserve all columns
        original_columns = set(sample_dataframe_with_extra_columns.columns)
        assert set(valid_df.columns) == original_columns
        assert set(invalid_df.columns) == original_columns

        # All rows should be valid
        assert stats.total_rows == 4
        assert stats.valid_rows == 4
        assert stats.invalid_rows == 0
        assert stats.validation_rate == 100.0

    def test_filter_columns_with_none_rules(self, sample_dataframe_with_extra_columns):
        """Test column filtering with None rules."""
        with pytest.raises(ValidationError):
            apply_column_rules(
                df=sample_dataframe_with_extra_columns,
                rules=None,
                stage="bronze",
                step="test_step",
                filter_columns_by_rules=True,
            )

    def test_filter_columns_data_integrity(self, sample_dataframe_with_extra_columns):
        """Test that data integrity is maintained regardless of column filtering."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0],
        }

        # Test with filtering enabled
        valid_df_filtered, _, stats_filtered = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=True,
        )

        # Test with filtering disabled
        valid_df_preserved, _, stats_preserved = apply_column_rules(
            df=sample_dataframe_with_extra_columns,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=False,
        )

        # Statistics should be identical
        assert stats_filtered.total_rows == stats_preserved.total_rows
        assert stats_filtered.valid_rows == stats_preserved.valid_rows
        assert stats_filtered.invalid_rows == stats_preserved.invalid_rows
        assert stats_filtered.validation_rate == stats_preserved.validation_rate

        # Data in common columns should be identical
        common_columns = set(rules.keys())
        filtered_common = valid_df_filtered.select(*common_columns)
        preserved_common = valid_df_preserved.select(*common_columns)

        # Compare the data
        filtered_data = sorted(filtered_common.collect(), key=lambda x: x[0])
        preserved_data = sorted(preserved_common.collect(), key=lambda x: x[0])

        assert len(filtered_data) == len(preserved_data)
        for f_row, p_row in zip(filtered_data, preserved_data):
            assert f_row == p_row

    def test_filter_columns_logging(self, sample_dataframe_with_extra_columns, caplog):
        """Test that appropriate logging messages are generated."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0],
        }

        # Test with filtering enabled
        with caplog.at_level("DEBUG"):
            apply_column_rules(
                df=sample_dataframe_with_extra_columns,
                rules=rules,
                stage="bronze",
                step="test_step",
                filter_columns_by_rules=True,
            )

        # Check for filtering log message
        assert any(
            "Filtering columns based on rules keys" in record.message
            for record in caplog.records
        )

        # Test with filtering disabled
        caplog.clear()
        with caplog.at_level("DEBUG"):
            apply_column_rules(
                df=sample_dataframe_with_extra_columns,
                rules=rules,
                stage="bronze",
                step="test_step",
                filter_columns_by_rules=False,
            )

        # Check for preservation log message
        assert any(
            "Preserving all columns" in record.message for record in caplog.records
        )
