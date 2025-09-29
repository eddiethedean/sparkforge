#!/usr/bin/env python3
"""
Test Improved User Experience Features

Tests for the new user experience improvements:
- Auto-inference of source_bronze for silver transforms
- Auto-inference of source_silvers for gold transforms
- Preset configurations
- Validation helper methods
- Timestamp detection
"""


import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkforge import PipelineBuilder
from sparkforge.errors import StepError


class TestImprovedUserExperience:
    """Test improved user experience features."""

    @pytest.fixture(autouse=True)
    def setup_test(self, spark_session):
        """Set up for each test."""
        self.builder = PipelineBuilder(spark=spark_session, schema="test_schema")
        self.mock_silver_transform = (
            lambda spark, bronze_df, prior_silvers: bronze_df.withColumn(
                "new_col", F.lit(1)
            )
        )
        self.mock_gold_transform = lambda spark, silvers: list(silvers.values())[
            0
        ].withColumn("gold_col", F.lit(1))
        self.rules = {"id": [F.col("id").isNotNull()]}
        self.spark = spark_session

    def test_auto_infer_gold_source_silvers(self):
        """Test auto-inference of source_silvers for gold transforms."""
        # Add bronze and silver steps
        self.builder.with_bronze_rules(name="bronze_events", rules=self.rules)
        self.builder.add_silver_transform(
            name="silver_events_1",
            transform=self.mock_silver_transform,
            rules=self.rules,
            table_name="silver_events_1",
        )
        self.builder.add_silver_transform(
            name="silver_events_2",
            transform=self.mock_silver_transform,
            rules=self.rules,
            table_name="silver_events_2",
        )

        # Add gold step without source_silvers - should auto-infer
        self.builder.add_gold_transform(
            name="gold_analytics",
            transform=self.mock_gold_transform,
            rules=self.rules,
            table_name="gold_analytics",
        )

        gold_step = self.builder.gold_steps["gold_analytics"]
        assert set(gold_step.source_silvers) == {"silver_events_1", "silver_events_2"}

    def test_auto_infer_gold_source_silvers_explicit(self):
        """Test that explicit source_silvers still works."""
        # Add bronze and silver steps
        self.builder.with_bronze_rules(name="bronze_events", rules=self.rules)
        self.builder.add_silver_transform(
            name="silver_events_1",
            transform=self.mock_silver_transform,
            rules=self.rules,
            table_name="silver_events_1",
        )
        self.builder.add_silver_transform(
            name="silver_events_2",
            transform=self.mock_silver_transform,
            rules=self.rules,
            table_name="silver_events_2",
        )

        # Add gold step with explicit source_silvers
        self.builder.add_gold_transform(
            name="gold_analytics",
            transform=self.mock_gold_transform,
            rules=self.rules,
            table_name="gold_analytics",
            source_silvers=["silver_events_1"],  # Explicit
        )

        gold_step = self.builder.gold_steps["gold_analytics"]
        assert gold_step.source_silvers == ["silver_events_1"]

    def test_auto_infer_gold_no_silver_steps_error(self):
        """Test error when no silver steps exist for auto-inference."""
        # Add only bronze step
        self.builder.with_bronze_rules(name="bronze_events", rules=self.rules)

        # Try to add gold step without silver steps
        with pytest.raises(
            StepError, match="No silver steps available for auto-inference"
        ):
            self.builder.add_gold_transform(
                name="gold_analytics",
                transform=self.mock_gold_transform,
                rules=self.rules,
                table_name="gold_analytics",
            )

    def test_preset_configurations_development(self):
        """Test development preset configuration."""
        builder = PipelineBuilder.for_development(spark=self.spark, schema="dev_schema")

        assert builder.config.thresholds.bronze == 80.0
        assert builder.config.thresholds.silver == 85.0
        assert builder.config.thresholds.gold == 90.0
        assert builder.config.verbose
        assert builder.config.parallel.enabled
        assert builder.config.parallel.max_workers == 2

    def test_preset_configurations_production(self):
        """Test production preset configuration."""
        builder = PipelineBuilder.for_production(spark=self.spark, schema="prod_schema")

        assert builder.config.thresholds.bronze == 95.0
        assert builder.config.thresholds.silver == 98.0
        assert builder.config.thresholds.gold == 99.0
        assert not builder.config.verbose
        assert builder.config.parallel.enabled
        assert builder.config.parallel.max_workers == 8

    def test_preset_configurations_testing(self):
        """Test testing preset configuration."""
        builder = PipelineBuilder.for_testing(spark=self.spark, schema="test_schema")

        assert builder.config.thresholds.bronze == 70.0
        assert builder.config.thresholds.silver == 75.0
        assert builder.config.thresholds.gold == 80.0
        assert builder.config.verbose
        assert not builder.config.parallel.enabled
        assert builder.config.parallel.max_workers == 1

    def test_validation_helper_not_null_rules(self):
        """Test not_null_rules helper method."""
        rules = PipelineBuilder.not_null_rules(["user_id", "timestamp", "value"])

        expected = {
            "user_id": [F.col("user_id").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
            "value": [F.col("value").isNotNull()],
        }

        assert len(rules) == 3
        for col, rule_list in rules.items():
            assert col in expected
            assert len(rule_list) == 1
            # Check that the rule is a Column expression (PySpark Column objects)
            assert hasattr(rule_list[0], "isNotNull")

    def test_validation_helper_positive_number_rules(self):
        """Test positive_number_rules helper method."""
        rules = PipelineBuilder.positive_number_rules(["value", "count"])

        assert len(rules) == 2
        for col in ["value", "count"]:
            assert col in rules
            assert len(rules[col]) == 2  # isNotNull + > 0

    def test_validation_helper_string_not_empty_rules(self):
        """Test string_not_empty_rules helper method."""
        rules = PipelineBuilder.string_not_empty_rules(["name", "category"])

        assert len(rules) == 2
        for col in ["name", "category"]:
            assert col in rules
            assert len(rules[col]) == 2  # isNotNull + length > 0

    def test_validation_helper_timestamp_rules(self):
        """Test timestamp_rules helper method."""
        rules = PipelineBuilder.timestamp_rules(["created_at", "updated_at"])

        assert len(rules) == 2
        for col in ["created_at", "updated_at"]:
            assert col in rules
            assert len(rules[col]) == 2  # isNotNull + isNotNull

    def test_detect_timestamp_columns(self):
        """Test timestamp column detection."""
        # Test with DataFrame schema
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        timestamp_cols = PipelineBuilder.detect_timestamp_columns(schema)
        assert "timestamp" in timestamp_cols
        assert "created_at" in timestamp_cols
        assert "user_id" not in timestamp_cols
        assert "value" not in timestamp_cols

    def test_detect_timestamp_columns_list(self):
        """Test timestamp column detection with column list."""
        columns = ["user_id", "timestamp", "event_time", "value", "updated_at"]

        timestamp_cols = PipelineBuilder.detect_timestamp_columns(columns)
        assert "timestamp" in timestamp_cols
        assert "event_time" in timestamp_cols
        assert "updated_at" in timestamp_cols
        assert "user_id" not in timestamp_cols
        assert "value" not in timestamp_cols

    def test_chaining_with_auto_inference(self):
        """Test method chaining works with auto-inference."""
        builder = (
            PipelineBuilder.for_development(spark=self.spark, schema="chained_schema")
            .with_bronze_rules(name="bronze_chain", rules=self.rules)
            .add_silver_transform(
                name="silver_chain_1",
                transform=self.mock_silver_transform,
                rules=self.rules,
                table_name="silver_chain_1",
            )
            .add_silver_transform(
                name="silver_chain_2",
                transform=self.mock_silver_transform,
                rules=self.rules,
                table_name="silver_chain_2",
            )
            .add_gold_transform(
                name="gold_chain",
                transform=self.mock_gold_transform,
                rules=self.rules,
                table_name="gold_chain",
            )
        )

        # Verify all steps were added
        assert len(builder.bronze_steps) == 1
        assert len(builder.silver_steps) == 2
        assert len(builder.gold_steps) == 1

        # Verify auto-inference worked
        gold_step = builder.gold_steps["gold_chain"]
        assert set(gold_step.source_silvers) == {"silver_chain_1", "silver_chain_2"}


if __name__ == "__main__":
    import unittest

    unittest.main()
