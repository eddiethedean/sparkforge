#!/usr/bin/env python3
"""
Tests for auto-inference of source_bronze in add_silver_transform.

This module tests the new feature that allows add_silver_transform to
automatically infer the source_bronze from the most recent with_bronze_rules call.
"""

import unittest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from sparkforge import PipelineBuilder
from sparkforge.errors import StepError


class TestAutoInferSourceBronze(unittest.TestCase):
    """Test auto-inference of source_bronze parameter."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        self.builder = PipelineBuilder(spark=self.spark, schema="test_schema")

    def tearDown(self):
        """Clean up after tests."""
        if self.spark:
            self.spark.stop()

    def test_auto_infer_single_bronze_step(self):
        """Test auto-inference with a single bronze step."""
        # Add bronze step
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )

        # Add silver step without source_bronze - should auto-infer
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        result = self.builder.add_silver_transform(
            name="clean_events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events"
        )

        # Should return self for chaining
        self.assertIs(result, self.builder)

        # Check that silver step was added with correct source_bronze
        self.assertIn("clean_events", self.builder.silver_steps)
        silver_step = self.builder.silver_steps["clean_events"]
        self.assertEqual(silver_step.source_bronze, "events")

    def test_auto_infer_multiple_bronze_steps(self):
        """Test auto-inference uses the most recent bronze step."""
        # Add multiple bronze steps
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )
        self.builder.with_bronze_rules(
            name="transactions",
            rules={"transaction_id": [F.col("transaction_id").isNotNull()]}
        )

        # Add silver step without source_bronze - should use most recent
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("transaction_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_transactions",
            transform=silver_transform,
            rules={"transaction_id": [F.col("transaction_id").isNotNull()]},
            table_name="clean_transactions"
        )

        # Should use the most recent bronze step
        silver_step = self.builder.silver_steps["clean_transactions"]
        self.assertEqual(silver_step.source_bronze, "transactions")

    def test_explicit_source_bronze_still_works(self):
        """Test that explicit source_bronze still works."""
        # Add multiple bronze steps
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )
        self.builder.with_bronze_rules(
            name="transactions",
            rules={"transaction_id": [F.col("transaction_id").isNotNull()]}
        )

        # Add silver step with explicit source_bronze
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",  # Explicit
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events"
        )

        # Should use the explicit source_bronze
        silver_step = self.builder.silver_steps["clean_events"]
        self.assertEqual(silver_step.source_bronze, "events")

    def test_no_bronze_steps_raises_error(self):
        """Test that error is raised when no bronze steps exist."""
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        with self.assertRaises(StepError) as context:
            self.builder.add_silver_transform(
                name="clean_events",
                transform=silver_transform,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name="clean_events"
            )

        error = context.exception
        self.assertIn("No bronze steps available for auto-inference", str(error))
        self.assertEqual(error.step_name, "clean_events")
        self.assertEqual(error.step_type, "silver")

    def test_invalid_source_bronze_raises_error(self):
        """Test that error is raised when source_bronze doesn't exist."""
        # Add bronze step
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )

        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        with self.assertRaises(StepError) as context:
            self.builder.add_silver_transform(
                name="clean_events",
                source_bronze="nonexistent",  # Invalid
                transform=silver_transform,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name="clean_events"
            )

        error = context.exception
        self.assertIn("Bronze step 'nonexistent' not found", str(error))
        self.assertEqual(error.step_name, "clean_events")
        self.assertEqual(error.step_type, "silver")

    def test_logging_auto_inference(self):
        """Test that auto-inference is logged."""
        # Add bronze step
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )

        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        # Mock the logger to capture log messages
        with patch.object(self.builder.logger, 'info') as mock_info:
            self.builder.add_silver_transform(
                name="clean_events",
                transform=silver_transform,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name="clean_events"
            )

            # Check that auto-inference was logged
            mock_info.assert_any_call("🔍 Auto-inferred source_bronze: events")

    def test_chaining_works_with_auto_inference(self):
        """Test that method chaining works with auto-inference."""
        # Add bronze step
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )

        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        # Test chaining
        result = (self.builder
                 .add_silver_transform(
                     name="clean_events",
                     transform=silver_transform,
                     rules={"user_id": [F.col("user_id").isNotNull()]},
                     table_name="clean_events"
                 )
                 .add_silver_transform(
                     name="enriched_events",
                     transform=silver_transform,
                     rules={"user_id": [F.col("user_id").isNotNull()]},
                     table_name="enriched_events"
                 ))

        # Should return self for chaining
        self.assertIs(result, self.builder)

        # Both silver steps should exist
        self.assertIn("clean_events", self.builder.silver_steps)
        self.assertIn("enriched_events", self.builder.silver_steps)

        # Both should use the same source_bronze
        self.assertEqual(self.builder.silver_steps["clean_events"].source_bronze, "events")
        self.assertEqual(self.builder.silver_steps["enriched_events"].source_bronze, "events")


if __name__ == "__main__":
    unittest.main()
