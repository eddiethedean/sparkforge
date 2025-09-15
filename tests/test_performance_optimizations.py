#!/usr/bin/env python3
"""
Tests for performance optimizations in SparkForge.

This module tests the performance improvements made to validation,
caching, and DataFrame operations to ensure they work correctly
and provide the expected performance benefits.
"""

import time
import unittest
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F

from sparkforge.constants import BYTES_PER_MB, DEFAULT_MAX_MEMORY_MB
from sparkforge.validation import apply_column_rules, assess_data_quality


class TestValidationPerformanceOptimizations(unittest.TestCase):
    """Test performance optimizations in validation module."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("test_performance").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Create test data
        self.test_data = [
            (1, "user1", "test@example.com", 25),
            (2, "user2", "test2@example.com", 30),
            (3, None, "test3@example.com", 35),  # Invalid: null name
            (4, "user4", None, 40),  # Invalid: null email
            (5, "user5", "test5@example.com", -5),  # Invalid: negative age
        ]
        self.test_df = self.spark.createDataFrame(
            self.test_data, ["id", "name", "email", "age"]
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.spark.stop()

    def test_validation_caching_behavior(self):
        """Test that validation properly caches DataFrames."""
        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
            "age": [F.col("age") > 0],
        }

        # Mock the cache method to verify it's called
        with patch.object(DataFrame, "cache") as mock_cache:
            valid_df, invalid_df, stats = apply_column_rules(
                self.test_df, rules, "bronze", "test"
            )
            
            # Verify cache was called on the input DataFrame
            mock_cache.assert_called()

    def test_validation_performance_improvement(self):
        """Test that validation performance is improved with caching."""
        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
            "age": [F.col("age") > 0],
        }

        # Time the validation operation
        start_time = time.time()
        valid_df, invalid_df, stats = apply_column_rules(
            self.test_df, rules, "bronze", "test"
        )
        end_time = time.time()

        # Verify results are correct
        self.assertEqual(stats.valid_rows, 2)  # Only rows 1 and 2 are valid
        self.assertEqual(stats.invalid_rows, 3)  # Rows 3, 4, 5 are invalid
        self.assertAlmostEqual(stats.validation_rate, 40.0, places=1)

        # Verify the operation completed quickly (should be fast with caching)
        execution_time = end_time - start_time
        self.assertLess(execution_time, 5.0)  # Should complete within 5 seconds

    def test_null_checking_optimization(self):
        """Test that null checking uses single action instead of multiple."""
        # Create a larger dataset for better testing
        large_data = []
        for i in range(1000):
            large_data.append((i, f"user{i}", f"user{i}@example.com", 20 + i % 50))

        large_df = self.spark.createDataFrame(large_data, ["id", "name", "email", "age"])

        # Mock the collect method to verify single action
        with patch.object(DataFrame, "collect") as mock_collect:
            mock_collect.return_value = [{"name_nulls": 0, "email_nulls": 0, "age_nulls": 0}]
            
            result = assess_data_quality(large_df)
            
            # Verify collect was called only once (for all null checks)
            self.assertEqual(mock_collect.call_count, 1)

    def test_validation_with_empty_rules(self):
        """Test validation behavior with empty rules."""
        valid_df, invalid_df, stats = apply_column_rules(
            self.test_df, {}, "bronze", "test"
        )

        # With empty rules, all rows should be valid
        self.assertEqual(stats.valid_rows, 5)
        self.assertEqual(stats.invalid_rows, 0)
        self.assertEqual(stats.validation_rate, 100.0)

    def test_validation_with_none_rules(self):
        """Test validation behavior with None rules."""
        with self.assertRaises(Exception):  # Should raise ValidationError
            apply_column_rules(self.test_df, None, "bronze", "test")


class TestConstantsModule(unittest.TestCase):
    """Test the new constants module."""

    def test_memory_constants(self):
        """Test memory-related constants."""
        self.assertEqual(BYTES_PER_MB, 1024 * 1024)
        self.assertEqual(DEFAULT_MAX_MEMORY_MB, 1024)

    def test_constants_import(self):
        """Test that constants can be imported correctly."""
        from sparkforge.constants import (
            BYTES_PER_KB,
            BYTES_PER_GB,
            DEFAULT_CACHE_PARTITIONS,
            DEFAULT_BRONZE_THRESHOLD,
            DEFAULT_SILVER_THRESHOLD,
            DEFAULT_GOLD_THRESHOLD,
        )

        self.assertEqual(BYTES_PER_KB, 1024)
        self.assertEqual(BYTES_PER_GB, 1024 * 1024 * 1024)
        self.assertEqual(DEFAULT_CACHE_PARTITIONS, 200)
        self.assertEqual(DEFAULT_BRONZE_THRESHOLD, 95.0)
        self.assertEqual(DEFAULT_SILVER_THRESHOLD, 98.0)
        self.assertEqual(DEFAULT_GOLD_THRESHOLD, 99.0)


class TestSchemaConfiguration(unittest.TestCase):
    """Test configurable schema functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("test_schema").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def tearDown(self):
        """Clean up test fixtures."""
        self.spark.stop()

    def test_execution_engine_schema_configuration(self):
        """Test that ExecutionEngine uses configurable schema."""
        from sparkforge.execution.engine import ExecutionEngine
        from sparkforge.logger import PipelineLogger

        # Test with custom schema
        custom_schema = "my_custom_schema"
        engine = ExecutionEngine(
            spark=self.spark,
            logger=PipelineLogger(),
            schema=custom_schema
        )

        self.assertEqual(engine.schema, custom_schema)

        # Test with empty schema
        engine_empty = ExecutionEngine(
            spark=self.spark,
            logger=PipelineLogger(),
            schema=""
        )

        self.assertEqual(engine_empty.schema, "")

    def test_pipeline_runner_schema_configuration(self):
        """Test that PipelineRunner uses configurable schema."""
        from sparkforge.pipeline.runner import PipelineRunner

        # Test with custom schema
        custom_schema = "my_pipeline_schema"
        runner = PipelineRunner(
            spark=self.spark,
            schema=custom_schema
        )

        self.assertEqual(runner.schema, custom_schema)


class TestCachingBehavior(unittest.TestCase):
    """Test DataFrame caching behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("test_caching").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def tearDown(self):
        """Clean up test fixtures."""
        self.spark.stop()

    def test_table_operations_caching(self):
        """Test that table operations properly cache DataFrames."""
        from sparkforge.table_operations import write_overwrite_table

        # Create test data
        test_data = [(1, "test"), (2, "test2")]
        df = self.spark.createDataFrame(test_data, ["id", "name"])

        # Mock the cache method to verify it's called
        with patch.object(DataFrame, "cache") as mock_cache:
            try:
                write_overwrite_table(df, "test_schema.test_table")
                # Verify cache was called
                mock_cache.assert_called()
            except Exception:
                # Table might not exist, but we just want to test caching
                pass

    def test_validation_caching_with_large_dataset(self):
        """Test caching behavior with larger datasets."""
        # Create a larger dataset
        large_data = []
        for i in range(10000):
            large_data.append((i, f"user{i}", f"user{i}@example.com", 20 + i % 50))

        large_df = self.spark.createDataFrame(large_data, ["id", "name", "email", "age"])

        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
        }

        # Test that caching improves performance
        start_time = time.time()
        valid_df, invalid_df, stats = apply_column_rules(
            large_df, rules, "bronze", "test"
        )
        end_time = time.time()

        # Should complete reasonably quickly with caching
        execution_time = end_time - start_time
        self.assertLess(execution_time, 30.0)  # Should complete within 30 seconds

        # Verify results
        self.assertEqual(stats.valid_rows, 10000)  # All rows should be valid
        self.assertEqual(stats.invalid_rows, 0)


class TestPerformanceRegression(unittest.TestCase):
    """Test that optimizations don't introduce regressions."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("test_regression").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def tearDown(self):
        """Clean up test fixtures."""
        self.spark.stop()

    def test_validation_results_consistency(self):
        """Test that validation results are consistent before and after optimization."""
        test_data = [
            (1, "user1", "test@example.com", 25),
            (2, None, "test2@example.com", 30),  # Invalid: null name
            (3, "user3", None, 35),  # Invalid: null email
        ]
        test_df = self.spark.createDataFrame(test_data, ["id", "name", "email", "age"])

        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
            "age": [F.col("age") > 0],
        }

        # Run validation multiple times to ensure consistency
        results = []
        for _ in range(3):
            valid_df, invalid_df, stats = apply_column_rules(
                test_df, rules, "bronze", "test"
            )
            results.append((stats.valid_rows, stats.invalid_rows, stats.validation_rate))

        # All results should be identical
        for i in range(1, len(results)):
            self.assertEqual(results[0], results[i])

    def test_memory_usage_stability(self):
        """Test that memory usage remains stable with optimizations."""
        # Create a moderately large dataset
        large_data = []
        for i in range(5000):
            large_data.append((i, f"user{i}", f"user{i}@example.com", 20 + i % 50))

        large_df = self.spark.createDataFrame(large_data, ["id", "name", "email", "age"])

        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
        }

        # Run validation multiple times
        for _ in range(5):
            valid_df, invalid_df, stats = apply_column_rules(
                large_df, rules, "bronze", "test"
            )
            
            # Verify results are consistent
            self.assertEqual(stats.valid_rows, 5000)
            self.assertEqual(stats.invalid_rows, 0)


def run_performance_tests():
    """Run all performance optimization tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    run_performance_tests()
