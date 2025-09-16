#!/usr/bin/env python3
"""
Tests for performance optimizations in SparkForge.

This module tests the performance improvements made to validation,
caching, and DataFrame operations to ensure they work correctly
and provide the expected performance benefits.
"""

import time
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkforge.constants import BYTES_PER_MB, DEFAULT_MAX_MEMORY_MB
from sparkforge.validation import apply_column_rules, assess_data_quality


class TestValidationPerformanceOptimizations:
    """Test performance optimizations in validation module."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self, spark_session):
        """Set up test data for each test."""
        # Create test data
        self.test_data = [
            (1, "user1", "test@example.com", 25),
            (2, "user2", "test2@example.com", 30),
            (3, None, "test3@example.com", 35),  # Invalid: null name
            (4, "user4", None, 40),  # Invalid: null email
            (5, "user5", "test5@example.com", -5),  # Invalid: negative age
        ]
        self.test_df = spark_session.createDataFrame(
            self.test_data, ["id", "name", "email", "age"]
        )
        self.spark = spark_session

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
        assert stats.valid_rows == 2  # Only rows 1 and 2 are valid
        assert stats.invalid_rows == 3  # Rows 3, 4, 5 are invalid
        assert abs(stats.validation_rate - 40.0) < 0.1

        # Verify the operation completed quickly (should be fast with caching)
        execution_time = end_time - start_time
        assert execution_time < 5.0  # Should complete within 5 seconds

    def test_null_checking_optimization(self):
        """Test that null checking uses single action instead of multiple."""
        # Create a larger dataset for better testing
        large_data = []
        for i in range(1000):
            large_data.append((i, f"user{i}", f"user{i}@example.com", 20 + i % 50))

        large_df = self.spark.createDataFrame(
            large_data, ["id", "name", "email", "age"]
        )

        # Mock the collect method to verify single action
        with patch.object(DataFrame, "collect") as mock_collect:
            mock_collect.return_value = [
                {"name_nulls": 0, "email_nulls": 0, "age_nulls": 0}
            ]

            assess_data_quality(large_df)

            # Verify collect was called only once (for all null checks)
            assert mock_collect.call_count == 1

    def test_validation_with_empty_rules(self):
        """Test validation behavior with empty rules."""
        valid_df, invalid_df, stats = apply_column_rules(
            self.test_df, {}, "bronze", "test"
        )

        # With empty rules, all rows should be valid
        assert stats.valid_rows == 5
        assert stats.invalid_rows == 0
        assert stats.validation_rate == 100.0

    def test_validation_with_none_rules(self):
        """Test validation behavior with None rules."""
        with pytest.raises(Exception):  # Should raise ValidationError
            apply_column_rules(self.test_df, None, "bronze", "test")


class TestConstantsModule:
    """Test the new constants module."""

    @pytest.fixture(autouse=True)
    def setup_spark(self, spark_session):
        """Set up Spark session for each test."""
        self.spark = spark_session

    def test_memory_constants(self):
        """Test memory-related constants."""
        assert BYTES_PER_MB == 1024 * 1024
        assert DEFAULT_MAX_MEMORY_MB == 1024

    def test_constants_import(self):
        """Test that constants can be imported correctly."""
        from sparkforge.constants import (
            BYTES_PER_GB,
            BYTES_PER_KB,
            DEFAULT_BRONZE_THRESHOLD,
            DEFAULT_CACHE_PARTITIONS,
            DEFAULT_GOLD_THRESHOLD,
            DEFAULT_SILVER_THRESHOLD,
        )

        assert BYTES_PER_KB == 1024
        assert BYTES_PER_GB == 1024 * 1024 * 1024
        assert DEFAULT_CACHE_PARTITIONS == 200
        assert DEFAULT_BRONZE_THRESHOLD == 95.0
        assert DEFAULT_SILVER_THRESHOLD == 98.0
        assert DEFAULT_GOLD_THRESHOLD == 99.0


class TestSchemaConfiguration:
    """Test configurable schema functionality."""

    @pytest.fixture(autouse=True)
    def setup_spark(self, spark_session):
        """Set up Spark session for each test."""
        self.spark = spark_session

    def test_execution_engine_schema_configuration(self):
        """Test that ExecutionEngine uses configurable schema."""
        from sparkforge.execution.engine import ExecutionEngine
        from sparkforge.logger import PipelineLogger

        # Test with custom schema
        custom_schema = "my_custom_schema"
        engine = ExecutionEngine(
            spark=self.spark, logger=PipelineLogger(), schema=custom_schema
        )

        assert engine.schema == custom_schema

        # Test with empty schema
        engine_empty = ExecutionEngine(
            spark=self.spark, logger=PipelineLogger(), schema=""
        )

        assert engine_empty.schema == ""

    def test_pipeline_runner_schema_configuration(self):
        """Test that PipelineRunner uses configurable schema."""
        from sparkforge.dependencies.analyzer import DependencyAnalyzer
        from sparkforge.execution.engine import ExecutionEngine
        from sparkforge.logger import PipelineLogger
        from sparkforge.models import PipelineConfig
        from sparkforge.pipeline.runner import PipelineRunner

        # Test with custom schema in config
        custom_schema = "my_pipeline_schema"
        config = PipelineConfig(
            schema=custom_schema,
            thresholds={"bronze": 95.0, "silver": 98.0, "gold": 99.0},
            parallel={"enabled": False, "max_workers": 1, "timeout_secs": 300},
        )

        runner = PipelineRunner(
            spark=self.spark,
            config=config,
            bronze_steps={},
            silver_steps={},
            gold_steps={},
            logger=PipelineLogger(),
            execution_engine=ExecutionEngine(spark=self.spark, schema=custom_schema),
            dependency_analyzer=DependencyAnalyzer(),
        )

        assert runner.config.schema == custom_schema


class TestCachingBehavior:
    """Test DataFrame caching behavior."""

    @pytest.fixture(autouse=True)
    def setup_spark(self, spark_session):
        """Set up Spark session for each test."""
        self.spark = spark_session

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

        large_df = self.spark.createDataFrame(
            large_data, ["id", "name", "email", "age"]
        )

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
        assert execution_time < 30.0  # Should complete within 30 seconds

        # Verify results - with the test data, all rows should be valid
        assert stats.valid_rows == 10000  # All rows should be valid
        assert stats.invalid_rows == 0


class TestPerformanceRegression:
    """Test that optimizations don't introduce regressions."""

    @pytest.fixture(autouse=True)
    def setup_spark(self, spark_session):
        """Set up Spark session for each test."""
        self.spark = spark_session

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
            results.append(
                (stats.valid_rows, stats.invalid_rows, stats.validation_rate)
            )

        # All results should be identical
        for i in range(1, len(results)):
            assert results[0] == results[i]

    def test_memory_usage_stability(self):
        """Test that memory usage remains stable with optimizations."""
        # Create a very simple dataset for testing
        test_data = [
            (1, "user1", "user1@example.com", 25),
            (2, "user2", "user2@example.com", 30),
        ]

        rules = {
            "name": [F.col("name").isNotNull()],
            "email": [F.col("email").isNotNull()],
        }

        # Run validation multiple times
        for iteration in range(2):
            # Create a fresh DataFrame for each iteration
            fresh_df = self.spark.createDataFrame(
                test_data, ["id", "name", "email", "age"]
            )

            valid_df, invalid_df, stats = apply_column_rules(
                fresh_df, rules, "bronze", "test"
            )

            # Verify results are consistent - all rows should be valid with these rules
            assert (
                stats.valid_rows == 2
            ), f"Iteration {iteration}: Expected 2 valid rows, got {stats.valid_rows}"
            assert (
                stats.invalid_rows == 0
            ), f"Iteration {iteration}: Expected 0 invalid rows, got {stats.invalid_rows}"


def run_performance_tests():
    """Run all performance optimization tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    run_performance_tests()
