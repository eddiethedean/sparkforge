#!/usr/bin/env python3
"""
Performance tests for SparkForge components.

This module contains performance tests for key SparkForge functions
including validation, model creation, and serialization operations.
"""

from unittest.mock import Mock

import pytest

from sparkforge.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from sparkforge.validation import (
    assess_data_quality,
    get_dataframe_info,
    safe_divide,
    validate_dataframe_schema,
)

from .performance_monitor import performance_monitor


class TestValidationPerformance:
    """Performance tests for validation functions."""

    @pytest.mark.performance
    @pytest.mark.benchmark
    def test_safe_divide_performance(
        self, performance_monitor_clean, benchmark_iterations, performance_tolerance
    ) -> None:
        """Test performance of safe_divide function."""
        iterations = 10000

        result = performance_monitor.benchmark_function(
            safe_divide,
            "safe_divide",
            iterations=iterations,
            args=(100.0, 5.0),
            warmup_iterations=100,
        )

        # Performance assertions
        assert result.success
        assert result.execution_time < 1.0  # Should complete within 1 second
        assert result.avg_time_per_iteration < 0.1  # Each call should be < 0.1ms
        assert result.throughput > 1000  # Should handle > 1000 calls/second

        # Check for regression
        regression = performance_monitor_clean.check_regression("safe_divide")
        if regression["status"] == "regression_detected":
            pytest.fail(f"Performance regression detected: {regression}")

    @pytest.mark.performance
    def test_safe_divide_zero_denominator_performance(self) -> None:
        """Test performance of safe_divide with zero denominator."""
        iterations = 5000

        result = performance_monitor.benchmark_function(
            safe_divide,
            "safe_divide_zero",
            iterations=iterations,
            args=(100.0, 0.0),
            warmup_iterations=50,
        )

        assert result.success
        assert result.execution_time < 0.5
        assert result.avg_time_per_iteration < 0.1

    def test_validate_dataframe_schema_performance(self) -> None:
        """Test performance of validate_dataframe_schema function."""
        iterations = 1000

        # Create mock DataFrame with many columns
        mock_df = Mock()
        mock_df.columns = [f"col{i}" for i in range(100)]

        expected_columns = [f"col{i}" for i in range(50)]

        def test_function():
            return validate_dataframe_schema(mock_df, expected_columns)

        result = performance_monitor.benchmark_function(
            test_function,
            "validate_dataframe_schema",
            iterations=iterations,
            warmup_iterations=10,
        )

        assert result.success
        assert result.execution_time < 0.5
        assert result.avg_time_per_iteration < 1.0

    def test_assess_data_quality_performance(self) -> None:
        """Test performance of assess_data_quality function."""
        iterations = 100

        # Create mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000

        rules = {f"col{i}": [f"col{i} > 0", f"col{i} IS NOT NULL"] for i in range(10)}

        def test_function():
            return assess_data_quality(mock_df, rules)

        result = performance_monitor.benchmark_function(
            test_function,
            "assess_data_quality",
            iterations=iterations,
            warmup_iterations=5,
        )

        assert result.success
        assert result.execution_time < 2.0
        assert result.avg_time_per_iteration < 20.0

    def test_get_dataframe_info_performance(self) -> None:
        """Test performance of get_dataframe_info function."""
        iterations = 500

        # Create mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 10000
        mock_df.columns = [f"col{i}" for i in range(50)]

        # Create mock schema
        mock_schema = Mock()
        mock_schema.__str__ = Mock(return_value="struct<col1:string,col2:string>")
        mock_df.schema = mock_schema

        def test_function():
            return get_dataframe_info(mock_df)

        result = performance_monitor.benchmark_function(
            test_function,
            "get_dataframe_info",
            iterations=iterations,
            warmup_iterations=5,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 2.0


class TestModelCreationPerformance:
    """Performance tests for model creation operations."""

    @pytest.mark.performance
    @pytest.mark.benchmark
    def test_validation_thresholds_creation_performance(self) -> None:
        """Test performance of ValidationThresholds creation."""
        iterations = 10000

        def create_thresholds():
            return ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)

        result = performance_monitor.benchmark_function(
            create_thresholds,
            "validation_thresholds_creation",
            iterations=iterations,
            warmup_iterations=100,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 0.1
        assert result.throughput > 5000

    def test_parallel_config_creation_performance(self) -> None:
        """Test performance of ParallelConfig creation."""
        iterations = 10000

        def create_parallel_config():
            return ParallelConfig(enabled=True, max_workers=4)

        result = performance_monitor.benchmark_function(
            create_parallel_config,
            "parallel_config_creation",
            iterations=iterations,
            warmup_iterations=100,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 0.1

    def test_pipeline_config_creation_performance(self) -> None:
        """Test performance of PipelineConfig creation."""
        iterations = 1000

        def create_pipeline_config():
            return PipelineConfig.create_default("test_schema")

        result = performance_monitor.benchmark_function(
            create_pipeline_config,
            "pipeline_config_creation",
            iterations=iterations,
            warmup_iterations=10,
        )

        assert result.success
        assert result.execution_time < 0.5
        assert result.avg_time_per_iteration < 0.5

    def test_bronze_step_creation_performance(self) -> None:
        """Test performance of BronzeStep creation."""
        iterations = 5000

        rules = {f"col{i}": [f"col{i} > 0", f"col{i} IS NOT NULL"] for i in range(5)}

        def create_bronze_step():
            return BronzeStep(
                name="test_step", rules=rules, incremental_col="updated_at"
            )

        result = performance_monitor.benchmark_function(
            create_bronze_step,
            "bronze_step_creation",
            iterations=iterations,
            warmup_iterations=50,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 0.2

    def test_silver_step_creation_performance(self) -> None:
        """Test performance of SilverStep creation."""
        iterations = 2000

        def mock_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        rules = {f"col{i}": [f"col{i} > 0"] for i in range(3)}

        def create_silver_step():
            return SilverStep(
                name="test_step",
                source_bronze="bronze1",
                transform=mock_transform,
                rules=rules,
                table_name="silver_table",
            )

        result = performance_monitor.benchmark_function(
            create_silver_step,
            "silver_step_creation",
            iterations=iterations,
            warmup_iterations=20,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 0.5

    def test_gold_step_creation_performance(self) -> None:
        """Test performance of GoldStep creation."""
        iterations = 2000

        def mock_transform(spark, silver_dfs):
            return silver_dfs

        rules = {"col1": ["col1 > 0"]}
        source_silvers = ["silver1", "silver2"]

        def create_gold_step():
            return GoldStep(
                name="test_step",
                table_name="gold_table",
                transform=mock_transform,
                rules=rules,
                source_silvers=source_silvers,
            )

        result = performance_monitor.benchmark_function(
            create_gold_step,
            "gold_step_creation",
            iterations=iterations,
            warmup_iterations=20,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 0.5


class TestSerializationPerformance:
    """Performance tests for serialization operations."""

    def test_model_to_dict_performance(self) -> None:
        """Test performance of model.to_dict() serialization."""
        iterations = 1000

        # Create a complex model
        config = PipelineConfig.create_default("test_schema")

        def serialize_model():
            return config.to_dict()

        result = performance_monitor.benchmark_function(
            serialize_model,
            "model_to_dict",
            iterations=iterations,
            warmup_iterations=10,
        )

        assert result.success
        assert result.execution_time < 0.5
        assert result.avg_time_per_iteration < 0.5

    def test_model_to_json_performance(self) -> None:
        """Test performance of model.to_json() serialization."""
        iterations = 1000

        # Create a complex model
        config = PipelineConfig.create_default("test_schema")

        def serialize_model():
            return config.to_json()

        result = performance_monitor.benchmark_function(
            serialize_model,
            "model_to_json",
            iterations=iterations,
            warmup_iterations=10,
        )

        assert result.success
        assert result.execution_time < 1.0
        assert result.avg_time_per_iteration < 1.0

    def test_model_validation_performance(self) -> None:
        """Test performance of model validation."""
        iterations = 500

        # Create a model
        config = PipelineConfig.create_default("test_schema")

        def validate_model():
            config.validate()

        result = performance_monitor.benchmark_function(
            validate_model,
            "model_validation",
            iterations=iterations,
            warmup_iterations=5,
        )

        assert result.success
        assert result.execution_time < 0.5
        assert result.avg_time_per_iteration < 1.0


class TestMemoryUsagePerformance:
    """Performance tests focusing on memory usage."""

    @pytest.mark.performance
    @pytest.mark.memory
    def test_model_creation_memory_usage(self, memory_limit_mb) -> None:
        """Test memory usage of model creation."""
        iterations = 1000

        def create_models():
            models = []
            for i in range(10):
                models.append(PipelineConfig.create_default(f"schema_{i}"))
            return models

        result = performance_monitor.benchmark_function(
            create_models,
            "model_creation_memory",
            iterations=iterations,
            warmup_iterations=5,
        )

        assert result.success
        assert result.memory_usage_mb < memory_limit_mb  # Should use < limit
        assert result.peak_memory_mb < memory_limit_mb * 2  # Peak should be < 2x limit

    def test_serialization_memory_usage(self) -> None:
        """Test memory usage of serialization operations."""
        iterations = 500

        # Create a complex model
        config = PipelineConfig.create_default("test_schema")

        def serialize_and_deserialize():
            data = config.to_dict()
            json_str = config.to_json()
            return data, json_str

        result = performance_monitor.benchmark_function(
            serialize_and_deserialize,
            "serialization_memory",
            iterations=iterations,
            warmup_iterations=5,
        )

        assert result.success
        assert result.memory_usage_mb < 50  # Should use < 50MB
        assert result.peak_memory_mb < 100  # Peak should be < 100MB


def test_performance_summary() -> None:
    """Test that generates a performance summary."""
    summary = performance_monitor.get_performance_summary()

    assert "total_tests" in summary
    assert "successful_tests" in summary
    assert "functions_tested" in summary
    assert summary["total_tests"] > 0

    # Print summary for manual review
    print("\nPerformance Test Summary:")
    print(f"Total tests: {summary['total_tests']}")
    print(f"Successful tests: {summary['successful_tests']}")
    print(f"Functions tested: {summary['functions_tested']}")
    print(f"Total execution time: {summary['total_execution_time']:.2f}s")
    print(f"Average execution time: {summary['avg_execution_time']:.4f}s")


def test_update_baselines() -> None:
    """Test function to update performance baselines."""
    # Update baselines for all tested functions
    test_functions = [
        "safe_divide",
        "safe_divide_zero",
        "validate_dataframe_schema",
        "assess_data_quality",
        "get_dataframe_info",
        "validation_thresholds_creation",
        "parallel_config_creation",
        "pipeline_config_creation",
        "bronze_step_creation",
        "silver_step_creation",
        "gold_step_creation",
        "model_to_dict",
        "model_to_json",
        "model_validation",
        "model_creation_memory",
        "serialization_memory",
    ]

    for func_name in test_functions:
        performance_monitor.update_baseline(func_name)

    print("Performance baselines updated successfully")


# Convenience functions for external use
def run_validation_performance_tests():
    """Run all validation performance tests."""
    test_class = TestValidationPerformance()
    # Run tests that don't require fixtures
    test_class.test_safe_divide_zero_denominator_performance()
    test_class.test_validate_dataframe_schema_performance()
    test_class.test_assess_data_quality_performance()
    test_class.test_get_dataframe_info_performance()


def run_model_creation_performance_tests():
    """Run all model creation performance tests."""
    test_class = TestModelCreationPerformance()
    test_class.test_validation_thresholds_creation_performance()
    test_class.test_parallel_config_creation_performance()
    test_class.test_pipeline_config_creation_performance()
    test_class.test_bronze_step_creation_performance()
    test_class.test_silver_step_creation_performance()
    test_class.test_gold_step_creation_performance()


def run_serialization_performance_tests():
    """Run all serialization performance tests."""
    test_class = TestSerializationPerformance()
    test_class.test_model_to_dict_performance()
    test_class.test_model_to_json_performance()
    test_class.test_model_validation_performance()
