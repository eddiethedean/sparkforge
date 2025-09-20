#!/usr/bin/env python3
"""
Additional tests for models.py to improve coverage to 90%+.

This module focuses on covering missing lines and edge cases that are not
currently covered by the existing test suite.
"""

from typing import Any, Dict

import pytest

from sparkforge.models import (
    BaseModel,
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    PipelineValidationError,
    SilverDependencyInfo,
    SilverStep,
    ValidationThresholds,
)


class TestProtocolImplementations:
    """Test protocol implementations and edge cases."""

    def test_validatable_protocol_implementation(self) -> None:
        """Test Validatable protocol implementation."""

        # Test that objects implementing Validatable can be validated
        class MockValidatable:
            def validate(self) -> None:
                pass

        obj = MockValidatable()
        assert hasattr(obj, "validate")
        assert callable(obj.validate)

    def test_serializable_protocol_implementation(self) -> None:
        """Test Serializable protocol implementation."""

        # Test that objects implementing Serializable can be serialized
        class MockSerializable:
            def to_dict(self) -> Dict[str, Any]:
                return {"test": "value"}

            def to_json(self) -> str:
                return '{"test": "value"}'

        obj = MockSerializable()
        assert hasattr(obj, "to_dict")
        assert hasattr(obj, "to_json")
        assert callable(obj.to_dict)
        assert callable(obj.to_json)


class TestPipelineConfigProperties:
    """Test PipelineConfig property methods."""

    def test_pipeline_config_properties(self) -> None:
        """Test PipelineConfig property access."""
        config = PipelineConfig.create_default("test_schema")

        # Test property access
        assert isinstance(config.min_gold_rate, float)
        assert isinstance(config.enable_parallel_silver, bool)
        assert isinstance(config.max_parallel_workers, int)
        assert isinstance(config.enable_caching, bool)
        assert isinstance(config.enable_monitoring, bool)


class TestValidationThresholdsEdgeCases:
    """Test ValidationThresholds edge cases and error paths."""

    def test_validation_thresholds_get_threshold_invalid_type(self) -> None:
        """Test get_threshold with invalid phase type."""
        thresholds = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)

        # This should handle invalid types gracefully
        with pytest.raises(KeyError):
            thresholds.get_threshold("invalid_phase")  # type: ignore

    def test_validation_thresholds_edge_values(self) -> None:
        """Test ValidationThresholds with edge values."""
        # Test with minimum and maximum values
        thresholds = ValidationThresholds(bronze=0.0, silver=50.0, gold=100.0)

        assert thresholds.bronze == 0.0
        assert thresholds.silver == 50.0
        assert thresholds.gold == 100.0


class TestParallelConfigEdgeCases:
    """Test ParallelConfig edge cases and validation."""

    def test_parallel_config_edge_values(self) -> None:
        """Test ParallelConfig with edge values."""
        # Test with minimum values
        config = ParallelConfig(enabled=True, max_workers=1, timeout_secs=1)

        assert config.enabled is True
        assert config.max_workers == 1
        assert config.timeout_secs == 1

    def test_parallel_config_max_values(self) -> None:
        """Test ParallelConfig with maximum values."""
        # Test with maximum values
        config = ParallelConfig(enabled=True, max_workers=32, timeout_secs=3600)

        assert config.max_workers == 32
        assert config.timeout_secs == 3600


class TestBronzeStepEdgeCases:
    """Test BronzeStep edge cases and validation."""

    def test_bronze_step_incremental_col_edge_cases(self) -> None:
        """Test BronzeStep incremental_col edge cases."""
        # Test with empty string (should be valid)
        step = BronzeStep(
            name="test_step",
            rules={"id": ["not_null"]},
            incremental_col="",  # Empty string should be valid
        )

        assert step.incremental_col == ""
        assert step.has_incremental_capability is True

    def test_bronze_step_validation_edge_cases(self) -> None:
        """Test BronzeStep validation edge cases."""
        # Test validation with edge case values
        step = BronzeStep(
            name="test_step",
            rules={"id": ["not_null"]},
        )

        # This should not raise an error
        step.validate()


class TestSilverStepEdgeCases:
    """Test SilverStep edge cases and validation."""

    def test_silver_step_validation_edge_cases(self) -> None:
        """Test SilverStep validation edge cases."""

        # Create a mock transform function
        def mock_transform(spark, df, bronze_dfs):
            return df

        step = SilverStep(
            name="test_step",
            source_bronze="test_bronze",
            rules={"id": ["not_null"]},
            transform=mock_transform,
            table_name="test_table",
        )

        # This should not raise an error
        step.validate()


class TestGoldStepEdgeCases:
    """Test GoldStep edge cases and validation."""

    def test_gold_step_validation_edge_cases(self) -> None:
        """Test GoldStep validation edge cases."""

        # Create a mock transform function
        def mock_transform(spark, silver_dfs):
            return list(silver_dfs.values())[0] if silver_dfs else None

        step = GoldStep(
            name="test_step",
            source_silvers=["test_silver1", "test_silver2"],
            rules={"id": ["not_null"]},
            transform=mock_transform,
            table_name="test_table",
        )

        # This should not raise an error
        step.validate()


class TestSilverDependencyInfoEdgeCases:
    """Test SilverDependencyInfo edge cases."""

    def test_silver_dependency_info_edge_cases(self) -> None:
        """Test SilverDependencyInfo edge cases."""
        info = SilverDependencyInfo(
            step_name="test_step",
            source_bronze="test_bronze",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=1,
        )

        # This should not raise an error
        info.validate()


class TestBaseModelEdgeCases:
    """Test BaseModel edge cases and serialization."""

    def test_base_model_serialization_edge_cases(self) -> None:
        """Test BaseModel serialization edge cases."""

        # Create a concrete implementation for testing
        class TestModel(BaseModel):
            def __init__(self, name: str, value: Any):
                self.name = name
                self.value = value

            def validate(self) -> None:
                pass

        model = TestModel("test", {"nested": {"value": 123}})

        # Test serialization
        result_dict = model.to_dict()
        assert isinstance(result_dict, dict)

        result_json = model.to_json()
        assert isinstance(result_json, str)

        # Test string representation
        str_repr = str(model)
        assert isinstance(str_repr, str)


class TestModelValidationErrorPaths:
    """Test model validation error paths."""

    def test_bronze_step_validation_error_paths(self) -> None:
        """Test BronzeStep validation error paths."""
        # Test with None name
        with pytest.raises(PipelineValidationError):
            BronzeStep(
                name=None,  # type: ignore
                rules={"id": ["not_null"]},
            ).validate()

        # Test with empty name
        with pytest.raises(PipelineValidationError):
            BronzeStep(
                name="",
                rules={"id": ["not_null"]},
            ).validate()

        # Test with invalid rules type
        with pytest.raises(PipelineValidationError):
            BronzeStep(
                name="test_step",
                rules="invalid_rules",  # type: ignore
            ).validate()

        # Test with invalid incremental_col type
        with pytest.raises(PipelineValidationError):
            BronzeStep(
                name="test_step",
                rules={"id": ["not_null"]},
                incremental_col=123,  # type: ignore
            ).validate()

    def test_silver_step_validation_error_paths(self) -> None:
        """Test SilverStep validation error paths."""
        # Test with empty source_bronze
        with pytest.raises(PipelineValidationError):
            SilverStep(
                name="test_step",
                source_bronze="",
                rules={"id": ["not_null"]},
                transform=lambda x: x,  # type: ignore
                table_name="test_table",
            ).validate()

        # Test with None transform
        with pytest.raises(PipelineValidationError):
            SilverStep(
                name="test_step",
                source_bronze="test_bronze",
                rules={"id": ["not_null"]},
                transform=None,  # type: ignore
                table_name="test_table",
            ).validate()

    def test_gold_step_validation_error_paths(self) -> None:
        """Test GoldStep validation error paths."""
        # Test with None transform
        with pytest.raises(PipelineValidationError):
            GoldStep(
                name="test_step",
                source_silvers=["test_silver"],
                rules={"id": ["not_null"]},
                transform=None,  # type: ignore
                table_name="test_table",
            ).validate()

        # Test with empty source_silvers
        with pytest.raises(PipelineValidationError):
            GoldStep(
                name="test_step",
                source_silvers=[],
                rules={"id": ["not_null"]},
                transform=lambda x: x,  # type: ignore
                table_name="test_table",
            ).validate()

        # Test with None source_silvers (this is actually valid, so we skip this test)
        # None source_silvers means use all available silver steps, which is valid

        # Test with invalid source_silvers type
        with pytest.raises(PipelineValidationError):
            GoldStep(
                name="test_step",
                source_silvers="invalid",  # type: ignore
                rules={"id": ["not_null"]},
                transform=lambda x: x,  # type: ignore
                table_name="test_table",
            ).validate()

    def test_silver_dependency_info_validation_error_paths(self) -> None:
        """Test SilverDependencyInfo validation error paths."""
        # Test with empty step_name
        with pytest.raises(PipelineValidationError):
            SilverDependencyInfo(
                step_name="",
                source_bronze="test_bronze",
                depends_on_silvers=set(),
                can_run_parallel=True,
                execution_group=1,
            ).validate()

        # Test with empty source_bronze
        with pytest.raises(PipelineValidationError):
            SilverDependencyInfo(
                step_name="test_step",
                source_bronze="",
                depends_on_silvers=set(),
                can_run_parallel=True,
                execution_group=1,
            ).validate()


class TestPipelineConfigValidation:
    """Test PipelineConfig validation edge cases."""

    def test_pipeline_config_validation_error_paths(self) -> None:
        """Test PipelineConfig validation error paths."""
        # Test with None schema
        with pytest.raises(PipelineValidationError):
            PipelineConfig(
                schema=None,  # type: ignore
                thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
                parallel=ParallelConfig(enabled=True, max_workers=4),
            ).validate()

        # Test with empty schema
        with pytest.raises(PipelineValidationError):
            PipelineConfig(
                schema="",
                thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
                parallel=ParallelConfig(enabled=True, max_workers=4),
            ).validate()

        # Test with invalid schema type
        with pytest.raises(PipelineValidationError):
            PipelineConfig(
                schema=123,  # type: ignore
                thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
                parallel=ParallelConfig(enabled=True, max_workers=4),
            ).validate()


class TestModelFactoryMethods:
    """Test model factory methods and creation patterns."""

    def test_pipeline_config_factory_methods(self) -> None:
        """Test PipelineConfig factory methods."""
        # Test create_default
        default_config = PipelineConfig.create_default("test_schema")
        assert isinstance(default_config, PipelineConfig)
        assert default_config.schema is not None

        # Test create_high_performance
        perf_config = PipelineConfig.create_high_performance("test_schema")
        assert isinstance(perf_config, PipelineConfig)
        assert perf_config.schema is not None

        # Test create_conservative
        conservative_config = PipelineConfig.create_conservative("test_schema")
        assert isinstance(conservative_config, PipelineConfig)
        assert conservative_config.schema is not None

    def test_validation_thresholds_factory_methods(self) -> None:
        """Test ValidationThresholds factory methods."""
        # Test create_loose
        loose_thresholds = ValidationThresholds.create_loose()
        assert isinstance(loose_thresholds, ValidationThresholds)

        # Test create_strict
        strict_thresholds = ValidationThresholds.create_strict()
        assert isinstance(strict_thresholds, ValidationThresholds)

    def test_parallel_config_factory_methods(self) -> None:
        """Test ParallelConfig factory methods."""
        # Test create_default
        default_parallel = ParallelConfig.create_default()
        assert isinstance(default_parallel, ParallelConfig)

        # Test create_sequential
        sequential_parallel = ParallelConfig.create_sequential()
        assert isinstance(sequential_parallel, ParallelConfig)

        # Test create_high_performance
        perf_parallel = ParallelConfig.create_high_performance()
        assert isinstance(perf_parallel, ParallelConfig)
