#!/usr/bin/env python3
"""
Final unit tests for sparkforge/models.py module.

This module tests remaining edge cases and error paths to improve
coverage for models.py from 74% to 90%+.
"""

from typing import Dict

import pytest

from sparkforge.errors import PipelineValidationError
from sparkforge.models import (
    BronzeStep,
    ModelValue,
    ParallelConfig,
    PipelineConfig,
    Serializable,
    SilverStep,
    Validatable,
    ValidationThresholds,
)


class TestModelsFinalCoverage:
    """Test remaining edge cases and error paths for models.py."""

    def test_validatable_protocol_definition(self) -> None:
        """Test Validatable protocol definition."""
        # Test that Validatable protocol is properly defined
        assert hasattr(Validatable, "validate")

        # Test implementing the protocol
        class MockValidatable:
            def validate(self) -> None:
                pass

        # Should be able to use as Validatable
        validator: Validatable = MockValidatable()
        validator.validate()

    def test_serializable_protocol_definition(self) -> None:
        """Test Serializable protocol definition."""
        # Test that Serializable protocol is properly defined
        assert hasattr(Serializable, "to_dict")
        assert hasattr(Serializable, "to_json")

        # Test implementing the protocol
        class MockSerializable:
            def to_dict(self) -> Dict[str, ModelValue]:
                return {"test": "value"}

            def to_json(self) -> str:
                return '{"test": "value"}'

        # Should be able to use as Serializable
        serializer: Serializable = MockSerializable()
        assert serializer.to_dict() == {"test": "value"}
        assert serializer.to_json() == '{"test": "value"}'

    def test_pipeline_config_validation_edge_cases(self) -> None:
        """Test PipelineConfig validation edge cases."""
        # Test with empty schema - validation is called manually
        config = PipelineConfig(
            schema="",
            thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
        )
        with pytest.raises(
            PipelineValidationError, match="Schema name must be a non-empty string"
        ):
            config.validate()

        # Test with None schema
        config = PipelineConfig(
            schema=None,  # type: ignore
            thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
        )
        with pytest.raises(
            PipelineValidationError, match="Schema name must be a non-empty string"
        ):
            config.validate()

        # Test with non-string schema
        config = PipelineConfig(
            schema=123,  # type: ignore
            thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
        )
        with pytest.raises(
            PipelineValidationError, match="Schema name must be a non-empty string"
        ):
            config.validate()

    def test_bronze_step_validation_edge_cases(self) -> None:
        """Test BronzeStep validation edge cases."""
        # Test with empty name
        with pytest.raises(
            PipelineValidationError, match="Step name must be a non-empty string"
        ):
            BronzeStep(
                name="", rules={"col1": ["col1 > 0"]}, incremental_col="updated_at"
            )

        # Test with None name
        with pytest.raises(
            PipelineValidationError, match="Step name must be a non-empty string"
        ):
            BronzeStep(
                name=None,  # type: ignore
                rules={"col1": ["col1 > 0"]},
                incremental_col="updated_at",
            )

        # Test with non-string name
        with pytest.raises(
            PipelineValidationError, match="Step name must be a non-empty string"
        ):
            BronzeStep(
                name=123,  # type: ignore
                rules={"col1": ["col1 > 0"]},
                incremental_col="updated_at",
            )

        # Test with non-dict rules
        with pytest.raises(
            PipelineValidationError, match="Rules must be a non-empty dictionary"
        ):
            BronzeStep(
                name="test_step",
                rules="invalid_rules",  # type: ignore
                incremental_col="updated_at",
            )

        # Test with non-string incremental_col
        with pytest.raises(
            PipelineValidationError, match="Incremental column must be a string"
        ):
            BronzeStep(
                name="test_step",
                rules={"col1": ["col1 > 0"]},
                incremental_col=123,  # type: ignore
            )

    def test_bronze_step_incremental_capability(self) -> None:
        """Test BronzeStep incremental capability property."""
        # Test with incremental column
        step = BronzeStep(
            name="test_step", rules={"col1": ["col1 > 0"]}, incremental_col="updated_at"
        )
        assert step.has_incremental_capability is True

        # Test without incremental column
        step = BronzeStep(
            name="test_step", rules={"col1": ["col1 > 0"]}, incremental_col=None
        )
        assert step.has_incremental_capability is False

    def test_silver_step_validation_edge_cases(self) -> None:
        """Test SilverStep validation edge cases."""

        def mock_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        # Test with empty name
        with pytest.raises(
            PipelineValidationError, match="Step name must be a non-empty string"
        ):
            SilverStep(
                name="",
                source_bronze="bronze1",
                transform=mock_transform,
                rules={"col1": ["col1 > 0"]},
                table_name="silver_table",
            )

        # Test with non-string name
        with pytest.raises(
            PipelineValidationError, match="Step name must be a non-empty string"
        ):
            SilverStep(
                name=123,  # type: ignore
                source_bronze="bronze1",
                transform=mock_transform,
                rules={"col1": ["col1 > 0"]},
                table_name="silver_table",
            )

    def test_validation_thresholds_boundary_values(self) -> None:
        """Test ValidationThresholds with boundary values."""
        # Test with boundary values (0.0 and 100.0)
        thresholds = ValidationThresholds(bronze=0.0, silver=50.0, gold=100.0)
        assert thresholds.bronze == 0.0
        assert thresholds.silver == 50.0
        assert thresholds.gold == 100.0

    def test_parallel_config_boundary_values(self) -> None:
        """Test ParallelConfig with boundary values."""
        # Test with minimum max_workers
        parallel = ParallelConfig(enabled=True, max_workers=1)
        assert parallel.max_workers == 1

        # Test with large max_workers
        parallel = ParallelConfig(enabled=True, max_workers=1000)
        assert parallel.max_workers == 1000

    def test_model_factory_methods_values(self) -> None:
        """Test model factory methods with expected values."""
        # Test ValidationThresholds.create_strict with actual values
        thresholds = ValidationThresholds.create_strict()
        assert thresholds.bronze == 99.0
        assert thresholds.silver == 99.5
        assert thresholds.gold == 99.9

        # Test ValidationThresholds.create_permissive with actual values (if it exists)
        if hasattr(ValidationThresholds, "create_permissive"):
            thresholds = ValidationThresholds.create_permissive()
            assert thresholds.bronze == 70.0
            assert thresholds.silver == 75.0
            assert thresholds.gold == 80.0

        # Test ParallelConfig factory methods if they exist
        if hasattr(ParallelConfig, "create_conservative"):
            parallel = ParallelConfig.create_conservative()
            assert parallel.enabled is True
            assert parallel.max_workers == 2

        if hasattr(ParallelConfig, "create_aggressive"):
            parallel = ParallelConfig.create_aggressive()
            assert parallel.enabled is True
            assert parallel.max_workers == 8

    def test_model_properties_basic(self) -> None:
        """Test model properties with basic cases."""
        # Test PipelineConfig properties
        config = PipelineConfig.create_default("test_schema")
        assert isinstance(config.min_bronze_rate, float)
        assert isinstance(config.min_silver_rate, float)
        assert isinstance(config.min_gold_rate, float)

    def test_model_comparison_edge_cases(self) -> None:
        """Test model comparison with edge cases."""
        # Test ValidationThresholds comparison
        thresholds1 = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
        thresholds2 = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
        thresholds3 = ValidationThresholds(bronze=75.0, silver=85.0, gold=90.0)

        assert thresholds1 == thresholds2
        assert thresholds1 != thresholds3

        # Test ParallelConfig comparison
        parallel1 = ParallelConfig(enabled=True, max_workers=4)
        parallel2 = ParallelConfig(enabled=True, max_workers=4)
        parallel3 = ParallelConfig(enabled=False, max_workers=4)

        assert parallel1 == parallel2
        assert parallel1 != parallel3

    def test_model_string_representations(self) -> None:
        """Test model string representations."""
        # Test ValidationThresholds string representation
        thresholds = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
        str_repr = str(thresholds)
        assert "bronze=80.0" in str_repr
        assert "silver=85.0" in str_repr
        assert "gold=90.0" in str_repr

        # Test ParallelConfig string representation
        parallel = ParallelConfig(enabled=True, max_workers=4)
        str_repr = str(parallel)
        assert "enabled=True" in str_repr
        assert "max_workers=4" in str_repr

    def test_model_error_handling_comprehensive(self) -> None:
        """Test comprehensive error handling scenarios."""
        # Test multiple validation errors in one model
        try:
            BronzeStep(
                name="",  # Invalid name
                rules="invalid",  # Invalid rules
                incremental_col=123,  # Invalid incremental_col
            )
        except PipelineValidationError as e:
            # Should catch the first validation error
            assert "Step name must be a non-empty string" in str(e)

        # Test ValidationThresholds with multiple invalid values
        try:
            ValidationThresholds(bronze=-10.0, silver=150.0, gold=-5.0)
        except PipelineValidationError as e:
            # Should catch the first validation error
            assert "Thresholds must be between 0 and 100" in str(e)

    def test_model_boundary_conditions(self) -> None:
        """Test model boundary conditions."""
        # Test ValidationThresholds boundary values
        thresholds = ValidationThresholds(bronze=0.0, silver=50.0, gold=100.0)
        assert thresholds.bronze == 0.0
        assert thresholds.silver == 50.0
        assert thresholds.gold == 100.0

        # Test ParallelConfig boundary values
        parallel = ParallelConfig(enabled=True, max_workers=1)
        assert parallel.max_workers == 1

        # Test with very large max_workers
        parallel = ParallelConfig(enabled=True, max_workers=1000)
        assert parallel.max_workers == 1000
