"""
Comprehensive tests for pipeline_builder_base config module.

Tests configuration factories and validators.
"""

from pipeline_builder_base.config.factories import (
    create_development_config,
    create_production_config,
    create_test_config,
)
from pipeline_builder_base.config.validators import (
    validate_pipeline_config,
    validate_thresholds,
)
from pipeline_builder_base.models import (
    PipelineConfig,
    ValidationThresholds,
)


class TestConfigurationFactories:
    """Test configuration factory functions."""

    def test_create_development_config(self):
        """Test development config creation."""
        config = create_development_config("dev_schema")

        assert config.schema == "dev_schema"
        assert config.thresholds.bronze == 80.0
        assert config.thresholds.silver == 85.0
        assert config.thresholds.gold == 90.0
        assert config.verbose is True

    def test_create_development_config_overrides(self):
        """Test config overrides."""
        config = create_development_config(
            "dev_schema", verbose=False, min_bronze_rate=75.0
        )

        assert config.verbose is False
        assert config.thresholds.bronze == 75.0

    def test_create_production_config(self):
        """Test production config creation."""
        config = create_production_config("prod_schema")

        assert config.schema == "prod_schema"
        assert config.thresholds.bronze == 95.0
        assert config.thresholds.silver == 98.0
        assert config.thresholds.gold == 99.5
        assert config.verbose is False

    def test_create_production_config_overrides(self):
        """Test production config overrides."""
        config = create_production_config("prod_schema", verbose=True)

        assert config.verbose is True

    def test_create_test_config(self):
        """Test test config creation."""
        config = create_test_config("test_schema")

        assert config.schema == "test_schema"
        assert config.thresholds.bronze == 50.0
        assert config.thresholds.silver == 50.0
        assert config.thresholds.gold == 50.0

    def test_create_test_config_overrides(self):
        """Test test config overrides."""
        config = create_test_config("test_schema", min_bronze_rate=60.0)

        assert config.thresholds.bronze == 60.0


class TestConfigurationValidators:
    """Test configuration validator functions."""

    def test_validate_pipeline_config_valid(self, pipeline_config):
        """Test valid config validation."""
        errors = validate_pipeline_config(pipeline_config)
        assert errors == []

    def test_validate_pipeline_config_invalid(self):
        """Test invalid config detection."""
        invalid_config = PipelineConfig(
            schema="",  # Empty schema
            thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
        )

        errors = validate_pipeline_config(invalid_config)
        assert len(errors) > 0

    # ParallelConfig tests removed - ParallelConfig no longer exists

    def test_validate_thresholds_valid(self):
        """Test valid thresholds."""
        thresholds = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
        errors = validate_thresholds(thresholds)
        assert errors == []

    def test_validate_thresholds_invalid(self):
        """Test invalid thresholds."""
        invalid_thresholds = ValidationThresholds(
            bronze=150.0, silver=85.0, gold=90.0
        )  # > 100

        errors = validate_thresholds(invalid_thresholds)
        assert len(errors) > 0

    def test_validate_thresholds_range(self):
        """Test threshold range validation."""
        # Test negative threshold
        thresholds = ValidationThresholds(bronze=-10.0, silver=85.0, gold=90.0)
        errors = validate_thresholds(thresholds)
        assert len(errors) > 0

        # Test threshold > 100
        thresholds = ValidationThresholds(bronze=80.0, silver=150.0, gold=90.0)
        errors = validate_thresholds(thresholds)
        assert len(errors) > 0

    def test_validate_thresholds_order(self):
        """Test threshold ordering validation."""
        # Test bronze > silver
        thresholds = ValidationThresholds(bronze=90.0, silver=85.0, gold=95.0)
        errors = validate_thresholds(thresholds)
        assert len(errors) > 0

        # Test silver > gold
        thresholds = ValidationThresholds(bronze=80.0, silver=95.0, gold=90.0)
        errors = validate_thresholds(thresholds)
        assert len(errors) > 0
