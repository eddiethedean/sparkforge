"""
Basic tests for sparkforge.pipeline.builder.PipelineBuilder that avoid PySpark dependencies.
"""

import os
from unittest.mock import patch

import pytest

from sparkforge.errors import ConfigurationError, ExecutionError
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    PipelineConfig,
)
from sparkforge.pipeline.builder import PipelineBuilder

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as MockF
else:
    MockF = None


class TestPipelineBuilderInitialization:
    """Tests for PipelineBuilder initialization."""

    def test_pipeline_builder_initialization_basic(self, spark_session):
        """Test basic PipelineBuilder initialization."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        assert builder.spark == spark_session
        assert builder.schema == "test_schema"
        assert isinstance(builder.config, PipelineConfig)
        assert isinstance(builder.logger, PipelineLogger)
        assert builder.bronze_steps == {}
        assert builder.silver_steps == {}
        assert builder.gold_steps == {}

    def test_pipeline_builder_initialization_with_quality_rates(self, spark_session):
        """Test PipelineBuilder initialization with custom quality rates."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=99.0,
            verbose=False,
        )

        assert builder.config.thresholds.bronze == 90.0
        assert builder.config.thresholds.silver == 95.0
        assert builder.config.thresholds.gold == 99.0
        assert builder.config.verbose is False

    def test_pipeline_builder_initialization_invalid_spark(self, spark_session):
        """Test PipelineBuilder initialization with invalid Spark session."""
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=None, schema="test_schema")

    def test_pipeline_builder_initialization_empty_schema(self, spark_session):
        """Test PipelineBuilder initialization with empty schema."""
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=spark_session, schema="")

    def test_pipeline_builder_initialization_pipeline_id(self, spark_session):
        """Test PipelineBuilder generates unique pipeline ID."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        assert builder.pipeline_id.startswith("pipeline_test_schema_")
        assert len(builder.pipeline_id) > 20  # Should have timestamp

    def test_pipeline_builder_initialization_validators(self, spark_session):
        """Test PipelineBuilder initializes validators correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        assert hasattr(builder, "validators")
        assert hasattr(builder, "validator")
        assert builder.validators == builder.validator.custom_validators


class TestValidatorManagement:
    """Tests for validator management."""

    def test_add_validator(self, spark_session):
        """Test adding custom validator."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        class CustomValidator:
            def validate(self, step, context):
                return []

        validator = CustomValidator()
        result = builder.add_validator(validator)

        assert result is builder
        assert validator in builder.validators

    def test_add_multiple_validators(self, spark_session):
        """Test adding multiple validators."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        class Validator1:
            def validate(self, step, context):
                return []

        class Validator2:
            def validate(self, step, context):
                return []

        builder.add_validator(Validator1())
        builder.add_validator(Validator2())

        assert len(builder.validators) == 2


class TestPipelineValidation:
    """Tests for pipeline validation."""

    def test_validate_pipeline_empty(self, spark_session):
        """Test validating empty pipeline."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        errors = builder.validate_pipeline()
        assert errors == []  # Empty pipeline should be valid

    def test_validate_pipeline_invalid_config(self, spark_session):
        """Test validating pipeline with invalid configuration."""
        # This should fail during initialization, not validation
        with pytest.raises(ConfigurationError):
            PipelineBuilder(
                spark=spark_session,
                schema="",  # Invalid empty schema
                min_bronze_rate=95.0,
                min_silver_rate=98.0,
                min_gold_rate=99.0,
            )


class TestToPipeline:
    """Tests for to_pipeline method."""

    def test_to_pipeline_empty(self, spark_session):
        """Test building empty pipeline."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        pipeline = builder.to_pipeline()

        assert pipeline is not None
        assert pipeline.bronze_steps == {}
        assert pipeline.silver_steps == {}
        assert pipeline.gold_steps == {}


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_effective_schema(self, spark_session):
        """Test _get_effective_schema method."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Test with None (should return default)
        effective_schema = builder._get_effective_schema(None)
        assert effective_schema == "test_schema"

        # Test with custom schema
        effective_schema = builder._get_effective_schema("custom_schema")
        assert effective_schema == "custom_schema"

    def test_create_schema_if_not_exists(self, spark_session):
        """Test _create_schema_if_not_exists method."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Create a new schema
        builder._create_schema_if_not_exists("new_schema_basic_test")

        # Verify schema was created by listing databases (works in mock-spark 1.4.0+)
        dbs = spark_session.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "new_schema_basic_test" in db_names

    def test_create_schema_if_not_exists_failure(self, spark_session):
        """Test _create_schema_if_not_exists with failure."""

        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Patch the spark.sql method to raise exception
        with patch.object(
            spark_session,
            "sql",
            side_effect=Exception("Permission denied"),
        ):
            with pytest.raises(ExecutionError):
                builder._create_schema_if_not_exists("new_schema")


class TestClassMethods:
    """Tests for class methods."""

    def test_for_development(self, spark_session):
        """Test for_development class method."""
        builder = PipelineBuilder.for_development(
            spark=spark_session, schema="test_schema"
        )

        assert isinstance(builder, PipelineBuilder)
        assert builder.schema == "test_schema"
        assert builder.config.verbose is True  # Development should be verbose

    def test_for_production(self, spark_session):
        """Test for_production class method."""
        builder = PipelineBuilder.for_production(
            spark=spark_session, schema="test_schema"
        )

        assert isinstance(builder, PipelineBuilder)
        assert builder.schema == "test_schema"
        assert builder.config.verbose is False  # Production should not be verbose


class TestErrorHandling:
    """Tests for error handling."""

    def test_with_bronze_rules_empty_name(self, spark_session):
        """Test adding bronze rules with empty name."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        rules = {"id": ["not_null"]}
        with pytest.raises(ExecutionError):
            builder.with_bronze_rules(name="", rules=rules)

    def test_with_silver_rules_empty_name(self, spark_session):
        """Test adding silver rules with empty name."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        rules = {"id": ["not_null"]}
        with pytest.raises(ExecutionError):
            builder.with_silver_rules(name="", table_name="test_table", rules=rules)

    def test_add_silver_transform_no_bronze_steps(self, spark_session):
        """Test silver transform with no bronze steps available."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        def transform_func(spark, df, silvers):
            return df

        with pytest.raises(ExecutionError):
            builder.add_silver_transform(
                name="test_silver",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_table",
            )

    def test_add_gold_transform_no_silver_steps(self, spark_session):
        """Test gold transform with no silver steps available."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        def transform_func(spark, silvers):
            return None

        with pytest.raises(ExecutionError):
            builder.add_gold_transform(
                name="test_gold",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_gold_table",
            )


class TestStepManagement:
    """Tests for step management without PySpark dependencies."""

    def test_bronze_steps_storage(self, spark_session):
        """Test that bronze steps are stored correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Test initial state
        assert len(builder.bronze_steps) == 0

        # Test that we can access the bronze_steps dictionary
        assert isinstance(builder.bronze_steps, dict)

    def test_silver_steps_storage(self, spark_session):
        """Test that silver steps are stored correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Test initial state
        assert len(builder.silver_steps) == 0

        # Test that we can access the silver_steps dictionary
        assert isinstance(builder.silver_steps, dict)

    def test_gold_steps_storage(self, spark_session):
        """Test that gold steps are stored correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Test initial state
        assert len(builder.gold_steps) == 0

        # Test that we can access the gold_steps dictionary
        assert isinstance(builder.gold_steps, dict)

    def test_pipeline_configuration(self, spark_session):
        """Test pipeline configuration properties."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            min_bronze_rate=85.0,
            min_silver_rate=90.0,
            min_gold_rate=95.0,
            verbose=True,
        )

        # Test configuration
        assert builder.config.schema == "test_schema"
        assert builder.config.thresholds.bronze == 85.0
        assert builder.config.thresholds.silver == 90.0
        assert builder.config.thresholds.gold == 95.0
        assert builder.config.verbose is True

    def test_pipeline_logger(self, spark_session):
        """Test pipeline logger properties."""
        builder = PipelineBuilder(
            spark=spark_session, schema="test_schema", verbose=True
        )

        # Test logger
        assert isinstance(builder.logger, PipelineLogger)
        assert builder.logger.verbose is True

    def test_pipeline_id_generation(self, spark_session):
        """Test pipeline ID generation."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Pipeline ID should start with expected prefix
        assert builder.pipeline_id.startswith("pipeline_test_schema_")

        # Pipeline ID should have timestamp format
        import re

        pattern = r"pipeline_test_schema_\d{8}_\d{6}"
        assert re.match(pattern, builder.pipeline_id) is not None

        # Pipeline ID should be reasonably long
        assert len(builder.pipeline_id) > 20

    def test_schema_property(self, spark_session):
        """Test schema property access."""
        builder = PipelineBuilder(spark=spark_session, schema="my_test_schema")

        assert builder.schema == "my_test_schema"

    def test_validators_property(self, spark_session):
        """Test validators property access."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Test initial validators
        assert isinstance(builder.validators, list)
        assert len(builder.validators) == 0

        # Add a validator
        class TestValidator:
            def validate(self, step, context):
                return []

        builder.add_validator(TestValidator())

        # Test validators after adding
        assert len(builder.validators) == 1
        assert isinstance(builder.validators[0], TestValidator)
