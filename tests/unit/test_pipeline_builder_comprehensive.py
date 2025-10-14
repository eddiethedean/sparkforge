"""
Comprehensive tests for sparkforge.pipeline.builder.PipelineBuilder.
"""

from unittest.mock import patch

import pytest
from mock_spark.functions import F

from sparkforge.errors import ConfigurationError, ExecutionError
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    BronzeStep,
    PipelineConfig,
    SilverStep,
)
from sparkforge.pipeline.builder import PipelineBuilder

# Use mock functions when in mock mode
MockF = F  # Already imported above


class TestPipelineBuilderInitialization:
    """Tests for PipelineBuilder initialization."""

    def test_pipeline_builder_initialization_basic(self, mock_spark_session):
        """Test basic PipelineBuilder initialization."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        assert builder.spark == mock_spark_session
        assert builder.schema == "test_schema"
        assert isinstance(builder.config, PipelineConfig)
        assert isinstance(builder.logger, PipelineLogger)
        assert builder.bronze_steps == {}
        assert builder.silver_steps == {}
        assert builder.gold_steps == {}

    def test_pipeline_builder_initialization_with_quality_rates(
        self, mock_spark_session
    ):
        """Test PipelineBuilder initialization with custom quality rates."""
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="test_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=99.0,
            verbose=False,
            functions=MockF,
        )

        assert builder.config.thresholds.bronze == 90.0
        assert builder.config.thresholds.silver == 95.0
        assert builder.config.thresholds.gold == 99.0
        assert builder.config.verbose is False

    def test_pipeline_builder_initialization_invalid_spark(self, mock_spark_session):
        """Test PipelineBuilder initialization with invalid Spark session."""
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=None, schema="test_schema")

    def test_pipeline_builder_initialization_empty_schema(self, mock_spark_session):
        """Test PipelineBuilder initialization with empty schema."""
        with pytest.raises(ConfigurationError):
            PipelineBuilder(spark=mock_spark_session, schema="")

    def test_pipeline_builder_initialization_pipeline_id(self, mock_spark_session):
        """Test PipelineBuilder generates unique pipeline ID."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        assert builder.pipeline_id.startswith("pipeline_test_schema_")
        assert len(builder.pipeline_id) > 20  # Should have timestamp

    def test_pipeline_builder_initialization_validators(self, mock_spark_session):
        """Test PipelineBuilder initializes validators correctly."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        assert hasattr(builder, "validators")
        assert hasattr(builder, "validator")
        assert builder.validators == builder.validator.custom_validators


class TestBronzeRules:
    """Tests for with_bronze_rules method."""

    def test_with_bronze_rules_basic(self, mock_spark_session):
        """Test adding basic bronze rules."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"], "name": ["not_null"]}
        result = builder.with_bronze_rules(name="test_bronze", rules=rules)

        assert result is builder  # Should return self for chaining
        assert "test_bronze" in builder.bronze_steps
        assert isinstance(builder.bronze_steps["test_bronze"], BronzeStep)
        assert builder.bronze_steps["test_bronze"].name == "test_bronze"

    def test_with_bronze_rules_with_incremental_col(self, mock_spark_session):
        """Test adding bronze rules with incremental column."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        builder.with_bronze_rules(
            name="test_bronze", rules=rules, incremental_col="timestamp"
        )

        bronze_step = builder.bronze_steps["test_bronze"]
        assert bronze_step.incremental_col == "timestamp"

    def test_with_bronze_rules_with_schema(self, mock_spark_session):
        """Test adding bronze rules with custom schema."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Mock the schema validation
        with patch.object(builder, "_validate_schema"):
            rules = {"id": ["not_null"]}
            builder.with_bronze_rules(
                name="test_bronze", rules=rules, schema="custom_schema"
            )

            bronze_step = builder.bronze_steps["test_bronze"]
            assert bronze_step.schema == "custom_schema"

    def test_with_bronze_rules_duplicate_name(self, mock_spark_session):
        """Test adding bronze rules with duplicate name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        builder.with_bronze_rules(name="test_bronze", rules=rules)

        with pytest.raises(ExecutionError):
            builder.with_bronze_rules(name="test_bronze", rules=rules)

    def test_with_bronze_rules_empty_name(self, mock_spark_session):
        """Test adding bronze rules with empty name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        with pytest.raises(ExecutionError):
            builder.with_bronze_rules(name="", rules=rules)

    def test_with_bronze_rules_pyspark_rules(self, mock_spark_session):
        """Test adding bronze rules with PySpark Column expressions."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": [F.col("id").isNotNull()]}
        builder.with_bronze_rules(name="test_bronze", rules=rules)

        assert "test_bronze" in builder.bronze_steps
        bronze_step = builder.bronze_steps["test_bronze"]
        assert bronze_step.rules is not None


class TestSilverRules:
    """Tests for with_silver_rules method."""

    def test_with_silver_rules_basic(self, mock_spark_session):
        """Test adding basic silver rules."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        result = builder.with_silver_rules(
            name="test_silver", table_name="test_table", rules=rules
        )

        assert result is builder
        assert "test_silver" in builder.silver_steps
        assert isinstance(builder.silver_steps["test_silver"], SilverStep)
        assert builder.silver_steps["test_silver"].table_name == "test_table"

    def test_with_silver_rules_with_watermark(self, mock_spark_session):
        """Test adding silver rules with watermark column."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        builder.with_silver_rules(
            name="test_silver",
            table_name="test_table",
            rules=rules,
            watermark_col="timestamp",
        )

        silver_step = builder.silver_steps["test_silver"]
        assert silver_step.watermark_col == "timestamp"

    def test_with_silver_rules_duplicate_name(self, mock_spark_session):
        """Test adding silver rules with duplicate name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        builder.with_silver_rules(
            name="test_silver", table_name="test_table", rules=rules
        )

        with pytest.raises(ExecutionError):
            builder.with_silver_rules(
                name="test_silver", table_name="test_table2", rules=rules
            )

    def test_with_silver_rules_empty_name(self, mock_spark_session):
        """Test adding silver rules with empty name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        rules = {"id": ["not_null"]}
        with pytest.raises(ExecutionError):
            builder.with_silver_rules(name="", table_name="test_table", rules=rules)


class TestSilverTransform:
    """Tests for add_silver_transform method."""

    def test_add_silver_transform_basic(self, mock_spark_session):
        """Test adding basic silver transform."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze step first
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})

        def transform_func(spark, df, silvers):
            return df.filter(F.col("id") > 0)

        result = builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        assert result is builder
        assert "test_silver" in builder.silver_steps
        silver_step = builder.silver_steps["test_silver"]
        assert silver_step.source_bronze == "test_bronze"
        assert silver_step.transform == transform_func

    def test_add_silver_transform_auto_inference(self, mock_spark_session):
        """Test silver transform with auto-inferred source."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze step first
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})

        def transform_func(spark, df, silvers):
            return df

        builder.add_silver_transform(
            name="test_silver",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        silver_step = builder.silver_steps["test_silver"]
        assert silver_step.source_bronze == "test_bronze"

    def test_add_silver_transform_no_bronze_steps(self, mock_spark_session):
        """Test silver transform with no bronze steps available."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        def transform_func(spark, df, silvers):
            return df

        with pytest.raises(ExecutionError):
            builder.add_silver_transform(
                name="test_silver",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_table",
            )

    def test_add_silver_transform_invalid_source(self, mock_spark_session):
        """Test silver transform with invalid source bronze."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        def transform_func(spark, df, silvers):
            return df

        with pytest.raises(ExecutionError):
            builder.add_silver_transform(
                name="test_silver",
                source_bronze="nonexistent_bronze",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_table",
            )

    def test_add_silver_transform_duplicate_name(self, mock_spark_session):
        """Test silver transform with duplicate name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})

        def transform_func(spark, df, silvers):
            return df

        builder.add_silver_transform(
            name="test_silver",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        with pytest.raises(ExecutionError):
            builder.add_silver_transform(
                name="test_silver",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_table2",
            )


class TestGoldTransform:
    """Tests for add_gold_transform method."""

    def test_add_gold_transform_basic(self, mock_spark_session):
        """Test adding basic gold transform."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze and silver steps first
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})
        builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        result = builder.add_gold_transform(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        assert result is builder
        assert "test_gold" in builder.gold_steps
        gold_step = builder.gold_steps["test_gold"]
        assert gold_step.source_silvers == ["test_silver"]
        assert gold_step.transform == transform_func

    def test_add_gold_transform_auto_inference(self, mock_spark_session):
        """Test gold transform with auto-inferred sources."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze and silver steps first
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})
        builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        builder.add_gold_transform(
            name="test_gold",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        gold_step = builder.gold_steps["test_gold"]
        assert gold_step.source_silvers == ["test_silver"]

    def test_add_gold_transform_no_silver_steps(self, mock_spark_session):
        """Test gold transform with no silver steps available."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        def transform_func(spark, silvers):
            return None

        with pytest.raises(ExecutionError):
            builder.add_gold_transform(
                name="test_gold",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_gold_table",
            )

    def test_add_gold_transform_invalid_sources(self, mock_spark_session):
        """Test gold transform with invalid source silvers."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        def transform_func(spark, silvers):
            return None

        with pytest.raises(ExecutionError):
            builder.add_gold_transform(
                name="test_gold",
                source_silvers=["nonexistent_silver"],
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_gold_table",
            )

    def test_add_gold_transform_duplicate_name(self, mock_spark_session):
        """Test gold transform with duplicate name."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze and silver steps first
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})
        builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        builder.add_gold_transform(
            name="test_gold",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        with pytest.raises(ExecutionError):
            builder.add_gold_transform(
                name="test_gold",
                transform=transform_func,
                rules={"id": ["not_null"]},
                table_name="test_gold_table2",
            )


class TestValidatorManagement:
    """Tests for validator management."""

    def test_add_validator(self, mock_spark_session):
        """Test adding custom validator."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        class CustomValidator:
            def validate(self, step, context):
                return []

        validator = CustomValidator()
        result = builder.add_validator(validator)

        assert result is builder
        assert validator in builder.validators

    def test_add_multiple_validators(self, mock_spark_session):
        """Test adding multiple validators."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

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

    def test_validate_pipeline_empty(self, mock_spark_session):
        """Test validating empty pipeline."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        errors = builder.validate_pipeline()
        assert errors == []  # Empty pipeline should be valid

    def test_validate_pipeline_with_steps(self, mock_spark_session):
        """Test validating pipeline with steps."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add valid steps
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})
        builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )
        builder.add_gold_transform(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=lambda spark, silvers: silvers["test_silver"],
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        errors = builder.validate_pipeline()
        assert errors == []  # Valid pipeline should have no errors

    def test_validate_pipeline_invalid_config(self, mock_spark_session):
        """Test validating pipeline with invalid configuration."""
        # This should fail during initialization due to empty schema
        with pytest.raises(ConfigurationError):
            PipelineBuilder(
                spark=mock_spark_session,
                schema="",  # Invalid empty schema
                min_bronze_rate=95.0,
                min_silver_rate=98.0,
                min_gold_rate=99.0,
            )


class TestToPipeline:
    """Tests for to_pipeline method."""

    def test_to_pipeline_basic(self, mock_spark_session):
        """Test building basic pipeline."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add valid steps
        builder.with_bronze_rules(name="test_bronze", rules={"id": ["not_null"]})
        builder.add_silver_transform(
            name="test_silver",
            source_bronze="test_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="test_table",
        )
        builder.add_gold_transform(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=lambda spark, silvers: silvers["test_silver"],
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        pipeline = builder.to_pipeline()

        assert pipeline is not None
        assert hasattr(pipeline, "spark")
        assert hasattr(pipeline, "config")
        assert hasattr(pipeline, "bronze_steps")
        assert hasattr(pipeline, "silver_steps")
        assert hasattr(pipeline, "gold_steps")

    def test_to_pipeline_validation_failure(self, mock_spark_session):
        """Test building pipeline with validation failure."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add invalid step (no source bronze for silver)
        # New implementation validates immediately in add_silver_transform
        with pytest.raises(ExecutionError):
            builder.add_silver_transform(
                name="test_silver",
                source_bronze="nonexistent_bronze",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="test_table",
            )

    def test_to_pipeline_empty(self, mock_spark_session):
        """Test building empty pipeline."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        pipeline = builder.to_pipeline()

        assert pipeline is not None
        assert pipeline.bronze_steps == {}
        assert pipeline.silver_steps == {}
        assert pipeline.gold_steps == {}


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_effective_schema(self, mock_spark_session):
        """Test _get_effective_schema method."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Test with None (should return default)
        effective_schema = builder._get_effective_schema(None)
        assert effective_schema == "test_schema"

        # Test with custom schema
        effective_schema = builder._get_effective_schema("custom_schema")
        assert effective_schema == "custom_schema"

    def test_validate_schema_existing(self, mock_spark_session):
        """Test _validate_schema with existing schema."""
        # Create the schema in mock-spark
        mock_spark_session.catalog.createDatabase("test_schema")

        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Should not raise exception
        builder._validate_schema("test_schema")

    def test_validate_schema_nonexistent(self, mock_spark_session):
        """Test _validate_schema with nonexistent schema."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Try to validate a schema that doesn't exist
        with pytest.raises(ExecutionError):
            builder._validate_schema("nonexistent_schema_that_does_not_exist")

    def test_create_schema_if_not_exists(self, mock_spark_session):
        """Test _create_schema_if_not_exists method."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Create a new schema
        builder._create_schema_if_not_exists("new_schema_to_test")

        # Verify schema was created by listing databases (works in mock-spark 1.4.0+)
        dbs = mock_spark_session.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "new_schema_to_test" in db_names

    def test_create_schema_if_not_exists_failure(self, mock_spark_session):
        """Test _create_schema_if_not_exists with failure."""
        from unittest.mock import patch

        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Patch the spark.sql method to raise exception
        with patch.object(
            mock_spark_session,
            "sql",
            side_effect=Exception("Permission denied"),
        ):
            with pytest.raises(ExecutionError):
                builder._create_schema_if_not_exists("new_schema")


class TestClassMethods:
    """Tests for class methods."""

    def test_for_development(self, mock_spark_session):
        """Test for_development class method."""
        builder = PipelineBuilder.for_development(
            spark=mock_spark_session, schema="test_schema"
        )

        assert isinstance(builder, PipelineBuilder)
        assert builder.schema == "test_schema"
        assert builder.config.verbose is True  # Development should be verbose

    def test_for_production(self, mock_spark_session):
        """Test for_production class method."""
        builder = PipelineBuilder.for_production(
            spark=mock_spark_session, schema="test_schema"
        )

        assert isinstance(builder, PipelineBuilder)
        assert builder.schema == "test_schema"
        assert builder.config.verbose is False  # Production should not be verbose


class TestIntegration:
    """Integration tests for PipelineBuilder."""

    def test_complete_pipeline_workflow(self, mock_spark_session):
        """Test complete pipeline workflow from start to finish."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add bronze step
        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": ["not_null"], "timestamp": ["not_null"]},
            incremental_col="timestamp",
        )

        # Add silver transform
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="raw_events",
            transform=lambda spark, df, silvers: df.filter(F.col("user_id") > 0),
            rules={"user_id": ["not_null"], "timestamp": ["not_null"]},
            table_name="clean_events",
            watermark_col="timestamp",
        )

        # Add gold transform
        builder.add_gold_transform(
            name="user_analytics",
            source_silvers=["clean_events"],
            transform=lambda spark, silvers: silvers["clean_events"]
            .groupBy("user_id")
            .count(),
            rules={"user_id": ["not_null"], "count": ["not_null"]},
            table_name="user_analytics",
        )

        # Validate pipeline
        errors = builder.validate_pipeline()
        assert errors == []

        # Build pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None
        assert len(pipeline.bronze_steps) == 1
        assert len(pipeline.silver_steps) == 1
        assert len(pipeline.gold_steps) == 1

    def test_pipeline_with_custom_validator(self, mock_spark_session):
        """Test pipeline with custom validator."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Add custom validator
        class CustomValidator:
            def validate(self, step, context):
                if step.name == "test_step":
                    return ["Custom validation error"]
                return []

        builder.add_validator(CustomValidator())

        # Add step that will trigger custom validation
        builder.with_bronze_rules(name="test_step", rules={"id": ["not_null"]})

        # Validation should pass (custom validator doesn't trigger for bronze steps)
        errors = builder.validate_pipeline()
        assert errors == []

    def test_pipeline_with_multiple_schemas(self, mock_spark_session):
        """Test pipeline with multiple schemas."""
        builder = PipelineBuilder(spark=mock_spark_session, schema="test_schema", functions=MockF)

        # Mock schema validation
        with patch.object(builder, "_validate_schema"):
            # Add bronze step in default schema
            builder.with_bronze_rules(name="raw_data", rules={"id": ["not_null"]})

            # Add silver step in different schema
            builder.add_silver_transform(
                name="processed_data",
                source_bronze="raw_data",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="processed_data",
                schema="processing_schema",
            )

            # Add gold step in another schema
            builder.add_gold_transform(
                name="analytics",
                source_silvers=["processed_data"],
                transform=lambda spark, silvers: silvers["processed_data"],
                rules={"id": ["not_null"]},
                table_name="analytics",
                schema="analytics_schema",
            )

            # Validate and build
            errors = builder.validate_pipeline()
            assert errors == []

            pipeline = builder.to_pipeline()
            assert pipeline is not None
