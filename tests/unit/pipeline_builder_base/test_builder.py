"""
Comprehensive tests for pipeline_builder_base builder module.

Tests BasePipelineBuilder, helpers, and step_classifier.
"""

import pytest

from pipeline_builder_base.builder.base_builder import BasePipelineBuilder
from pipeline_builder_base.builder.helpers import (
    create_bronze_step_dict,
    create_gold_step_dict,
    create_silver_step_dict,
)
from pipeline_builder_base.builder.step_classifier import StepClassifier
from pipeline_builder_base.errors import ValidationError


class TestBasePipelineBuilder:
    """Test BasePipelineBuilder class."""

    def test_base_builder_initialization(self, pipeline_config, logger):
        """Test constructor with config and logger."""
        builder = BasePipelineBuilder(config=pipeline_config, logger=logger)

        assert builder.config == pipeline_config
        assert builder.logger == logger
        assert builder.validator is not None
        assert builder.step_validator is not None
        assert builder.bronze_steps == {}
        assert builder.silver_steps == {}
        assert builder.gold_steps == {}

    def test_base_builder_default_logger(self, pipeline_config):
        """Test default logger creation."""
        builder = BasePipelineBuilder(config=pipeline_config)

        assert builder.logger is not None
        assert builder.logger.name == "PipelineRunner"

    def test_check_duplicate_step_name_bronze(self, pipeline_config):
        """Test duplicate name detection for bronze."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.bronze_steps["existing_step"] = {"name": "existing_step"}

        with pytest.raises(ValidationError) as exc_info:
            builder._check_duplicate_step_name("existing_step", "bronze")

        assert "Bronze step 'existing_step' already exists" in str(exc_info.value)

    def test_check_duplicate_step_name_silver(self, pipeline_config):
        """Test duplicate name detection for silver."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.silver_steps["existing_step"] = {"name": "existing_step"}

        with pytest.raises(ValidationError) as exc_info:
            builder._check_duplicate_step_name("existing_step", "silver")

        assert "Silver step 'existing_step' already exists" in str(exc_info.value)

    def test_check_duplicate_step_name_gold(self, pipeline_config):
        """Test duplicate name detection for gold."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.gold_steps["existing_step"] = {"name": "existing_step"}

        with pytest.raises(ValidationError) as exc_info:
            builder._check_duplicate_step_name("existing_step", "gold")

        assert "Gold step 'existing_step' already exists" in str(exc_info.value)

    def test_check_duplicate_step_name_no_duplicate(self, pipeline_config):
        """Test no error when step name is unique."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.bronze_steps["existing_step"] = {"name": "existing_step"}

        # Should not raise
        builder._check_duplicate_step_name("new_step", "bronze")

    def test_validate_step_dependencies_silver_valid(self, pipeline_config):
        """Test silver step dependency validation with valid dependency."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.bronze_steps["bronze_step"] = {"name": "bronze_step"}

        class MockSilverStep:
            def __init__(self):
                self.name = "silver_step"
                self.source_bronze = "bronze_step"

        step = MockSilverStep()
        # Should not raise
        builder._validate_step_dependencies(step, "silver")

    def test_validate_step_dependencies_silver_missing(self, pipeline_config):
        """Test silver step dependency validation with missing dependency."""
        builder = BasePipelineBuilder(config=pipeline_config)

        class MockSilverStep:
            def __init__(self):
                self.name = "silver_step"
                self.source_bronze = "missing_bronze"

        step = MockSilverStep()

        with pytest.raises(ValidationError) as exc_info:
            builder._validate_step_dependencies(step, "silver")

        assert "Bronze step 'missing_bronze' not found" in str(exc_info.value)

    def test_validate_step_dependencies_gold_valid(self, pipeline_config):
        """Test gold step dependency validation with valid dependencies."""
        builder = BasePipelineBuilder(config=pipeline_config)
        builder.silver_steps["silver_step"] = {"name": "silver_step"}

        class MockGoldStep:
            def __init__(self):
                self.name = "gold_step"
                self.source_silvers = ["silver_step"]

        step = MockGoldStep()
        # Should not raise
        builder._validate_step_dependencies(step, "gold")

    def test_validate_step_dependencies_gold_missing(self, pipeline_config):
        """Test gold step dependency validation with missing dependency."""
        builder = BasePipelineBuilder(config=pipeline_config)

        class MockGoldStep:
            def __init__(self):
                self.name = "gold_step"
                self.source_silvers = ["missing_silver"]

        step = MockGoldStep()

        with pytest.raises(ValidationError) as exc_info:
            builder._validate_step_dependencies(step, "gold")

        assert "Silver step 'missing_silver' not found" in str(exc_info.value)

    def test_validate_step_dependencies_gold_invalid_list(self, pipeline_config):
        """Test gold step dependency validation with invalid source_silvers type."""
        builder = BasePipelineBuilder(config=pipeline_config)

        class MockGoldStep:
            def __init__(self):
                self.name = "gold_step"
                self.source_silvers = "not_a_list"  # Should be a list

        step = MockGoldStep()

        with pytest.raises(ValidationError) as exc_info:
            builder._validate_step_dependencies(step, "gold")

        assert "Gold step source_silvers must be a list" in str(exc_info.value)

    def test_validate_step_dependencies_auto_inference(self, pipeline_config):
        """Test auto-inference of dependencies when available_sources provided."""
        builder = BasePipelineBuilder(config=pipeline_config)
        custom_sources = {"custom_bronze": {"name": "custom_bronze"}}

        class MockSilverStep:
            def __init__(self):
                self.name = "silver_step"
                self.source_bronze = "custom_bronze"

        step = MockSilverStep()
        # Should not raise
        builder._validate_step_dependencies(
            step, "silver", available_sources=custom_sources
        )

    def test_validate_schema_valid(self, pipeline_config):
        """Test schema validation with valid schema."""
        builder = BasePipelineBuilder(config=pipeline_config)
        # Should not raise
        builder._validate_schema("valid_schema")

    def test_validate_schema_invalid(self, pipeline_config):
        """Test schema validation with invalid schema."""
        builder = BasePipelineBuilder(config=pipeline_config)

        with pytest.raises(ValidationError):
            builder._validate_schema("")  # Empty schema

    def test_validate_pipeline(self, pipeline_config):
        """Test pipeline validation."""
        builder = BasePipelineBuilder(config=pipeline_config)

        class MockBronzeStep:
            def __init__(self):
                self.name = "bronze_step"
                self.rules = {"user_id": ["not_null"]}

        builder.bronze_steps["bronze_step"] = MockBronzeStep()

        errors = builder.validate_pipeline()
        assert isinstance(errors, list)


class TestStepClassifier:
    """Test StepClassifier class."""

    def test_classify_step_type_bronze(self):
        """Test bronze step classification."""

        class BronzeStep:
            pass

        step = BronzeStep()
        result = StepClassifier.classify_step_type(step)
        assert result == "bronze"

    def test_classify_step_type_silver(self):
        """Test silver step classification."""

        class SilverStep:
            pass

        step = SilverStep()
        result = StepClassifier.classify_step_type(step)
        assert result == "silver"

    def test_classify_step_type_gold(self):
        """Test gold step classification."""

        class GoldStep:
            pass

        step = GoldStep()
        result = StepClassifier.classify_step_type(step)
        assert result == "gold"

    def test_classify_step_type_from_attribute(self):
        """Test step type classification from type attribute."""

        class MockStep:
            def __init__(self):
                self.type = "bronze"

        step = MockStep()
        result = StepClassifier.classify_step_type(step)
        assert result == "bronze"

    def test_classify_step_type_unknown(self):
        """Test unknown step type classification."""

        class UnknownStep:
            pass

        step = UnknownStep()
        result = StepClassifier.classify_step_type(step)
        assert result == "unknown"

    def test_extract_step_dependencies_silver(self):
        """Test dependency extraction from silver step."""

        class MockSilverStep:
            def __init__(self):
                self.source_bronze = "bronze_step"

        step = MockSilverStep()
        deps = StepClassifier.extract_step_dependencies(step)
        assert deps == ["bronze_step"]

    def test_extract_step_dependencies_gold(self):
        """Test dependency extraction from gold step."""

        class MockGoldStep:
            def __init__(self):
                self.source_silvers = ["silver1", "silver2"]

        step = MockGoldStep()
        deps = StepClassifier.extract_step_dependencies(step)
        assert set(deps) == {"silver1", "silver2"}

    def test_extract_step_dependencies_none(self):
        """Test dependency extraction from step with no dependencies."""

        class MockStep:
            pass

        step = MockStep()
        deps = StepClassifier.extract_step_dependencies(step)
        assert deps == []

    def test_group_steps_by_type(self):
        """Test grouping steps by type."""
        bronze_steps = {"b1": "bronze1"}
        silver_steps = {"s1": "silver1"}
        gold_steps = {"g1": "gold1"}

        result = StepClassifier.group_steps_by_type(
            bronze_steps, silver_steps, gold_steps
        )

        assert result["bronze"] == bronze_steps
        assert result["silver"] == silver_steps
        assert result["gold"] == gold_steps

    def test_get_all_step_names(self):
        """Test getting all step names."""
        bronze_steps = {"b1": "bronze1", "b2": "bronze2"}
        silver_steps = {"s1": "silver1"}
        gold_steps = {"g1": "gold1"}

        names = StepClassifier.get_all_step_names(
            bronze_steps, silver_steps, gold_steps
        )

        assert names == {"b1", "b2", "s1", "g1"}

    def test_build_dependency_graph(self):
        """Test building dependency graph."""

        class MockBronzeStep:
            pass

        class MockSilverStep:
            def __init__(self):
                self.source_bronze = "bronze1"

        class MockGoldStep:
            def __init__(self):
                self.source_silvers = ["silver1"]

        bronze_steps = {"bronze1": MockBronzeStep()}
        silver_steps = {"silver1": MockSilverStep()}
        gold_steps = {"gold1": MockGoldStep()}

        graph = StepClassifier.build_dependency_graph(
            bronze_steps, silver_steps, gold_steps
        )

        # Graph should have nodes for all steps
        assert len(graph.nodes) == 3
        assert "bronze1" in graph.nodes
        assert "silver1" in graph.nodes
        assert "gold1" in graph.nodes

        # Check dependencies
        assert "bronze1" in graph.get_dependencies("silver1")
        assert "silver1" in graph.get_dependencies("gold1")

    def test_get_execution_order(self):
        """Test getting execution order."""

        class MockBronzeStep:
            pass

        class MockSilverStep:
            def __init__(self):
                self.source_bronze = "bronze1"

        bronze_steps = {"bronze1": MockBronzeStep()}
        silver_steps = {"silver1": MockSilverStep()}
        gold_steps = {}

        order = StepClassifier.get_execution_order(
            bronze_steps, silver_steps, gold_steps
        )

        assert "bronze1" in order
        assert "silver1" in order
        # Bronze should come before silver in execution order
        assert order.index("bronze1") < order.index("silver1")


class TestHelperFunctions:
    """Test helper functions for creating step dictionaries."""

    def test_create_bronze_step_dict(self):
        """Test bronze step dict creation."""
        result = create_bronze_step_dict(
            name="test_bronze",
            rules={"user_id": ["not_null"]},
            incremental_col="timestamp",
        )

        assert result["name"] == "test_bronze"
        assert result["rules"] == {"user_id": ["not_null"]}
        assert result["incremental_col"] == "timestamp"

    def test_create_bronze_step_dict_with_metadata(self):
        """Test bronze step dict with metadata."""
        result = create_bronze_step_dict(
            name="test_bronze",
            rules={"user_id": ["not_null"]},
            custom_field="custom_value",
        )

        assert result["custom_field"] == "custom_value"

    def test_create_silver_step_dict(self):
        """Test silver step dict creation."""

        def transform_func(x):
            return x

        result = create_silver_step_dict(
            name="test_silver",
            source_bronze="bronze_step",
            transform=transform_func,
            rules={"user_id": ["not_null"]},
            table_name="silver_table",
        )

        assert result["name"] == "test_silver"
        assert result["source_bronze"] == "bronze_step"
        assert result["transform"] == transform_func
        assert result["table_name"] == "silver_table"

    def test_create_silver_step_dict_with_watermark(self):
        """Test silver step dict with watermark."""
        result = create_silver_step_dict(
            name="test_silver",
            source_bronze="bronze_step",
            transform=lambda x: x,
            rules={},
            table_name="silver_table",
            watermark_col="timestamp",
        )

        assert result["watermark_col"] == "timestamp"

    def test_create_gold_step_dict(self):
        """Test gold step dict creation."""

        def transform_func(x):
            return x

        result = create_gold_step_dict(
            name="test_gold",
            transform=transform_func,
            rules={"metric": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1", "silver2"],
        )

        assert result["name"] == "test_gold"
        assert result["transform"] == transform_func
        assert result["table_name"] == "gold_table"
        assert result["source_silvers"] == ["silver1", "silver2"]

    def test_create_gold_step_dict_no_sources(self):
        """Test gold step dict without source_silvers."""
        result = create_gold_step_dict(
            name="test_gold",
            transform=lambda x: x,
            rules={},
            table_name="gold_table",
        )

        assert result["source_silvers"] == []
