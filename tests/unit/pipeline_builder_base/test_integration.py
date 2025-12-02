"""
Integration tests for pipeline_builder_base components.

Tests component interactions and full workflows.
"""

from pipeline_builder_base.builder.base_builder import BasePipelineBuilder
from pipeline_builder_base.config import create_development_config
from pipeline_builder_base.runner.base_runner import BaseRunner


class TestBuilderRunnerIntegration:
    """Test builder and runner working together."""

    def test_builder_runner_integration(self, logger):
        """Test builder and runner integration."""
        config = create_development_config("test_schema")
        builder = BasePipelineBuilder(config=config, logger=logger)
        runner = BaseRunner(config=config, logger=logger)

        # Both should use same config
        assert builder.config == runner.config
        assert builder.logger == runner.logger

    def test_validator_builder_integration(self, logger):
        """Test validator with builder."""
        config = create_development_config("test_schema")
        builder = BasePipelineBuilder(config=config, logger=logger)

        # Builder should have validator
        assert builder.validator is not None

        # Test validation
        errors = builder.validate_pipeline()
        assert isinstance(errors, list)

    def test_config_factory_validation(self):
        """Test config factories with validators."""
        from pipeline_builder_base.config.validators import validate_pipeline_config

        dev_config = create_development_config("dev_schema")
        errors = validate_pipeline_config(dev_config)
        assert errors == []

        prod_config = create_development_config("prod_schema")
        errors = validate_pipeline_config(prod_config)
        assert errors == []

    def test_dependency_graph_builder(self):
        """Test dependency graph with builder."""
        from pipeline_builder_base.builder.step_classifier import StepClassifier
        from pipeline_builder_base.dependencies.graph import DependencyGraph

        class MockBronzeStep:
            pass

        class MockSilverStep:
            def __init__(self):
                self.source_bronze = "bronze1"

        bronze_steps = {"bronze1": MockBronzeStep()}
        silver_steps = {"silver1": MockSilverStep()}
        gold_steps = {}

        graph = StepClassifier.build_dependency_graph(
            bronze_steps, silver_steps, gold_steps
        )

        assert isinstance(graph, DependencyGraph)
        assert "bronze1" in graph.nodes
        assert "silver1" in graph.nodes

    def test_error_context_full_flow(self):
        """Test error context in full execution flow."""
        from pipeline_builder_base.errors.context import build_execution_context

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.type = "bronze"

        step = MockStep()
        error = ValueError("Test error")

        context = build_execution_context(step, error)

        assert context["step_name"] == "step1"
        assert context["error_type"] == "ValueError"
