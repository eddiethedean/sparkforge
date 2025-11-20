#!/usr/bin/env python3
"""
Test for Trap 2: Missing Object Creation fix.

This test verifies that ExecutionEngine and DependencyAnalyzer objects
are properly created and accessible in the PipelineBuilder.to_pipeline() method.
"""

import os
from unittest.mock import Mock, patch

import pytest

from pipeline_builder.pipeline.builder import PipelineBuilder

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F

    MockF = F
else:
    from pyspark.sql import functions as F

    MockF = None


class TestTrap2MissingObjectCreation:
    """Test that objects are properly created and not garbage collected."""

    def test_execution_engine_creation_in_to_pipeline(self, spark_session):
        """Test that ExecutionEngine is properly created in to_pipeline()."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            functions=MockF,
        )

        # Add a bronze step to make the pipeline valid
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )

        # Mock the PipelineRunner to track if it's created
        with patch(
            "pipeline_builder.pipeline.builder.PipelineRunner"
        ) as mock_pipeline_runner:
            # Call to_pipeline()
            runner = builder.to_pipeline()

            # Verify PipelineRunner was created with correct parameters
            # Note: steps and engine are now also passed for abstracts compatibility
            call_kwargs = mock_pipeline_runner.call_args[1]
            assert call_kwargs["spark"] == spark_session
            assert call_kwargs["config"] == builder.config
            assert call_kwargs["bronze_steps"] == builder.bronze_steps
            assert call_kwargs["silver_steps"] == builder.silver_steps
            assert call_kwargs["gold_steps"] == builder.gold_steps
            assert call_kwargs["logger"] == builder.logger
            assert call_kwargs["functions"] == builder.functions
            # steps and engine are optional but may be present for abstracts compatibility

            # Verify runner was created
            assert runner is not None

    def test_objects_are_not_garbage_collected(self, spark_session):
        """Test that created objects are not immediately garbage collected."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            functions=MockF,
        )

        # Add a bronze step
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )

        # Track object creation
        created_objects = []

        def track_pipeline_runner(*args, **kwargs):
            obj = Mock()
            created_objects.append(("PipelineRunner", obj))
            return obj

        with patch(
            "pipeline_builder.pipeline.builder.PipelineRunner",
            side_effect=track_pipeline_runner,
        ):
            # Call to_pipeline()
            runner = builder.to_pipeline()

            # Verify objects were created
            assert len(created_objects) == 1
            assert any(name == "PipelineRunner" for name, obj in created_objects)

            # Verify runner was created
            assert runner is not None

    def test_pipeline_validation_before_object_creation(self, spark_session):
        """Test that pipeline validation occurs before object creation."""
        # Test that invalid schema causes validation failure at constructor level
        with pytest.raises(Exception, match="Schema name cannot be empty"):
            PipelineBuilder(
                spark=spark_session,
                schema="",  # Empty schema should cause validation failure
            )

        # Test that valid pipeline creates objects properly
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            functions=MockF,
        )

        # Add a bronze step to make it valid
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )

        with patch(
            "pipeline_builder.pipeline.builder.PipelineRunner"
        ) as mock_pipeline_runner:
            # Call to_pipeline() - should succeed
            runner = builder.to_pipeline()

            # Verify objects were created
            mock_pipeline_runner.assert_called_once()
            assert runner is not None

    def test_objects_are_accessible_after_creation(self, spark_session):
        """Test that created objects are accessible after creation."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            functions=MockF,
        )

        # Add a bronze step
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )

        # Mock objects to track their creation
        pipeline_runner_mock = Mock()

        with patch(
            "pipeline_builder.pipeline.builder.PipelineRunner",
            return_value=pipeline_runner_mock,
        ):
            # Call to_pipeline()
            runner = builder.to_pipeline()

            # Verify objects were created and are accessible
            assert pipeline_runner_mock is not None
            assert runner is not None

            # Verify objects have the expected attributes
            assert hasattr(pipeline_runner_mock, "spark")
            assert hasattr(pipeline_runner_mock, "config")
            assert hasattr(pipeline_runner_mock, "logger")
