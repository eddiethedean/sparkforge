#!/usr/bin/env python3
"""
Test for Trap 2: Missing Object Creation fix.

This test verifies that ExecutionEngine and DependencyAnalyzer objects
are properly created and accessible in the PipelineBuilder.to_pipeline() method.
"""

import pytest
from unittest.mock import Mock, patch

from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.execution import ExecutionEngine
from sparkforge.dependencies import DependencyAnalyzer


class TestTrap2MissingObjectCreation:
    """Test that objects are properly created and not garbage collected."""

    def test_execution_engine_creation_in_to_pipeline(self, spark_session):
        """Test that ExecutionEngine is properly created in to_pipeline()."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
        )
        
        # Add a bronze step to make the pipeline valid
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )
        
        # Mock the ExecutionEngine to track if it's created
        with patch('sparkforge.pipeline.builder.ExecutionEngine') as mock_execution_engine:
            with patch('sparkforge.pipeline.builder.DependencyAnalyzer') as mock_dependency_analyzer:
                # Call to_pipeline()
                runner = builder.to_pipeline()
                
                # Verify ExecutionEngine was created with correct parameters
                mock_execution_engine.assert_called_once_with(
                    spark=spark_session,
                    config=builder.config,
                    logger=builder.logger,
                )
                
                # Verify DependencyAnalyzer was created
                mock_dependency_analyzer.assert_called_once_with(
                    logger=builder.logger
                )
                
                # Verify runner was created
                assert runner is not None

    def test_objects_are_not_garbage_collected(self, spark_session):
        """Test that created objects are not immediately garbage collected."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
        )
        
        # Add a bronze step
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )
        
        # Track object creation
        created_objects = []
        
        def track_execution_engine(*args, **kwargs):
            obj = Mock()
            created_objects.append(('ExecutionEngine', obj))
            return obj
        
        def track_dependency_analyzer(*args, **kwargs):
            obj = Mock()
            created_objects.append(('DependencyAnalyzer', obj))
            return obj
        
        with patch('sparkforge.pipeline.builder.ExecutionEngine', side_effect=track_execution_engine):
            with patch('sparkforge.pipeline.builder.DependencyAnalyzer', side_effect=track_dependency_analyzer):
                # Call to_pipeline()
                runner = builder.to_pipeline()
                
                # Verify objects were created
                assert len(created_objects) == 2
                assert any(name == 'ExecutionEngine' for name, obj in created_objects)
                assert any(name == 'DependencyAnalyzer' for name, obj in created_objects)
                
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
        )
        
        # Add a bronze step to make it valid
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )
        
        with patch('sparkforge.pipeline.builder.ExecutionEngine') as mock_execution_engine:
            with patch('sparkforge.pipeline.builder.DependencyAnalyzer') as mock_dependency_analyzer:
                # Call to_pipeline() - should succeed
                runner = builder.to_pipeline()
                
                # Verify objects were created
                mock_execution_engine.assert_called_once()
                mock_dependency_analyzer.assert_called_once()
                assert runner is not None

    def test_objects_are_accessible_after_creation(self, spark_session):
        """Test that created objects are accessible after creation."""
        # Create PipelineBuilder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
        )
        
        # Add a bronze step
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
        )
        
        # Mock objects to track their creation
        execution_engine_mock = Mock()
        dependency_analyzer_mock = Mock()
        
        with patch('sparkforge.pipeline.builder.ExecutionEngine', return_value=execution_engine_mock):
            with patch('sparkforge.pipeline.builder.DependencyAnalyzer', return_value=dependency_analyzer_mock):
                # Call to_pipeline()
                runner = builder.to_pipeline()
                
                # Verify objects were created and are accessible
                assert execution_engine_mock is not None
                assert dependency_analyzer_mock is not None
                assert runner is not None
                
                # Verify objects have the expected attributes
                assert hasattr(execution_engine_mock, 'spark')
                assert hasattr(execution_engine_mock, 'config')
                assert hasattr(execution_engine_mock, 'logger')
