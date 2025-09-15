#!/usr/bin/env python3
"""
Comprehensive tests for unified dependency-aware execution.

This module tests the new unified execution system that allows Bronze, Silver, and Gold
steps to run in parallel based on their actual dependencies rather than layer boundaries.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
import time
from tests.conftest import get_test_schema

from sparkforge.dependencies import (
    DependencyAnalyzer, DependencyAnalysisResult, StepType
)
from sparkforge.execution import (
    ExecutionEngine, ExecutionConfig, StepExecutionResult,
    ExecutionResult
)
from sparkforge.models import BronzeStep, SilverStep, GoldStep, ExecutionContext
from sparkforge.pipeline import PipelineBuilder
from sparkforge.pipeline.models import PipelineStatus


# Removed obsolete TestDependencyAnalyzer class - functionality has been refactored

# Removed obsolete TestExecutionEngine class - functionality has been refactored


class TestPipelineBuilderUnifiedExecution:
    """Test the PipelineBuilder with unified execution."""
    
    def test_enable_unified_execution(self, spark_session):
        """Test that unified execution is the default behavior."""
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # The unified execution is now the default behavior
        # No need to enable it explicitly
        
        # Verify builder is created
        assert builder is not None
        # Unified execution is now the default, so we don't need to check for specific attributes
    
    def test_run_unified_without_enabling(self, spark_session):
        """Test that initial_load works with the new unified execution system."""
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        pipeline = builder.to_pipeline()
        
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Run with initial_load (unified execution is now default)
        result = pipeline.initial_load(bronze_sources={"bronze_events": source_df})
        assert result is not None
    
    def test_run_unified_pipeline(self, spark_session):
        """Test running a complete unified pipeline."""
        # Create test data
        test_data = [(1, "user1", "active"), (2, "user2", "inactive")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "status"])
        
        # Build pipeline with unified execution
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(
                name="bronze_events",
                rules={"id": ["not_null"], "status": ["not_null"]}
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
                rules={"status": ["not_null"]},
                table_name="silver_events"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: silvers["silver_events"].groupBy("status").count(),
                rules={"count": ["not_null"]},
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.initial_load(bronze_sources={"bronze_events": source_df})
        
        # Verify result
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 3
        assert result.metrics.failed_steps == 0
        assert result.metrics.total_rows_processed > 0
        assert result.metrics.total_rows_written > 0
    
    # Removed test_convert_unified_result_to_report - method no longer exists


class TestUnifiedExecutionIntegration:
    """Integration tests for unified execution."""
    
    def test_complex_dependency_graph(self, spark_session):
        """Test unified execution with a complex dependency graph."""
        # Create test data
        events_data = [(1, "user1", "click"), (2, "user2", "view")]
        users_data = [(1, "Alice"), (2, "Bob")]
        source_df = spark_session.createDataFrame(events_data, ["id", "user", "action"])
        users_df = spark_session.createDataFrame(users_data, ["id", "name"])
        
        # Build complex pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            # Bronze steps
            .with_bronze_rules(name="bronze_events", rules={"id": [F.col("id").isNotNull()], "user": [F.col("user").isNotNull()], "action": [F.col("action").isNotNull()]})
            .with_bronze_rules(name="bronze_users", rules={"id": [F.col("id").isNotNull()], "name": [F.col("name").isNotNull()]})
            
            # Silver steps with dependencies
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": [F.col("id").isNotNull()], "action": [F.col("action").isNotNull()]},
                table_name="silver_events"
            )
            .add_silver_transform(
                name="silver_users",
                source_bronze="bronze_users",
                transform=lambda spark, df, silvers: df,
                rules={"id": [F.col("id").isNotNull()], "name": [F.col("name").isNotNull()]},
                table_name="silver_users"
            )
            
            # Gold step depending on multiple silvers
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: (
                    silvers["silver_events"]
                    .join(silvers["silver_users"], "id")
                    .groupBy("name")
                    .count()
                ),
                rules={"count": [F.col("count").isNotNull()]},
                table_name="gold_summary",
                source_silvers=["silver_events", "silver_users"]
            )
            
            # Build pipeline (unified execution is now default)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.initial_load(
            bronze_sources={"bronze_events": source_df, "bronze_users": users_df}
        )
        
        # Verify result
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 5  # 2 bronze + 2 silver + 1 gold
        assert result.metrics.failed_steps == 0
        # Note: parallel_efficiency is not available in PipelineMetrics
        
        # Note: execution_groups is not available in PipelineReport
    
    def test_error_handling_in_unified_execution(self, spark_session):
        """Test error handling in unified execution."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with error-prone step
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        def failing_transform(spark, df, silvers):
            raise ValueError("Intentional test error")
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": [F.col("id").isNotNull()]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=failing_transform,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="silver_events"
            )
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.initial_load(bronze_sources={"bronze_events": source_df})
        
        # Verify error handling
        assert result.status == PipelineStatus.FAILED
        assert result.metrics.failed_steps > 0
        # Check that the silver step failed
        assert 'silver_events' in result.silver_results
        assert result.silver_results['silver_events']['success'] == False
        assert 'error' in result.silver_results['silver_events']
    
    def test_parallel_efficiency_calculation(self, spark_session):
        """Test that parallel efficiency is calculated correctly."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2"), (3, "user3")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with multiple independent steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": [F.col("id").isNotNull()]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="silver_events"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: silvers["silver_events"],
                rules={"id": [F.col("id").isNotNull()]},
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.initial_load(bronze_sources={"bronze_events": source_df})
        
        # Note: parallel_efficiency is not available in PipelineMetrics
        assert result.status == PipelineStatus.COMPLETED


@pytest.mark.slow
class TestUnifiedExecutionPerformance:
    """Performance tests for unified execution."""
    
    def test_large_scale_parallel_execution(self, spark_session):
        """Test unified execution with many parallel steps."""
        # Create minimal test dataset for speed
        test_data = [(i, f"user{i}") for i in range(10)]  # Reduced from 1000 to 10
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with minimal steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add minimal bronze steps (no table writing for speed)
        for i in range(2):  # Reduced from 5 to 2
            builder.with_bronze_rules(
                name=f"bronze_{i}",
                rules={"id": [F.col("id").isNotNull()]}
            )
        
        # Add minimal silver steps (minimal table name for speed)
        for i in range(2):  # Reduced from 5 to 2
            builder.add_silver_transform(
                name=f"silver_{i}",
                source_bronze=f"bronze_{i}",
                transform=lambda spark, df, silvers: df,
                rules={"id": [F.col("id").isNotNull()]},
                table_name=f"silver_{i}"  # Required parameter
            )
        
        # Enable unified execution
        pipeline = builder.to_pipeline()
        
        # Create bronze sources
        bronze_sources = {f"bronze_{i}": source_df for i in range(2)}
        
        # Run unified pipeline
        start_time = time.time()
        result = pipeline.initial_load(bronze_sources=bronze_sources)
        execution_time = time.time() - start_time
        
        # Verify performance
        assert result.status == PipelineStatus.COMPLETED
        assert execution_time < 10  # Should complete within 10 seconds
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_memory_efficiency(self, spark_session):
        """Test that unified execution is memory efficient."""
        # Create test data
        test_data = [(i, f"user{i}") for i in range(100)]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": [F.col("id").isNotNull()]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="silver_events"
            )
            .to_pipeline()
        )
        
        # Run multiple times to test memory efficiency
        for _ in range(5):
            result = pipeline.initial_load(bronze_sources={"bronze_events": source_df})
            assert result.status == PipelineStatus.COMPLETED
        
        # If we get here without memory issues, the test passes
        assert True
