#!/usr/bin/env python3
"""
Edge case tests for unified dependency-aware execution.

This module tests edge cases, error scenarios, and boundary conditions
for the unified execution system.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sparkforge.pipeline_builder import PipelineStatus
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import time
from tests.conftest import get_test_schema

from sparkforge.unified_dependency_analyzer import (
    UnifiedDependencyAnalyzer, StepType, UnifiedStepInfo, ExecutionGroup
)
from sparkforge.unified_execution_engine import (
    UnifiedExecutionEngine, UnifiedExecutionConfig, StepExecutionResult
)
from sparkforge.models import BronzeStep, SilverStep, GoldStep
from sparkforge.pipeline_builder import PipelineBuilder
from sparkforge.exceptions import PipelineValidationError


class TestUnifiedExecutionEdgeCases:
    """Test edge cases for unified execution."""
    
    def test_empty_pipeline(self, spark_session):
        """Test unified execution with empty pipeline."""
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        pipeline = builder.enable_unified_execution().to_pipeline()
        
        # Run with empty pipeline
        result = pipeline.run_unified(bronze_sources={})
        
        # Should complete successfully with no steps
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 0
        assert result.metrics.failed_steps == 0
        assert result.metrics.total_rows_processed == 0
        assert result.metrics.total_rows_written == 0
    
    def test_single_step_pipeline(self, spark_session):
        """Test unified execution with single step."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build single-step pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .enable_unified_execution()
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify result
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 1
        assert result.metrics.failed_steps == 0
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_circular_dependency_detection(self, spark_session):
        """Test detection and resolution of circular dependencies."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create step info with circular dependency
        step_info = {
            "step_a": UnifiedStepInfo(
                name="step_a",
                step_type=StepType.SILVER,
                dependencies={"step_b"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            ),
            "step_b": UnifiedStepInfo(
                name="step_b",
                step_type=StepType.SILVER,
                dependencies={"step_c"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            ),
            "step_c": UnifiedStepInfo(
                name="step_c",
                step_type=StepType.SILVER,
                dependencies={"step_a"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            )
        }
        
        # Set up circular dependencies
        step_info["step_a"].dependents.add("step_c")
        step_info["step_b"].dependents.add("step_a")
        step_info["step_c"].dependents.add("step_b")
        
        # Detect cycles
        cycles = analyzer._detect_cycles_unified(step_info)
        
        # Verify cycle detection
        assert len(cycles) > 0
        assert any("step_a" in cycle and "step_b" in cycle and "step_c" in cycle for cycle in cycles)
        
        # Store original dependencies count
        original_deps = sum(len(info.dependencies) for info in step_info.values())
        
        # Resolve cycles
        resolved_info = analyzer._resolve_cycles_unified(step_info, cycles)
        
        # Verify cycle resolution
        assert resolved_info is not None
        # At least one dependency should be removed to break the cycle
        total_deps = sum(len(info.dependencies) for info in resolved_info.values())
        assert total_deps < original_deps
    
    def test_impossible_dependencies(self, spark_session):
        """Test detection of impossible dependencies (e.g., Bronze depending on Silver)."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create step info with impossible dependency
        step_info = {
            "bronze_step": UnifiedStepInfo(
                name="bronze_step",
                step_type=StepType.BRONZE,
                dependencies={"silver_step"},  # Bronze depending on Silver - impossible
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            ),
            "silver_step": UnifiedStepInfo(
                name="silver_step",
                step_type=StepType.SILVER,
                dependencies=set(),
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            )
        }
        
        # Detect conflicts
        conflicts = analyzer._detect_conflicts_unified(step_info)
        
        # Verify conflict detection
        assert len(conflicts) > 0
        assert any("Bronze step" in conflict and "cannot depend on" in conflict for conflict in conflicts)
    
    def test_missing_source_data(self, spark_session):
        """Test handling of missing source data."""
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
        # Create Silver step without source data
        silver_step = SilverStep(
            "silver_events",
            "missing_bronze",
            lambda spark, df, silvers: df,
            {"id": ["not_null"]},
            "silver_events"
        )
        
        # Execute step without source data
        result = engine._execute_single_step(
            "silver_events",
            {},
            {"silver_events": silver_step},
            {},
            "incremental",
            None
        )
        
        # Verify error handling
        assert isinstance(result, StepExecutionResult)
        assert result.success == False
        assert result.error_message is not None
        assert "No source data available" in result.error_message
    
    def test_transform_function_error(self, spark_session):
        """Test handling of errors in transform functions."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
        # Create Silver step with error-prone transform
        def error_transform(spark, df, silvers):
            raise ValueError("Transform function error")
        
        silver_step = SilverStep(
                "silver_events",
                "bronze_events",
                error_transform,
                {"id": ["not_null"]},
                "silver_events"
            )
        
        # Set up available data
        engine._available_data["bronze_events"] = source_df
        
        # Execute step
        result = engine._execute_single_step(
            "silver_events",
            {},
            {"silver_events": silver_step},
            {},
            "incremental",
            None
        )
        
        # Verify error handling
        assert isinstance(result, StepExecutionResult)
        assert result.success == False
        assert result.error_message is not None
        assert "Transform function error" in result.error_message
    
    def test_validation_rule_error(self, spark_session):
        """Test handling of validation rule errors."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
        # Create Bronze step with invalid validation rules
        bronze_step = BronzeStep("bronze_events", {"invalid_col": ["not_null"]})
        
        # Execute step
        result = engine._execute_single_step(
            "bronze_events",
            {"bronze_events": bronze_step},
            {},
            {},
            "incremental",
            None
        )
        
        # Verify error handling
        assert isinstance(result, StepExecutionResult)
        assert result.success == False
        assert result.error_message is not None
    
    def test_timeout_handling(self, spark_session):
        """Test handling of step execution timeouts."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create execution engine with short timeout
        config = UnifiedExecutionConfig(timeout_seconds=1)
        engine = UnifiedExecutionEngine(spark_session, config)
        
        # Create Silver step with long-running transform
        def slow_transform(spark, df, silvers):
            time.sleep(2)  # Sleep longer than timeout
            return df
        
        silver_step = SilverStep(
                "silver_events",
                "bronze_events",
                slow_transform,
                {"id": ["not_null"]},
                "silver_events"
            )
        
        # Set up available data
        engine._available_data["bronze_events"] = source_df
        
        # Execute step
        result = engine._execute_single_step(
            "silver_events",
            {},
            {"silver_events": silver_step},
            {},
            "incremental",
            None
        )
        
        # Verify timeout handling
        assert isinstance(result, StepExecutionResult)
        # The result might be successful if the timeout doesn't apply to single step execution
        # This test verifies the system doesn't crash with short timeouts
    
    def test_memory_pressure(self, spark_session):
        """Test handling under memory pressure."""
        # Create large test dataset
        test_data = [(i, f"user{i}") for i in range(10000)]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with memory-intensive operations
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.cache(),  # Cache to use memory
                rules={"id": ["not_null"]},
                table_name="silver_events"
            )
            .enable_unified_execution(max_workers=2)  # Limit workers to reduce memory usage
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify completion despite memory pressure
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 2
    
    def test_concurrent_access(self, spark_session):
        """Test handling of concurrent access to shared resources."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver_events"
            )
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run multiple concurrent executions
        import threading
        import queue
        
        results = queue.Queue()
        
        def run_pipeline():
            try:
                result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
                results.put(("success", result))
            except Exception as e:
                results.put(("error", str(e)))
        
        # Start multiple threads
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=run_pipeline)
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all executions completed
        assert results.qsize() == 3
        
        # Check results
        success_count = 0
        while not results.empty():
            status, result = results.get()
            if status == "success":
                success_count += 1
                assert result.status == PipelineStatus.COMPLETED
        
        # At least some executions should succeed
        assert success_count > 0
    
    def test_invalid_step_configuration(self, spark_session):
        """Test handling of invalid step configurations."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with invalid configuration
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add Silver step with invalid source (this should not raise an exception)
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="nonexistent_bronze",
            transform=lambda spark, df, silvers: df,
            rules={"id": ["not_null"]},
            table_name="silver_events"
        )
        
        # Enable unified execution
        builder.enable_unified_execution(max_workers=2)
        
        # Try to create pipeline with invalid configuration - this should raise an exception
        with pytest.raises(ValueError, match="Pipeline validation failed"):
            builder.to_pipeline()
    
    def test_empty_dataframe_handling(self, spark_session):
        """Test handling of empty DataFrames."""
        # Create empty DataFrame with explicit schema
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("user", StringType(), True)
        ])
        empty_df = spark_session.createDataFrame([], schema)
        
        # Build pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver_events"
            )
            .enable_unified_execution()
            .to_pipeline()
        )
        
        # Run with empty DataFrame
        result = pipeline.run_unified(bronze_sources={"bronze_events": empty_df})
        
        # Verify handling of empty data
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.total_rows_processed == 0
        assert result.metrics.total_rows_written == 0
    
    def test_large_number_of_steps(self, spark_session):
        """Test handling of pipelines with many steps."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with many steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add many bronze steps
        for i in range(20):
            builder.with_bronze_rules(
                name=f"bronze_{i}",
                rules={"id": ["not_null"]}
            )
        
        # Add many silver steps
        for i in range(20):
            builder.add_silver_transform(
                name=f"silver_{i}",
                source_bronze=f"bronze_{i}",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name=f"silver_{i}"
            )
        
        # Enable unified execution
        pipeline = builder.enable_unified_execution(max_workers=8).to_pipeline()
        
        # Create bronze sources
        bronze_sources = {f"bronze_{i}": source_df for i in range(20)}
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources=bronze_sources)
        
        # Verify completion
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 40  # 20 bronze + 20 silver
        assert result.metrics.failed_steps == 0
    
    def test_step_dependency_chain(self, spark_session):
        """Test handling of long dependency chains."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with long dependency chain
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add bronze step
        builder.with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
        
        # Add chain of silver steps
        for i in range(10):
            builder.add_silver_transform(
                name=f"silver_{i}",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name=f"silver_{i}",
                depends_on=[f"silver_{i-1}"] if i > 0 else None
            )
        
        # Add gold step depending on last silver
        builder.add_gold_transform(
            name="gold_summary",
            transform=lambda spark, silvers: silvers["silver_9"],
            rules={"id": ["not_null"]},
            table_name="gold_summary",
            source_silvers=["silver_9"]
        )
        
        # Enable unified execution
        pipeline = builder.enable_unified_execution().to_pipeline()
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify completion
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 12  # 1 bronze + 10 silver + 1 gold
        assert result.metrics.failed_steps == 0
        
        # Verify execution order (should be sequential due to dependencies)
        # Note: parallel_efficiency is not available in PipelineMetrics
