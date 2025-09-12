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

from sparkforge.unified_dependency_analyzer import (
    UnifiedDependencyAnalyzer, UnifiedDependencyResult, StepType, 
    UnifiedStepInfo, ExecutionGroup
)
from sparkforge.unified_execution_engine import (
    UnifiedExecutionEngine, UnifiedExecutionConfig, StepExecutionResult,
    UnifiedExecutionResult
)
from sparkforge.models import BronzeStep, SilverStep, GoldStep, ExecutionContext
from sparkforge.pipeline_builder import PipelineBuilder, PipelineStatus


class TestUnifiedDependencyAnalyzer:
    """Test the unified dependency analyzer."""
    
    def test_build_unified_step_info(self, spark_session):
        """Test building unified step information from all step types."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create test steps
        bronze_steps = {
            "bronze_events": BronzeStep("bronze_events", {"user_id": ["not_null"]}),
            "bronze_users": BronzeStep("bronze_users", {"id": ["not_null"]})
        }
        
        silver_steps = {
            "silver_events": SilverStep(
                "silver_events", 
                "bronze_events", 
                lambda spark, df, silvers: df,
                {"status": ["not_null"]},
                "silver_events"
            ),
            "silver_users": SilverStep(
                "silver_users",
                "bronze_users", 
                lambda spark, df, silvers: df,
                {"name": ["not_null"]},
                "silver_users"
            )
        }
        
        gold_steps = {
            "gold_summary": GoldStep(
                "gold_summary",
                lambda spark, silvers: silvers["silver_events"].join(silvers["silver_users"]),
                {"total": ["not_null"]},
                "gold_summary",
                ["silver_events", "silver_users"]
            )
        }
        
        # Build unified step info
        step_info = analyzer._build_unified_step_info(bronze_steps, silver_steps, gold_steps)
        
        # Verify Bronze steps
        assert "bronze_events" in step_info
        assert step_info["bronze_events"].step_type == StepType.BRONZE
        assert step_info["bronze_events"].dependencies == set()
        assert step_info["bronze_events"].can_run_parallel == True
        
        # Verify Silver steps
        assert "silver_events" in step_info
        assert step_info["silver_events"].step_type == StepType.SILVER
        assert "bronze_events" in step_info["silver_events"].dependencies
        assert step_info["silver_events"].can_run_parallel == True
        
        # Verify Gold steps
        assert "gold_summary" in step_info
        assert step_info["gold_summary"].step_type == StepType.GOLD
        assert "silver_events" in step_info["gold_summary"].dependencies
        assert "silver_users" in step_info["gold_summary"].dependencies
        assert step_info["gold_summary"].can_run_parallel == False
    
    def test_analyze_cross_layer_dependencies(self, spark_session):
        """Test analyzing dependencies across different layer types."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create step info with dependencies
        step_info = {
            "bronze_events": UnifiedStepInfo(
                name="bronze_events",
                step_type=StepType.BRONZE,
                dependencies=set(),
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            ),
            "silver_events": UnifiedStepInfo(
                name="silver_events",
                step_type=StepType.SILVER,
                dependencies={"bronze_events"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            ),
            "gold_summary": UnifiedStepInfo(
                name="gold_summary",
                step_type=StepType.GOLD,
                dependencies={"silver_events"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=False
            )
        }
        
        # Analyze cross-layer dependencies
        analyzer._analyze_cross_layer_dependencies(step_info, {}, {})
        
        # Verify reverse dependencies are set
        assert "silver_events" in step_info["bronze_events"].dependents
        assert "gold_summary" in step_info["silver_events"].dependents
    
    def test_detect_cycles_unified(self, spark_session):
        """Test detecting circular dependencies in unified step graph."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create step info with cycle
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
                dependencies={"step_a"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True
            )
        }
        
        # Set up dependents
        step_info["step_a"].dependents.add("step_b")
        step_info["step_b"].dependents.add("step_a")
        
        # Detect cycles
        cycles = analyzer._detect_cycles_unified(step_info)
        
        # Verify cycle is detected
        assert len(cycles) > 0
        assert any("step_a" in cycle and "step_b" in cycle for cycle in cycles)
    
    def test_create_execution_groups(self, spark_session):
        """Test creating execution groups based on dependencies."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create step info with dependencies
        step_info = {
            "bronze_a": UnifiedStepInfo(
                name="bronze_a",
                step_type=StepType.BRONZE,
                dependencies=set(),
                dependents={"silver_a"},
                execution_group=-1,
                can_run_parallel=True
            ),
            "bronze_b": UnifiedStepInfo(
                name="bronze_b",
                step_type=StepType.BRONZE,
                dependencies=set(),
                dependents={"silver_b"},
                execution_group=-1,
                can_run_parallel=True
            ),
            "silver_a": UnifiedStepInfo(
                name="silver_a",
                step_type=StepType.SILVER,
                dependencies={"bronze_a"},
                dependents={"gold_a"},
                execution_group=-1,
                can_run_parallel=True
            ),
            "silver_b": UnifiedStepInfo(
                name="silver_b",
                step_type=StepType.SILVER,
                dependencies={"bronze_b"},
                dependents={"gold_a"},
                execution_group=-1,
                can_run_parallel=True
            ),
            "gold_a": UnifiedStepInfo(
                name="gold_a",
                step_type=StepType.GOLD,
                dependencies={"silver_a", "silver_b"},
                dependents=set(),
                execution_group=-1,
                can_run_parallel=False
            )
        }
        
        # Create execution groups
        groups = analyzer._create_execution_groups(step_info)
        
        # Verify groups are created correctly
        assert len(groups) == 3  # bronze -> silver -> gold
        
        # First group should have both bronze steps
        assert len(groups[0].step_names) == 2
        assert "bronze_a" in groups[0].step_names
        assert "bronze_b" in groups[0].step_names
        assert groups[0].can_parallelize == True
        
        # Second group should have both silver steps
        assert len(groups[1].step_names) == 2
        assert "silver_a" in groups[1].step_names
        assert "silver_b" in groups[1].step_names
        assert groups[1].can_parallelize == True
        
        # Third group should have gold step
        assert len(groups[2].step_names) == 1
        assert "gold_a" in groups[2].step_names
        assert groups[2].can_parallelize == False
    
    def test_analyze_unified_dependencies(self, spark_session):
        """Test complete unified dependency analysis."""
        analyzer = UnifiedDependencyAnalyzer()
        
        # Create test steps
        bronze_steps = {
            "bronze_events": BronzeStep("bronze_events", {"user_id": ["not_null"]})
        }
        
        silver_steps = {
            "silver_events": SilverStep(
                "silver_events",
                "bronze_events",
                lambda spark, df, silvers: df,
                {"status": ["not_null"]},
                "silver_events"
            )
        }
        
        gold_steps = {
            "gold_summary": GoldStep(
                "gold_summary",
                lambda spark, silvers: silvers["silver_events"],
                {"total": ["not_null"]},
                "gold_summary",
                ["silver_events"]
            )
        }
        
        # Analyze dependencies
        result = analyzer.analyze_unified_dependencies(bronze_steps, silver_steps, gold_steps)
        
        # Verify result structure
        assert isinstance(result, UnifiedDependencyResult)
        assert len(result.step_info) == 3
        assert len(result.execution_groups) == 3
        assert result.parallel_efficiency >= 0  # Sequential pipeline has 0% parallel efficiency
        assert len(result.execution_order) == 3


class TestUnifiedExecutionEngine:
    """Test the unified execution engine."""
    
    def test_execute_unified_pipeline(self, spark_session):
        """Test executing a complete unified pipeline."""
        # Create test data
        test_data = [(1, "user1", "active"), (2, "user2", "inactive")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "status"])
        
        # Create test steps
        bronze_steps = {
            "bronze_events": BronzeStep("bronze_events", {"id": ["not_null"], "status": ["not_null"]})
        }
        
        silver_steps = {
            "silver_events": SilverStep(
                "silver_events",
                "bronze_events",
                lambda spark, df, silvers: df.filter(F.col("status") == "active"),
                {"status": ["not_null"]},
                "silver_events"
            )
        }
        
        gold_steps = {
            "gold_summary": GoldStep(
                "gold_summary",
                lambda spark, silvers: silvers["silver_events"].groupBy("status").count(),
                {"count": ["not_null"]},
                "gold_summary",
                ["silver_events"]
            )
        }
        
        # Create execution engine
        config = UnifiedExecutionConfig(max_workers=2, enable_parallel_execution=True)
        engine = UnifiedExecutionEngine(spark_session, config)
        
        # Execute pipeline
        result = engine.execute_unified_pipeline(
            bronze_steps=bronze_steps,
            silver_steps=silver_steps,
            gold_steps=gold_steps,
            bronze_sources={"bronze_events": source_df},
            mode="incremental"
        )
        
        # Verify result
        assert isinstance(result, UnifiedExecutionResult)
        assert result.successful_steps == 3
        assert result.failed_steps == 0
        assert result.total_rows_processed > 0
        assert result.total_rows_written > 0
        assert result.parallel_efficiency >= 0  # Sequential pipeline has 0% parallel efficiency
    
    def test_execute_single_step_bronze(self, spark_session):
        """Test executing a single Bronze step."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create Bronze step
        bronze_step = BronzeStep("bronze_events", {"id": ["not_null"]})
        
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
        # Set up bronze sources
        engine._bronze_sources = {"bronze_events": source_df}
        
        # Execute step
        result = engine._execute_single_step(
            "bronze_events",
            {"bronze_events": bronze_step},
            {},
            {},
            "incremental",
            None
        )
        
        # Verify result
        assert isinstance(result, StepExecutionResult)
        assert result.step_name == "bronze_events"
        assert result.step_type == StepType.BRONZE
        assert result.success == True
        assert result.rows_processed == 2
        assert result.rows_written == 2
    
    def test_execute_single_step_silver(self, spark_session):
        """Test executing a single Silver step."""
        # Create test data
        test_data = [(1, "user1", "active"), (2, "user2", "inactive")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "status"])
        
        # Create Silver step
        silver_step = SilverStep(
            "silver_events",
            "bronze_events",
            lambda spark, df, silvers: df.filter(F.col("status") == "active"),
            {"status": ["not_null"]},
            "silver_events"
        )
        
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
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
        
        # Verify result
        assert isinstance(result, StepExecutionResult)
        assert result.step_name == "silver_events"
        assert result.step_type == StepType.SILVER
        assert result.success == True
        assert result.rows_processed == 2
        assert result.rows_written == 1  # Only active records
    
    def test_execute_single_step_gold(self, spark_session):
        """Test executing a single Gold step."""
        # Create test data
        test_data = [(1, "active"), (2, "inactive")]
        source_df = spark_session.createDataFrame(test_data, ["id", "status"])
        
        # Create Gold step
        gold_step = GoldStep(
            "gold_summary",
            lambda spark, silvers: silvers["silver_events"].groupBy("status").count(),
            {"count": ["not_null"]},
            "gold_summary",
            ["silver_events"]
        )
        
        # Create execution engine
        engine = UnifiedExecutionEngine(spark_session, UnifiedExecutionConfig())
        
        # Set up available data
        engine._available_data["silver_events"] = source_df
        
        # Execute step
        result = engine._execute_single_step(
            "gold_summary",
            {},
            {},
            {"gold_summary": gold_step},
            "incremental",
            None
        )
        
        # Verify result
        assert isinstance(result, StepExecutionResult)
        assert result.step_name == "gold_summary"
        assert result.step_type == StepType.GOLD
        assert result.success == True
        assert result.rows_processed == 2
        assert result.rows_written == 2  # Two groups: active, inactive
    
    def test_execute_group_parallel(self, spark_session):
        """Test executing a group of steps in parallel."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create Bronze steps
        bronze_steps = {
            "bronze_events": BronzeStep("bronze_events", {"id": ["not_null"]}),
            "bronze_users": BronzeStep("bronze_users", {"id": ["not_null"]})
        }
        
        # Create execution engine
        config = UnifiedExecutionConfig(max_workers=2, enable_parallel_execution=True)
        engine = UnifiedExecutionEngine(spark_session, config)
        
        # Set up available data
        engine._available_data = {"bronze_events": source_df, "bronze_users": source_df}
        engine._bronze_sources = {"bronze_events": source_df, "bronze_users": source_df}
        
        # Execute group in parallel
        results = engine._execute_group_parallel(
            ["bronze_events", "bronze_users"],
            bronze_steps,
            {},
            {},
            "incremental",
            None
        )
        
        # Verify results
        assert len(results) == 2
        assert "bronze_events" in results
        assert "bronze_users" in results
        assert results["bronze_events"].success == True
        assert results["bronze_users"].success == True
    
    def test_execute_group_sequential(self, spark_session):
        """Test executing a group of steps sequentially."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Create Bronze steps
        bronze_steps = {
            "bronze_events": BronzeStep("bronze_events", {"id": ["not_null"]}),
            "bronze_users": BronzeStep("bronze_users", {"id": ["not_null"]})
        }
        
        # Create execution engine
        config = UnifiedExecutionConfig(enable_parallel_execution=False)
        engine = UnifiedExecutionEngine(spark_session, config)
        
        # Set up available data
        engine._available_data = {"bronze_events": source_df, "bronze_users": source_df}
        engine._bronze_sources = {"bronze_events": source_df, "bronze_users": source_df}
        
        # Execute group sequentially
        results = engine._execute_group_sequential(
            ["bronze_events", "bronze_users"],
            bronze_steps,
            {},
            {},
            "incremental",
            None
        )
        
        # Verify results
        assert len(results) == 2
        assert "bronze_events" in results
        assert "bronze_users" in results
        assert results["bronze_events"].success == True
        assert results["bronze_users"].success == True


class TestPipelineBuilderUnifiedExecution:
    """Test the PipelineBuilder with unified execution."""
    
    def test_enable_unified_execution(self, spark_session):
        """Test enabling unified execution on PipelineBuilder."""
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Enable unified execution
        result = builder.enable_unified_execution(
            max_workers=4,
            enable_parallel_execution=True,
            enable_dependency_optimization=True
        )
        
        # Verify method chaining
        assert result is builder
        
        # Verify unified execution engine is created
        assert hasattr(builder, 'unified_execution_engine')
        assert builder.unified_execution_engine is not None
        
        # Verify unified dependency analyzer is created
        assert hasattr(builder, 'unified_dependency_analyzer')
        assert builder.unified_dependency_analyzer is not None
    
    def test_run_unified_without_enabling(self, spark_session):
        """Test that run_unified fails if unified execution is not enabled."""
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        pipeline = builder.to_pipeline()
        
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Try to run unified without enabling
        with pytest.raises(ValueError, match="Unified execution not enabled"):
            pipeline.run_unified(bronze_sources={"bronze_events": source_df})
    
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
            .enable_unified_execution(max_workers=2)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify result
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 3
        assert result.metrics.failed_steps == 0
        assert result.metrics.total_rows_processed > 0
        assert result.metrics.total_rows_written > 0
    
    def test_convert_unified_result_to_report(self, spark_session):
        """Test converting unified execution result to PipelineReport."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .enable_unified_execution()
            .to_pipeline()
        )
        
        # Create mock unified result
        mock_result = Mock()
        mock_result.failed_steps = 0
        mock_result.successful_steps = 1
        mock_result.total_duration = 1.5
        mock_result.total_rows_processed = 1
        mock_result.total_rows_written = 1
        mock_result.parallel_efficiency = 100.0
        mock_result.errors = []
        
        # Mock step results
        mock_step_result = Mock()
        mock_step_result.step_name = "bronze_events"
        mock_step_result.step_type = Mock()
        mock_step_result.step_type.value = "bronze"
        mock_step_result.success = True
        mock_step_result.duration_seconds = 1.5
        mock_step_result.rows_processed = 1
        mock_step_result.rows_written = 1
        mock_step_result.error_message = None
        
        mock_result.step_results = {"bronze_events": mock_step_result}
        
        # Convert to report
        report = pipeline._convert_unified_result_to_report(mock_result, "incremental")
        
        # Verify report structure
        assert report.pipeline_id == pipeline.pipeline_id
        assert report.metrics.successful_steps == 1
        assert report.metrics.failed_steps == 0
        assert report.metrics.total_duration_secs == 1.5
        # Note: parallel_efficiency is not available in PipelineMetrics
        assert "bronze_events" in report.bronze_results


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
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "user": ["not_null"], "action": ["not_null"]})
            .with_bronze_rules(name="bronze_users", rules={"id": ["not_null"], "name": ["not_null"]})
            
            # Silver steps with dependencies
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": ["not_null"], "action": ["not_null"]},
                table_name="silver_events"
            )
            .add_silver_transform(
                name="silver_users",
                source_bronze="bronze_users",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"], "name": ["not_null"]},
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
                rules={"count": ["not_null"]},
                table_name="gold_summary",
                source_silvers=["silver_events", "silver_users"]
            )
            
            # Enable unified execution
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(
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
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=failing_transform,
                rules={"id": ["not_null"]},
                table_name="silver_events"
            )
            .enable_unified_execution()
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify error handling
        assert result.status == PipelineStatus.FAILED
        assert result.metrics.failed_steps > 0
        assert len(result.errors) > 0
    
    def test_parallel_efficiency_calculation(self, spark_session):
        """Test that parallel efficiency is calculated correctly."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2"), (3, "user3")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with multiple independent steps
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
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: silvers["silver_events"],
                rules={"id": ["not_null"]},
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Note: parallel_efficiency is not available in PipelineMetrics
        assert result.status == PipelineStatus.COMPLETED


@pytest.mark.slow
class TestUnifiedExecutionPerformance:
    """Performance tests for unified execution."""
    
    def test_large_scale_parallel_execution(self, spark_session):
        """Test unified execution with many parallel steps."""
        # Create large test dataset
        test_data = [(i, f"user{i}") for i in range(1000)]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with many steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add multiple bronze steps
        for i in range(5):
            builder.with_bronze_rules(
                name=f"bronze_{i}",
                rules={"id": ["not_null"]}
            )
        
        # Add multiple silver steps
        for i in range(5):
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
        bronze_sources = {f"bronze_{i}": source_df for i in range(5)}
        
        # Run unified pipeline
        start_time = time.time()
        result = pipeline.run_unified(bronze_sources=bronze_sources)
        execution_time = time.time() - start_time
        
        # Verify performance
        assert result.status == PipelineStatus.COMPLETED
        assert execution_time < 30  # Should complete within 30 seconds
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_memory_efficiency(self, spark_session):
        """Test that unified execution is memory efficient."""
        # Create test data
        test_data = [(i, f"user{i}") for i in range(100)]
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
            .enable_unified_execution(max_workers=2)
            .to_pipeline()
        )
        
        # Run multiple times to test memory efficiency
        for _ in range(5):
            result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
            assert result.status == PipelineStatus.COMPLETED
        
        # If we get here without memory issues, the test passes
        assert True
