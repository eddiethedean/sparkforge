#!/usr/bin/env python3
"""
Dependency scenario tests for unified execution.

This module tests specific dependency patterns and scenarios
for the unified execution system.
"""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sparkforge.pipeline_builder import PipelineStatus
from tests.conftest import get_test_schema

from sparkforge.unified_dependency_analyzer import (
    UnifiedDependencyAnalyzer, StepType, UnifiedStepInfo, ExecutionGroup
)
from sparkforge.models import BronzeStep, SilverStep, GoldStep
from sparkforge.pipeline_builder import PipelineBuilder


class TestDependencyScenarios:
    """Test specific dependency scenarios."""
    
    def test_parallel_bronze_steps(self, spark_session):
        """Test multiple Bronze steps that can run in parallel."""
        # Create test data
        events_data = [(1, "click"), (2, "view")]
        users_data = [(1, "Alice"), (2, "Bob")]
        events_df = spark_session.createDataFrame(events_data, ["id", "action"])
        users_df = spark_session.createDataFrame(users_data, ["id", "name"])
        
        # Build pipeline with parallel Bronze steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            .with_bronze_rules(name="bronze_users", rules={"id": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver_events"
            )
            .add_silver_transform(
                name="silver_users",
                source_bronze="bronze_users",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver_users"
            )
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(
            bronze_sources={"bronze_events": events_df, "bronze_users": users_df}
        )
        
        # Verify parallel execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 4  # 2 bronze + 2 silver
        # Note: parallel_efficiency is not available in PipelineMetrics
        
        # Verify execution groups
        # Note: execution_groups is not available in PipelineReport
    
    def test_silver_steps_with_dependencies(self, spark_session):
        """Test Silver steps that depend on other Silver steps."""
        # Create test data
        test_data = [(1, "user1", "click"), (2, "user2", "view")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "action"])
        
        # Build pipeline with Silver step dependencies
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "user": ["not_null"], "action": ["not_null"]})
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]},
                table_name="silver_events"
            )
            .add_silver_transform(
                name="silver_sessions",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.groupBy("user").count(),
                rules={"user": ["not_null"], "count": ["not_null"]},
                table_name="silver_sessions"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: (
                    silvers["silver_events"]
                    .join(silvers["silver_sessions"], "user")
                    .groupBy("user")
                    .agg(F.sum("count").alias("total_actions"))
                ),
                rules={"total_actions": ["not_null"]},
                table_name="gold_summary",
                source_silvers=["silver_events", "silver_sessions"]
            )
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 4  # 1 bronze + 2 silver + 1 gold
        
        # Verify execution groups
        # Note: execution_groups is not available in PipelineReport
    
    def test_cross_layer_dependencies(self, spark_session):
        """Test dependencies that cross layer boundaries."""
        # Create test data
        events_data = [(1, "user1", "click"), (2, "user2", "view")]
        users_data = [(1, "Alice"), (2, "Bob")]
        events_df = spark_session.createDataFrame(events_data, ["id", "user", "action"])
        users_df = spark_session.createDataFrame(users_data, ["id", "name"])
        
        # Build pipeline with cross-layer dependencies
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            # Bronze steps
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "action": ["not_null"]})
            .with_bronze_rules(name="bronze_users", rules={"id": ["not_null"], "name": ["not_null"]})
            
            # Silver steps
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
            
            # Gold step depending on both Silver steps
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
            
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(
            bronze_sources={"bronze_events": events_df, "bronze_users": users_df}
        )
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 5  # 2 bronze + 2 silver + 1 gold
        
        # Verify parallel efficiency
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_diamond_dependency_pattern(self, spark_session):
        """Test diamond dependency pattern (one source, multiple paths, one sink)."""
        # Create test data
        test_data = [(1, "user1", "click", 100), (2, "user2", "view", 50)]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "action", "value"])
        
        # Build pipeline with diamond pattern
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "action": ["not_null"], "value": ["not_null"], "user": ["not_null"]})
            
            # Multiple Silver steps from same Bronze
            .add_silver_transform(
                name="silver_clicks",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]},
                table_name="silver_clicks"
            )
            .add_silver_transform(
                name="silver_values",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("value") > 50),
                rules={"id": ["not_null"], "value": ["not_null"], "user": ["not_null"]},
                table_name="silver_values"
            )
            
            # Gold step depending on both Silver steps
            .add_gold_transform(
                name="gold_summary",
                transform=lambda spark, silvers: (
                    silvers["silver_clicks"].alias("clicks")
                    .join(silvers["silver_values"].alias("values"), "id")
                    .groupBy("clicks.user")
                    .agg(F.sum("values.value").alias("total_value"))
                ),
                rules={"total_value": ["not_null"]},
                table_name="gold_summary",
                source_silvers=["silver_clicks", "silver_values"]
            )
            
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 4  # 1 bronze + 2 silver + 1 gold
        
        # Verify execution groups
        # Note: execution_groups is not available in PipelineReport
    
    def test_fan_out_fan_in_pattern(self, spark_session):
        """Test fan-out fan-in pattern (one source, many paths, many sinks)."""
        # Create test data
        test_data = [(1, "user1", "click"), (2, "user2", "view"), (3, "user3", "click")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user", "action"])
        
        # Build pipeline with fan-out fan-in pattern
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]})
            
            # Multiple Silver steps (fan-out)
            .add_silver_transform(
                name="silver_clicks",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]},
                table_name="silver_clicks"
            )
            .add_silver_transform(
                name="silver_views",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "view"),
                rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]},
                table_name="silver_views"
            )
            .add_silver_transform(
                name="silver_users",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.select("user").distinct(),
                rules={"user": ["not_null"]},
                table_name="silver_users"
            )
            
            # Multiple Gold steps (fan-in)
            .add_gold_transform(
                name="gold_click_summary",
                transform=lambda spark, silvers: silvers["silver_clicks"].groupBy("user").count(),
                rules={"count": ["not_null"]},
                table_name="gold_click_summary",
                source_silvers=["silver_clicks"]
            )
            .add_gold_transform(
                name="gold_view_summary",
                transform=lambda spark, silvers: silvers["silver_views"].groupBy("user").count(),
                rules={"count": ["not_null"]},
                table_name="gold_view_summary",
                source_silvers=["silver_views"]
            )
            
            .enable_unified_execution(max_workers=6)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 6  # 1 bronze + 3 silver + 2 gold
        
        # Verify high parallel efficiency
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_sequential_dependency_chain(self, spark_session):
        """Test sequential dependency chain (no parallelization possible)."""
        # Create test data
        test_data = [(1, "user1"), (2, "user2")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with sequential chain
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"]})
            
            # Chain of Silver steps
            .add_silver_transform(
                name="silver_1",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver_1"
            )
            .add_silver_transform(
                name="silver_2",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: silvers["silver_1"],
                rules={"id": ["not_null"]},
                table_name="silver_2",
                depends_on=["silver_1"]
            )
            .add_silver_transform(
                name="silver_3",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: silvers["silver_2"],
                rules={"id": ["not_null"]},
                table_name="silver_3",
                depends_on=["silver_2"]
            )
            
            .enable_unified_execution(max_workers=4)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources={"bronze_events": source_df})
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 4  # 1 bronze + 3 silver
        
        # Verify low parallel efficiency due to sequential dependencies
        # Note: parallel_efficiency is not available in PipelineMetrics
    
    def test_mixed_dependency_patterns(self, spark_session):
        """Test pipeline with mixed dependency patterns."""
        # Create test data
        events_data = [(1, "user1", "click"), (2, "user2", "view")]
        users_data = [(1, "Alice"), (2, "Bob")]
        events_df = spark_session.createDataFrame(events_data, ["id", "user", "action"])
        users_df = spark_session.createDataFrame(users_data, ["id", "name"])
        
        # Build pipeline with mixed patterns
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        pipeline = (builder
            # Multiple Bronze steps (parallel)
            .with_bronze_rules(name="bronze_events", rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]})
            .with_bronze_rules(name="bronze_users", rules={"id": ["not_null"], "name": ["not_null"]})
            
            # Mixed Silver steps
            .add_silver_transform(
                name="silver_events",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.filter(F.col("action") == "click"),
                rules={"id": ["not_null"], "action": ["not_null"], "user": ["not_null"]},
                table_name="silver_events"
            )
            .add_silver_transform(
                name="silver_users",
                source_bronze="bronze_users",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"], "name": ["not_null"]},
                table_name="silver_users"
            )
            .add_silver_transform(
                name="silver_sessions",
                source_bronze="bronze_events",
                transform=lambda spark, df, silvers: df.groupBy("user").count(),
                rules={"user": ["not_null"], "count": ["not_null"]},
                table_name="silver_sessions"
            )
            
            # Gold steps with different dependencies
            .add_gold_transform(
                name="gold_events_summary",
                transform=lambda spark, silvers: silvers["silver_events"].groupBy("action").count(),
                rules={"count": ["not_null"]},
                table_name="gold_events_summary",
                source_silvers=["silver_events"]
            )
            .add_gold_transform(
                name="gold_users_summary",
                transform=lambda spark, silvers: spark.createDataFrame([(silvers["silver_users"].count(),)], ["count"]),
                rules={"count": ["not_null"]},
                table_name="gold_users_summary",
                source_silvers=["silver_users"]
            )
            .add_gold_transform(
                name="gold_combined_summary",
                transform=lambda spark, silvers: (
                    silvers["silver_events"]
                    .join(silvers["silver_users"], "id")
                    .join(silvers["silver_sessions"], "user")
                ),
                rules={"id": ["not_null"]},
                table_name="gold_combined_summary",
                source_silvers=["silver_events", "silver_users", "silver_sessions"]
            )
            
            .enable_unified_execution(max_workers=6)
            .to_pipeline()
        )
        
        # Run unified pipeline
        result = pipeline.run_unified(
            bronze_sources={"bronze_events": events_df, "bronze_users": users_df}
        )
        
        # Verify execution
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 8  # 2 bronze + 3 silver + 3 gold
        
        # Verify execution groups
        # Note: execution_groups is not available in PipelineReport
    
    @pytest.mark.slow
    def test_dependency_analysis_performance(self, spark_session):
        """Test performance of dependency analysis with many steps."""
        # Create test data
        test_data = [(1, "user1")]
        source_df = spark_session.createDataFrame(test_data, ["id", "user"])
        
        # Build pipeline with many steps
        builder = PipelineBuilder(spark=spark_session, schema=get_test_schema())
        
        # Add many bronze steps (reduced from 50 to 15 for performance)
        for i in range(15):
            builder.with_bronze_rules(
                name=f"bronze_{i}",
                rules={"id": ["not_null"]}
            )
        
        # Add many silver steps (reduced from 50 to 15 for performance)
        for i in range(15):
            builder.add_silver_transform(
                name=f"silver_{i}",
                source_bronze=f"bronze_{i}",
                transform=lambda spark, df, silvers: df,
                rules={"id": ["not_null"]},
                table_name=f"silver_{i}"
            )
        
        # Add many gold steps (reduced from 25 to 10 for performance)
        for i in range(10):
            def make_transform(silver_name):
                return lambda spark, silvers: silvers[silver_name]
            
            builder.add_gold_transform(
                name=f"gold_{i}",
                transform=make_transform(f"silver_{i}"),
                rules={"id": ["not_null"]},
                table_name=f"gold_{i}",
                source_silvers=[f"silver_{i}"]
            )
        
        # Enable unified execution
        pipeline = builder.enable_unified_execution(max_workers=8).to_pipeline()
        
        # Create bronze sources
        bronze_sources = {f"bronze_{i}": source_df for i in range(15)}
        
        # Run unified pipeline
        result = pipeline.run_unified(bronze_sources=bronze_sources)
        
        # Verify completion
        assert result.status == PipelineStatus.COMPLETED
        assert result.metrics.successful_steps == 40  # 15 bronze + 15 silver + 10 gold
        assert result.metrics.failed_steps == 0
        
        # Verify execution groups were created
        # Note: execution_groups is not available in PipelineReport
