#!/usr/bin/env python3
"""
Comprehensive tests for the pipeline_builder module.

This module tests all pipeline building and execution functionality, including
the fluent API, validation, execution modes, error handling, and reporting.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
import time
from datetime import datetime

from sparkforge.pipeline_builder import (
    PipelineBuilder, PipelineRunner, PipelineMode, PipelineStatus,
    PipelineMetrics, PipelineReport, StepValidator
)
from sparkforge.models import BronzeStep, SilverStep, GoldStep, ExecutionContext
from sparkforge.logger import PipelineLogger


class TestPipelineMode(unittest.TestCase):
    """Test PipelineMode enum."""
    
    def test_pipeline_mode_values(self):
        """Test pipeline mode enum values."""
        self.assertEqual(PipelineMode.INITIAL.value, "initial")
        self.assertEqual(PipelineMode.INCREMENTAL.value, "incremental")
        self.assertEqual(PipelineMode.FULL_REFRESH.value, "full_refresh")
        self.assertEqual(PipelineMode.VALIDATION_ONLY.value, "validation_only")


class TestPipelineStatus(unittest.TestCase):
    """Test PipelineStatus enum."""
    
    def test_pipeline_status_values(self):
        """Test pipeline status enum values."""
        self.assertEqual(PipelineStatus.PENDING.value, "pending")
        self.assertEqual(PipelineStatus.RUNNING.value, "running")
        self.assertEqual(PipelineStatus.COMPLETED.value, "completed")
        self.assertEqual(PipelineStatus.FAILED.value, "failed")
        self.assertEqual(PipelineStatus.CANCELLED.value, "cancelled")
        self.assertEqual(PipelineStatus.PAUSED.value, "paused")


class TestPipelineMetrics(unittest.TestCase):
    """Test PipelineMetrics dataclass."""
    
    def test_pipeline_metrics_creation(self):
        """Test pipeline metrics creation."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=8,
            failed_steps=1,
            skipped_steps=1,
            total_duration=5.0,
            bronze_duration=1.0,
            silver_duration=3.0,
            gold_duration=1.0,
            total_rows_processed=1000,
            total_rows_written=950,
            parallel_efficiency=0.8,
            cache_hit_rate=0.6,
            error_count=2,
            retry_count=1
        )
        
        self.assertEqual(metrics.total_steps, 10)
        self.assertEqual(metrics.successful_steps, 8)
        self.assertEqual(metrics.failed_steps, 1)
        self.assertEqual(metrics.skipped_steps, 1)
        self.assertEqual(metrics.total_duration, 5.0)
        self.assertEqual(metrics.bronze_duration, 1.0)
        self.assertEqual(metrics.silver_duration, 3.0)
        self.assertEqual(metrics.gold_duration, 1.0)
        self.assertEqual(metrics.total_rows_processed, 1000)
        self.assertEqual(metrics.total_rows_written, 950)
        self.assertEqual(metrics.parallel_efficiency, 0.8)
        self.assertEqual(metrics.cache_hit_rate, 0.6)
        self.assertEqual(metrics.error_count, 2)
        self.assertEqual(metrics.retry_count, 1)


class TestPipelineReport(unittest.TestCase):
    """Test PipelineReport dataclass."""
    
    def test_pipeline_report_creation(self):
        """Test pipeline report creation."""
        report = PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="test_execution",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=5.0
        )
        
        self.assertEqual(report.pipeline_id, "test_pipeline")
        self.assertEqual(report.execution_id, "test_execution")
        self.assertEqual(report.mode, PipelineMode.INITIAL)
        self.assertEqual(report.status, PipelineStatus.COMPLETED)
        self.assertIsInstance(report.start_time, datetime)
        self.assertIsInstance(report.end_time, datetime)
        self.assertEqual(report.duration_seconds, 5.0)
    
    def test_pipeline_report_to_dict(self):
        """Test pipeline report to dictionary conversion."""
        report = PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="test_execution",
            mode=PipelineMode.INITIAL,
            status=PipelineStatus.COMPLETED,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=5.0
        )
        
        report_dict = report.to_dict()
        
        self.assertIn("pipeline_id", report_dict)
        self.assertIn("execution_id", report_dict)
        self.assertIn("mode", report_dict)
        self.assertIn("status", report_dict)
        self.assertIn("start_time", report_dict)
        self.assertIn("end_time", report_dict)
        self.assertIn("duration_seconds", report_dict)
        self.assertIn("metrics", report_dict)
        self.assertIn("bronze_results", report_dict)
        self.assertIn("silver_results", report_dict)
        self.assertIn("gold_results", report_dict)


class TestStepValidator(unittest.TestCase):
    """Test StepValidator protocol."""
    
    def test_step_validator_implementation(self):
        """Test step validator implementation."""
        class TestValidator(StepValidator):
            def validate(self, step, context):
                return ["Test validation error"] if step.name == "invalid_step" else []
        
        validator = TestValidator()
        
        # Test with valid step
        valid_step = BronzeStep("valid_step", {"id": ["not_null"]}, "created_at")
        context = ExecutionContext(mode="initial", start_time=datetime.now())
        errors = validator.validate(valid_step, context)
        self.assertEqual(errors, [])
        
        # Test with invalid step
        invalid_step = BronzeStep("invalid_step", {"id": ["not_null"]}, "created_at")
        errors = validator.validate(invalid_step, context)
        self.assertEqual(errors, ["Test validation error"])


class TestPipelineBuilder(unittest.TestCase):
    """Test PipelineBuilder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.schema = "test_schema"
        
        # Create pipeline builder
        self.builder = PipelineBuilder(
            spark=self.spark,
            schema=self.schema,
            verbose=False
        )
    
    def test_builder_creation(self):
        """Test pipeline builder creation."""
        self.assertEqual(self.builder.spark, self.spark)
        self.assertEqual(self.builder.schema, self.schema)
        self.assertIsNotNone(self.builder.pipeline_id)
        self.assertIsInstance(self.builder.logger, PipelineLogger)
        self.assertEqual(len(self.builder.bronze_steps), 0)
        self.assertEqual(len(self.builder.silver_steps), 0)
        self.assertEqual(len(self.builder.gold_steps), 0)
    
    def test_builder_creation_with_custom_params(self):
        """Test pipeline builder creation with custom parameters."""
        builder = PipelineBuilder(
            spark=self.spark,
            schema=self.schema,
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=99.5,
            verbose=True,
            enable_parallel_silver=False,
            max_parallel_workers=8
        )
        
        self.assertEqual(builder.config.thresholds.bronze, 90.0)
        self.assertEqual(builder.config.thresholds.silver, 95.0)
        self.assertEqual(builder.config.thresholds.gold, 99.5)
        self.assertTrue(builder.config.verbose)
        self.assertFalse(builder.config.parallel.enabled)
        self.assertEqual(builder.config.parallel.max_workers, 8)
    
    def test_with_bronze_rules(self):
        """Test adding bronze rules."""
        result = self.builder.with_bronze_rules(
            name="bronze1",
            rules={"id": ["not_null"]},
            incremental_col="created_at",
            description="Test bronze step"
        )
        
        # Should return self for chaining
        self.assertEqual(result, self.builder)
        
        # Should add bronze step
        self.assertIn("bronze1", self.builder.bronze_steps)
        bronze_step = self.builder.bronze_steps["bronze1"]
        self.assertEqual(bronze_step.name, "bronze1")
        self.assertEqual(bronze_step.rules, {"id": ["not_null"]})
        self.assertEqual(bronze_step.incremental_col, "created_at")
    
    def test_with_silver_rules(self):
        """Test adding existing silver rules."""
        result = self.builder.with_silver_rules(
            name="silver1",
            table_name="silver_table",
            rules={"id": ["not_null"]},
            watermark_col="updated_at",
            description="Test silver step"
        )
        
        # Should return self for chaining
        self.assertEqual(result, self.builder)
        
        # Should add silver step
        self.assertIn("silver1", self.builder.silver_steps)
        silver_step = self.builder.silver_steps["silver1"]
        self.assertEqual(silver_step.name, "silver1")
        self.assertEqual(silver_step.table_name, "silver_table")
        self.assertEqual(silver_step.rules, {"id": ["not_null"]})
        self.assertEqual(silver_step.watermark_col, "updated_at")
        self.assertTrue(silver_step.existing)
    
    def test_add_silver_transform(self):
        """Test adding silver transform step."""
        def mock_transform(spark, df):
            return df
        
        result = self.builder.add_silver_transform(
            name="silver_transform",
            source_bronze="bronze1",
            transform=mock_transform,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at",
            description="Test silver transform",
            depends_on=["silver1"]
        )
        
        # Should return self for chaining
        self.assertEqual(result, self.builder)
        
        # Should add silver step
        self.assertIn("silver_transform", self.builder.silver_steps)
        silver_step = self.builder.silver_steps["silver_transform"]
        self.assertEqual(silver_step.name, "silver_transform")
        self.assertEqual(silver_step.source_bronze, "bronze1")
        self.assertEqual(silver_step.transform, mock_transform)
        self.assertEqual(silver_step.table_name, "silver_table")
        self.assertEqual(silver_step.watermark_col, "updated_at")
        self.assertFalse(silver_step.existing)
    
    def test_add_gold_transform(self):
        """Test adding gold transform step."""
        def mock_transform(spark, silvers):
            return list(silvers.values())[0]
        
        result = self.builder.add_gold_transform(
            name="gold_transform",
            transform=mock_transform,
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1", "silver2"],
            description="Test gold transform"
        )
        
        # Should return self for chaining
        self.assertEqual(result, self.builder)
        
        # Should add gold step
        self.assertIn("gold_transform", self.builder.gold_steps)
        gold_step = self.builder.gold_steps["gold_transform"]
        self.assertEqual(gold_step.name, "gold_transform")
        self.assertEqual(gold_step.transform, mock_transform)
        self.assertEqual(gold_step.table_name, "gold_table")
        self.assertEqual(gold_step.source_silvers, ["silver1", "silver2"])
    
    def test_add_validator(self):
        """Test adding custom validator."""
        class TestValidator(StepValidator):
            def validate(self, step, context):
                return []
        
        validator = TestValidator()
        result = self.builder.add_validator(validator)
        
        # Should return self for chaining
        self.assertEqual(result, self.builder)
        
        # Should add validator
        self.assertIn(validator, self.builder.validators)
    
    def test_validate_pipeline_success(self):
        """Test successful pipeline validation."""
        # Add valid steps
        self.builder.with_bronze_rules(
            name="bronze1",
            rules={"id": ["not_null"]},
            incremental_col="created_at"
        )
        
        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at"
        )
        
        self.builder.add_gold_transform(
            name="gold1",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1"]
        )
        
        errors = self.builder.validate_pipeline()
        self.assertEqual(errors, [])
    
    def test_validate_pipeline_errors(self):
        """Test pipeline validation with errors."""
        # Add invalid silver step (missing source bronze)
        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="nonexistent_bronze",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at"
        )
        
        errors = self.builder.validate_pipeline()
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("source bronze 'nonexistent_bronze' not found" in error for error in errors))
    
    def test_to_pipeline_success(self):
        """Test successful pipeline creation."""
        # Add valid steps
        self.builder.with_bronze_rules(
            name="bronze1",
            rules={"id": ["not_null"]},
            incremental_col="created_at"
        )
        
        pipeline = self.builder.to_pipeline()
        
        self.assertIsInstance(pipeline, PipelineRunner)
        self.assertEqual(pipeline.pipeline_id, self.builder.pipeline_id)
    
    def test_to_pipeline_validation_error(self):
        """Test pipeline creation with validation errors."""
        # Add invalid step
        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="nonexistent_bronze",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at"
        )
        
        with self.assertRaises(ValueError) as context:
            self.builder.to_pipeline()
        
        self.assertIn("Pipeline validation failed", str(context.exception))


class TestPipelineRunner(unittest.TestCase):
    """Test PipelineRunner class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.schema = "test_schema"
        
        # Create pipeline builder and add steps
        self.builder = PipelineBuilder(
            spark=self.spark,
            schema=self.schema,
            verbose=False
        )
        
        self.builder.with_bronze_rules(
            name="bronze1",
            rules={"id": ["not_null"]},
            incremental_col="created_at"
        )
        
        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at"
        )
        
        self.builder.add_gold_transform(
            name="gold1",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1"]
        )
        
        self.pipeline = self.builder.to_pipeline()
        
        # Create mock bronze sources
        self.bronze_sources = {
            "bronze1": Mock()
        }
        self.bronze_sources["bronze1"].count.return_value = 100
    
    def test_runner_creation(self):
        """Test pipeline runner creation."""
        self.assertEqual(self.pipeline.pipeline_id, self.builder.pipeline_id)
        self.assertEqual(self.pipeline.spark, self.spark)
        self.assertEqual(len(self.pipeline.bronze_steps), 1)
        self.assertEqual(len(self.pipeline.silver_steps), 1)
        self.assertEqual(len(self.pipeline.gold_steps), 1)
        self.assertEqual(self.pipeline.get_status(), PipelineStatus.PENDING)
    
    def test_initial_load(self):
        """Test initial pipeline load."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            
            # Mock different validation rates for different stages
            def mock_apply_rules_side_effect(df, rules, stage, step):
                if stage == "bronze":
                    mock_stats = StageStats(
                        stage="bronze",
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                elif stage == "gold":
                    # Return higher validation rate for gold
                    mock_stats = StageStats(
                        stage="gold",
                        step=step,
                        total_rows=100,
                        valid_rows=99,
                        invalid_rows=1,
                        validation_rate=99.0,
                        duration_secs=1.0
                    )
                else:
                    mock_stats = StageStats(
                        stage=stage,
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                return (mock_df, mock_df, mock_stats)
            
            mock_apply_rules.side_effect = mock_apply_rules_side_effect
            
            # Mock execution engine
            with patch.object(self.pipeline.execution_engine, 'execute_silver_steps') as mock_execute_silver:
                # Return actual DataFrame objects for gold step processing
                mock_silver_df = Mock()
                mock_silver_df.count.return_value = 100
                mock_execute_silver.return_value = {"silver1": mock_silver_df}
                
                # Mock write operation
                with patch('sparkforge.pipeline_builder.time_write_operation') as mock_write:
                    mock_write.return_value = (100, 0.5, None, None)
                    
                    report = self.pipeline.initial_load(bronze_sources=self.bronze_sources)
                    
                    self.assertIsInstance(report, PipelineReport)
                    self.assertEqual(report.mode, PipelineMode.INITIAL)
                    self.assertEqual(report.status, PipelineStatus.COMPLETED)
                    self.assertGreater(report.duration_seconds, 0)
    
    def test_run_incremental(self):
        """Test incremental pipeline run."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            
            # Mock different validation rates for different stages
            def mock_apply_rules_side_effect(df, rules, stage, step):
                if stage == "bronze":
                    mock_stats = StageStats(
                        stage="bronze",
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                elif stage == "gold":
                    # Return higher validation rate for gold
                    mock_stats = StageStats(
                        stage="gold",
                        step=step,
                        total_rows=100,
                        valid_rows=99,
                        invalid_rows=1,
                        validation_rate=99.0,
                        duration_secs=1.0
                    )
                else:
                    mock_stats = StageStats(
                        stage=stage,
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                return (mock_df, mock_df, mock_stats)
            
            mock_apply_rules.side_effect = mock_apply_rules_side_effect
            
            # Mock execution engine
            with patch.object(self.pipeline.execution_engine, 'execute_silver_steps') as mock_execute_silver:
                # Return actual DataFrame objects for gold step processing
                mock_silver_df = Mock()
                mock_silver_df.count.return_value = 100
                mock_execute_silver.return_value = {"silver1": mock_silver_df}
                
                # Mock write operation
                with patch('sparkforge.pipeline_builder.time_write_operation') as mock_write:
                    mock_write.return_value = (100, 0.5, None, None)
                    
                    report = self.pipeline.run_incremental(bronze_sources=self.bronze_sources)
                    
                    self.assertIsInstance(report, PipelineReport)
                    self.assertEqual(report.mode, PipelineMode.INCREMENTAL)
                    self.assertEqual(report.status, PipelineStatus.COMPLETED)
    
    def test_run_full_refresh(self):
        """Test full refresh pipeline run."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            
            # Mock different validation rates for different stages
            def mock_apply_rules_side_effect(df, rules, stage, step):
                if stage == "bronze":
                    mock_stats = StageStats(
                        stage="bronze",
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                elif stage == "gold":
                    # Return higher validation rate for gold
                    mock_stats = StageStats(
                        stage="gold",
                        step=step,
                        total_rows=100,
                        valid_rows=99,
                        invalid_rows=1,
                        validation_rate=99.0,
                        duration_secs=1.0
                    )
                else:
                    mock_stats = StageStats(
                        stage=stage,
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                return (mock_df, mock_df, mock_stats)
            
            mock_apply_rules.side_effect = mock_apply_rules_side_effect
            
            # Mock execution engine
            with patch.object(self.pipeline.execution_engine, 'execute_silver_steps') as mock_execute_silver:
                # Return actual DataFrame objects for gold step processing
                mock_silver_df = Mock()
                mock_silver_df.count.return_value = 100
                mock_execute_silver.return_value = {"silver1": mock_silver_df}
                
                # Mock write operation
                with patch('sparkforge.pipeline_builder.time_write_operation') as mock_write:
                    mock_write.return_value = (100, 0.5, None, None)
                    
                    report = self.pipeline.run_full_refresh(bronze_sources=self.bronze_sources)
                    
                    self.assertIsInstance(report, PipelineReport)
                    self.assertEqual(report.mode, PipelineMode.FULL_REFRESH)
                    self.assertEqual(report.status, PipelineStatus.COMPLETED)
    
    def test_run_validation_only(self):
        """Test validation-only pipeline run."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            
            # Mock different validation rates for different stages
            def mock_apply_rules_side_effect(df, rules, stage, step):
                if stage == "bronze":
                    mock_stats = StageStats(
                        stage="bronze",
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                elif stage == "gold":
                    # Return higher validation rate for gold
                    mock_stats = StageStats(
                        stage="gold",
                        step=step,
                        total_rows=100,
                        valid_rows=99,
                        invalid_rows=1,
                        validation_rate=99.0,
                        duration_secs=1.0
                    )
                else:
                    mock_stats = StageStats(
                        stage=stage,
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                return (mock_df, mock_df, mock_stats)
            
            mock_apply_rules.side_effect = mock_apply_rules_side_effect
            
            # Mock execution engine
            with patch.object(self.pipeline.execution_engine, 'execute_silver_steps') as mock_execute_silver:
                # Return actual DataFrame objects for gold step processing
                mock_silver_df = Mock()
                mock_silver_df.count.return_value = 100
                mock_execute_silver.return_value = {"silver1": mock_silver_df}
                
                # Mock write operation
                with patch('sparkforge.pipeline_builder.time_write_operation') as mock_write:
                    mock_write.return_value = (100, 0.5, None, None)
                    
                    report = self.pipeline.run_validation_only(bronze_sources=self.bronze_sources)
                    
                    self.assertIsInstance(report, PipelineReport)
                    self.assertEqual(report.mode, PipelineMode.VALIDATION_ONLY)
                    self.assertEqual(report.status, PipelineStatus.COMPLETED)
    
    def test_cancel_execution(self):
        """Test cancelling pipeline execution."""
        # Pipeline should not be running initially
        self.assertFalse(self.pipeline._is_running)
        
        # Cancel should work even when not running
        self.pipeline.cancel()
        # Just verify the method doesn't raise an exception
        self.assertTrue(True)  # Test passes if no exception is raised
    
    def test_get_status(self):
        """Test getting pipeline status."""
        # Initially pending
        self.assertEqual(self.pipeline.get_status(), PipelineStatus.PENDING)
        
        # After cancellation, status should still be pending since we're not running
        self.pipeline.cancel()
        self.assertEqual(self.pipeline.get_status(), PipelineStatus.PENDING)
    
    def test_get_current_report(self):
        """Test getting current execution report."""
        # Initially no report
        self.assertIsNone(self.pipeline.get_current_report())
    
    def test_get_performance_stats(self):
        """Test getting performance statistics."""
        stats = self.pipeline.get_performance_stats()
        
        self.assertIsInstance(stats, dict)
        # When no report exists, should return empty dict
        if not self.pipeline._current_report:
            self.assertEqual(stats, {})
        else:
            self.assertIn("execution_engine_stats", stats)
            self.assertIn("cache_stats", stats)
            self.assertIn("pipeline_metrics", stats)
            self.assertIn("dependency_analysis", stats)
    
    def test_execution_context_manager(self):
        """Test execution context manager."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            
            # Mock different validation rates for different stages
            def mock_apply_rules_side_effect(df, rules, stage, step):
                if stage == "bronze":
                    mock_stats = StageStats(
                        stage="bronze",
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                elif stage == "gold":
                    # Return higher validation rate for gold
                    mock_stats = StageStats(
                        stage="gold",
                        step=step,
                        total_rows=100,
                        valid_rows=99,
                        invalid_rows=1,
                        validation_rate=99.0,
                        duration_secs=1.0
                    )
                else:
                    mock_stats = StageStats(
                        stage=stage,
                        step=step,
                        total_rows=100,
                        valid_rows=95,
                        invalid_rows=5,
                        validation_rate=95.0,
                        duration_secs=1.0
                    )
                return (mock_df, mock_df, mock_stats)
            
            mock_apply_rules.side_effect = mock_apply_rules_side_effect
            
            # Mock execution engine
            with patch.object(self.pipeline.execution_engine, 'execute_silver_steps') as mock_execute_silver:
                # Return actual DataFrame objects for gold step processing
                mock_silver_df = Mock()
                mock_silver_df.count.return_value = 100
                mock_execute_silver.return_value = {"silver1": mock_silver_df}
                
                # Mock write operation
                with patch('sparkforge.pipeline_builder.time_write_operation') as mock_write:
                    mock_write.return_value = (100, 0.5, None, None)
                    
                    with self.pipeline.execution_context(PipelineMode.INITIAL, self.bronze_sources) as report:
                        self.assertIsInstance(report, PipelineReport)
                        self.assertEqual(report.mode, PipelineMode.INITIAL)
    
    def test_error_handling(self):
        """Test error handling during execution."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 100
        self.spark.table.return_value = mock_df
        
        # Mock validation to fail
        with patch('sparkforge.pipeline_builder.apply_column_rules') as mock_apply_rules:
            from sparkforge.models import StageStats
            mock_stats = StageStats(
                stage="bronze",
                step="bronze1",
                total_rows=100,
                valid_rows=50,  # Low validation rate
                invalid_rows=50,
                validation_rate=50.0,  # Below threshold
                duration_secs=1.0
            )
            mock_apply_rules.return_value = (mock_df, mock_df, mock_stats)
            
            with self.assertRaises(ValueError) as context:
                self.pipeline.initial_load(bronze_sources=self.bronze_sources)
            
            self.assertIn("validation 50.00% below required", str(context.exception))


class TestPipelineBuilderIntegration(unittest.TestCase):
    """Test PipelineBuilder integration scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.schema = "test_schema"
    
    def test_complex_pipeline_construction(self):
        """Test construction of a complex pipeline."""
        builder = PipelineBuilder(
            spark=self.spark,
            schema=self.schema,
            verbose=False
        )
        
        # Add multiple bronze steps
        builder.with_bronze_rules(
            name="bronze1",
            rules={"id": ["not_null"]},
            incremental_col="created_at"
        ).with_bronze_rules(
            name="bronze2",
            rules={"id": ["not_null"]},
            incremental_col="updated_at"
        )
        
        # Add multiple silver steps
        builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver1_table",
            watermark_col="updated_at"
        ).add_silver_transform(
            name="silver2",
            source_bronze="bronze2",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver2_table",
            watermark_col="updated_at"
        )
        
        # Add gold step
        builder.add_gold_transform(
            name="gold1",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1", "silver2"]
        )
        
        # Validate pipeline
        errors = builder.validate_pipeline()
        self.assertEqual(errors, [])
        
        # Create pipeline
        pipeline = builder.to_pipeline()
        self.assertIsInstance(pipeline, PipelineRunner)
    
    def test_pipeline_with_custom_validator(self):
        """Test pipeline with custom validator."""
        class CustomValidator(StepValidator):
            def validate(self, step, context):
                if hasattr(step, 'name') and 'test' in step.name.lower():
                    return ["Step name contains 'test'"]
                return []
        
        builder = PipelineBuilder(
            spark=self.spark,
            schema=self.schema,
            verbose=False
        )
        
        builder.add_validator(CustomValidator())
        
        # Add step with 'test' in name
        builder.with_bronze_rules(
            name="test_bronze",
            rules={"id": ["not_null"]},
            incremental_col="created_at"
        )
        
        # Validate should find error
        errors = builder.validate_pipeline()
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("Step name contains 'test'" in error for error in errors))


def run_pipeline_builder_tests():
    """Run all pipeline builder tests."""
    print("🧪 Running Pipeline Builder Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestPipelineMode,
        TestPipelineStatus,
        TestPipelineMetrics,
        TestPipelineReport,
        TestStepValidator,
        TestPipelineBuilder,
        TestPipelineRunner,
        TestPipelineBuilderIntegration
    ]
    
    for test_class in test_classes:
        test_suite.addTest(unittest.makeSuite(test_class))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n❌ Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_pipeline_builder_tests()
    if success:
        print("\n🎉 All pipeline builder tests passed!")
    else:
        print("\n❌ Some pipeline builder tests failed!")
