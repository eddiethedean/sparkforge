#!/usr/bin/env python3
"""
Comprehensive tests for the pipeline_builder module.

This module tests all pipeline building and execution functionality, including
the fluent API, validation, execution modes, error handling, and reporting.
"""

import unittest
from datetime import datetime
from unittest.mock import Mock

from sparkforge.errors import StepError
from sparkforge.logging import PipelineLogger
from sparkforge.pipeline import (
    PipelineBuilder,
    PipelineMetrics,
    PipelineMode,
    PipelineReport,
    PipelineRunner,
    PipelineStatus,
)


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
            retry_count=1,
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
            duration_seconds=5.0,
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
            duration_seconds=5.0,
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


class TestPipelineBuilder(unittest.TestCase):
    """Test PipelineBuilder class."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.schema = "test_schema"

        # Create pipeline builder
        self.builder = PipelineBuilder(
            spark=self.spark, schema=self.schema, verbose=False
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
            max_parallel_workers=8,
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
            description="Test bronze step",
        )

        # Should return self for chaining
        self.assertEqual(result, self.builder)

        # Should add bronze step
        self.assertIn("bronze1", self.builder.bronze_steps)
        bronze_step = self.builder.bronze_steps["bronze1"]
        self.assertEqual(bronze_step.name, "bronze1")
        # Rules should be converted to PySpark Column objects
        self.assertIn("id", bronze_step.rules)
        self.assertEqual(len(bronze_step.rules["id"]), 1)
        # Check that it's a PySpark Column object (not a string)
        self.assertTrue(hasattr(bronze_step.rules["id"][0], "expr"))
        self.assertEqual(bronze_step.incremental_col, "created_at")

    def test_with_silver_rules(self):
        """Test adding existing silver rules."""
        result = self.builder.with_silver_rules(
            name="silver1",
            table_name="silver_table",
            rules={"id": ["not_null"]},
            watermark_col="updated_at",
            description="Test silver step",
        )

        # Should return self for chaining
        self.assertEqual(result, self.builder)

        # Should add silver step
        self.assertIn("silver1", self.builder.silver_steps)
        silver_step = self.builder.silver_steps["silver1"]
        self.assertEqual(silver_step.name, "silver1")
        self.assertEqual(silver_step.table_name, "silver_table")
        # Rules should be converted to PySpark Column objects
        self.assertIn("id", silver_step.rules)
        self.assertEqual(len(silver_step.rules["id"]), 1)
        # Check that it's a PySpark Column object (not a string)
        self.assertTrue(hasattr(silver_step.rules["id"][0], "expr"))
        self.assertEqual(silver_step.watermark_col, "updated_at")
        self.assertTrue(silver_step.existing)

    def test_add_silver_transform(self):
        """Test adding silver transform step."""

        def mock_transform(spark, df):
            return df

        # Add required bronze step first
        self.builder.with_bronze_rules(
            name="bronze1", rules={"id": ["not_null"]}, incremental_col="created_at"
        )

        result = self.builder.add_silver_transform(
            name="silver_transform",
            source_bronze="bronze1",
            transform=mock_transform,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at",
            description="Test silver transform",
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

        # Add required bronze and silver steps first
        self.builder.with_bronze_rules(
            name="bronze1", rules={"id": ["not_null"]}, incremental_col="created_at"
        )

        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver1_table",
            watermark_col="updated_at",
        )

        self.builder.add_silver_transform(
            name="silver2",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver2_table",
            watermark_col="updated_at",
        )

        result = self.builder.add_gold_transform(
            name="gold_transform",
            transform=mock_transform,
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1", "silver2"],
            description="Test gold transform",
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

    def test_validate_pipeline_success(self):
        """Test successful pipeline validation."""
        # Add valid steps
        self.builder.with_bronze_rules(
            name="bronze1", rules={"id": ["not_null"]}, incremental_col="created_at"
        )

        self.builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver_table",
            watermark_col="updated_at",
        )

        self.builder.add_gold_transform(
            name="gold1",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1"],
        )

        errors = self.builder.validate_pipeline()
        self.assertEqual(errors, [])

    def test_validate_pipeline_errors(self):
        """Test pipeline validation with errors."""
        # Add invalid silver step (missing source bronze) - this should now raise an error immediately
        with self.assertRaises(StepError) as context:
            self.builder.add_silver_transform(
                name="silver1",
                source_bronze="nonexistent_bronze",
                transform=lambda spark, df: df,
                rules={"id": ["not_null"]},
                table_name="silver_table",
                watermark_col="updated_at",
            )

        self.assertIn(
            "Bronze step 'nonexistent_bronze' not found", str(context.exception)
        )

    def test_to_pipeline_success(self):
        """Test successful pipeline creation."""
        # Add valid steps
        self.builder.with_bronze_rules(
            name="bronze1", rules={"id": ["not_null"]}, incremental_col="created_at"
        )

        pipeline = self.builder.to_pipeline()

        self.assertIsInstance(pipeline, PipelineRunner)

    def test_to_pipeline_validation_error(self):
        """Test pipeline creation with validation errors."""
        # Add invalid step - this should now raise an error immediately
        with self.assertRaises(StepError) as context:
            self.builder.add_silver_transform(
                name="silver1",
                source_bronze="nonexistent_bronze",
                transform=lambda spark, df: df,
                rules={"id": ["not_null"]},
                table_name="silver_table",
                watermark_col="updated_at",
            )

        self.assertIn(
            "Bronze step 'nonexistent_bronze' not found", str(context.exception)
        )


class TestPipelineBuilderIntegration(unittest.TestCase):
    """Test PipelineBuilder integration scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock()
        self.schema = "test_schema"

    def test_complex_pipeline_construction(self):
        """Test construction of a complex pipeline."""
        builder = PipelineBuilder(spark=self.spark, schema=self.schema, verbose=False)

        # Add multiple bronze steps
        builder.with_bronze_rules(
            name="bronze1", rules={"id": ["not_null"]}, incremental_col="created_at"
        ).with_bronze_rules(
            name="bronze2", rules={"id": ["not_null"]}, incremental_col="updated_at"
        )

        # Add multiple silver steps
        builder.add_silver_transform(
            name="silver1",
            source_bronze="bronze1",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver1_table",
            watermark_col="updated_at",
        ).add_silver_transform(
            name="silver2",
            source_bronze="bronze2",
            transform=lambda spark, df: df,
            rules={"id": ["not_null"]},
            table_name="silver2_table",
            watermark_col="updated_at",
        )

        # Add gold step
        builder.add_gold_transform(
            name="gold1",
            transform=lambda spark, silvers: list(silvers.values())[0],
            rules={"id": ["not_null"]},
            table_name="gold_table",
            source_silvers=["silver1", "silver2"],
        )

        # Validate pipeline
        errors = builder.validate_pipeline()
        self.assertEqual(errors, [])

        # Create pipeline
        pipeline = builder.to_pipeline()
        self.assertIsInstance(pipeline, PipelineRunner)


if __name__ == "__main__":
    unittest.main()
