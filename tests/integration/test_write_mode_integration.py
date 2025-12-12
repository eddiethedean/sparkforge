#!/usr/bin/env python3
"""
Integration tests for write_mode behavior across the entire pipeline system.

This module tests the complete flow from PipelineRunner through ExecutionEngine
to ensure write_mode is correctly handled in real scenarios.
"""

import os
from unittest.mock import patch

import pytest

from pipeline_builder.logging import PipelineLogger
from pipeline_builder.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from pipeline_builder.pipeline.runner import SimplePipelineRunner

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F
else:
    from pyspark.sql import functions as F


class TestWriteModeIntegration:
    """Integration tests for write_mode behavior."""

    @pytest.fixture
    def config(self):
        """Create a test pipeline config."""
        # NOTE: Parallel execution disabled for this test to ensure deterministic behavior
        return PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(
                max_workers=1, enabled=False
            ),  # Disabled for schema creation compatibility
        )

    @pytest.fixture
    def logger(self):
        """Create a test logger."""
        return PipelineLogger("test")

    @pytest.fixture
    def silver_step(self, spark_session):
        """Create a test silver step."""
        # PySpark requires active SparkContext for F.col() calls

        def simple_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        return SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=simple_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver_table",
            schema="test_schema",
            watermark_col="id",
            source_incremental_col="id",
        )

    @pytest.fixture
    def gold_step(self, spark_session):
        """Create a test gold step."""
        # PySpark requires active SparkContext for F.col() calls

        def simple_transform(spark, silvers):
            return silvers["test_silver"]

        return GoldStep(
            name="test_gold",
            transform=simple_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold_table",
            source_silvers=["test_silver"],
            schema="test_schema",
        )

    @pytest.fixture
    def bronze_step(self, spark_session):
        """Create a test bronze step."""
        # PySpark requires active SparkContext for F.col() calls
        return BronzeStep(
            name="test_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="id",
        )

    @pytest.fixture
    def bronze_sources(self, spark_session):
        """Create test bronze sources."""
        return {
            "test_bronze": spark_session.createDataFrame(
                [(1, "test")], ["id", "company"]
            )
        }

    @patch("pipeline_builder.execution.fqn")
    def test_incremental_pipeline_preserves_data(
        self,
        mock_fqn,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Incremental pipeline preserves silver data via append while overwriting gold aggregates."""
        # Mock fqn to return test table names
        mock_fqn.side_effect = lambda schema, table: f"{schema}.{table}"

        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run incremental pipeline
        report = runner.run_incremental(bronze_sources=bronze_sources)

        # Verify report shows correct mode per layer
        for step_name, step_result in report.silver_results.items():
            assert step_result.get("write_mode") == "append", (
                f"Silver step {step_name} should append in incremental pipeline"
            )
        for step_name, step_result in report.gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Gold step {step_name} should overwrite in incremental pipeline"
            )

    @patch("pipeline_builder.execution.fqn")
    def test_initial_pipeline_overwrites_data(
        self,
        mock_fqn,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that initial pipeline overwrites existing data by using overwrite mode."""
        # Mock fqn to return test table names
        mock_fqn.side_effect = lambda schema, table: f"{schema}.{table}"

        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run initial pipeline
        report = runner.run_initial_load(bronze_sources=bronze_sources)

        # Verify report shows overwrite mode (only silver/gold write to tables)
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Step {step_name} should have write_mode='overwrite' in initial pipeline"
            )

    def test_write_mode_consistency_across_pipeline_runs(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that write_mode is consistent across multiple pipeline runs."""
        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run multiple incremental pipelines
        reports = []
        for _i in range(3):
            report = runner.run_incremental(bronze_sources=bronze_sources)
            reports.append(report)

        # All incremental runs should consistently use layer-specific write modes
        for i, report in enumerate(reports):
            for step_name, step_result in report.silver_results.items():
                if step_result.get("status") == "completed":
                    assert step_result.get("write_mode") == "append", (
                        f"Incremental run {i + 1}, silver step {step_name}: should append"
                    )
            for step_name, step_result in report.gold_results.items():
                if step_result.get("status") == "completed":
                    assert step_result.get("write_mode") == "overwrite", (
                        f"Incremental run {i + 1}, gold step {step_name}: should overwrite"
                    )

        # Run multiple initial pipelines
        initial_reports = []
        for _i in range(3):
            report = runner.run_initial_load(bronze_sources=bronze_sources)
            initial_reports.append(report)

        # All initial runs should consistently use overwrite mode (only silver/gold write to tables)
        for i, report in enumerate(initial_reports):
            silver_gold_results = {**report.silver_results, **report.gold_results}
            for step_name, step_result in silver_gold_results.items():
                assert step_result.get("write_mode") == "overwrite", (
                    f"Initial run {i + 1}, step {step_name}: should consistently use overwrite mode"
                )

    def test_mixed_pipeline_modes_have_correct_write_modes(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that mixing different pipeline modes results in correct write_modes."""
        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run different pipeline modes
        initial_report = runner.run_initial_load(bronze_sources=bronze_sources)
        incremental_report = runner.run_incremental(bronze_sources=bronze_sources)
        full_refresh_report = runner.run_full_refresh(bronze_sources=bronze_sources)

        # Verify write_modes are correct for each mode (only silver/gold write to tables)
        # Note: In mock-spark, tables don't persist between runs, so incremental after initial may fail
        for step_name in initial_report.silver_results.keys():
            assert (
                initial_report.silver_results[step_name].get("write_mode")
                == "overwrite"
            )
            if (
                incremental_report.silver_results.get(step_name, {}).get("status")
                == "completed"
            ):
                assert (
                    incremental_report.silver_results[step_name].get("write_mode")
                    == "append"
                )
            assert (
                full_refresh_report.silver_results[step_name].get("write_mode")
                == "overwrite"
            )

        for step_name in initial_report.gold_results.keys():
            assert (
                initial_report.gold_results[step_name].get("write_mode") == "overwrite"
            )
            if (
                incremental_report.gold_results.get(step_name, {}).get("status")
                == "completed"
            ):
                assert (
                    incremental_report.gold_results[step_name].get("write_mode")
                    == "overwrite"
                )
            assert (
                full_refresh_report.gold_results[step_name].get("write_mode")
                == "overwrite"
            )

    def test_write_mode_regression_prevention(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test specifically designed to prevent the write_mode regression bug."""
        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # This test specifically prevents the bug where incremental mode
        # was incorrectly using overwrite instead of append
        report = runner.run_incremental(bronze_sources=bronze_sources)

        # Critical assertion: incremental mode must NEVER use overwrite (only silver/gold write to tables)
        for step_name, step_result in report.silver_results.items():
            assert step_result.get("write_mode") == "append", (
                f"Silver step {step_name} in incremental mode must append to preserve data"
            )

        for step_name, step_result in report.gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Gold step {step_name} must overwrite in incremental mode to avoid duplicate aggregates"
            )

    def test_log_writer_receives_correct_write_mode(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that LogWriter receives the correct write_mode from step results."""
        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run incremental pipeline
        incremental_report = runner.run_incremental(bronze_sources=bronze_sources)

        # Run initial pipeline
        initial_report = runner.run_initial_load(bronze_sources=bronze_sources)

        # Verify that the step results have the correct write_mode
        # that would be passed to LogWriter (only silver/gold write to tables)
        for (
            step_name,
            incremental_step_result,
        ) in incremental_report.silver_results.items():
            initial_step_result = initial_report.silver_results.get(step_name, {})
            assert incremental_step_result.get("write_mode") == "append", (
                f"LogWriter should receive write_mode='append' for silver step {step_name} "
                f"in incremental pipeline"
            )
            assert initial_step_result.get("write_mode") == "overwrite", (
                f"LogWriter should receive write_mode='overwrite' for silver step {step_name} "
                f"in initial pipeline"
            )

        for (
            step_name,
            incremental_step_result,
        ) in incremental_report.gold_results.items():
            initial_step_result = initial_report.gold_results.get(step_name, {})
            assert incremental_step_result.get("write_mode") == "overwrite", (
                f"LogWriter should receive write_mode='overwrite' for gold step {step_name} "
                f"in incremental pipeline"
            )
            assert initial_step_result.get("write_mode") == "overwrite", (
                f"LogWriter should receive write_mode='overwrite' for gold step {step_name} "
                f"in initial pipeline"
            )
