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
    from mock_spark import MockSparkSession as SparkSession
    from mock_spark import functions as F
else:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F


class TestWriteModeIntegration:
    """Integration tests for write_mode behavior."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        session = SparkSession()
        # Ensure test_schema exists (required in mock-spark 2.16.1+)
        try:
            session.storage.create_schema("test_schema")
        except Exception:
            # Try SQL approach if storage API doesn't work
            try:
                session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
            except Exception:
                pass  # Schema might already exist
        return session

    @pytest.fixture
    def config(self):
        """Create a test pipeline config."""
        # NOTE: Parallel execution disabled due to mock-spark 2.16.1 threading issue
        # where DuckDB connections in worker threads don't see schemas created in main thread.
        # This is a known limitation of mock-spark 2.16.1 with parallel execution.
        return PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=1, enabled=False),  # Disabled for schema creation compatibility
        )

    @pytest.fixture
    def logger(self):
        """Create a test logger."""
        return PipelineLogger("test")

    @pytest.fixture
    def silver_step(self):
        """Create a test silver step."""

        def simple_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        return SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=simple_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver_table",
            schema="test_schema",
        )

    @pytest.fixture
    def gold_step(self):
        """Create a test gold step."""

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
    def bronze_step(self):
        """Create a test bronze step."""
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
        """Test that incremental pipeline preserves existing data by using append mode."""
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

        # Verify report shows append mode (only silver/gold write to tables)
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") == "append", (
                f"Step {step_name} should have write_mode='append' in incremental pipeline"
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
        self, spark_session, config, logger, bronze_step, silver_step, gold_step, bronze_sources
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

        # All incremental runs should consistently use append mode (only silver/gold write to tables)
        # Note: In mock-spark, tables don't persist between runs, so later runs may fail
        # We only check write_mode for successful steps
        for i, report in enumerate(reports):
            silver_gold_results = {**report.silver_results, **report.gold_results}
            for step_name, step_result in silver_gold_results.items():
                # Only check write_mode if step succeeded (mock-spark tables don't persist)
                if step_result.get("status") == "completed":
                    assert step_result.get("write_mode") == "append", (
                        f"Incremental run {i + 1}, step {step_name}: should consistently use append mode"
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
        self, spark_session, config, logger, bronze_step, silver_step, gold_step, bronze_sources
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
        initial_silver_gold = {**initial_report.silver_results, **initial_report.gold_results}
        incremental_silver_gold = {**incremental_report.silver_results, **incremental_report.gold_results}
        full_refresh_silver_gold = {**full_refresh_report.silver_results, **full_refresh_report.gold_results}
        for step_name in initial_silver_gold.keys():
            # Initial should use overwrite
            assert initial_silver_gold[step_name].get("write_mode") == "overwrite"

            # Incremental should use append (only check if step succeeded - mock-spark limitation)
            if incremental_silver_gold.get(step_name, {}).get("status") == "completed":
                assert incremental_silver_gold[step_name].get("write_mode") == "append"

            # Full refresh should use overwrite
            assert full_refresh_silver_gold[step_name].get("write_mode") == "overwrite"

    def test_write_mode_regression_prevention(
        self, spark_session, config, logger, bronze_step, silver_step, gold_step, bronze_sources
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
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") != "overwrite", (
                f"CRITICAL BUG DETECTED: Step {step_name} in incremental mode "
                f"is using overwrite! This will cause data loss. "
                f"Bug fix may have been reverted."
            )

            assert step_result.get("write_mode") == "append", (
                f"Step {step_name} in incremental mode must use append to preserve data"
            )

    def test_log_writer_receives_correct_write_mode(
        self, spark_session, config, logger, bronze_step, silver_step, gold_step, bronze_sources
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
        incremental_silver_gold = {**incremental_report.silver_results, **incremental_report.gold_results}
        initial_silver_gold = {**initial_report.silver_results, **initial_report.gold_results}
        for step_name in incremental_silver_gold.keys():
            incremental_step_result = incremental_silver_gold[step_name]
            initial_step_result = initial_silver_gold[step_name]

            # These write_modes should be passed to LogWriter and logged correctly
            assert incremental_step_result.get("write_mode") == "append", (
                f"LogWriter should receive write_mode='append' for step {step_name} "
                f"in incremental pipeline"
            )

            assert initial_step_result.get("write_mode") == "overwrite", (
                f"LogWriter should receive write_mode='overwrite' for step {step_name} "
                f"in initial pipeline"
            )
