#!/usr/bin/env python3
"""
Tests for PipelineRunner write_mode behavior to ensure incremental pipelines work correctly.

This module tests that the PipelineRunner correctly propagates execution modes
and that write_mode is properly set for incremental vs initial runs.
"""

import os
from unittest.mock import Mock, patch

import pytest

from pipeline_builder.logging import PipelineLogger
from pipeline_builder.models import (
    BronzeStep,
    GoldStep,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from pipeline_builder.pipeline.models import PipelineMode
from pipeline_builder.pipeline.runner import SimplePipelineRunner

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F


# Skip all tests in this file when running in real mode
pytestmark = pytest.mark.skipif(
    os.environ.get("SPARK_MODE", "mock").lower() == "real",
    reason="This test module is designed for sparkless/mock mode only",
)


class TestPipelineRunnerWriteMode:
    """Test cases for PipelineRunner write_mode behavior."""

    @pytest.fixture
    def config(self):
        """Create a test pipeline config."""
        # NOTE: Parallel execution disabled for this test to ensure deterministic behavior
        return PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),  # Disabled for schema creation compatibility
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

    def test_run_incremental_sets_expected_write_modes(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Incremental runs append silver data but overwrite gold aggregates."""
        # Create pipeline runner with steps
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

        for step_name, step_result in report.silver_results.items():
            assert step_result.get("write_mode") == "append", (
                f"Silver step {step_name} in incremental run should append, "
                f"but got '{step_result.get('write_mode')}'"
            )

        for step_name, step_result in report.gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Gold step {step_name} in incremental run should overwrite, "
                f"but got '{step_result.get('write_mode')}'"
            )

    def test_run_initial_load_uses_overwrite_mode(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that run_initial_load uses overwrite mode for all steps."""
        # Create pipeline runner with steps
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run initial load pipeline
        report = runner.run_initial_load(bronze_sources=bronze_sources)

        # Verify all step results have overwrite write_mode (only silver/gold write to tables)
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Step {step_name} in initial run should have write_mode='overwrite', "
                f"but got '{step_result.get('write_mode')}'"
            )

    def test_run_full_refresh_uses_overwrite_mode(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that run_full_refresh uses overwrite mode for all steps."""
        # Create pipeline runner with steps
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run full refresh pipeline
        report = runner.run_full_refresh(bronze_sources=bronze_sources)

        # Verify all step results have overwrite write_mode (only silver/gold write to tables)
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Step {step_name} in full_refresh run should have write_mode='overwrite', "
                f"but got '{step_result.get('write_mode')}'"
            )

    def test_run_pipeline_with_incremental_mode_sets_expected_write_modes(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """run_pipeline(INCREMENTAL) should append silver data and overwrite gold."""
        # Create pipeline runner with steps
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run pipeline with incremental mode
        steps = [bronze_step, silver_step, gold_step]
        report = runner.run_pipeline(steps, PipelineMode.INCREMENTAL, bronze_sources)

        for step_name, step_result in report.silver_results.items():
            assert step_result.get("write_mode") == "append", (
                f"Silver step {step_name} in incremental pipeline should append, "
                f"but got '{step_result.get('write_mode')}'"
            )

        for step_name, step_result in report.gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Gold step {step_name} in incremental pipeline should overwrite, "
                f"but got '{step_result.get('write_mode')}'"
            )

    def test_run_pipeline_with_initial_mode_uses_overwrite(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that run_pipeline with INITIAL mode uses overwrite for all steps."""
        # Create pipeline runner with steps
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run pipeline with initial mode
        steps = [bronze_step, silver_step, gold_step]
        report = runner.run_pipeline(steps, PipelineMode.INITIAL, bronze_sources)

        # Verify all step results have overwrite write_mode (only silver/gold write to tables)
        silver_gold_results = {**report.silver_results, **report.gold_results}
        for step_name, step_result in silver_gold_results.items():
            assert step_result.get("write_mode") == "overwrite", (
                f"Step {step_name} in initial pipeline should have write_mode='overwrite', "
                f"but got '{step_result.get('write_mode')}'"
            )

    def test_pipeline_mode_mapping_to_execution_mode(self):
        """Test that PipelineMode correctly maps to ExecutionMode."""
        from pipeline_builder.execution import ExecutionMode

        # Test mode mapping (this is done in the runner's _convert_mode method)
        expected_mappings = {
            PipelineMode.INITIAL: ExecutionMode.INITIAL,
            PipelineMode.INCREMENTAL: ExecutionMode.INCREMENTAL,
            PipelineMode.FULL_REFRESH: ExecutionMode.FULL_REFRESH,
            PipelineMode.VALIDATION_ONLY: ExecutionMode.VALIDATION_ONLY,
        }

        for pipeline_mode, expected_execution_mode in expected_mappings.items():
            # This test ensures the mapping is correct
            assert pipeline_mode.value == expected_execution_mode.value, (
                f"PipelineMode {pipeline_mode.value} should map to "
                f"ExecutionMode {expected_execution_mode.value}"
            )

    def test_incremental_vs_initial_write_mode_difference(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Silver incremental runs differ from initial; gold always overwrites."""
        # Create pipeline runner with steps
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

        for step_name in incremental_report.silver_results.keys():
            incremental_mode = incremental_report.silver_results[step_name].get(
                "write_mode"
            )
            initial_mode = initial_report.silver_results[step_name].get("write_mode")
            assert incremental_mode == "append", (
                f"Silver step {step_name}: incremental mode should append, got '{incremental_mode}'"
            )
            assert initial_mode == "overwrite", (
                f"Silver step {step_name}: initial mode should overwrite, got '{initial_mode}'"
            )

        for step_name in incremental_report.gold_results.keys():
            incremental_mode = incremental_report.gold_results[step_name].get(
                "write_mode"
            )
            initial_mode = initial_report.gold_results[step_name].get("write_mode")
            assert incremental_mode == "overwrite", (
                f"Gold step {step_name}: incremental mode should overwrite, got '{incremental_mode}'"
            )
            assert initial_mode == "overwrite", (
                f"Gold step {step_name}: initial mode should overwrite, got '{initial_mode}'"
            )

    def test_no_data_loss_in_incremental_mode_for_silver_steps(
        self,
        spark_session,
        config,
        logger,
        bronze_step,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Incremental mode must append silver tables to preserve existing data."""
        # Create pipeline runner with steps
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            bronze_steps={"test_bronze": bronze_step},
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run incremental pipeline multiple times
        report1 = runner.run_incremental(bronze_sources=bronze_sources)
        report2 = runner.run_incremental(bronze_sources=bronze_sources)

        silver_results1 = report1.silver_results
        silver_results2 = report2.silver_results
        for step_name, step_result in silver_results1.items():
            assert step_result.get("write_mode") == "append", (
                f"First incremental run for silver step {step_name} should append"
            )
            if silver_results2.get(step_name, {}).get("status") == "completed":
                assert silver_results2[step_name].get("write_mode") == "append", (
                    f"Second incremental run for silver step {step_name} should append"
                )

    @patch("pipeline_builder.execution.ExecutionEngine.execute_step")
    def test_execution_engine_receives_correct_mode(
        self,
        mock_execute_step,
        spark_session,
        config,
        logger,
        silver_step,
        gold_step,
        bronze_sources,
    ):
        """Test that ExecutionEngine receives the correct execution mode from PipelineRunner."""
        # Mock the execute_step method to capture the mode parameter
        mock_execute_step.return_value = Mock()
        mock_execute_step.return_value.write_mode = "test_mode"

        # Create pipeline runner
        runner = SimplePipelineRunner(
            spark=spark_session,
            config=config,
            logger=logger,
            silver_steps={"test_silver": silver_step},
            gold_steps={"test_gold": gold_step},
        )

        # Run incremental pipeline
        runner.run_incremental(bronze_sources=bronze_sources)

        # Verify that execute_step was called with INCREMENTAL mode
        assert mock_execute_step.called, "execute_step should have been called"

        # Check the mode parameter passed to execute_step
        call_args = mock_execute_step.call_args
        mode_passed = call_args[0][2]  # Third positional argument should be the mode

        from pipeline_builder.execution import ExecutionMode

        assert mode_passed == ExecutionMode.INCREMENTAL, (
            f"execute_step should have been called with ExecutionMode.INCREMENTAL, "
            f"but got {mode_passed}"
        )
