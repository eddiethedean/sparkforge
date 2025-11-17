#!/usr/bin/env python3
"""
Comprehensive tests for ExecutionEngine write_mode behavior.

This module tests that the ExecutionEngine correctly sets write_mode based on
execution mode to prevent data overwriting issues in incremental pipelines.
"""

import os

import pytest

from pipeline_builder.execution import ExecutionEngine, ExecutionMode
from pipeline_builder.logging import PipelineLogger
from pipeline_builder.models import (
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import SparkSession
    from mock_spark import functions as F
else:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F


class TestExecutionEngineWriteMode:
    """Test cases for ExecutionEngine write_mode behavior."""

    @pytest.fixture
    def spark_session(self):
        """Create a mock Spark session."""
        return SparkSession()

    @pytest.fixture
    def config(self):
        """Create a test pipeline config."""
        return PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

    @pytest.fixture
    def logger(self):
        """Create a test logger."""
        return PipelineLogger("test")

    @pytest.fixture
    def execution_engine(self, spark_session, config, logger):
        """Create an execution engine for testing."""
        return ExecutionEngine(spark_session, config, logger)

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
    def context(self, spark_session):
        """Create test execution context."""
        test_df = spark_session.createDataFrame([(1, "test")], ["id", "name"])
        return {
            "test_bronze": test_df,
            "test_silver": test_df,  # For gold step tests
        }

    def test_incremental_mode_uses_append_for_silver_step(
        self, execution_engine, silver_step, context
    ):
        """Test that incremental mode uses append write_mode for Silver steps."""
        # Execute step in incremental mode
        result = execution_engine.execute_step(
            silver_step, context, ExecutionMode.INCREMENTAL
        )

        # Verify write_mode is set to append
        assert result.write_mode == "append", (
            f"Expected write_mode='append' for incremental mode, "
            f"but got '{result.write_mode}'"
        )

    def test_incremental_mode_uses_overwrite_for_gold_step(
        self, execution_engine, gold_step, context
    ):
        """Gold steps should always overwrite, even during incremental runs."""
        # Execute step in incremental mode
        result = execution_engine.execute_step(
            gold_step, context, ExecutionMode.INCREMENTAL
        )

        # Verify write_mode is set to overwrite
        assert result.write_mode == "overwrite", (
            f"Expected write_mode='overwrite' for gold incremental mode, "
            f"but got '{result.write_mode}'"
        )

    def test_initial_mode_uses_overwrite_for_silver_step(
        self, execution_engine, silver_step, context
    ):
        """Test that initial mode uses overwrite write_mode for Silver steps."""
        # Execute step in initial mode
        result = execution_engine.execute_step(
            silver_step, context, ExecutionMode.INITIAL
        )

        # Verify write_mode is set to overwrite
        assert result.write_mode == "overwrite", (
            f"Expected write_mode='overwrite' for initial mode, "
            f"but got '{result.write_mode}'"
        )

    def test_initial_mode_uses_overwrite_for_gold_step(
        self, execution_engine, gold_step, context
    ):
        """Test that initial mode uses overwrite write_mode for Gold steps."""
        # Execute step in initial mode
        result = execution_engine.execute_step(
            gold_step, context, ExecutionMode.INITIAL
        )

        # Verify write_mode is set to overwrite
        assert result.write_mode == "overwrite", (
            f"Expected write_mode='overwrite' for initial mode, "
            f"but got '{result.write_mode}'"
        )

    def test_full_refresh_mode_uses_overwrite_for_silver_step(
        self, execution_engine, silver_step, context
    ):
        """Test that full_refresh mode uses overwrite write_mode for Silver steps."""
        # Execute step in full_refresh mode
        result = execution_engine.execute_step(
            silver_step, context, ExecutionMode.FULL_REFRESH
        )

        # Verify write_mode is set to overwrite
        assert result.write_mode == "overwrite", (
            f"Expected write_mode='overwrite' for full_refresh mode, "
            f"but got '{result.write_mode}'"
        )

    def test_full_refresh_mode_uses_overwrite_for_gold_step(
        self, execution_engine, gold_step, context
    ):
        """Test that full_refresh mode uses overwrite write_mode for Gold steps."""
        # Execute step in full_refresh mode
        result = execution_engine.execute_step(
            gold_step, context, ExecutionMode.FULL_REFRESH
        )

        # Verify write_mode is set to overwrite
        assert result.write_mode == "overwrite", (
            f"Expected write_mode='overwrite' for full_refresh mode, "
            f"but got '{result.write_mode}'"
        )

    def test_validation_only_mode_has_no_write_mode_for_silver_step(
        self, execution_engine, silver_step, context
    ):
        """Test that validation_only mode has no write_mode for Silver steps."""
        # Execute step in validation_only mode
        result = execution_engine.execute_step(
            silver_step, context, ExecutionMode.VALIDATION_ONLY
        )

        # Verify write_mode is None (no writing occurs)
        assert result.write_mode is None, (
            f"Expected write_mode=None for validation_only mode, "
            f"but got '{result.write_mode}'"
        )

    def test_validation_only_mode_has_no_write_mode_for_gold_step(
        self, execution_engine, gold_step, context
    ):
        """Test that validation_only mode has no write_mode for Gold steps."""
        # Execute step in validation_only mode
        result = execution_engine.execute_step(
            gold_step, context, ExecutionMode.VALIDATION_ONLY
        )

        # Verify write_mode is None (no writing occurs)
        assert result.write_mode is None, (
            f"Expected write_mode=None for validation_only mode, "
            f"but got '{result.write_mode}'"
        )

    def test_spark_write_mode_matches_result_write_mode(
        self, execution_engine, silver_step, gold_step, context
    ):
        """Test that result write_mode correctly reflects the execution mode."""
        # Test Silver step
        result_inc_silver = execution_engine.execute_step(
            silver_step, context, ExecutionMode.INCREMENTAL
        )
        assert result_inc_silver.write_mode == "append"

        result_init_silver = execution_engine.execute_step(
            silver_step, context, ExecutionMode.INITIAL
        )
        assert result_init_silver.write_mode == "overwrite"

        # Test Gold step
        result_inc_gold = execution_engine.execute_step(
            gold_step, context, ExecutionMode.INCREMENTAL
        )
        assert result_inc_gold.write_mode == "overwrite"

        result_init_gold = execution_engine.execute_step(
            gold_step, context, ExecutionMode.INITIAL
        )
        assert result_init_gold.write_mode == "overwrite"

    def test_all_execution_modes_covered(self):
        """Test that all execution modes are properly handled."""
        # This test ensures we don't miss any execution modes
        execution_modes = list(ExecutionMode)
        expected_modes = [
            ExecutionMode.INITIAL,
            ExecutionMode.INCREMENTAL,
            ExecutionMode.FULL_REFRESH,
            ExecutionMode.VALIDATION_ONLY,
        ]

        assert len(execution_modes) == len(expected_modes), (
            f"Expected {len(expected_modes)} execution modes, "
            f"but found {len(execution_modes)}"
        )

        for mode in expected_modes:
            assert mode in execution_modes, f"Execution mode {mode} not found"

    def test_write_mode_consistency_across_step_types(
        self, execution_engine, silver_step, gold_step, context
    ):
        """Test that write_mode behavior is consistent across step types."""
        # Test all modes with both step types
        silver_expectations = {
            ExecutionMode.INITIAL: "overwrite",
            ExecutionMode.INCREMENTAL: "append",
            ExecutionMode.FULL_REFRESH: "overwrite",
            ExecutionMode.VALIDATION_ONLY: None,
        }
        gold_expectations = {
            ExecutionMode.INITIAL: "overwrite",
            ExecutionMode.INCREMENTAL: "overwrite",
            ExecutionMode.FULL_REFRESH: "overwrite",
            ExecutionMode.VALIDATION_ONLY: None,
        }

        for mode, expected_write_mode in silver_expectations.items():
            silver_result = execution_engine.execute_step(silver_step, context, mode)
            assert silver_result.write_mode == expected_write_mode, (
                f"Silver step in {mode.value} mode: expected write_mode={expected_write_mode}, "
                f"got {silver_result.write_mode}"
            )

        for mode, expected_write_mode in gold_expectations.items():
            gold_result = execution_engine.execute_step(gold_step, context, mode)
            assert gold_result.write_mode == expected_write_mode, (
                f"Gold step in {mode.value} mode: expected write_mode={expected_write_mode}, "
                f"got {gold_result.write_mode}"
            )

    def test_bronze_step_has_no_write_mode(self, execution_engine, context):
        """Test that Bronze steps have no write_mode (they don't write to tables)."""
        from pipeline_builder.models import BronzeStep

        # Create a bronze step
        bronze_step = BronzeStep(
            name="test_bronze",
            rules={"id": [F.col("id").isNotNull()]},
        )

        # Test all modes - Bronze steps should never have write_mode
        for mode in ExecutionMode:
            result = execution_engine.execute_step(bronze_step, context, mode)
            assert result.write_mode is None, (
                f"Bronze step in {mode.value} mode should have write_mode=None, "
                f"but got {result.write_mode}"
            )


class TestWriteModeRegression:
    """Test cases to prevent regression of the write_mode bug."""

    def test_incremental_mode_uses_append_for_silver_steps(self, spark_session):
        """Incremental silver writes must append to prevent data loss."""
        from pipeline_builder.models import (
            ParallelConfig,
            PipelineConfig,
            SilverStep,
            ValidationThresholds,
        )

        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        logger = PipelineLogger("test")
        execution_engine = ExecutionEngine(spark_session, config, logger)

        def simple_transform(spark, bronze_df, prior_silvers):
            return bronze_df

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=simple_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        context = {
            "test_bronze": spark_session.createDataFrame([(1, "test")], ["id", "name"])
        }

        result = execution_engine.execute_step(
            silver_step, context, ExecutionMode.INCREMENTAL
        )

        assert result.write_mode == "append", (
            "Incremental mode must use append write_mode to preserve existing data"
        )

    def test_gold_incremental_mode_uses_overwrite(self, spark_session):
        """Regression test ensuring gold steps keep overwrite semantics."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )
        logger = PipelineLogger("test")
        execution_engine = ExecutionEngine(spark_session, config, logger)

        def simple_transform(_spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            transform=simple_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold_table",
            source_silvers=["test_silver"],
            schema="test_schema",
        )

        context = {
            "test_silver": spark_session.createDataFrame([(1, "test")], ["id", "name"])
        }

        result = execution_engine.execute_step(
            gold_step, context, ExecutionMode.INCREMENTAL
        )
        assert result.write_mode == "overwrite", (
            "Gold steps must overwrite during incremental runs to avoid double counting"
        )
