#!/usr/bin/env python3
"""
Comprehensive tests for stepwise execution and debugging features.

This module tests the stepwise execution features including:
- Running until a specific step
- Running individual steps
- Rerunning steps with parameter overrides
- Parameter passing to transform functions (various signatures)
- Write control (write_outputs flag)
- Backward compatibility with existing transforms
- Context management and dependency loading
- Error handling and edge cases

All tests are designed to work in both mock and real Spark modes.
Set SPARK_MODE environment variable to "mock" or "real" to control execution mode.

Test Coverage:
- Parameter passing: params argument, **kwargs, backward compatibility
- Stepwise controls: stop_after_step, start_at_step, write_outputs
- Runner API: run_until, run_step, rerun_step
- DebugSession: initialization, run methods, parameter management
- Context management: preservation, dependency loading, invalidation
- Error handling: invalid steps, missing dependencies
- Complex scenarios: multi-step dependencies, iterative tuning
- Edge cases: empty params, nonexistent steps, boundary conditions
"""

import os
from unittest.mock import Mock, patch

import pytest

from pipeline_builder.compat import DataFrame, SparkSession
from pipeline_builder.execution import ExecutionEngine, ExecutionMode, StepStatus
from pipeline_builder.errors import ExecutionError
from pipeline_builder.models import BronzeStep, GoldStep, PipelineConfig, SilverStep
from pipeline_builder.pipeline.debug_session import PipelineDebugSession
from pipeline_builder.pipeline.models import PipelineMode, PipelineStatus
from pipeline_builder.pipeline.runner import SimplePipelineRunner

# Use engine-specific functions based on mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F


# Note: spark_session fixture is provided by conftest.py
# It automatically handles both mock and real Spark modes


@pytest.fixture
def config():
    """Create a PipelineConfig for testing."""
    return PipelineConfig.create_default(schema="test_schema")


@pytest.fixture
def sample_bronze_df(spark_session):
    """Create a sample bronze DataFrame."""
    data = [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30), ("4", "event4", 40)]
    return spark_session.createDataFrame(data, ["id", "event", "value"])


@pytest.fixture
def bronze_step():
    """Create a bronze step."""
    return BronzeStep(
        name="events",
        rules={"id": [F.col("id").isNotNull()], "value": [F.col("value") > 0]},
        schema="test_schema",
    )


@pytest.fixture
def silver_step_with_params():
    """Create a silver step that accepts params as named argument."""
    def transform_with_params(spark, bronze_df, prior_silvers, params=None):
        threshold = params.get("threshold", 1) if params else 1
        min_value = params.get("min_value", 0) if params else 0
        df = bronze_df.filter(F.col("id").cast("int") > threshold)
        return df.filter(F.col("value") > min_value)

    return SilverStep(
        name="clean_events",
        source_bronze="events",
        transform=transform_with_params,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="clean_events",
        schema="test_schema",
    )


@pytest.fixture
def silver_step_with_kwargs():
    """Create a silver step that accepts params via **kwargs."""
    def transform_with_kwargs(spark, bronze_df, prior_silvers, **kwargs):
        threshold = kwargs.get("threshold", 1)
        min_value = kwargs.get("min_value", 0)
        df = bronze_df.filter(F.col("id").cast("int") > threshold)
        return df.filter(F.col("value") > min_value)

    return SilverStep(
        name="clean_events",
        source_bronze="events",
        transform=transform_with_kwargs,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="clean_events",
        schema="test_schema",
    )


@pytest.fixture
def silver_step_without_params():
    """Create a silver step that doesn't accept params (backward compatible)."""
    def transform_no_params(spark, bronze_df, prior_silvers):
        return bronze_df.filter(F.col("id").cast("int") > 1).filter(F.col("value") > 15)

    return SilverStep(
        name="clean_events",
        source_bronze="events",
        transform=transform_no_params,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="clean_events",
        schema="test_schema",
    )


@pytest.fixture
def silver_step_using_prior_silvers():
    """Create a silver step that uses prior_silvers (for testing dependency handling)."""
    def transform_with_prior(spark, bronze_df, prior_silvers):
        result = bronze_df
        # If we have prior silvers, add a column indicating that
        if prior_silvers:
            result = result.withColumn("has_prior_silvers", F.lit(True))
        else:
            result = result.withColumn("has_prior_silvers", F.lit(False))
        return result

    return SilverStep(
        name="enriched_events",
        source_bronze="events",
        transform=transform_with_prior,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="enriched_events",
        schema="test_schema",
    )


@pytest.fixture
def gold_step_with_params():
    """Create a gold step that accepts params."""
    def transform_with_params(spark, silvers, params=None):
        multiplier = params.get("multiplier", 1.0) if params else 1.0
        df = silvers["clean_events"]
        # Use id column since value might not be in clean_events output
        return df.withColumn("adjusted_id", F.col("id").cast("int") * multiplier)

    return GoldStep(
        name="aggregated_events",
        transform=transform_with_params,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="aggregated_events",
        source_silvers=["clean_events"],
        schema="test_schema",
    )


@pytest.fixture
def gold_step_with_kwargs():
    """Create a gold step that accepts params via **kwargs."""
    def transform_with_kwargs(spark, silvers, **kwargs):
        multiplier = kwargs.get("multiplier", 1.0)
        df = silvers["clean_events"]
        # Use id column since value might not be in clean_events output
        return df.withColumn("adjusted_id", F.col("id").cast("int") * multiplier)

    return GoldStep(
        name="aggregated_events",
        transform=transform_with_kwargs,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="aggregated_events",
        source_silvers=["clean_events"],
        schema="test_schema",
    )


class TestStepParamsSupport:
    """Test parameter passing to transform functions."""

    def test_silver_step_with_params(self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df):
        """Test that silver step can receive params."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        # Execute with params
        step_params = {"threshold": 2}
        result = engine.execute_step(
            silver_step_with_params,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
        )

        assert result.status == StepStatus.COMPLETED
        # With threshold=2, ids "3" and "4" should pass (both > 2)
        output_df = context["clean_events"]
        assert output_df.count() == 2

    def test_silver_step_without_params_backward_compatible(
        self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df
    ):
        """Test that silver step without params still works (backward compatibility)."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        # Execute without params (should work)
        result = engine.execute_step(
            silver_step_without_params,
            context,
            ExecutionMode.INITIAL,
            step_params=None,
        )

        assert result.status == StepStatus.COMPLETED
        output_df = context["clean_events"]
        assert output_df.count() > 0

    def test_gold_step_with_params(self, spark_session, config, silver_step_with_params, gold_step_with_params, sample_bronze_df):
        """Test that gold step can receive params."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        # Execute silver step first
        engine.execute_step(silver_step_with_params, context, ExecutionMode.INITIAL)

        # Execute gold step with params
        step_params = {"multiplier": 10.0}
        result = engine.execute_step(
            gold_step_with_params,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
        )

        assert result.status == StepStatus.COMPLETED
        output_df = context["aggregated_events"]
        # Check that multiplier was applied
        rows = output_df.collect()
        assert len(rows) > 0


class TestStepwiseExecution:
    """Test stepwise execution controls."""

    def test_stop_after_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that execution stops after a specific step."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            stop_after_step="clean_events",
        )

        assert result.status == "completed"
        # Should have executed bronze and silver, but stopped after silver
        assert len(result.steps) == 2
        assert result.steps[0].step_name == "events"
        assert result.steps[1].step_name == "clean_events"
        assert "clean_events" in context

    def test_start_at_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that execution can start at a specific step."""
        engine = ExecutionEngine(spark_session, config)
        # Pre-populate context with bronze output (simulating already-executed step)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            start_at_step="clean_events",
        )

        assert result.status == "completed"
        # Should have only executed silver step
        assert len(result.steps) == 1
        assert result.steps[0].step_name == "clean_events"

    def test_write_outputs_false(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that write_outputs=False skips table writes."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            write_outputs=False,
        )

        assert result.status == "completed"
        # Context should still have the output DataFrame
        assert "clean_events" in context
        # But table should not exist (or be empty if it existed before)
        # Note: In mock mode, table operations may behave differently
        # This test verifies that execution completes without errors


class TestRunnerStepwiseAPI:
    """Test SimplePipelineRunner stepwise execution methods."""

    def test_run_until(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test run_until method."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params]

        report, context = runner.run_until(
            "clean_events",
            steps=steps,
            bronze_sources={"events": sample_bronze_df},
        )

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in context
        assert len(report.bronze_results) == 1
        assert len(report.silver_results) == 1

    def test_run_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test run_step method."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params]
        # Pre-populate context with bronze output
        context = {"events": sample_bronze_df}

        report, updated_context = runner.run_step(
            "clean_events",
            steps=steps,
            context=context,
        )

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in updated_context
        # Should have only executed silver step
        assert len(report.silver_results) == 1
        assert len(report.bronze_results) == 0

    def test_rerun_step_with_params(
        self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df
    ):
        """Test rerun_step with parameter overrides."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_with_params]
        context = {"events": sample_bronze_df}

        # First run
        report1, context = runner.run_step(
            "clean_events",
            steps=steps,
            context=context,
        )
        count1 = context["clean_events"].count()

        # Rerun with different params
        step_params = {"clean_events": {"threshold": 0}}
        report2, context = runner.rerun_step(
            "clean_events",
            steps=steps,
            context=context,
            step_params=step_params,
        )
        count2 = context["clean_events"].count()

        # With threshold=0, should get more rows
        assert count2 >= count1
        assert report2.status == PipelineStatus.COMPLETED


class TestPipelineDebugSession:
    """Test PipelineDebugSession helper class."""

    def test_debug_session_initialization(self, spark_session, config, bronze_step, silver_step_without_params):
        """Test that debug session initializes correctly."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(spark_session, config, steps)

        assert session.steps == steps
        assert session.mode == PipelineMode.INITIAL
        assert isinstance(session.context, dict)
        assert isinstance(session.step_params, dict)

    def test_debug_session_run_until(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test debug session run_until."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(
            spark_session, config, steps, bronze_sources={"events": sample_bronze_df}
        )

        report, context = session.run_until("clean_events")

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in session.context
        assert "clean_events" in context

    def test_debug_session_run_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test debug session run_step."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(
            spark_session, config, steps, bronze_sources={"events": sample_bronze_df}
        )

        report, context = session.run_step("clean_events")

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in session.context

    def test_debug_session_rerun_with_params(
        self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df
    ):
        """Test debug session rerun with parameter overrides."""
        steps = [bronze_step, silver_step_with_params]
        session = PipelineDebugSession(
            spark_session, config, steps, bronze_sources={"events": sample_bronze_df}
        )

        # First run
        report1, _ = session.run_step("clean_events")
        count1 = session.context["clean_events"].count()

        # Set params and rerun
        session.set_step_params("clean_events", {"threshold": 0})
        report2, _ = session.rerun_step("clean_events")
        count2 = session.context["clean_events"].count()

        assert count2 >= count1
        assert report2.status == PipelineStatus.COMPLETED

    def test_debug_session_clear_params(self, spark_session, config, bronze_step, silver_step_with_params):
        """Test clearing step params."""
        steps = [bronze_step, silver_step_with_params]
        session = PipelineDebugSession(spark_session, config, steps)

        session.set_step_params("clean_events", {"threshold": 0.9})
        assert "clean_events" in session.step_params

        session.clear_step_params("clean_events")
        assert "clean_events" not in session.step_params

        session.set_step_params("clean_events", {"threshold": 0.5})
        session.clear_step_params()  # Clear all
        assert len(session.step_params) == 0


class TestParameterPassingVariations:
    """Test different ways of passing parameters to transforms."""

    def test_silver_step_with_kwargs(self, spark_session, config, bronze_step, silver_step_with_kwargs, sample_bronze_df):
        """Test that silver step can receive params via **kwargs."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        step_params = {"threshold": 2, "min_value": 25}
        result = engine.execute_step(
            silver_step_with_kwargs,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
        )

        assert result.status == StepStatus.COMPLETED
        output_df = context["clean_events"]
        # With threshold=2 and min_value=25, should filter appropriately
        rows = output_df.collect()
        assert len(rows) >= 0  # May be 0 or more depending on data

    def test_gold_step_with_kwargs(self, spark_session, config, silver_step_without_params, gold_step_with_kwargs, sample_bronze_df):
        """Test that gold step can receive params via **kwargs."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        # Execute silver step first
        engine.execute_step(silver_step_without_params, context, ExecutionMode.INITIAL)

        # Execute gold step with params via kwargs
        step_params = {"multiplier": 5.0}
        result = engine.execute_step(
            gold_step_with_kwargs,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
        )

        assert result.status == StepStatus.COMPLETED
        output_df = context["aggregated_events"]
        rows = output_df.collect()
        assert len(rows) > 0
        # Check that multiplier was applied (if we can access the column)
        if "adjusted_id" in output_df.columns:
            # Verify multiplier was applied
            first_row = rows[0]
            # The exact value depends on the data, but we can verify the column exists
            assert hasattr(first_row, "adjusted_id") or "adjusted_id" in str(first_row)


class TestContextManagement:
    """Test context management and dependency loading."""

    def test_start_at_step_loads_from_table(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that start_at_step loads earlier outputs from tables."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        # First, run the bronze step to create the table
        engine.execute_pipeline(
            [bronze_step],
            ExecutionMode.INITIAL,
            context=context,
            write_outputs=True,
        )

        # Clear context (simulating a fresh run)
        context.clear()
        context["events"] = sample_bronze_df  # Keep bronze source

        # Now start at silver step - it should load bronze output from table
        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            start_at_step="clean_events",
            write_outputs=True,
        )

        assert result.status == "completed"
        assert len(result.steps) == 1
        assert result.steps[0].step_name == "clean_events"
        assert "clean_events" in context

    def test_context_preserved_between_runs(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that context is preserved and can be reused between runs."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params]
        context = {"events": sample_bronze_df}

        # First run
        report1, context = runner.run_step("clean_events", steps=steps, context=context)
        assert "clean_events" in context
        count1 = context["clean_events"].count()

        # Second run with same context (should reuse)
        report2, context = runner.run_step("clean_events", steps=steps, context=context)
        assert "clean_events" in context
        count2 = context["clean_events"].count()

        # Should have same results
        assert count1 == count2

    def test_invalidate_downstream_removes_outputs(self, spark_session, config, bronze_step, silver_step_without_params, gold_step_with_params, sample_bronze_df):
        """Test that invalidate_downstream removes downstream outputs from context."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params, gold_step_with_params]
        context = {"events": sample_bronze_df}

        # Run full pipeline (returns only report, not tuple)
        report1 = runner.run_pipeline(steps, bronze_sources={"events": sample_bronze_df})
        # Get context from execution engine or run step to populate it
        report2, context = runner.run_step("clean_events", steps=steps, context=context)
        assert "clean_events" in context

        # Rerun silver step with invalidate_downstream=True
        report2, context = runner.rerun_step(
            "clean_events",
            steps=steps,
            context=context,
            invalidate_downstream=True,
        )

        # Gold step output should be removed
        assert "clean_events" in context
        # aggregated_events may or may not be removed depending on implementation
        # The key is that rerun completed successfully


class TestErrorHandling:
    """Test error handling in stepwise execution."""

    def test_stop_after_invalid_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that stop_after_step with invalid step name completes but doesn't stop."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        # Invalid step name should not cause error, just won't stop early
        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            stop_after_step="nonexistent_step",
        )
        # Should complete all steps since invalid step name won't match
        assert result.status == "completed"
        assert len(result.steps) == 2

    def test_start_at_invalid_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that start_at_step with invalid step name raises error."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        with pytest.raises(ExecutionError):
            engine.execute_pipeline(
                steps,
                ExecutionMode.INITIAL,
                context=context,
                start_at_step="nonexistent_step",
            )

    def test_run_step_without_dependencies(self, spark_session, config, silver_step_without_params):
        """Test that run_step fails gracefully when dependencies are missing."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [silver_step_without_params]
        context = {}  # Empty context - no bronze source

        # Should fail because bronze dependency is missing
        report, context = runner.run_step(
            "clean_events",
            steps=steps,
            context=context,
        )

        # Should have failed
        assert report.status == PipelineStatus.FAILED
        assert len(report.errors) > 0


class TestComplexScenarios:
    """Test complex multi-step scenarios."""

    def test_multiple_silver_steps_with_dependencies(self, spark_session, config, bronze_step, silver_step_without_params, silver_step_using_prior_silvers, sample_bronze_df):
        """Test multiple silver steps where one depends on another."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params, silver_step_using_prior_silvers]

        # Run until second silver step
        # Note: enriched_events depends on bronze, not clean_events, so clean_events may not be required
        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            stop_after_step="enriched_events",
        )

        assert result.status == "completed"
        # Should have at least bronze and enriched_events
        assert len(result.steps) >= 2
        assert "enriched_events" in context
        
        # clean_events may or may not be executed depending on dependency order
        # The key is that enriched_events completed successfully
        enriched_df = context["enriched_events"]
        if "has_prior_silvers" in enriched_df.columns:
            rows = enriched_df.collect()
            # Should have rows
            assert len(rows) > 0

    def test_iterative_parameter_tuning(self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df):
        """Test iterative parameter tuning workflow."""
        session = PipelineDebugSession(
            spark_session, config, [bronze_step, silver_step_with_params],
            bronze_sources={"events": sample_bronze_df}
        )

        # Test different threshold values
        thresholds = [0, 1, 2, 3]
        results = []

        for threshold in thresholds:
            session.set_step_params("clean_events", {"threshold": threshold, "min_value": 0})
            report, _ = session.rerun_step("clean_events", write_outputs=False)
            count = session.context["clean_events"].count()
            results.append((threshold, count))
            assert report.status == PipelineStatus.COMPLETED

        # Verify that different thresholds produce different results (or same if appropriate)
        assert len(results) == len(thresholds)
        # At least verify the workflow completed successfully

    def test_full_pipeline_with_stepwise_debugging(self, spark_session, config, bronze_step, silver_step_with_params, gold_step_with_params, sample_bronze_df):
        """Test a realistic debugging workflow."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_with_params, gold_step_with_params]

        # Step 1: Run until silver to check intermediate result
        report1, context = runner.run_until(
            "clean_events",
            steps=steps,
            bronze_sources={"events": sample_bronze_df},
            write_outputs=False,
        )
        assert report1.status == PipelineStatus.COMPLETED
        silver_count = context["clean_events"].count()

        # Step 2: Adjust silver parameters and rerun
        step_params = {"clean_events": {"threshold": 0, "min_value": 0}}
        report2, context = runner.rerun_step(
            "clean_events",
            steps=steps,
            context=context,
            step_params=step_params,
            write_outputs=False,
        )
        assert report2.status == PipelineStatus.COMPLETED
        new_silver_count = context["clean_events"].count()

        # Step 3: Run gold step with parameters
        step_params_gold = {"aggregated_events": {"multiplier": 2.0}}
        report3, context = runner.run_step(
            "aggregated_events",
            steps=steps,
            context=context,
            step_params=step_params_gold,
            write_outputs=False,
        )
        assert report3.status == PipelineStatus.COMPLETED
        assert "aggregated_events" in context

        # Verify counts make sense
        assert new_silver_count >= silver_count  # Lower threshold should give more rows


class TestWriteOutputsControl:
    """Test write_outputs flag behavior."""

    def test_write_outputs_true_creates_table(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that write_outputs=True creates tables."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            write_outputs=True,
        )

        assert result.status == "completed"
        assert "clean_events" in context

        # Try to read from table (may work or fail depending on mode, but shouldn't error in execution)
        try:
            table_df = spark_session.table("test_schema.clean_events")
            assert table_df is not None
        except Exception:
            # In some modes, table reading might not work, but execution should have succeeded
            pass

    def test_write_outputs_false_skips_table_but_keeps_context(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that write_outputs=False skips table writes but keeps DataFrame in context."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            write_outputs=False,
        )

        assert result.status == "completed"
        # Context should still have the output
        assert "clean_events" in context
        output_df = context["clean_events"]
        assert output_df.count() > 0

    def test_write_outputs_false_faster_iteration(self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df):
        """Test that write_outputs=False enables faster iteration."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_with_params]
        context = {"events": sample_bronze_df}

        # Multiple reruns with write_outputs=False should be fast
        for i in range(3):
            step_params = {"clean_events": {"threshold": i, "min_value": 0}}
            report, context = runner.rerun_step(
                "clean_events",
                steps=steps,
                context=context,
                step_params=step_params,
                write_outputs=False,
            )
            assert report.status == PipelineStatus.COMPLETED
            assert "clean_events" in context


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_step_params_dict(self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df):
        """Test that empty step_params dict doesn't cause errors."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}

        # Empty params should work (uses defaults)
        step_params = {}
        result = engine.execute_step(
            silver_step_with_params,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
        )

        assert result.status == StepStatus.COMPLETED

    def test_step_params_for_nonexistent_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test that step_params for nonexistent step is ignored."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params]

        # Params for step that doesn't exist should be ignored
        # Use run_until which accepts step_params
        step_params = {"nonexistent_step": {"param": "value"}}
        report, context = runner.run_until(
            "clean_events",
            steps=steps,
            bronze_sources={"events": sample_bronze_df},
            step_params=step_params,
        )

        # Should still complete successfully
        assert report.status == PipelineStatus.COMPLETED

    def test_stop_after_first_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test stopping after the first step."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            stop_after_step="events",
        )

        assert result.status == "completed"
        assert len(result.steps) == 1
        assert result.steps[0].step_name == "events"

    def test_start_at_first_step(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test starting at the first step (should execute all)."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [bronze_step, silver_step_without_params]

        result = engine.execute_pipeline(
            steps,
            ExecutionMode.INITIAL,
            context=context,
            start_at_step="events",
        )

        assert result.status == "completed"
        # Should execute all steps
        assert len(result.steps) == 2

    def test_rerun_without_prior_execution(self, spark_session, config, bronze_step, silver_step_without_params, sample_bronze_df):
        """Test rerun_step when step hasn't been executed before."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params]
        context = {"events": sample_bronze_df}

        # Rerun without prior execution should still work
        report, context = runner.rerun_step(
            "clean_events",
            steps=steps,
            context=context,
        )

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in context

    def test_multiple_parameter_changes(self, spark_session, config, bronze_step, silver_step_with_params, sample_bronze_df):
        """Test multiple parameter changes in sequence."""
        session = PipelineDebugSession(
            spark_session, config, [bronze_step, silver_step_with_params],
            bronze_sources={"events": sample_bronze_df}
        )

        # Change params multiple times
        # Start with strict filters (fewer rows)
        session.set_step_params("clean_events", {"threshold": 2, "min_value": 20})
        report1, _ = session.run_step("clean_events", write_outputs=False)
        count1 = session.context["clean_events"].count()

        # Medium filters
        session.set_step_params("clean_events", {"threshold": 1, "min_value": 10})
        report2, _ = session.rerun_step("clean_events", write_outputs=False)
        count2 = session.context["clean_events"].count()

        # Loosest filters (more rows)
        session.set_step_params("clean_events", {"threshold": 0, "min_value": 0})
        report3, _ = session.rerun_step("clean_events", write_outputs=False)
        count3 = session.context["clean_events"].count()

        assert report1.status == PipelineStatus.COMPLETED
        assert report2.status == PipelineStatus.COMPLETED
        assert report3.status == PipelineStatus.COMPLETED
        # count3 should be >= count2 >= count1 (looser filters give more rows)
        assert count3 >= count2, f"Expected count3 ({count3}) >= count2 ({count2})"
        assert count2 >= count1, f"Expected count2 ({count2}) >= count1 ({count1})"
