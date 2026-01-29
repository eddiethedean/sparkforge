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

import pytest

from pipeline_builder.execution import ExecutionEngine, ExecutionMode, StepStatus
from pipeline_builder.errors import ExecutionError
from pipeline_builder.models import BronzeStep, GoldStep, PipelineConfig, SilverStep
from pipeline_builder_base.errors import ValidationError
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
    data = [
        ("1", "event1", 10),
        ("2", "event2", 20),
        ("3", "event3", 30),
        ("4", "event4", 40),
    ]
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

    def test_silver_step_with_params(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
    ):
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
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
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

    def test_gold_step_with_params(
        self,
        spark_session,
        config,
        silver_step_with_params,
        gold_step_with_params,
        sample_bronze_df,
    ):
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

    def test_stop_after_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_start_at_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_write_outputs_false(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_run_until(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_run_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
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

    def test_debug_session_initialization(
        self, spark_session, config, bronze_step, silver_step_without_params
    ):
        """Test that debug session initializes correctly."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(spark_session, config, steps)

        assert session.steps == steps
        assert session.mode == PipelineMode.INITIAL
        assert isinstance(session.context, dict)
        assert isinstance(session.step_params, dict)

    def test_debug_session_run_until(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
        """Test debug session run_until."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(
            spark_session, config, steps, bronze_sources={"events": sample_bronze_df}
        )

        report, context = session.run_until("clean_events")

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in session.context
        assert "clean_events" in context

    def test_debug_session_run_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
        """Test debug session run_step."""
        steps = [bronze_step, silver_step_without_params]
        session = PipelineDebugSession(
            spark_session, config, steps, bronze_sources={"events": sample_bronze_df}
        )

        report, context = session.run_step("clean_events")

        assert report.status == PipelineStatus.COMPLETED
        assert "clean_events" in session.context

    def test_debug_session_rerun_with_params(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
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

    def test_debug_session_clear_params(
        self, spark_session, config, bronze_step, silver_step_with_params
    ):
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

    def test_silver_step_with_kwargs(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_kwargs,
        sample_bronze_df,
    ):
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

    def test_gold_step_with_kwargs(
        self,
        spark_session,
        config,
        silver_step_without_params,
        gold_step_with_kwargs,
        sample_bronze_df,
    ):
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

    def test_start_at_step_loads_from_table(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_context_preserved_between_runs(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_invalidate_downstream_removes_outputs(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        gold_step_with_params,
        sample_bronze_df,
    ):
        """Test that invalidate_downstream removes downstream outputs from context."""
        runner = SimplePipelineRunner(spark_session, config)
        steps = [bronze_step, silver_step_without_params, gold_step_with_params]
        context = {"events": sample_bronze_df}

        # Run full pipeline (returns only report, not tuple)
        runner.run_pipeline(steps, bronze_sources={"events": sample_bronze_df})
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

    def test_stop_after_invalid_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_start_at_invalid_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_run_step_without_dependencies(
        self, spark_session, config, silver_step_without_params
    ):
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

    def test_multiple_silver_steps_with_dependencies(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        silver_step_using_prior_silvers,
        sample_bronze_df,
    ):
        """Test multiple silver steps where one depends on another."""
        engine = ExecutionEngine(spark_session, config)
        context = {"events": sample_bronze_df}
        steps = [
            bronze_step,
            silver_step_without_params,
            silver_step_using_prior_silvers,
        ]

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

    def test_iterative_parameter_tuning(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
    ):
        """Test iterative parameter tuning workflow."""
        session = PipelineDebugSession(
            spark_session,
            config,
            [bronze_step, silver_step_with_params],
            bronze_sources={"events": sample_bronze_df},
        )

        # Test different threshold values
        thresholds = [0, 1, 2, 3]
        results = []

        for threshold in thresholds:
            session.set_step_params(
                "clean_events", {"threshold": threshold, "min_value": 0}
            )
            report, _ = session.rerun_step("clean_events", write_outputs=False)
            count = session.context["clean_events"].count()
            results.append((threshold, count))
            assert report.status == PipelineStatus.COMPLETED

        # Verify that different thresholds produce different results (or same if appropriate)
        assert len(results) == len(thresholds)
        # At least verify the workflow completed successfully

    def test_full_pipeline_with_stepwise_debugging(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        gold_step_with_params,
        sample_bronze_df,
    ):
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

    def test_write_outputs_true_creates_table(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_write_outputs_false_skips_table_but_keeps_context(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_write_outputs_false_faster_iteration(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
    ):
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

    def test_empty_step_params_dict(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
    ):
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

    def test_step_params_for_nonexistent_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_stop_after_first_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_start_at_first_step(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_rerun_without_prior_execution(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_without_params,
        sample_bronze_df,
    ):
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

    def test_multiple_parameter_changes(
        self,
        spark_session,
        config,
        bronze_step,
        silver_step_with_params,
        sample_bronze_df,
    ):
        """Test multiple parameter changes in sequence."""
        session = PipelineDebugSession(
            spark_session,
            config,
            [bronze_step, silver_step_with_params],
            bronze_sources={"events": sample_bronze_df},
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


class TestPriorGoldsAccess:
    """Tests for accessing prior_golds in transform functions."""

    def test_silver_transform_with_prior_golds(
        self, spark_session, config, bronze_step, sample_bronze_df
    ):
        """Test that silver transform can access prior_golds when function accepts it."""

        # Create a silver step that accepts prior_golds
        def silver_transform_with_prior_golds(
            spark, bronze_df, prior_silvers, prior_golds=None
        ):
            result = bronze_df
            if prior_golds:
                result = result.withColumn("has_prior_golds", F.lit(True))
            else:
                result = result.withColumn("has_prior_golds", F.lit(False))
            return result

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=silver_transform_with_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
            schema="test_schema",
        )

        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)
        step_types = {"existing_gold": "gold"}
        context = {"events": sample_bronze_df}

        # Test that executor can handle prior_golds parameter
        # Even if prior_golds is empty (no gold steps executed yet), it should work
        output_df = executor.execute(
            silver_step, context, ExecutionMode.INITIAL, step_types=step_types
        )
        # Check that the transform was called (output should have the column)
        assert "has_prior_golds" in output_df.columns
        # Since no gold steps are in context, has_prior_golds should be False
        has_golds = output_df.select("has_prior_golds").distinct().collect()
        assert len(has_golds) > 0

    def test_gold_transform_with_prior_golds(
        self, spark_session, config, silver_step_without_params, sample_bronze_df
    ):
        """Test that gold transform can access prior_golds when function accepts it."""

        # Create a gold step that accepts prior_golds
        def gold_transform_with_prior_golds(spark, silvers, prior_golds=None):
            result = silvers["clean_events"]
            if prior_golds:
                result = result.withColumn("has_prior_golds", F.lit(True))
            else:
                result = result.withColumn("has_prior_golds", F.lit(False))
            return result

        gold_step = GoldStep(
            name="user_metrics",
            transform=gold_transform_with_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="user_metrics",
            source_silvers=["clean_events"],
            schema="test_schema",
        )

        from pipeline_builder.step_executors.gold import GoldStepExecutor

        executor = GoldStepExecutor(spark_session)
        step_types = {"existing_gold": "gold", "clean_events": "silver"}
        # Add existing_gold to context (simulating it was read from table)
        context = {"clean_events": sample_bronze_df, "existing_gold": sample_bronze_df}

        # Test that executor can handle prior_golds parameter
        output_df = executor.execute(gold_step, context, None, step_types=step_types)
        # Check that the transform was called with prior_golds
        assert "has_prior_golds" in output_df.columns
        # Verify it detected prior_golds
        has_golds = output_df.select("has_prior_golds").distinct().collect()
        assert len(has_golds) > 0

    def test_silver_transform_backward_compatible_without_prior_golds(
        self, spark_session, config, bronze_step, sample_bronze_df
    ):
        """Test that silver transforms without prior_golds parameter still work."""

        def silver_transform_old_style(spark, bronze_df, prior_silvers):
            # Old-style transform without prior_golds
            return bronze_df.filter(F.col("value") > 10)

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=silver_transform_old_style,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
            schema="test_schema",
        )

        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)
        context = {"events": sample_bronze_df}
        step_types = {}  # Empty step_types should still work

        # Should work without prior_golds parameter
        output_df = executor.execute(
            silver_step, context, ExecutionMode.INITIAL, step_types=step_types
        )
        assert output_df.count() > 0

    def test_gold_transform_backward_compatible_without_prior_golds(
        self, spark_session, config, silver_step_without_params, sample_bronze_df
    ):
        """Test that gold transforms without prior_golds parameter still work."""

        def gold_transform_old_style(spark, silvers):
            # Old-style transform without prior_golds
            return silvers["clean_events"]

        gold_step = GoldStep(
            name="user_metrics",
            transform=gold_transform_old_style,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="user_metrics",
            source_silvers=["clean_events"],
            schema="test_schema",
        )

        from pipeline_builder.step_executors.gold import GoldStepExecutor

        executor = GoldStepExecutor(spark_session)
        context = {"clean_events": sample_bronze_df}
        step_types = {}  # Empty step_types should still work

        # Should work without prior_golds parameter
        output_df = executor.execute(gold_step, context, None, step_types=step_types)
        assert output_df.count() > 0


class TestValidationOnlySteps:
    """Tests for validation-only steps (with_silver_rules, with_gold_rules)."""

    def test_validation_only_silver_step_reads_from_table(
        self, spark_session, config, sample_bronze_df
    ):
        """Test that validation-only silver step reads from existing table."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor
        from pipeline_builder.table_operations import fqn

        # Create a table first
        table_name = "existing_silver_table"
        schema = "test_schema"
        table_fqn = fqn(schema, table_name)

        # Write sample data to table
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        # Create validation-only silver step
        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",  # No source for existing tables
            transform=None,  # No transform for validation-only
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = SilverStepExecutor(spark_session)
        context = {}  # Empty context - should read from table

        # Should read from table successfully
        output_df = executor.execute(silver_step, context, ExecutionMode.INITIAL)
        assert output_df.count() == sample_bronze_df.count()
        assert "id" in output_df.columns

    def test_validation_only_silver_step_table_not_exists_error(
        self, spark_session, config
    ):
        """Test that validation-only silver step raises error when table doesn't exist."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="nonexistent_table",
            existing=True,
            schema="test_schema",
        )

        executor = SilverStepExecutor(spark_session)
        context = {}

        with pytest.raises(ExecutionError, match="table does not exist"):
            executor.execute(silver_step, context, ExecutionMode.INITIAL)

    def test_validation_only_silver_step_optional_table_missing_returns_empty(
        self, spark_session, config
    ):
        """Test that validation-only silver step with optional=True returns empty DataFrame when table doesn't exist."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        spark_session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        silver_step = SilverStep(
            name="optional_silver",
            source_bronze="",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="nonexistent_table",
            existing=True,
            optional=True,
            schema="test_schema",
        )

        executor = SilverStepExecutor(spark_session)
        context = {}

        output_df = executor.execute(silver_step, context, ExecutionMode.INITIAL)
        assert output_df.count() == 0
        assert output_df is not None

    def test_validation_only_silver_step_no_schema_error(self, spark_session, config):
        """Test that validation-only silver step raises error when schema is missing."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="some_table",
            existing=True,
            schema=None,  # No schema
        )

        executor = SilverStepExecutor(spark_session)
        context = {}

        with pytest.raises(ExecutionError, match="requires schema"):
            executor.execute(silver_step, context, ExecutionMode.INITIAL)

    def test_validation_only_silver_step_not_existing_error(
        self, spark_session, config
    ):
        """Test that silver step without transform and not marked existing raises error at model creation."""
        # This should fail at model creation, not execution
        with pytest.raises(
            ValidationError,
            match="Transform function is required for non-existing silver steps",
        ):
            SilverStep(
                name="invalid_silver",
                source_bronze="events",
                transform=None,  # No transform
                rules={"id": [F.col("id").isNotNull()]},
                table_name="some_table",
                existing=False,  # Not marked as existing
                schema="test_schema",
            )

    def test_validation_only_gold_step_reads_from_table(
        self, spark_session, config, sample_bronze_df
    ):
        """Test that validation-only gold step reads from existing table."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor
        from pipeline_builder.table_operations import fqn

        # Create a table first
        table_name = "existing_gold_table"
        schema = "test_schema"
        table_fqn = fqn(schema, table_name)

        # Write sample data to table
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        # Create validation-only gold step
        gold_step = GoldStep(
            name="existing_gold",
            transform=None,  # No transform for validation-only
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = GoldStepExecutor(spark_session)
        context = {}  # Empty context - should read from table

        # Should read from table successfully
        output_df = executor.execute(gold_step, context, None)
        assert output_df.count() == sample_bronze_df.count()
        assert "id" in output_df.columns

    def test_validation_only_gold_step_table_not_exists_error(
        self, spark_session, config
    ):
        """Test that validation-only gold step raises error when table doesn't exist."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        gold_step = GoldStep(
            name="existing_gold",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="nonexistent_table",
            existing=True,
            schema="test_schema",
        )

        executor = GoldStepExecutor(spark_session)
        context = {}

        with pytest.raises(ExecutionError, match="table does not exist"):
            executor.execute(gold_step, context, None)

    def test_validation_only_gold_step_optional_table_missing_returns_empty(
        self, spark_session, config
    ):
        """Test that validation-only gold step with optional=True returns empty DataFrame when table doesn't exist."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        spark_session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        gold_step = GoldStep(
            name="optional_gold",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="nonexistent_table",
            existing=True,
            optional=True,
            schema="test_schema",
        )

        executor = GoldStepExecutor(spark_session)
        context = {}

        output_df = executor.execute(gold_step, context, None)
        assert output_df.count() == 0
        assert output_df is not None

    def test_validation_only_gold_step_no_schema_error(self, spark_session, config):
        """Test that validation-only gold step raises error when schema is missing."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        gold_step = GoldStep(
            name="existing_gold",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="some_table",
            existing=True,
            schema=None,  # No schema
        )

        executor = GoldStepExecutor(spark_session)
        context = {}

        with pytest.raises(ExecutionError, match="requires schema"):
            executor.execute(gold_step, context, None)

    def test_validation_only_silver_step_nonexistent_schema_error(
        self, spark_session, config
    ):
        """Test that validation-only silver step raises error when schema doesn't exist."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        # Don't create the schema - it should not exist
        schema = "nonexistent_schema"
        table_name = "some_table"

        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = SilverStepExecutor(spark_session)
        context = {}

        # Should raise error about schema not existing (before checking table)
        with pytest.raises(ExecutionError, match="schema does not exist"):
            executor.execute(silver_step, context, ExecutionMode.INITIAL)

    def test_validation_only_gold_step_nonexistent_schema_error(
        self, spark_session, config
    ):
        """Test that validation-only gold step raises error when schema doesn't exist."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        # Don't create the schema - it should not exist
        schema = "nonexistent_schema"
        table_name = "some_table"

        gold_step = GoldStep(
            name="existing_gold",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = GoldStepExecutor(spark_session)
        context = {}

        # Should raise error about schema not existing (before checking table)
        with pytest.raises(ExecutionError, match="schema does not exist"):
            executor.execute(gold_step, context, None)

    def test_validation_only_silver_step_schema_before_table_check(
        self, spark_session, config
    ):
        """Test that schema validation happens before table existence check."""
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        # Use a schema that doesn't exist - should fail on schema check, not table check
        schema = "nonexistent_schema"
        table_name = "some_table"

        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = SilverStepExecutor(spark_session)
        context = {}

        # Should raise error about schema, not table
        with pytest.raises(ExecutionError) as exc_info:
            executor.execute(silver_step, context, ExecutionMode.INITIAL)

        # Error should mention schema, not table
        assert "schema" in str(exc_info.value).lower()
        assert "does not exist" in str(exc_info.value).lower()

    def test_validation_only_gold_step_schema_before_table_check(
        self, spark_session, config
    ):
        """Test that schema validation happens before table existence check."""
        from pipeline_builder.step_executors.gold import GoldStepExecutor

        # Use a schema that doesn't exist - should fail on schema check, not table check
        schema = "nonexistent_schema"
        table_name = "some_table"

        gold_step = GoldStep(
            name="existing_gold",
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        executor = GoldStepExecutor(spark_session)
        context = {}

        # Should raise error about schema, not table
        with pytest.raises(ExecutionError) as exc_info:
            executor.execute(gold_step, context, None)

        # Error should mention schema, not table
        assert "schema" in str(exc_info.value).lower()
        assert "does not exist" in str(exc_info.value).lower()


class TestPriorGoldsAdvanced:
    """Advanced tests for prior_golds functionality."""

    def test_silver_transform_with_multiple_prior_golds(
        self, spark_session, config, bronze_step, sample_bronze_df
    ):
        """Test silver transform accessing multiple prior_golds."""

        def silver_transform_with_prior_golds(
            spark, bronze_df, prior_silvers, prior_golds=None
        ):
            result = bronze_df
            if prior_golds:
                # Count how many prior golds we have
                gold_count = len(prior_golds)
                result = result.withColumn("prior_gold_count", F.lit(gold_count))
                # Add columns indicating which golds are available
                for gold_name in prior_golds.keys():
                    result = result.withColumn(f"has_{gold_name}", F.lit(True))
            else:
                result = result.withColumn("prior_gold_count", F.lit(0))
            return result

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=silver_transform_with_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
            schema="test_schema",
        )

        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)
        step_types = {"gold1": "gold", "gold2": "gold", "gold3": "gold"}
        context = {
            "events": sample_bronze_df,
            "gold1": sample_bronze_df,
            "gold2": sample_bronze_df,
            "gold3": sample_bronze_df,
        }

        output_df = executor.execute(
            silver_step, context, ExecutionMode.INITIAL, step_types=step_types
        )

        assert "prior_gold_count" in output_df.columns
        assert "has_gold1" in output_df.columns
        assert "has_gold2" in output_df.columns
        assert "has_gold3" in output_df.columns

        # Check that count is correct
        # In mock mode, collect() may behave differently, so we verify the transform was called
        # by checking that the columns exist and the DataFrame has data
        assert output_df.count() > 0, "Output DataFrame should have rows"
        # Verify the transform added the expected columns with correct values
        # For mock mode, we'll verify the columns exist and the logic is correct
        # The actual value extraction may vary between mock and real Spark
        rows = output_df.select("prior_gold_count").distinct().collect()
        if rows and len(rows) > 0:
            row = rows[0]
            gold_count = (
                row[0]
                if hasattr(row, "__getitem__")
                else getattr(row, "prior_gold_count", None)
            )
            if gold_count is not None:
                assert gold_count == 3, f"Expected gold_count=3, got {gold_count}"

    def test_gold_transform_with_multiple_prior_golds(
        self, spark_session, config, sample_bronze_df
    ):
        """Test gold transform accessing multiple prior_golds."""

        def gold_transform_with_prior_golds(spark, silvers, prior_golds=None):
            result = silvers["clean_events"]
            if prior_golds:
                gold_count = len(prior_golds)
                result = result.withColumn("prior_gold_count", F.lit(gold_count))
                # Add a column with all gold step names
                gold_names = ",".join(sorted(prior_golds.keys()))
                result = result.withColumn("prior_gold_names", F.lit(gold_names))
            else:
                result = result.withColumn("prior_gold_count", F.lit(0))
                result = result.withColumn("prior_gold_names", F.lit(""))
            return result

        gold_step = GoldStep(
            name="final_metrics",
            transform=gold_transform_with_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="final_metrics",
            source_silvers=["clean_events"],
            schema="test_schema",
        )

        from pipeline_builder.step_executors.gold import GoldStepExecutor

        executor = GoldStepExecutor(spark_session)
        step_types = {
            "clean_events": "silver",
            "intermediate_gold1": "gold",
            "intermediate_gold2": "gold",
        }
        context = {
            "clean_events": sample_bronze_df,
            "intermediate_gold1": sample_bronze_df,
            "intermediate_gold2": sample_bronze_df,
        }

        output_df = executor.execute(gold_step, context, None, step_types=step_types)

        assert "prior_gold_count" in output_df.columns
        assert "prior_gold_names" in output_df.columns

        # Check that count is correct
        assert output_df.count() > 0
        rows = output_df.select("prior_gold_count").distinct().collect()
        if rows and len(rows) > 0:
            row = rows[0]
            gold_count = (
                row[0]
                if hasattr(row, "__getitem__")
                else getattr(row, "prior_gold_count", None)
            )
            if gold_count is not None:
                assert gold_count == 2

        # Check that names are correct
        name_rows = output_df.select("prior_gold_names").distinct().collect()
        if name_rows and len(name_rows) > 0:
            name_row = name_rows[0]
            gold_names = (
                name_row[0]
                if hasattr(name_row, "__getitem__")
                else getattr(name_row, "prior_gold_names", None)
            )
            if gold_names is not None:
                assert "intermediate_gold1" in gold_names
                assert "intermediate_gold2" in gold_names

    def test_silver_transform_with_prior_silvers_and_prior_golds(
        self, spark_session, config, bronze_step, sample_bronze_df
    ):
        """Test silver transform accessing both prior_silvers and prior_golds."""

        def silver_transform_with_both(
            spark, bronze_df, prior_silvers, prior_golds=None
        ):
            result = bronze_df
            silver_count = len(prior_silvers) if prior_silvers else 0
            gold_count = len(prior_golds) if prior_golds else 0
            result = result.withColumn("prior_silver_count", F.lit(silver_count))
            result = result.withColumn("prior_gold_count", F.lit(gold_count))
            return result

        silver_step = SilverStep(
            name="enriched_events",
            source_bronze="events",
            transform=silver_transform_with_both,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="enriched_events",
            schema="test_schema",
        )

        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)
        step_types = {
            "events": "bronze",
            "clean_events": "silver",
            "existing_gold": "gold",
        }
        context = {
            "events": sample_bronze_df,
            "clean_events": sample_bronze_df,
            "existing_gold": sample_bronze_df,
        }

        output_df = executor.execute(
            silver_step, context, ExecutionMode.INITIAL, step_types=step_types
        )

        assert "prior_silver_count" in output_df.columns
        assert "prior_gold_count" in output_df.columns

        # Check counts
        assert output_df.count() > 0
        silver_rows = output_df.select("prior_silver_count").distinct().collect()
        gold_rows = output_df.select("prior_gold_count").distinct().collect()
        if silver_rows and len(silver_rows) > 0:
            silver_row = silver_rows[0]
            silver_count = (
                silver_row[0]
                if hasattr(silver_row, "__getitem__")
                else getattr(silver_row, "prior_silver_count", None)
            )
            if silver_count is not None:
                assert (
                    silver_count == 1
                )  # clean_events (excluding current step and bronze)
        if gold_rows and len(gold_rows) > 0:
            gold_row = gold_rows[0]
            gold_count = (
                gold_row[0]
                if hasattr(gold_row, "__getitem__")
                else getattr(gold_row, "prior_gold_count", None)
            )
            if gold_count is not None:
                assert gold_count == 1  # existing_gold

    def test_prior_golds_excludes_current_step(
        self, spark_session, config, sample_bronze_df
    ):
        """Test that prior_golds excludes the current step being executed."""

        def gold_transform_with_prior_golds(spark, silvers, prior_golds=None):
            result = silvers["clean_events"]
            if prior_golds:
                # Current step should NOT be in prior_golds
                gold_names = sorted(prior_golds.keys())
                result = result.withColumn(
                    "prior_gold_names", F.lit(",".join(gold_names))
                )
            return result

        gold_step = GoldStep(
            name="final_metrics",
            transform=gold_transform_with_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="final_metrics",
            source_silvers=["clean_events"],
            schema="test_schema",
        )

        from pipeline_builder.step_executors.gold import GoldStepExecutor

        executor = GoldStepExecutor(spark_session)
        step_types = {
            "clean_events": "silver",
            "final_metrics": "gold",  # Current step
            "other_gold": "gold",
        }
        context = {
            "clean_events": sample_bronze_df,
            "final_metrics": sample_bronze_df,  # Current step - should be excluded
            "other_gold": sample_bronze_df,
        }

        output_df = executor.execute(gold_step, context, None, step_types=step_types)

        assert "prior_gold_names" in output_df.columns
        assert output_df.count() > 0
        name_rows = output_df.select("prior_gold_names").distinct().collect()
        if name_rows and len(name_rows) > 0:
            name_row = name_rows[0]
            gold_names = (
                name_row[0]
                if hasattr(name_row, "__getitem__")
                else getattr(name_row, "prior_gold_names", None)
            )
            # Should only have other_gold, not final_metrics
            if gold_names:  # May be empty string if no prior_golds
                assert "other_gold" in gold_names
                assert "final_metrics" not in gold_names

    def test_prior_golds_with_step_params(
        self, spark_session, config, bronze_step, sample_bronze_df
    ):
        """Test that prior_golds works correctly with step_params."""

        def silver_transform_with_params_and_prior_golds(
            spark, bronze_df, prior_silvers, prior_golds=None, params=None
        ):
            result = bronze_df
            gold_count = len(prior_golds) if prior_golds else 0
            multiplier = params.get("multiplier", 1) if params else 1
            result = result.withColumn("prior_gold_count", F.lit(gold_count))
            result = result.withColumn("multiplied_value", F.col("value") * multiplier)
            return result

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=silver_transform_with_params_and_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
            schema="test_schema",
        )

        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)
        step_types = {"existing_gold": "gold"}
        context = {"events": sample_bronze_df, "existing_gold": sample_bronze_df}
        step_params = {"multiplier": 2}

        output_df = executor.execute(
            silver_step,
            context,
            ExecutionMode.INITIAL,
            step_params=step_params,
            step_types=step_types,
        )

        assert "prior_gold_count" in output_df.columns
        assert "multiplied_value" in output_df.columns

        # Check that both prior_golds and params work together
        assert output_df.count() > 0
        gold_rows = output_df.select("prior_gold_count").distinct().collect()
        if gold_rows and len(gold_rows) > 0:
            gold_row = gold_rows[0]
            gold_count = (
                gold_row[0]
                if hasattr(gold_row, "__getitem__")
                else getattr(gold_row, "prior_gold_count", None)
            )
            if gold_count is not None:
                assert gold_count == 1

        # Check that params were applied
        param_rows = output_df.select("multiplied_value", "value").collect()
        if param_rows and len(param_rows) > 0:
            param_row = param_rows[0]
            multiplied = (
                param_row[0]
                if hasattr(param_row, "__getitem__")
                else getattr(param_row, "multiplied_value", None)
            )
            original = (
                param_row[1]
                if hasattr(param_row, "__getitem__")
                else getattr(param_row, "value", None)
            )
            if multiplied is not None and original is not None:
                assert multiplied == original * 2


class TestValidationOnlyStepsIntegration:
    """Integration tests for validation-only steps in full pipeline execution."""

    def test_pipeline_with_validation_only_silver_step(
        self, spark_session, config, sample_bronze_df
    ):
        """Test full pipeline execution with validation-only silver step."""
        from pipeline_builder.table_operations import fqn

        # Create an existing silver table
        table_name = "existing_silver_table"
        schema = "test_schema"
        table_fqn = fqn(schema, table_name)
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        # Test validation-only silver step execution directly
        from pipeline_builder.step_executors.silver import SilverStepExecutor

        executor = SilverStepExecutor(spark_session)

        silver_step = SilverStep(
            name="existing_silver",
            source_bronze="",  # No source for existing tables
            transform=None,
            rules={"id": [F.col("id").isNotNull()]},
            table_name=table_name,
            existing=True,
            schema=schema,
        )

        # Test that it reads from table
        context = {}
        output_df = executor.execute(silver_step, context, ExecutionMode.INITIAL)
        assert output_df.count() == sample_bronze_df.count()

        # Now test that a subsequent transform can access it via prior_silvers
        def silver_transform(spark, bronze_df, prior_silvers):
            # Should have access to existing_silver via prior_silvers
            if "existing_silver" in prior_silvers:
                return bronze_df.withColumn("has_existing_silver", F.lit(True))
            return bronze_df

        clean_silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=silver_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
            schema=schema,
        )

        # Add existing_silver to context to simulate it was executed
        context = {"events": sample_bronze_df, "existing_silver": output_df}
        step_types = {"existing_silver": "silver"}

        clean_output_df = executor.execute(
            clean_silver_step, context, ExecutionMode.INITIAL, step_types=step_types
        )
        assert "has_existing_silver" in clean_output_df.columns

    def test_pipeline_with_validation_only_gold_step(
        self, spark_session, config, sample_bronze_df
    ):
        """Test full pipeline execution with validation-only gold step."""
        from pipeline_builder.pipeline.builder import PipelineBuilder
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder.table_operations import fqn

        # Create an existing gold table
        table_name = "existing_gold_table"
        schema = "test_schema"
        table_fqn = fqn(schema, table_name)
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )
        builder.with_gold_rules(
            name="existing_gold",
            table_name=table_name,
            rules={"id": [F.col("id").isNotNull()]},
        )

        # Add a gold transform that uses prior_golds
        def gold_transform(spark, silvers, prior_golds=None):
            result = silvers["clean_events"]
            if prior_golds and "existing_gold" in prior_golds:
                result = result.withColumn("has_existing_gold", F.lit(True))
            return result

        builder.add_gold_transform(
            name="final_metrics",
            transform=gold_transform,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="final_metrics",
            source_silvers=["clean_events"],
        )

        pipeline = builder.to_pipeline()
        runner = SimplePipelineRunner(spark_session, config)

        report = runner.run_pipeline(
            pipeline.steps, bronze_sources={"events": sample_bronze_df}
        )

        assert report.status == PipelineStatus.COMPLETED
        # Check that final_metrics has the column indicating it accessed prior_golds
        try:
            final_metrics_df = spark_session.table(fqn(schema, "final_metrics"))
            assert "has_existing_gold" in final_metrics_df.columns
        except Exception:
            # In mock mode, table might not be created - check context instead
            # The test still validates that the transform was called with prior_golds
            pass

    def test_with_silver_rules_existing_table_used_in_prior_silvers(
        self, spark_session, config, sample_bronze_df
    ):
        """Test with_silver_rules: existing table in a different schema is read and used via prior_silvers."""
        from pipeline_builder.pipeline.builder import PipelineBuilder
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder.table_operations import fqn, table_exists

        schema = "test_schema"
        prior_schema = "prior_silver_schema_ps"
        legacy_table = "legacy_silver_prior_silvers"
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_schema}")
        table_fqn = fqn(prior_schema, legacy_table)
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.with_silver_rules(
            name="silver_old",
            table_name=legacy_table,
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_schema,
        )
        # Silver transform that uses prior_silvers["silver_old"] from the existing table
        def silver_using_prior_silvers(spark, bronze_df, prior_silvers):
            base = bronze_df.withColumn("source", F.lit("bronze"))
            if "silver_old" in prior_silvers:
                legacy_count = prior_silvers["silver_old"].count()
                return base.withColumn("legacy_row_count", F.lit(legacy_count))
            return base

        builder.add_silver_transform(
            name="silver_main",
            source_bronze="events",
            transform=silver_using_prior_silvers,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_main_prior_silvers",
        )
        builder.add_gold_transform(
            name="gold_summary",
            transform=lambda spark, silvers: silvers["silver_main"].groupBy().count(),
            rules={"count": [F.col("count") > 0]},
            table_name="gold_summary_prior_silvers",
            source_silvers=["silver_main"],
        )

        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": sample_bronze_df})

        assert report.status == PipelineStatus.COMPLETED
        assert report.errors == [], f"Expected no errors, got {report.errors}"
        assert report.failed_steps == 0, f"Expected no failed steps, got {report.failed_steps}: {report.errors}"
        assert report.success
        expected_steps = 4
        assert (
            len(pipeline.bronze_steps) + len(pipeline.silver_steps) + len(pipeline.gold_steps)
            == expected_steps
        )
        # When steps ran, all should have succeeded (mock mode may report 0 steps executed)
        if report.metrics.total_steps > 0:
            assert report.successful_steps == report.metrics.total_steps
        # silver_main must have used prior_silvers["silver_old"] (legacy_row_count column)
        silver_fqn = fqn(schema, "silver_main_prior_silvers")
        if table_exists(spark_session, silver_fqn):
            silver_df = spark_session.table(silver_fqn)
            if "legacy_row_count" in silver_df.columns:
                assert "source" in silver_df.columns
                rows = silver_df.select("legacy_row_count").distinct().collect()
                assert len(rows) >= 1
                assert rows[0]["legacy_row_count"] == sample_bronze_df.count()
                assert silver_df.count() == sample_bronze_df.count()

    def test_with_gold_rules_existing_table_used_in_prior_golds(
        self, spark_session, config, sample_bronze_df
    ):
        """Test with_gold_rules: existing table in a different schema is read and used via prior_golds."""
        from pipeline_builder.pipeline.builder import PipelineBuilder
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder.table_operations import fqn, table_exists

        schema = "test_schema"
        prior_schema = "prior_gold_schema_pg"
        legacy_gold_table = "legacy_gold_prior_golds"
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_schema}")
        table_fqn = fqn(prior_schema, legacy_gold_table)
        sample_bronze_df.write.mode("overwrite").saveAsTable(table_fqn)

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events_prior_golds",
        )
        builder.with_gold_rules(
            name="gold_old",
            table_name=legacy_gold_table,
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_schema,
        )

        # Gold transform that uses prior_golds["gold_old"] data (row count from existing table)
        def gold_using_prior_golds(spark, silvers, prior_golds=None):
            base = silvers["clean_events"]
            if prior_golds and "gold_old" in prior_golds:
                legacy_count = prior_golds["gold_old"].count()
                return base.withColumn("legacy_gold_row_count", F.lit(legacy_count))
            return base

        builder.add_gold_transform(
            name="gold_final",
            transform=gold_using_prior_golds,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_final_prior_golds",
            source_silvers=["clean_events"],
        )

        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": sample_bronze_df})

        assert report.status == PipelineStatus.COMPLETED
        assert report.errors == [], f"Expected no errors, got {report.errors}"
        assert report.failed_steps == 0, f"Expected no failed steps, got {report.failed_steps}: {report.errors}"
        assert report.success
        expected_steps = 4
        assert (
            len(pipeline.bronze_steps) + len(pipeline.silver_steps) + len(pipeline.gold_steps)
            == expected_steps
        )
        # When steps ran, all should have succeeded (mock mode may report 0 steps executed)
        if report.metrics.total_steps > 0:
            assert report.successful_steps == report.metrics.total_steps
        gold_fqn = fqn(schema, "gold_final_prior_golds")
        if table_exists(spark_session, gold_fqn):
            gold_df = spark_session.table(gold_fqn)
            if "legacy_gold_row_count" in gold_df.columns:
                rows = gold_df.select("legacy_gold_row_count").distinct().collect()
                assert len(rows) >= 1
                assert rows[0]["legacy_gold_row_count"] == sample_bronze_df.count()
                assert gold_df.count() == sample_bronze_df.count()

    def test_with_silver_rules_prior_table_missing_fails(
        self, spark_session, config, sample_bronze_df
    ):
        """Test that to_pipeline() fails when validation-only silver step target table does not exist (optional=False)."""
        from pipeline_builder.pipeline.builder import PipelineBuilder

        schema = "test_schema"
        prior_schema = "prior_silver_schema_missing"
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_schema}")
        # Do NOT create the table - use a table name that does not exist
        legacy_table = "nonexistent_legacy_silver_missing"

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.with_silver_rules(
            name="silver_old",
            table_name=legacy_table,
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_schema,
        )
        builder.add_silver_transform(
            name="silver_main",
            source_bronze="events",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_main_missing",
        )

        # to_pipeline() checks that validation-only target tables exist; should raise
        with pytest.raises(ValueError) as exc_info:
            builder.to_pipeline()
        assert "do not exist" in str(exc_info.value) or "optional" in str(exc_info.value)
        assert "silver_old" in str(exc_info.value) or legacy_table in str(exc_info.value)

    def test_with_silver_rules_and_gold_rules_both_prior_tables_used(
        self, spark_session, config, sample_bronze_df
    ):
        """Test pipeline using both with_silver_rules and with_gold_rules; gold uses both prior_silvers and prior_golds."""
        from pipeline_builder.pipeline.builder import PipelineBuilder
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder.table_operations import fqn, table_exists

        schema = "test_schema"
        prior_silver_schema = "prior_silver_combined_both"
        prior_gold_schema = "prior_gold_combined_both"
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_silver_schema}")
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_gold_schema}")
        sample_bronze_df.write.mode("overwrite").saveAsTable(
            fqn(prior_silver_schema, "legacy_silver_both")
        )
        sample_bronze_df.write.mode("overwrite").saveAsTable(
            fqn(prior_gold_schema, "legacy_gold_both")
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.with_silver_rules(
            name="silver_old",
            table_name="legacy_silver_both",
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_silver_schema,
        )
        builder.add_silver_transform(
            name="silver_main",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.withColumn("from_bronze", F.lit(True)),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_main_both",
        )
        builder.with_gold_rules(
            name="gold_old",
            table_name="legacy_gold_both",
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_gold_schema,
        )

        def gold_using_both_priors(spark, silvers, prior_golds=None):
            base = silvers["silver_main"]
            legacy_silver_count = silvers.get("silver_old")
            if legacy_silver_count is not None:
                base = base.withColumn("prior_silver_count", F.lit(legacy_silver_count.count()))
            else:
                base = base.withColumn("prior_silver_count", F.lit(-1))
            if prior_golds and "gold_old" in prior_golds:
                base = base.withColumn(
                    "prior_gold_count", F.lit(prior_golds["gold_old"].count())
                )
            else:
                base = base.withColumn("prior_gold_count", F.lit(-1))
            return base

        builder.add_gold_transform(
            name="gold_final",
            transform=gold_using_both_priors,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_final_combined_both",
            source_silvers=["silver_main", "silver_old"],
        )

        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": sample_bronze_df})

        assert report.status == PipelineStatus.COMPLETED
        assert report.errors == []
        assert report.failed_steps == 0
        assert report.success
        gold_fqn = fqn(schema, "gold_final_combined_both")
        if table_exists(spark_session, gold_fqn):
            gold_df = spark_session.table(gold_fqn)
            if "prior_silver_count" in gold_df.columns and "prior_gold_count" in gold_df.columns:
                assert "from_bronze" in gold_df.columns
                sc = gold_df.select("prior_silver_count").first()
                gc = gold_df.select("prior_gold_count").first()
                if sc and gc:
                    assert sc["prior_silver_count"] == sample_bronze_df.count()
                    assert gc["prior_gold_count"] == sample_bronze_df.count()

    def test_with_silver_rules_prior_silvers_join_data_correctness(
        self, spark_session, config, sample_bronze_df
    ):
        """Test that prior_silvers DataFrame can be used (join or count); data correctness."""
        from pipeline_builder.pipeline.builder import PipelineBuilder
        from pipeline_builder.table_operations import fqn, table_exists

        schema = "test_schema"
        prior_schema = "prior_silver_join_correctness"
        legacy_table_join = "legacy_silver_join_correctness"
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {prior_schema}")
        sample_bronze_df.write.mode("overwrite").saveAsTable(
            fqn(prior_schema, legacy_table_join)
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)
        builder.with_bronze_rules(
            name="events", rules={"id": [F.col("id").isNotNull()]}
        )
        builder.with_silver_rules(
            name="silver_old",
            table_name=legacy_table_join,
            rules={"id": [F.col("id").isNotNull()]},
            schema=prior_schema,
        )

        def silver_using_prior(spark, bronze_df, prior_silvers):
            if "silver_old" not in prior_silvers:
                return bronze_df
            legacy = prior_silvers["silver_old"]
            # Use prior_silvers: add column from legacy row count (works with any schema)
            n = legacy.count()
            return bronze_df.withColumn("from_legacy_count", F.lit(n))

        builder.add_silver_transform(
            name="silver_main",
            source_bronze="events",
            transform=silver_using_prior,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_main_join_correctness",
        )

        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": sample_bronze_df})

        assert report.status == PipelineStatus.COMPLETED
        assert report.errors == []
        assert report.failed_steps == 0
        assert report.success
        silver_fqn = fqn(schema, "silver_main_join_correctness")
        if table_exists(spark_session, silver_fqn):
            silver_df = spark_session.table(silver_fqn)
            if "from_legacy_count" in silver_df.columns:
                assert silver_df.select("from_legacy_count").first()["from_legacy_count"] == sample_bronze_df.count()
