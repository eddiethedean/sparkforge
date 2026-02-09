"""
Tests that run the exact example code from docs/guides/STEPWISE_EXECUTION_GUIDE.md.

These tests ensure all guide examples are valid and executable (with mock Spark).
"""

import os

import pytest

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]


class TestStepwiseGuideMinimalExample:
    """Run the Minimal Example from the guide."""

    def test_minimal_example_run_until(self, mock_spark_session):
        # From guide: Minimal Example - Basic Setup and run_until
        from pipeline_builder.models import BronzeStep, SilverStep
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder_base.models import PipelineConfig

        config = PipelineConfig.create_default(schema="analytics")
        runner = SimplePipelineRunner(mock_spark_session, config)

        bronze_step = BronzeStep(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            schema="analytics",
        )

        def clean_transform(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("value") > 10)

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=clean_transform,
            rules={"value": [F.col("value").isNotNull()]},
            table_name="clean_events",
            schema="analytics",
        )

        source_df = mock_spark_session.createDataFrame(
            [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
            ["id", "event", "value"],
        )

        report, context = runner.run_until(
            "clean_events",
            steps=[bronze_step, silver_step],
            bronze_sources={"events": source_df},
        )
        assert report is not None
        assert report.status.value == "completed"
        assert "clean_events" in context
        assert context["clean_events"].count() == 2


class TestStepwiseGuideRunStepExample:
    """Run the run_step example from the guide."""

    def test_run_step_example(self, mock_spark_session):
        # From guide: run_step() Example
        from pipeline_builder.models import BronzeStep, GoldStep, SilverStep
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder_base.models import PipelineConfig

        config = PipelineConfig.create_default(schema="analytics")
        runner = SimplePipelineRunner(mock_spark_session, config)

        bronze_step = BronzeStep(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            schema="analytics",
        )
        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("value") > 15),
            rules={"value": [F.col("value").isNotNull()]},
            table_name="clean_events",
            schema="analytics",
        )
        gold_step = GoldStep(
            name="aggregated",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="aggregated",
            source_silvers=["clean_events"],
            schema="analytics",
        )

        source_df = mock_spark_session.createDataFrame(
            [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
            ["id", "event", "value"],
        )
        context = {"events": source_df}

        report, context = runner.run_step(
            "clean_events",
            steps=[bronze_step, silver_step, gold_step],
            context=context,
        )
        assert report.status.value == "completed"
        assert "clean_events" in context


class TestStepwiseGuideDebugSessionExample:
    """Run the PipelineDebugSession example from the guide."""

    def test_debug_session_run_until_and_run_step(self, mock_spark_session):
        # From guide: PipelineDebugSession Basic Usage
        from pipeline_builder.models import BronzeStep, GoldStep, SilverStep
        from pipeline_builder.pipeline.debug_session import PipelineDebugSession
        from pipeline_builder_base.models import PipelineConfig

        config = PipelineConfig.create_default(schema="analytics")
        bronze_step = BronzeStep(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            schema="analytics",
        )
        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("value") > 10),
            rules={"value": [F.col("value").isNotNull()]},
            table_name="clean_events",
            schema="analytics",
        )
        gold_step = GoldStep(
            name="aggregated",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="aggregated",
            source_silvers=["clean_events"],
            schema="analytics",
        )

        source_df = mock_spark_session.createDataFrame(
            [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
            ["id", "event", "value"],
        )

        session = PipelineDebugSession(
            mock_spark_session,
            config,
            steps=[bronze_step, silver_step, gold_step],
            bronze_sources={"events": source_df},
        )
        report, context = session.run_until("clean_events")
        assert report.status.value == "completed"
        assert "clean_events" in session.context

        report2, _ = session.run_step("clean_events")
        assert report2.status.value == "completed"


class TestStepwiseGuideCompleteExample1:
    """Run Example 1: Basic Stepwise Execution from the guide."""

    def test_complete_example1_run_until_and_parameter_override(
        self, mock_spark_session
    ):
        # From guide: Complete Examples - Example 1
        from pipeline_builder.models import BronzeStep, GoldStep, SilverStep
        from pipeline_builder.pipeline.debug_session import PipelineDebugSession
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder_base.models import PipelineConfig

        config = PipelineConfig.create_default(schema="analytics")
        runner = SimplePipelineRunner(mock_spark_session, config)

        bronze_step = BronzeStep(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            schema="analytics",
        )

        def clean_transform(spark, bronze_df, prior_silvers, params=None):
            threshold = params.get("threshold", 0) if params else 0
            return bronze_df.filter(F.col("value") > threshold)

        silver_step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=clean_transform,
            rules={"value": [F.col("value").isNotNull()]},
            table_name="clean_events",
            schema="analytics",
        )

        def aggregate_transform(spark, silvers, params=None):
            multiplier = params.get("multiplier", 1.0) if params else 1.0
            df = silvers["clean_events"]
            return df.withColumn(
                "adjusted_value", F.col("value") * multiplier
            )

        gold_step = GoldStep(
            name="aggregated_events",
            transform=aggregate_transform,
            rules={"adjusted_value": [F.col("adjusted_value") > 0]},
            table_name="aggregated_events",
            source_silvers=["clean_events"],
            schema="analytics",
        )

        data = [
            ("1", "event1", 10),
            ("2", "event2", 20),
            ("3", "event3", 30),
            ("4", "event4", 40),
        ]
        source_df = mock_spark_session.createDataFrame(
            data, ["id", "event", "value"]
        )

        report, context = runner.run_until(
            "clean_events",
            steps=[bronze_step, silver_step, gold_step],
            bronze_sources={"events": source_df},
        )
        assert report.status.value == "completed"
        assert context["clean_events"].count() == 4

        context = {"events": source_df}
        step_params = {"clean_events": {"threshold": 15}}
        report2, context = runner.run_step(
            "clean_events",
            steps=[silver_step],
            context=context,
            step_params=step_params,
        )
        assert report2.status.value == "completed"
        assert context["clean_events"].count() == 3

        session = PipelineDebugSession(
            mock_spark_session,
            config,
            steps=[bronze_step, silver_step],
            bronze_sources={"events": source_df},
        )
        report3, _ = session.run_until("clean_events")
        assert report3.status.value == "completed"
        assert "clean_events" in session.context

        session.set_step_params("clean_events", {"threshold": 20})
        report4, _ = session.rerun_step("clean_events", write_outputs=False)
        assert report4.status.value == "completed"
        assert session.context["clean_events"].count() == 2
