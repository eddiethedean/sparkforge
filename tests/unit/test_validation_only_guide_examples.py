"""
Tests that run the exact example code from docs/guides/VALIDATION_ONLY_STEPS_GUIDE.md.

These tests ensure all guide examples are valid and executable (with mock Spark).
"""

import os

import pytest

from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions

spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql import functions as F
else:
    from sparkless import functions as F  # type: ignore[import]


class TestValidationOnlyGuideBuilderExamples:
    """Run builder examples from the guide (build only)."""

    def test_basic_usage_example(self, mock_spark_session):
        # From guide: Example Basic Usage
        F_local = get_default_functions()
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F_local
        )
        builder.with_silver_rules(
            name="existing_clean_events",
            table_name="clean_events",
            rules={
                "user_id": [F_local.col("user_id").isNotNull()],
                "event_date": [F_local.col("event_date").isNotNull()],
                "value": [F_local.col("value") > 0],
            },
        )
        assert "existing_clean_events" in builder.silver_steps
        step = builder.silver_steps["existing_clean_events"]
        assert step.transform is None
        assert step.existing is True
        assert step.table_name == "clean_events"

    def test_with_string_rules_example(self, mock_spark_session):
        # From guide: With String Rules
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F
        )
        builder.with_silver_rules(
            name="validated_events",
            table_name="events",
            rules={
                "user_id": ["not_null"],
                "value": ["gt", 0],
                "status": ["in", ["active", "inactive"]],
            },
        )
        assert "validated_events" in builder.silver_steps

    def test_with_gold_rules_example(self, mock_spark_session):
        # From guide: with_gold_rules basic usage
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F
        )
        builder.with_gold_rules(
            name="existing_user_metrics",
            table_name="user_metrics",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "total_events": [F.col("total_events") > 0],
                "last_activity": [F.col("last_activity").isNotNull()],
            },
        )
        assert "existing_user_metrics" in builder.gold_steps
        step = builder.gold_steps["existing_user_metrics"]
        assert step.transform is None
        assert step.existing is True


class TestValidationOnlyGuideCompleteExample:
    """Run the complete example from the guide (build and run with optional=True for validation-only)."""

    def test_complete_example_builds_and_runs(self, mock_spark_session):
        # Complete Example: build pipeline and run_initial_load
        # Use optional=True for validation-only steps so we don't need existing Delta tables
        F_local = get_default_functions()
        builder = PipelineBuilder(
            spark=mock_spark_session, schema="analytics", functions=F_local
        )

        builder.with_bronze_rules(
            name="events",
            rules={"id": [F_local.col("id").isNotNull()]},
            incremental_col="timestamp",
        )
        builder.with_silver_rules(
            name="existing_clean_events",
            table_name="clean_events",
            rules={"id": [F_local.col("id").isNotNull()]},
            optional=True,
        )
        builder.with_gold_rules(
            name="existing_user_metrics",
            table_name="user_metrics",
            rules={"user_id": [F_local.col("user_id").isNotNull()]},
            optional=True,
        )

        def enriched_silver_transform(
            spark, bronze_df, prior_silvers, prior_golds=None
        ):
            result = bronze_df
            if "existing_clean_events" in prior_silvers:
                existing = prior_silvers["existing_clean_events"]
                result = result.withColumn("has_existing", F_local.lit(True))
            if prior_golds and "existing_user_metrics" in prior_golds:
                metrics = prior_golds["existing_user_metrics"]
                result = result.withColumn("has_metrics", F_local.lit(True))
            return result

        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events",
            transform=enriched_silver_transform,
            rules={"id": [F_local.col("id").isNotNull()]},
            table_name="enriched_events",
        )

        def enhanced_gold_transform(spark, silvers, prior_golds=None):
            result = silvers["enriched_events"]
            if prior_golds and "existing_user_metrics" in prior_golds:
                existing_metrics = prior_golds["existing_user_metrics"]
                result = result.join(existing_metrics, "user_id", "left")
            return result

        builder.add_gold_transform(
            name="enhanced_metrics",
            transform=enhanced_gold_transform,
            rules={"user_id": [F_local.col("user_id").isNotNull()]},
            table_name="enhanced_metrics",
            source_silvers=["enriched_events"],
        )

        pipeline = builder.to_pipeline()
        assert pipeline is not None
        assert len(pipeline.execution_order) == 5

        source_df = mock_spark_session.createDataFrame(
            [(1, "a", "2024-01-01 00:00:00")], ["id", "event", "timestamp"]
        )
        report = pipeline.run_initial_load(bronze_sources={"events": source_df})
        assert report is not None
        assert hasattr(report, "status")
