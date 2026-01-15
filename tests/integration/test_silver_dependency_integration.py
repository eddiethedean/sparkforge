"""
Integration tests for silver step dependencies with PipelineBuilder.

These tests verify that silver step dependencies work correctly in the
full pipeline execution context.
"""

import pytest

from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions


class TestSilverDependencyIntegration:
    """Integration tests for silver step dependencies."""

    @pytest.fixture
    def spark(self, mock_spark_session):
        """Get configured Spark session."""
        # Engine is already configured in conftest.py
        return mock_spark_session

    @pytest.fixture
    def F(self):
        """Get default functions."""
        return get_default_functions()

    def test_pipeline_builder_with_silver_dependencies(self, spark, F):
        """Test PipelineBuilder correctly handles silver step dependencies."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df.withColumn("processed", F.lit(True))

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            assert "silver_1" in prior_silvers, "silver_1 should be in prior_silvers"
            return prior_silvers["silver_1"].withColumn("enriched", F.lit(True))

        # Create sample data
        data = [("user1", "event1", 100), ("user2", "event2", 200)]
        bronze_df = spark.createDataFrame(data, ["user_id", "event_type", "value"])

        # Build pipeline
        builder = PipelineBuilder(spark=spark, schema="test")

        # Add bronze step
        builder.with_bronze_rules(
            name="bronze_step",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        # Add first silver step
        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        # Add second silver step that depends on first
        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],  # Declare dependency
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"bronze_step": bronze_df})

        # Verify execution order
        assert execution_order == ["silver_1", "silver_2"], (
            f"Expected silver_1 before silver_2, got: {execution_order}"
        )

        # Verify pipeline completed
        assert result.status.value == "completed", (
            "Pipeline should complete successfully"
        )

    def test_pipeline_builder_chain_dependencies(self, spark, F):
        """Test PipelineBuilder with chain of silver dependencies."""
        execution_order = []

        def make_transform(name):
            def transform(spark, bronze_df, prior_silvers):
                execution_order.append(name)
                if name == "silver_2":
                    assert "silver_1" in prior_silvers
                elif name == "silver_3":
                    assert "silver_1" in prior_silvers
                    assert "silver_2" in prior_silvers
                return bronze_df

            return transform

        data = [("user1", "event1", 100)]
        bronze_df = spark.createDataFrame(data, ["user_id", "event_type", "value"])

        builder = PipelineBuilder(spark=spark, schema="test")
        builder.with_bronze_rules(
            name="bronze_step",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_step",
            transform=make_transform("silver_1"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_step",
            transform=make_transform("silver_2"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],
        )

        builder.add_silver_transform(
            name="silver_3",
            source_bronze="bronze_step",
            transform=make_transform("silver_3"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_2"],
        )

        pipeline = builder.to_pipeline()
        pipeline.run_initial_load(bronze_sources={"bronze_step": bronze_df})

        # Verify chain execution order
        assert execution_order == ["silver_1", "silver_2", "silver_3"], (
            f"Expected chain order, got: {execution_order}"
        )

    def test_pipeline_builder_multiple_dependencies(self, spark, F):
        """Test PipelineBuilder with silver step depending on multiple silvers."""
        execution_order = []
        prior_silvers_keys = {}

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            return bronze_df

        def silver_3_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_3")
            prior_silvers_keys["silver_3"] = set(prior_silvers.keys())
            return bronze_df

        data = [("user1", "event1", 100)]
        bronze_df = spark.createDataFrame(data, ["user_id", "event_type", "value"])

        builder = PipelineBuilder(spark=spark, schema="test")
        builder.with_bronze_rules(
            name="bronze_step",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
        )

        builder.add_silver_transform(
            name="silver_3",
            source_bronze="bronze_step",
            transform=silver_3_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_1", "silver_2"],  # Depends on both
        )

        pipeline = builder.to_pipeline()
        pipeline.run_initial_load(bronze_sources={"bronze_step": bronze_df})

        # Verify silver_3 has both dependencies in prior_silvers
        assert "silver_1" in prior_silvers_keys["silver_3"]
        assert "silver_2" in prior_silvers_keys["silver_3"]

        # Verify execution order (silver_1 and silver_2 can be in any order, but before silver_3)
        assert "silver_3" not in execution_order[:2], "silver_3 should execute last"
        assert execution_order[-1] == "silver_3", "silver_3 should be last"

    def test_backward_compatibility_no_source_silvers(self, spark, F):
        """Test that pipelines without source_silvers still work (backward compatibility)."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            # prior_silvers should be a dict (may be empty)
            assert isinstance(prior_silvers, dict)
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            # Without source_silvers, prior_silvers may include all previous steps
            assert isinstance(prior_silvers, dict)
            return bronze_df

        data = [("user1", "event1", 100)]
        bronze_df = spark.createDataFrame(data, ["user_id", "event_type", "value"])

        builder = PipelineBuilder(spark=spark, schema="test")
        builder.with_bronze_rules(
            name="bronze_step",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        # Add silver steps without source_silvers
        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            # No source_silvers - backward compatible
        )

        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            # No source_silvers - backward compatible
        )

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"bronze_step": bronze_df})

        # Should complete successfully
        assert result.status.value == "completed"
        assert len(execution_order) == 2
