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

    def test_full_pipeline_bronze_to_two_silvers_with_dependency(self, spark, F):
        """Test full pipeline: single bronze -> two silvers, second uses first via prior_silvers.
        
        This test verifies:
        1. A single bronze step feeds into two separate silver steps
        2. The second silver step declares dependency on the first via source_silvers
        3. The second silver step can access the first silver's output via prior_silvers
        4. Execution order is correct (bronze -> silver_1 -> silver_2)
        5. The pipeline completes successfully with initial load
        """
        execution_order = []
        prior_silvers_accessed = {}

        # Create sample bronze data
        data = [
            ("user1", "event1", 100, "2024-01-01"),
            ("user2", "event2", 200, "2024-01-02"),
            ("user3", "event3", 300, "2024-01-03"),
        ]
        bronze_df = spark.createDataFrame(
            data, ["user_id", "event_type", "value", "date"]
        )

        # First silver step: processes bronze data
        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            # Process bronze data - add a processed flag
            result = bronze_df.withColumn("processed", F.lit(True)).withColumn(
                "processing_date", F.current_timestamp()
            )
            return result

        # Second silver step: uses first silver's output via prior_silvers
        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            # Verify silver_1 is in prior_silvers
            assert "silver_1" in prior_silvers, (
                "silver_1 should be available in prior_silvers for silver_2"
            )
            prior_silvers_accessed["silver_2"] = list(prior_silvers.keys())

            # Get silver_1's output
            silver_1_df = prior_silvers["silver_1"]

            # Verify we can access silver_1's data
            # Note: In mock-spark, the DataFrame might have all columns from the transform
            # We'll work with what's available
            available_columns = silver_1_df.columns
            assert "user_id" in available_columns, (
                f"silver_1 output should have 'user_id' column, got: {available_columns}"
            )

            # If processed column is available, use it; otherwise just use the DataFrame as-is
            if "processed" in available_columns:
                # Enrich bronze data with silver_1's processed information
                # Join bronze with silver_1 to create enriched dataset
                enriched = bronze_df.join(
                    silver_1_df.select("user_id", "processed", "processing_date"),
                    on="user_id",
                    how="inner",
                ).withColumn("enriched", F.lit(True))
            else:
                # Fallback: just join on user_id and add enriched flag
                # This handles cases where mock-spark might not preserve all columns
                enriched = bronze_df.join(
                    silver_1_df.select("user_id"),
                    on="user_id",
                    how="inner",
                ).withColumn("enriched", F.lit(True)).withColumn(
                    "processed", F.lit(True)
                )

            return enriched

        # Build pipeline
        builder = PipelineBuilder(spark=spark, schema="test_schema")

        # Add single bronze step
        builder.with_bronze_rules(
            name="bronze_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="date",
        )

        # Add first silver step (depends only on bronze)
        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_events",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_processed_events",
        )

        # Add second silver step (depends on bronze AND first silver)
        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_events",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_enriched_events",
            source_silvers=["silver_1"],  # Declare dependency on silver_1
        )

        # Validate pipeline
        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0, (
            f"Pipeline validation should pass, got errors: {validation_errors}"
        )

        # Build pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None, "Pipeline should be created successfully"

        # Run initial load
        result = pipeline.run_initial_load(
            bronze_sources={"bronze_events": bronze_df}
        )

        # Verify execution order: bronze -> silver_1 -> silver_2
        assert execution_order == ["silver_1", "silver_2"], (
            f"Expected execution order [silver_1, silver_2], got: {execution_order}"
        )

        # Verify pipeline completed successfully
        assert result.status.value == "completed", (
            f"Pipeline should complete successfully, got status: {result.status.value}"
        )

        # Verify silver_2 accessed silver_1 via prior_silvers
        assert "silver_2" in prior_silvers_accessed, (
            "silver_2 should have accessed prior_silvers"
        )
        assert "silver_1" in prior_silvers_accessed["silver_2"], (
            "silver_1 should be in prior_silvers for silver_2"
        )

        # Verify step results exist
        assert result.silver_results is not None, "Silver results should exist"
        assert "silver_1" in result.silver_results, "silver_1 result should exist"
        assert "silver_2" in result.silver_results, "silver_2 result should exist"

        # Verify both silver steps completed
        silver_1_result = result.silver_results["silver_1"]
        silver_2_result = result.silver_results["silver_2"]

        assert silver_1_result["status"] == "completed", (
            "silver_1 should complete successfully"
        )
        assert silver_2_result["status"] == "completed", (
            "silver_2 should complete successfully"
        )

        # Verify data was processed
        assert silver_1_result["rows_processed"] > 0, (
            "silver_1 should process rows"
        )
        assert silver_2_result["rows_processed"] > 0, (
            "silver_2 should process rows"
        )

    def test_execution_order_available_after_validation(self, spark, F):
        """Test that execution_order is populated after pipeline validation."""
        # Create sample bronze data
        data = [("user1", "event1", 100), ("user2", "event2", 200)]
        bronze_df = spark.createDataFrame(data, ["user_id", "event_type", "value"])

        # Build pipeline
        builder = PipelineBuilder(spark=spark, schema="test_schema")

        # Add steps
        builder.with_bronze_rules(
            name="bronze_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        builder.add_silver_transform(
            name="silver_1",
            source_bronze="bronze_events",
            transform=lambda spark, bronze_df, prior_silvers: bronze_df.withColumn(
                "processed", F.lit(True)
            ),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        builder.add_silver_transform(
            name="silver_2",
            source_bronze="bronze_events",
            transform=lambda spark, bronze_df, prior_silvers: bronze_df.withColumn(
                "enriched", F.lit(True)
            ),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],
        )

        # Before validation, execution_order should be None
        assert builder.execution_order is None, (
            "execution_order should be None before validation"
        )

        # Validate pipeline
        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0, (
            f"Pipeline validation should pass, got errors: {validation_errors}"
        )

        # After validation, execution_order should be populated
        assert builder.execution_order is not None, (
            "execution_order should be populated after validation"
        )
        assert isinstance(builder.execution_order, list), (
            "execution_order should be a list"
        )
        assert len(builder.execution_order) == 3, (
            f"execution_order should have 3 steps, got {len(builder.execution_order)}"
        )

        # Verify execution order is correct
        assert "bronze_events" in builder.execution_order, (
            "bronze_events should be in execution order"
        )
        assert "silver_1" in builder.execution_order, (
            "silver_1 should be in execution order"
        )
        assert "silver_2" in builder.execution_order, (
            "silver_2 should be in execution order"
        )

        # Verify order: bronze_events should come before silver steps
        bronze_idx = builder.execution_order.index("bronze_events")
        silver_1_idx = builder.execution_order.index("silver_1")
        silver_2_idx = builder.execution_order.index("silver_2")

        assert bronze_idx < silver_1_idx, (
            "bronze_events should execute before silver_1"
        )
        assert bronze_idx < silver_2_idx, (
            "bronze_events should execute before silver_2"
        )
        assert silver_1_idx < silver_2_idx, (
            "silver_1 should execute before silver_2 (due to source_silvers dependency)"
        )

        # Verify the order matches expected: bronze_events → silver_1 → silver_2
        expected_order = ["bronze_events", "silver_1", "silver_2"]
        assert builder.execution_order == expected_order, (
            f"Expected execution order {expected_order}, got {builder.execution_order}"
        )
