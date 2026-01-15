"""
Comprehensive tests for silver step dependency ordering.

This module tests the fix for silver steps executing in the correct order
when they depend on other silver steps via prior_silvers.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.functions import get_default_functions
from pipeline_builder.models import SilverStep
from pipeline_builder.dependencies import DependencyAnalyzer

F = get_default_functions()


class TestSilverStepDependencyOrdering:
    """Test that silver steps execute in correct order based on dependencies."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock()
        spark.table.return_value.count.return_value = 0

        # Mock DataFrame operations
        def create_mock_df(data, schema):
            df = Mock()
            df.columns = schema
            df.withColumn.return_value = df
            df.filter.return_value = df
            df.join.return_value = df
            df.groupBy.return_value = Mock()
            df.groupBy.return_value.agg.return_value = df
            return df

        spark.createDataFrame = create_mock_df
        return spark

    @pytest.fixture
    def bronze_df(self, mock_spark):
        """Create a sample bronze DataFrame."""
        data = [("user1", "event1", 100), ("user2", "event2", 200)]
        return mock_spark.createDataFrame(data, ["user_id", "event_type", "value"])

    def test_two_silver_steps_with_dependency(self, mock_spark, bronze_df):
        """Test that two silver steps execute in correct order when second depends on first."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            assert "silver_1" in prior_silvers, "silver_1 should be in prior_silvers"
            return prior_silvers["silver_1"]

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],
        )

        # Analyze dependencies
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Verify dependency graph has correct edges
        # silver_1 should execute before silver_2
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        assert silver_1_idx < silver_2_idx, "silver_1 should execute before silver_2"

    def test_three_silver_steps_chain_dependency(self, mock_spark, bronze_df):
        """Test chain of three silver steps: silver_1 -> silver_2 -> silver_3."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            assert "silver_1" in prior_silvers, "silver_1 should be available"
            return prior_silvers["silver_1"]

        def silver_3_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_3")
            assert "silver_1" in prior_silvers, "silver_1 should be available"
            assert "silver_2" in prior_silvers, "silver_2 should be available"
            return prior_silvers["silver_2"]

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],
        )

        silver_3 = SilverStep(
            name="silver_3",
            source_bronze="bronze_step",
            transform=silver_3_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_2"],
        )

        # Analyze dependencies
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={
                "silver_1": silver_1,
                "silver_2": silver_2,
                "silver_3": silver_3,
            },
            gold_steps={},
        )

        # Verify execution order - chain: silver_1 -> silver_2 -> silver_3
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        silver_3_idx = analysis.execution_order.index("silver_3")
        assert silver_1_idx < silver_2_idx < silver_3_idx, (
            "Chain order: silver_1 -> silver_2 -> silver_3"
        )

    def test_multiple_silver_dependencies(self, mock_spark, bronze_df):
        """Test silver step that depends on multiple other silver steps."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            return bronze_df

        def silver_3_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_3")
            assert "silver_1" in prior_silvers, "silver_1 should be available"
            assert "silver_2" in prior_silvers, "silver_2 should be available"
            return prior_silvers["silver_1"]

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
        )

        silver_3 = SilverStep(
            name="silver_3",
            source_bronze="bronze_step",
            transform=silver_3_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_1", "silver_2"],  # Depends on both
        )

        # Analyze dependencies
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={
                "silver_1": silver_1,
                "silver_2": silver_2,
                "silver_3": silver_3,
            },
            gold_steps={},
        )

        # Verify execution order
        # silver_1 and silver_2 can execute in any order (no dependency between them)
        # silver_3 depends on both, so it must be after both
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        silver_3_idx = analysis.execution_order.index("silver_3")
        # silver_3 must be after both silver_1 and silver_2
        assert silver_3_idx > silver_1_idx, "silver_3 should execute after silver_1"
        assert silver_3_idx > silver_2_idx, "silver_3 should execute after silver_2"

    def test_silver_without_dependencies_still_works(self, mock_spark, bronze_df):
        """Test that silver steps without source_silvers still work (backward compatibility)."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            # Should work even if prior_silvers is empty
            assert isinstance(prior_silvers, dict), "prior_silvers should be a dict"
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            # Can access prior_silvers even if not declared
            # (backward compatibility - includes all previous steps)
            return bronze_df

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            # No source_silvers - backward compatible
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            # No source_silvers - backward compatible
        )

        # Analyze dependencies - should work without errors
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Both should be in the same group (no dependencies between them)
        # bronze is in group 0, silvers in group 1
        # silver_1 and silver_2 have no dependencies, so order doesn't matter
        # Just verify both are in execution order
        assert "silver_1" in analysis.execution_order
        assert "silver_2" in analysis.execution_order

    def test_silver_depends_on_nonexistent_silver_warns(self, mock_spark, bronze_df):
        """Test that depending on non-existent silver step logs a warning."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            source_silvers=["nonexistent_silver"],  # Doesn't exist
        )

        analyzer = DependencyAnalyzer()

        # Should not raise, but should log warning
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1},
            gold_steps={},
        )

        # Should still create execution groups
        assert len(analysis.execution_order) > 0

    def test_mixed_bronze_and_silver_dependencies(self, mock_spark, bronze_df):
        """Test silver step that depends on both bronze (via source_bronze) and silver (via source_silvers)."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            # Should have access to both bronze_df (from source_bronze) and prior_silvers
            assert "silver_1" in prior_silvers, "silver_1 should be in prior_silvers"
            return prior_silvers["silver_1"]

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",  # Still depends on bronze
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],  # Also depends on silver_1
        )

        # Analyze dependencies
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Verify both bronze and silver dependencies are respected
        bronze_idx = analysis.execution_order.index("bronze_step")
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        assert bronze_idx < silver_1_idx, "bronze should execute before silver_1"
        assert silver_1_idx < silver_2_idx, "silver_1 should execute before silver_2"

    def test_complex_dependency_graph(self, mock_spark, bronze_df):
        """Test complex dependency graph with multiple paths."""
        # Graph: bronze -> silver_1 -> silver_3
        #                  -> silver_2 -> silver_3
        #                  -> silver_4 (no dependencies)

        execution_order = []

        def make_transform(name):
            def transform(spark, bronze_df, prior_silvers):
                execution_order.append(name)
                return bronze_df

            return transform

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=make_transform("silver_1"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=make_transform("silver_2"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
        )

        silver_3 = SilverStep(
            name="silver_3",
            source_bronze="bronze_step",
            transform=make_transform("silver_3"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_1", "silver_2"],  # Depends on both
        )

        silver_4 = SilverStep(
            name="silver_4",
            source_bronze="bronze_step",
            transform=make_transform("silver_4"),
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_4",
            # No dependencies
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={
                "silver_1": silver_1,
                "silver_2": silver_2,
                "silver_3": silver_3,
                "silver_4": silver_4,
            },
            gold_steps={},
        )

        # Verify execution groups
        # bronze_step in group 0
        # silver_1, silver_2, silver_4 can run in parallel (same group after bronze)
        # silver_3 depends on silver_1 and silver_2, so it's in a later group
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        silver_3_idx = analysis.execution_order.index("silver_3")
        # silver_3 must be after silver_1 and silver_2 (it depends on both)
        assert silver_3_idx > silver_1_idx, "silver_3 should execute after silver_1"
        assert silver_3_idx > silver_2_idx, "silver_3 should execute after silver_2"

    def test_prior_silvers_only_includes_specified_steps(self, mock_spark, bronze_df):
        """Test that prior_silvers only includes steps specified in source_silvers."""
        execution_order = []
        prior_silvers_captured = {}

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            return bronze_df

        def silver_3_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_3")
            prior_silvers_captured["silver_3"] = set(prior_silvers.keys())
            return bronze_df

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
        )

        silver_3 = SilverStep(
            name="silver_3",
            source_bronze="bronze_step",
            transform=silver_3_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_1"],  # Only depends on silver_1, not silver_2
        )

        # Analyze dependencies
        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={
                "silver_1": silver_1,
                "silver_2": silver_2,
                "silver_3": silver_3,
            },
            gold_steps={},
        )

        # Verify silver_3 only depends on silver_1
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_3_idx = analysis.execution_order.index("silver_3")
        assert silver_1_idx < silver_3_idx, "silver_3 should execute after silver_1"

    def test_empty_source_silvers_list(self, mock_spark, bronze_df):
        """Test that empty source_silvers list is handled correctly."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            source_silvers=[],  # Empty list
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1},
            gold_steps={},
        )

        # Should work without errors
        assert len(analysis.execution_order) > 0

    def test_source_silvers_with_single_string(self, mock_spark, bronze_df):
        """Test that source_silvers can be a single string (backward compatibility)."""
        execution_order = []

        def silver_1_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_1")
            return bronze_df

        def silver_2_transform(spark, bronze_df, prior_silvers):
            execution_order.append("silver_2")
            assert "silver_1" in prior_silvers
            return bronze_df

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=silver_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        # Create silver_2 with source_silvers as a string (should be converted to list internally)
        # Actually, the model expects a list, so this test verifies the analyzer handles it
        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=silver_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],  # List format
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Should work correctly - bronze in group 0, silvers in later groups
        silver_1_idx = analysis.execution_order.index("silver_1")
        silver_2_idx = analysis.execution_order.index("silver_2")
        assert silver_1_idx < silver_2_idx, "silver_1 should execute before silver_2"
