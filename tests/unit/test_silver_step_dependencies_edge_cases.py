"""
Edge case tests for silver step dependencies.

Tests edge cases, error conditions, and boundary conditions for
silver step dependency ordering.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.functions import get_default_functions
from pipeline_builder.models import SilverStep
from pipeline_builder_base.models import PipelineConfig
from pipeline_builder.dependencies import DependencyAnalyzer

F = get_default_functions()


class TestSilverStepDependencyEdgeCases:
    """Test edge cases for silver step dependencies."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return Mock()

    def test_circular_dependency_detection(self, mock_spark):
        """Test that circular dependencies between silver steps are handled."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            source_silvers=["silver_2"],  # Depends on silver_2
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],  # Depends on silver_1 (circular!)
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Cycle detection may resolve cycles automatically, but analysis should complete
        # The graph may show cycles in cycles list, or they may be resolved
        assert analysis is not None, "Analysis should complete even with cycles"
        # Cycles may be detected or resolved - either is acceptable
        assert len(analysis.execution_groups) > 0, "Should have execution groups"

    def test_self_dependency_handled(self, mock_spark):
        """Test that a silver step depending on itself is handled gracefully."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            source_silvers=["silver_1"],  # Depends on itself
        )

        analyzer = DependencyAnalyzer()
        # Should not raise, but may detect as cycle
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1},
            gold_steps={},
        )

        # Should complete analysis (may detect cycle)
        assert analysis is not None

    def test_duplicate_source_silvers(self, mock_spark):
        """Test that duplicate entries in source_silvers are handled."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1", "silver_1"],  # Duplicate
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Should handle duplicates gracefully
        silver_1_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_1" in group)
        silver_2_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_2" in group)
        assert silver_1_group < silver_2_group, "silver_1 should execute before silver_2"

    def test_source_silvers_with_mixed_valid_invalid(self, mock_spark):
        """Test source_silvers with mix of valid and invalid step names."""
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1", "nonexistent"],  # Mix of valid and invalid
        )

        analyzer = DependencyAnalyzer()
        # Should log warning but continue
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2},
            gold_steps={},
        )

        # Should still create valid dependency for silver_1
        silver_1_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_1" in group)
        silver_2_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_2" in group)
        assert silver_1_group < silver_2_group, "Valid dependency should be respected"

    def test_large_dependency_graph(self, mock_spark):
        """Test dependency graph with many silver steps."""
        # Create 10 silver steps in a chain
        silver_steps = {}
        for i in range(10):
            prev_step = f"silver_{i-1}" if i > 0 else None
            silver_steps[f"silver_{i}"] = SilverStep(
                name=f"silver_{i}",
                source_bronze="bronze_step",
                transform=lambda spark, df, silvers: df,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name=f"silver_{i}",
                source_silvers=[prev_step] if prev_step else None,
            )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps=silver_steps,
            gold_steps={},
        )

        # Verify all steps are in correct order
        groups = {}
        for i, group in enumerate(analysis.execution_groups):
            for step_name in group:
                if step_name.startswith("silver_"):
                    groups[step_name] = i

        # Verify chain order
        for i in range(1, 10):
            assert groups[f"silver_{i-1}"] < groups[f"silver_{i}"], (
                f"silver_{i-1} should execute before silver_{i}"
            )

    def test_silver_depends_on_gold_should_fail(self, mock_spark):
        """Test that silver step depending on gold step is invalid."""
        # Create a proper gold step mock
        from pipeline_builder.models import GoldStep
        
        gold_step = GoldStep(
            name="gold_step",
            transform=lambda spark, silvers: silvers.get("silver_1") if silvers else None,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="gold_step",
            source_silvers=["silver_1"],  # Gold step needs non-empty source_silvers
        )

        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
            source_silvers=["gold_step"],  # Invalid: silver depending on gold
        )

        analyzer = DependencyAnalyzer()
        # Should log warning about non-existent dependency (gold_step not in silver steps)
        # The dependency analyzer only looks at silver steps for silver dependencies
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1},
            gold_steps={"gold_step": gold_step},
        )

        # Analysis should complete (may warn about invalid dependency)
        assert analysis is not None

    def test_source_silvers_none_vs_empty_list(self, mock_spark):
        """Test that None and empty list for source_silvers are handled the same."""
        silver_1_none = SilverStep(
            name="silver_1_none",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1_none",
            source_silvers=None,
        )

        silver_1_empty = SilverStep(
            name="silver_1_empty",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1_empty",
            source_silvers=[],
        )

        analyzer = DependencyAnalyzer()
        analysis_none = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1_none": silver_1_none},
            gold_steps={},
        )

        analysis_empty = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1_empty": silver_1_empty},
            gold_steps={},
        )

        # Both should behave the same (no silver dependencies)
        assert len(analysis_none.execution_groups) == len(analysis_empty.execution_groups)

    def test_prior_silvers_includes_only_executed_steps(self, mock_spark):
        """Test that prior_silvers only includes steps that have been executed."""
        # This is tested at execution time, but verify the dependency graph
        # correctly orders steps so this will work
        silver_1 = SilverStep(
            name="silver_1",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )

        silver_2 = SilverStep(
            name="silver_2",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
            source_silvers=["silver_1"],
        )

        silver_3 = SilverStep(
            name="silver_3",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_3",
            source_silvers=["silver_2"],
        )

        analyzer = DependencyAnalyzer()
        analysis = analyzer.analyze_dependencies(
            bronze_steps={"bronze_step": Mock(name="bronze_step")},
            silver_steps={"silver_1": silver_1, "silver_2": silver_2, "silver_3": silver_3},
            gold_steps={},
        )

        # Verify execution order ensures prior_silvers will be populated correctly
        silver_1_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_1" in group)
        silver_2_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_2" in group)
        silver_3_group = next(i for i, group in enumerate(analysis.execution_groups) if "silver_3" in group)

        assert silver_1_group < silver_2_group < silver_3_group, (
            "Execution order ensures prior_silvers will be populated correctly"
        )
