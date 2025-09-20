"""
Tests for the dependencies/graph.py module.
"""

from unittest.mock import patch

import pytest

from sparkforge.dependencies.graph import DependencyGraph, StepNode, StepType


class TestDependencyGraph:
    """Test the DependencyGraph class."""

    def test_add_dependency_missing_nodes(self):
        """Test add_dependency with missing nodes."""
        graph = DependencyGraph()

        # Add one node
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))

        # Try to add dependency with missing node
        with pytest.raises(
            ValueError, match="Steps step1 or missing_step not found in graph"
        ):
            graph.add_dependency("step1", "missing_step")

        with pytest.raises(
            ValueError, match="Steps missing_step or step1 not found in graph"
        ):
            graph.add_dependency("missing_step", "step1")

    def test_get_dependencies_missing_node(self):
        """Test get_dependencies with missing node."""
        graph = DependencyGraph()

        # Get dependencies for non-existent node
        deps = graph.get_dependencies("missing_step")
        assert deps == set()  # Should return empty set

    def test_get_dependents_missing_node(self):
        """Test get_dependents with missing node."""
        graph = DependencyGraph()

        # Get dependents for non-existent node
        deps = graph.get_dependents("missing_step")
        assert deps == set()  # Should return empty set

    def test_detect_cycles(self):
        """Test detect_cycles method."""
        graph = DependencyGraph()

        # Create a cycle: step1 -> step2 -> step1
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))
        graph.add_dependency("step1", "step2")
        graph.add_dependency("step2", "step1")

        # Test cycle detection
        cycles = graph.detect_cycles()

        assert len(cycles) > 0
        assert any("step1" in cycle and "step2" in cycle for cycle in cycles)

    def test_get_execution_groups_missing_dependency(self):
        """Test get_execution_groups with missing dependency."""
        graph = DependencyGraph()

        # Add nodes
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))

        # Manually add a dependency to a missing node
        graph.nodes["step2"].dependencies.add("missing_step")

        with patch("sparkforge.dependencies.graph.logger") as mock_logger:
            groups = graph.get_execution_groups()

            # Check that warning was logged
            mock_logger.warning.assert_any_call(
                "Dependency missing_step not found in levels for node step2"
            )

            # Check that groups were still calculated
            assert len(groups) > 0

    def test_validate_cycles(self):
        """Test validate method with cycles."""
        graph = DependencyGraph()

        # Create a cycle
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))
        graph.add_dependency("step1", "step2")
        graph.add_dependency("step2", "step1")

        issues = graph.validate()

        assert len(issues) > 0
        assert any("Circular dependency detected" in issue for issue in issues)

    def test_validate_missing_dependencies(self):
        """Test validate method with missing dependencies."""
        graph = DependencyGraph()

        # Add node with missing dependency
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.nodes["step1"].dependencies.add("missing_step")

        issues = graph.validate()

        assert len(issues) > 0
        assert any("depends on missing node missing_step" in issue for issue in issues)

    def test_get_execution_groups(self):
        """Test get_execution_groups method."""
        graph = DependencyGraph()

        # Add nodes
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))
        graph.add_dependency("step1", "step2")

        groups = graph.get_execution_groups()

        assert len(groups) == 2
        assert ["step1"] in groups
        assert ["step2"] in groups

    def test_get_stats(self):
        """Test get_stats method."""
        graph = DependencyGraph()

        # Add nodes
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))
        graph.add_dependency("step1", "step2")

        stats = graph.get_stats()

        assert stats["total_nodes"] == 2
        assert stats["total_edges"] == 1
        assert stats["average_dependencies"] == 0.5

    def test_get_parallel_candidates(self):
        """Test get_parallel_candidates method."""
        graph = DependencyGraph()

        # Add nodes
        graph.add_node(StepNode("step1", StepType.BRONZE, set()))
        graph.add_node(StepNode("step2", StepType.SILVER, set()))
        graph.add_dependency("step1", "step2")

        candidates = graph.get_parallel_candidates()

        assert len(candidates) == 2
        assert ["step1"] in candidates
        assert ["step2"] in candidates
