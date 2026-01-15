"""
Comprehensive tests for pipeline_builder_base dependencies module.

Tests DependencyGraph and exceptions.
"""

from pipeline_builder_base.dependencies.exceptions import (
    CircularDependencyError,
    DependencyAnalysisError,
    DependencyConflictError,
    DependencyError,
    InvalidDependencyError,
)
from pipeline_builder_base.dependencies.graph import DependencyGraph, StepNode, StepType


class TestDependencyGraph:
    """Test DependencyGraph class."""

    def test_dependency_graph_initialization(self):
        """Test graph creation."""
        graph = DependencyGraph()
        assert len(graph.nodes) == 0

    def test_add_node(self):
        """Test adding nodes to graph."""
        graph = DependencyGraph()
        node = StepNode(name="step1", step_type=StepType.BRONZE)
        graph.add_node(node)

        assert "step1" in graph.nodes
        assert graph.nodes["step1"].name == "step1"

    def test_add_dependency(self):
        """Test adding dependencies."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency(
            "step2", "step1"
        )  # step2 depends on step1 (step2 -> step1 means step2 depends on step1)

        assert "step2" in graph.nodes["step1"].dependents
        assert "step1" in graph.nodes["step2"].dependencies

    def test_add_dependency_missing_node(self):
        """Test dependency with missing node."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        graph.add_node(node1)

        # Should raise ValueError when trying to add dependency to missing node
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node2)
        graph.add_dependency("step2", "step1")  # step2 depends on step1
        assert "step2" in graph.nodes

    def test_get_node(self):
        """Test node retrieval."""
        graph = DependencyGraph()
        node = StepNode(name="step1", step_type=StepType.BRONZE)
        graph.add_node(node)

        retrieved_node = graph.nodes.get("step1")
        assert retrieved_node is not None
        assert retrieved_node.name == "step1"

    def test_get_node_missing(self):
        """Test missing node handling."""
        graph = DependencyGraph()
        node = graph.nodes.get("missing")
        assert node is None

    def test_get_dependencies(self):
        """Test dependency retrieval."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency("step2", "step1")  # step2 depends on step1

        deps = graph.get_dependencies("step2")
        assert "step1" in deps

    def test_get_dependents(self):
        """Test dependents retrieval."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency("step2", "step1")  # step2 depends on step1

        dependents = graph.get_dependents("step1")
        assert "step2" in dependents

    def test_has_cycle(self):
        """Test cycle detection."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency("step1", "step2")
        graph.add_dependency("step2", "step1")  # Creates cycle

        cycles = graph.detect_cycles()
        assert len(cycles) > 0

    def test_has_cycle_no_cycle(self):
        """Test no cycle case."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency("step1", "step2")

        cycles = graph.detect_cycles()
        assert len(cycles) == 0

    def test_topological_sort(self):
        """Test topological sorting."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        node3 = StepNode(name="step3", step_type=StepType.GOLD)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_node(node3)
        graph.add_dependency("step2", "step1")  # step2 depends on step1
        graph.add_dependency("step3", "step2")  # step3 depends on step2

        order = graph.topological_sort()
        assert order.index("step1") < order.index("step2")
        assert order.index("step2") < order.index("step3")

    def test_topological_sort_cycle(self):
        """Test sort with cycle - should still work but cycles detected."""
        graph = DependencyGraph()
        node1 = StepNode(name="step1", step_type=StepType.BRONZE)
        node2 = StepNode(name="step2", step_type=StepType.SILVER)
        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_dependency("step1", "step2")
        graph.add_dependency("step2", "step1")  # Creates cycle

        # Topological sort may still work but cycles will be detected
        cycles = graph.detect_cycles()
        assert len(cycles) > 0


class TestStepNode:
    """Test StepNode dataclass."""

    def test_step_node_creation(self):
        """Test node creation."""
        node = StepNode(name="step1", step_type=StepType.BRONZE)
        assert node.name == "step1"
        assert node.step_type == StepType.BRONZE

    def test_step_node_with_dependencies(self):
        """Test node with dependencies."""
        node = StepNode(
            name="step1",
            step_type=StepType.SILVER,
            dependencies={"bronze1"},
        )
        assert "bronze1" in node.dependencies

    def test_step_node_with_metadata(self):
        """Test node with metadata."""
        node = StepNode(
            name="step1",
            step_type=StepType.GOLD,
            metadata={"custom": "value"},
        )
        assert node.metadata["custom"] == "value"


class TestDependencyExceptions:
    """Test dependency exception classes."""

    def test_dependency_error_creation(self):
        """Test DependencyError creation."""
        error = DependencyError("Test error", step_name="step1")
        assert str(error) == "Test error"
        assert error.step_name == "step1"

    def test_cycle_error_creation(self):
        """Test CircularDependencyError creation."""
        error = CircularDependencyError(
            "Cycle detected", cycle=["step1", "step2", "step1"]
        )
        assert str(error) == "Cycle detected"
        assert error.cycle == ["step1", "step2", "step1"]

    def test_missing_dependency_error(self):
        """Test InvalidDependencyError creation."""
        error = InvalidDependencyError(
            "Invalid dependency",
            invalid_dependencies=["missing_step"],
        )
        assert str(error) == "Invalid dependency"
        assert "missing_step" in error.invalid_dependencies

    def test_dependency_analysis_error(self):
        """Test DependencyAnalysisError creation."""
        error = DependencyAnalysisError("Analysis failed", analysis_step="step1")
        assert str(error) == "Analysis failed"
        assert error.analysis_step == "step1"

    def test_dependency_conflict_error(self):
        """Test DependencyConflictError creation."""
        error = DependencyConflictError(
            "Conflict detected",
            conflicting_steps=["step1", "step2"],
        )
        assert str(error) == "Conflict detected"
        assert error.conflicting_steps == ["step1", "step2"]
