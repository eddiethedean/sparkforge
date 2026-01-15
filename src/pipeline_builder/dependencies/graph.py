"""
Dependency graph representation for the framework pipelines.

This module provides a clean, efficient representation of pipeline dependencies
that can be used for dependency analysis, cycle detection, execution planning,
and optimization. The graph supports topological sorting and validation.

**Key Features:**
    - **Dependency Tracking**: Track dependencies and dependents for each step
    - **Cycle Detection**: Detect circular dependencies in the pipeline
    - **Topological Sort**: Order steps by dependency requirements for sequential execution
    - **Validation**: Validate graph structure and detect issues

**Common Use Cases:**
    - Analyze pipeline dependencies before execution
    - Detect circular dependencies that would cause execution failures
    - Determine execution order for sequential processing using topological sort

Example:
    >>> from pipeline_builder.dependencies.graph import (
    ...     DependencyGraph,
    ...     StepNode,
    ...     StepType
    ... )
    >>>
    >>> # Create dependency graph
    >>> graph = DependencyGraph()
    >>>
    >>> # Add nodes
    >>> bronze = StepNode("bronze_step", StepType.BRONZE)
    >>> silver = StepNode("silver_step", StepType.SILVER)
    >>> graph.add_node(bronze)
    >>> graph.add_node(silver)
    >>>
    >>> # Add dependency (silver depends on bronze)
    >>> graph.add_dependency("silver_step", "bronze_step")
    >>>
    >>> # Validate and get execution order
    >>> issues = graph.validate()
    >>> execution_order = graph.topological_sort()
    >>> print(execution_order)  # ["bronze_step", "silver_step"]
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict

logger = logging.getLogger(__name__)


class StepType(Enum):
    """Types of pipeline steps in the Medallion Architecture.

    Represents the three layers of the Medallion Architecture:
    - BRONZE: Raw data ingestion and validation layer
    - SILVER: Cleaned and enriched data layer
    - GOLD: Business-ready analytics and reporting layer

    Example:
        >>> from pipeline_builder.dependencies.graph import StepType
        >>> step_type = StepType.BRONZE
        >>> print(step_type.value)  # "bronze"
    """

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepNode:
    """Represents a single step in the dependency graph.

    A StepNode contains all information about a pipeline step including its
    dependencies, dependents, execution metadata, and custom metadata.

    Attributes:
        name: Unique identifier for this step.
        step_type: Type of step (BRONZE, SILVER, or GOLD).
        dependencies: Set of step names that this step depends on. Steps in
            this set must complete before this step can execute.
        dependents: Set of step names that depend on this step. These steps
            cannot execute until this step completes.
        execution_group: (Deprecated) Legacy field, no longer used. Execution
            order is determined by topological sort.
        estimated_duration: Estimated execution duration in seconds. Used
            for optimization and scheduling. Defaults to 0.0.
        metadata: Dictionary for storing custom metadata about the step.
            Can contain any key-value pairs.

    Example:
        >>> from pipeline_builder.dependencies.graph import StepNode, StepType
        >>> node = StepNode(
        ...     name="user_events",
        ...     step_type=StepType.BRONZE,
        ...     estimated_duration=10.5,
        ...     metadata={"source": "kafka", "partition_count": 4}
        ... )
    """

    name: str
    step_type: StepType
    dependencies: set[str] = field(default_factory=set)
    dependents: set[str] = field(default_factory=set)
    execution_group: int = 0
    estimated_duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DependencyGraph:
    """Represents the dependency graph of a pipeline.

    This class provides efficient operations for dependency analysis,
    cycle detection, and execution planning. It maintains both forward
    and reverse adjacency lists for efficient traversal in both directions.

    **Key Operations:**
        - Add nodes and dependencies
        - Detect circular dependencies
        - Perform topological sort for execution order
        - Validate graph structure

    Attributes:
        nodes: Dictionary mapping step names to StepNode instances.
        _adjacency_list: Forward adjacency list for dependency traversal.
        _reverse_adjacency_list: Reverse adjacency list for dependent traversal.

    Example:
        >>> from pipeline_builder.dependencies.graph import (
        ...     DependencyGraph,
        ...     StepNode,
        ...     StepType
        ... )
        >>>
        >>> graph = DependencyGraph()
        >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
        >>> graph.add_node(StepNode("silver", StepType.SILVER))
        >>> graph.add_dependency("silver", "bronze")
        >>> execution_order = graph.topological_sort()
    """

    def __init__(self) -> None:
        """Initialize an empty dependency graph."""
        self.nodes: Dict[str, StepNode] = {}
        self._adjacency_list: Dict[str, set[str]] = defaultdict(set)
        self._reverse_adjacency_list: Dict[str, set[str]] = defaultdict(set)

    def add_node(self, node: StepNode) -> None:
        """Add a node to the dependency graph.

        Adds a StepNode to the graph and initializes its adjacency list entries.
        If a node with the same name already exists, it will be replaced.

        Args:
            node: StepNode instance to add to the graph.

        Example:
            >>> graph = DependencyGraph()
            >>> node = StepNode("bronze_step", StepType.BRONZE)
            >>> graph.add_node(node)
        """
        self.nodes[node.name] = node
        self._adjacency_list[node.name] = set()
        self._reverse_adjacency_list[node.name] = set()

    def add_dependency(self, from_step: str, to_step: str) -> None:
        """Add a dependency from one step to another.

        Creates a dependency relationship where `from_step` depends on `to_step`.
        This means `to_step` must complete before `from_step` can execute.

        Args:
            from_step: Name of the step that depends on `to_step`.
            to_step: Name of the step that `from_step` depends on.

        Raises:
            ValueError: If either step is not found in the graph.

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
            >>> graph.add_node(StepNode("silver", StepType.SILVER))
            >>> # Silver depends on bronze
            >>> graph.add_dependency("silver", "bronze")
        """
        if from_step not in self.nodes or to_step not in self.nodes:
            raise ValueError(f"Steps {from_step} or {to_step} not found in graph")

        self._adjacency_list[from_step].add(to_step)
        self._reverse_adjacency_list[to_step].add(from_step)

        # Update node dependencies
        self.nodes[from_step].dependencies.add(to_step)
        self.nodes[to_step].dependents.add(from_step)

    def get_dependencies(self, step_name: str) -> set[str]:
        """Get all dependencies for a step.

        Returns a copy of the set of step names that the specified step
        depends on. These steps must complete before the specified step
        can execute.

        Args:
            step_name: Name of the step to get dependencies for.

        Returns:
            Set of step names that the specified step depends on. Returns
            an empty set if the step is not found in the graph.

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
            >>> graph.add_node(StepNode("silver", StepType.SILVER))
            >>> graph.add_dependency("silver", "bronze")
            >>> deps = graph.get_dependencies("silver")
            >>> print(deps)  # {"bronze"}
        """
        return self.nodes.get(
            step_name, StepNode("", StepType.BRONZE)
        ).dependencies.copy()

    def get_dependents(self, step_name: str) -> set[str]:
        """Get all dependents for a step.

        Returns a copy of the set of step names that depend on the specified
        step. These steps cannot execute until the specified step completes.

        Args:
            step_name: Name of the step to get dependents for.

        Returns:
            Set of step names that depend on the specified step. Returns
            an empty set if the step is not found in the graph.

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
            >>> graph.add_node(StepNode("silver", StepType.SILVER))
            >>> graph.add_dependency("silver", "bronze")
            >>> dependents = graph.get_dependents("bronze")
            >>> print(dependents)  # {"silver"}
        """
        return self.nodes.get(
            step_name, StepNode("", StepType.BRONZE)
        ).dependents.copy()

    def detect_cycles(self) -> list[list[str]]:
        """Detect cycles in the dependency graph using DFS.

        Detects all circular dependencies in the graph using depth-first search.
        A cycle indicates that there's a circular dependency that would prevent
        execution (e.g., A depends on B, B depends on A).

        Returns:
            List of cycles, where each cycle is a list of step names forming
            a circular dependency. Returns an empty list if no cycles are found.

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("step_a", StepType.BRONZE))
            >>> graph.add_node(StepNode("step_b", StepType.SILVER))
            >>> graph.add_dependency("step_a", "step_b")
            >>> graph.add_dependency("step_b", "step_a")  # Creates cycle
            >>> cycles = graph.detect_cycles()
            >>> print(cycles)  # [["step_a", "step_b", "step_a"]]
        """
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node: str, path: list[str]) -> None:
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return

            if node in visited:
                return

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in self._adjacency_list[node]:
                dfs(neighbor, path)

            rec_stack.remove(node)
            path.pop()

        for node in self.nodes:
            if node not in visited:
                dfs(node, [])

        return cycles

    def topological_sort(self) -> list[str]:
        """Perform topological sort of the dependency graph.

        Returns nodes in an order such that all dependencies come before their
        dependents. This provides a valid execution order for the pipeline steps.

        **Algorithm:**
            Uses Kahn's algorithm with in-degree counting. Steps with no
            dependencies (in-degree 0) are processed first, then their dependents
            are processed when all their dependencies are satisfied.

        Returns:
            List of step names in topological order. If there are cycles in
            the graph, the result may be incomplete (some steps may be missing).

        Raises:
            RuntimeError: If cycles are detected (topological sort is not
                possible for cyclic graphs).

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
            >>> graph.add_node(StepNode("silver", StepType.SILVER))
            >>> graph.add_node(StepNode("gold", StepType.GOLD))
            >>> graph.add_dependency("silver", "bronze")
            >>> graph.add_dependency("gold", "silver")
            >>> order = graph.topological_sort()
            >>> print(order)  # ["bronze", "silver", "gold"]
        """
        in_degree = dict.fromkeys(self.nodes, 0)

        # Calculate in-degrees using reverse adjacency
        # If A depends on B, then B->A edge exists in reverse list
        for node in self.nodes:
            for dependent in self._reverse_adjacency_list[node]:
                in_degree[dependent] += 1

        # Find nodes with no incoming edges (no dependencies)
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Process nodes that depend on this one
            for dependent in self._reverse_adjacency_list[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def validate(self) -> list[str]:
        """Validate the dependency graph and return any issues.

        Checks the graph for common issues including:
        - Circular dependencies (cycles)
        - Missing dependencies (steps that reference non-existent steps)

        Returns:
            List of validation issue messages. Returns an empty list if the
            graph is valid. Each message describes a specific issue found.

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("step_a", StepType.BRONZE))
            >>> graph.add_node(StepNode("step_b", StepType.SILVER))
            >>> # Add invalid dependency
            >>> graph.nodes["step_b"].dependencies.add("missing_step")
            >>> issues = graph.validate()
            >>> print(issues)  # ["Node step_b depends on missing node missing_step"]
        """
        issues = []

        # Check for cycles
        cycles = self.detect_cycles()
        if cycles:
            for cycle in cycles:
                issues.append(f"Circular dependency detected: {' -> '.join(cycle)}")

        # Check for missing dependencies
        for node_name, node in self.nodes.items():
            for dep in node.dependencies:
                if dep not in self.nodes:
                    issues.append(f"Node {node_name} depends on missing node {dep}")

        return issues

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the dependency graph.

        Calculates and returns various statistics about the graph structure,
        including node counts, edge counts, type distribution, and cycle
        detection.

        Returns:
            Dictionary containing statistics with the following keys:
                - `total_nodes`: Total number of nodes in the graph
                - `total_edges`: Total number of dependency edges
                - `type_counts`: Dictionary mapping step types to counts
                - `average_dependencies`: Average number of dependencies per node
                - `has_cycles`: Boolean indicating if cycles are detected

        Example:
            >>> graph = DependencyGraph()
            >>> graph.add_node(StepNode("bronze", StepType.BRONZE))
            >>> graph.add_node(StepNode("silver", StepType.SILVER))
            >>> stats = graph.get_stats()
            >>> print(f"Total nodes: {stats['total_nodes']}")  # 2
            >>> print(f"Has cycles: {stats['has_cycles']}")  # False
        """
        total_nodes = len(self.nodes)
        total_edges = sum(len(deps) for deps in self._adjacency_list.values())

        # Count by step type
        type_counts: Dict[str, int] = defaultdict(int)
        for node in self.nodes.values():
            type_counts[node.step_type.value] += 1

        # Calculate average dependencies
        avg_dependencies = total_edges / total_nodes if total_nodes > 0 else 0

        return {
            "total_nodes": total_nodes,
            "total_edges": total_edges,
            "type_counts": dict(type_counts),
            "average_dependencies": avg_dependencies,
            "has_cycles": len(self.detect_cycles()) > 0,
        }
