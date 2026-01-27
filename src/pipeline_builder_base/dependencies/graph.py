"""
Dependency graph representation for the framework pipelines.

This module provides a clean, efficient representation of pipeline dependencies
that can be used for analysis and optimization.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class StepType(Enum):
    """Types of pipeline steps."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepNode:
    """Represents a single step in the dependency graph.

    Attributes:
        name: Unique identifier for this step.
        step_type: Type of step (BRONZE, SILVER, or GOLD).
        dependencies: Set of step names that this step depends on.
        dependents: Set of step names that depend on this step.
        execution_group: (Deprecated) Legacy field, no longer used. Execution
            order is determined by topological sort.
        estimated_duration: Estimated execution duration in seconds.
        metadata: Dictionary for storing custom metadata about the step.
    """

    name: str
    step_type: StepType
    dependencies: set[str] = field(default_factory=set)
    dependents: set[str] = field(default_factory=set)
    execution_group: int = 0
    estimated_duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DependencyGraph:
    """
    Represents the dependency graph of a pipeline.

    This class provides efficient operations for dependency analysis,
    cycle detection, and execution planning.
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, StepNode] = {}
        self._adjacency_list: Dict[str, set[str]] = defaultdict(set)
        self._reverse_adjacency_list: Dict[str, set[str]] = defaultdict(set)

    def add_node(self, node: StepNode) -> None:
        """Add a node to the dependency graph."""
        self.nodes[node.name] = node
        self._adjacency_list[node.name] = set()
        self._reverse_adjacency_list[node.name] = set()

    def add_dependency(self, from_step: str, to_step: str) -> None:
        """Add a dependency from one step to another."""
        if from_step not in self.nodes or to_step not in self.nodes:
            raise ValueError(f"Steps {from_step} or {to_step} not found in graph")

        self._adjacency_list[from_step].add(to_step)
        self._reverse_adjacency_list[to_step].add(from_step)

        # Update node dependencies
        self.nodes[from_step].dependencies.add(to_step)
        self.nodes[to_step].dependents.add(from_step)

    def get_dependencies(self, step_name: str) -> set[str]:
        """Get all dependencies for a step."""
        return self.nodes.get(
            step_name, StepNode("", StepType.BRONZE)
        ).dependencies.copy()

    def get_dependents(self, step_name: str) -> set[str]:
        """Get all dependents for a step."""
        return self.nodes.get(
            step_name, StepNode("", StepType.BRONZE)
        ).dependents.copy()

    def detect_cycles(self) -> list[list[str]]:
        """Detect cycles in the dependency graph using DFS."""
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

    def topological_sort(
        self, creation_order: Optional[Dict[str, int]] = None
    ) -> list[str]:
        """
        Perform topological sort of the dependency graph.

        Returns nodes in an order such that dependencies come before dependents.
        Uses reverse adjacency list since add_dependency(A, B) means A depends on B,
        so B must come before A in the sort.

        **Explicit dependencies (e.g., source_silvers) always override creation order.**
        When multiple nodes have the same in-degree, creation_order is used as a
        tie-breaker for deterministic ordering based on step creation order.

        Args:
            creation_order: Optional dictionary mapping step names to creation order
                (lower number = created earlier). Used as tie-breaker for deterministic
                ordering when steps have no explicit dependencies. Explicit dependencies
                (via source_silvers, source_bronze, etc.) always take precedence.
        """
        in_degree = dict.fromkeys(self.nodes, 0)

        # Calculate in-degrees using reverse adjacency
        # If A depends on B, then B->A edge exists in reverse list
        for node in self.nodes:
            for dependent in self._reverse_adjacency_list[node]:
                in_degree[dependent] += 1

        # Helper function to get creation order for sorting
        def get_sort_key(node_name: str) -> tuple[int, int]:
            """Return sort key: (in_degree, creation_order).

            Lower creation_order (earlier created) comes first.
            If creation_order not available, use a large number to sort to end.
            """
            creation_ord = (
                creation_order.get(node_name, float("inf"))
                if creation_order
                else float("inf")
            )
            return (in_degree[node_name], creation_ord)

        # Find nodes with no incoming edges (no dependencies)
        # Sort by creation order for deterministic ordering
        ready_nodes = [node for node, degree in in_degree.items() if degree == 0]
        if creation_order:
            ready_nodes.sort(key=get_sort_key)
        queue = deque(ready_nodes)
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Process nodes that depend on this one
            for dependent in self._reverse_adjacency_list[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
                    # Re-sort queue to maintain creation order when adding new nodes
                    # Convert to list, sort, convert back to deque
                    if creation_order and len(queue) > 1:
                        queue_list = list(queue)
                        queue_list.sort(key=get_sort_key)
                        queue = deque(queue_list)

        return result

    def validate(self) -> list[str]:
        """Validate the dependency graph and return any issues."""
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
        """Get statistics about the dependency graph."""
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
