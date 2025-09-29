"""
Dependency graph representation for SparkForge pipelines.

This module provides a clean, efficient representation of pipeline dependencies
that can be used for analysis and optimization.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict

logger = logging.getLogger(__name__)


class StepType(Enum):
    """Types of pipeline steps."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepNode:
    """Represents a single step in the dependency graph."""

    name: str
    step_type: StepType
    dependencies: set[str] = field(default_factory=set)
    dependents: set[str] = field(default_factory=set)
    execution_group: int = 0
    can_run_parallel: bool = True
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

    def topological_sort(self) -> list[str]:
        """Perform topological sort of the dependency graph."""
        in_degree = dict.fromkeys(self.nodes, 0)

        # Calculate in-degrees
        for node in self.nodes:
            for neighbor in self._adjacency_list[node]:
                in_degree[neighbor] += 1

        # Find nodes with no incoming edges
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Remove this node and update in-degrees
            for neighbor in self._adjacency_list[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result

    def get_execution_groups(self) -> list[list[str]]:
        """Get execution groups for parallel execution."""
        # Use topological sort to determine execution order
        sorted_nodes = self.topological_sort()

        # Group nodes by their level in the dependency tree
        levels = {}
        for node in sorted_nodes:
            if not self.nodes[node].dependencies:
                levels[node] = 0
            else:
                # Ensure all dependencies have been processed
                max_dep_level = 0
                for dep in self.nodes[node].dependencies:
                    if dep in levels:
                        max_dep_level = max(max_dep_level, levels[dep])
                    else:
                        # If dependency not found, it might be missing from the graph
                        # This could happen if the dependency graph is incomplete
                        logger.warning(
                            f"Dependency {dep} not found in levels for node {node}"
                        )
                        max_dep_level = max(max_dep_level, 0)
                levels[node] = max_dep_level + 1

        # Group nodes by level
        groups = defaultdict(list)
        for node, level in levels.items():
            groups[level].append(node)

        return [groups[level] for level in sorted(groups.keys())]

    def get_parallel_candidates(self) -> list[list[str]]:
        """Get groups of steps that can run in parallel."""
        execution_groups = self.get_execution_groups()
        return execution_groups

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
