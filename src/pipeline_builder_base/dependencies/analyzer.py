"""
Unified dependency analyzer for the framework pipelines.

This module provides a single, consolidated dependency analyzer that works
with any step implementation that follows the step protocols.

# Depends on:
#   dependencies.exceptions
#   dependencies.graph
#   logging
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Protocol

from ..logging import PipelineLogger
from .exceptions import DependencyError
from .graph import DependencyGraph, StepNode, StepType


class AnalysisStrategy(Enum):
    """Strategies for dependency analysis."""

    CONSERVATIVE = "conservative"  # Assume all dependencies exist
    OPTIMISTIC = "optimistic"  # Assume minimal dependencies
    HYBRID = "hybrid"  # Balance between conservative and optimistic


@dataclass
class DependencyAnalysisResult:
    """Result of dependency analysis."""

    graph: DependencyGraph
    execution_order: list[
        str
    ]  # Topologically sorted step names for sequential execution
    cycles: list[list[str]]
    conflicts: list[str]
    recommendations: list[str]
    stats: Dict[str, Any]
    analysis_duration: float


# Protocol for step objects that can be analyzed
class StepProtocol(Protocol):
    """Protocol for steps that can be analyzed for dependencies."""

    name: str


class BronzeStepProtocol(StepProtocol, Protocol):
    """Protocol for bronze steps."""

    incremental_col: Optional[str]


class SilverStepProtocol(StepProtocol, Protocol):
    """Protocol for silver steps."""

    source_bronze: str


class GoldStepProtocol(StepProtocol, Protocol):
    """Protocol for gold steps."""

    source_silvers: Optional[list[str]]


class DependencyAnalyzer:
    """
    Unified dependency analyzer for all pipeline step types.

    This analyzer works with any step implementation that follows the step protocols.
    It analyzes dependencies across bronze, silver, and gold steps.

    Features:
        - Single analyzer for all step types (Bronze, Silver, Gold)
        - Multiple analysis strategies
        - Cycle detection and resolution
        - Topological sort for execution order
        - Performance analysis and recommendations
    """

    def __init__(
        self,
        strategy: AnalysisStrategy = AnalysisStrategy.HYBRID,
        logger: Optional[PipelineLogger] = None,
    ):
        self.strategy = strategy
        if logger is None:
            self.logger = PipelineLogger()
        else:
            self.logger = logger
        self._analysis_cache: Dict[str, DependencyAnalysisResult] = {}

    def analyze_dependencies(
        self,
        bronze_steps: Optional[Dict[str, BronzeStepProtocol]] = None,
        silver_steps: Optional[Dict[str, SilverStepProtocol]] = None,
        gold_steps: Optional[Dict[str, GoldStepProtocol]] = None,
        force_refresh: bool = False,
        creation_order: Optional[Dict[str, int]] = None,
    ) -> DependencyAnalysisResult:
        """
        Analyze dependencies across all step types.

        Args:
            bronze_steps: Dictionary of bronze steps (any object with name and incremental_col)
            silver_steps: Dictionary of silver steps (any object with name and source_bronze)
            gold_steps: Dictionary of gold steps (any object with name and source_silvers)
            force_refresh: Whether to force refresh of cached results

        Returns:
            DependencyAnalysisResult containing analysis results
        """
        start_time = time.time()

        # Create cache key
        cache_key = self._create_cache_key(bronze_steps, silver_steps, gold_steps)

        if not force_refresh and cache_key in self._analysis_cache:
            self.logger.info(f"Using cached dependency analysis: {cache_key}")
            return self._analysis_cache[cache_key]

        self.logger.info(
            f"Starting dependency analysis with strategy: {self.strategy.value}"
        )

        try:
            # Step 1: Build dependency graph with creation order
            graph = self._build_dependency_graph(
                bronze_steps, silver_steps, gold_steps, creation_order=creation_order
            )

            # Step 2: Detect cycles
            cycles = graph.detect_cycles()
            if cycles:
                self.logger.warning(f"Detected {len(cycles)} circular dependencies")
                graph = self._resolve_cycles(graph, cycles)

            # Step 3: Detect conflicts
            conflicts = self._detect_conflicts(graph)
            if conflicts:
                self.logger.warning(f"Detected {len(conflicts)} dependency conflicts")

            # Step 4: Generate execution order (topological sort with creation order tie-breaker)
            execution_order = graph.topological_sort(creation_order=creation_order)

            # Step 5: Generate recommendations
            recommendations = self._generate_recommendations(graph, cycles, conflicts)

            # Step 6: Calculate statistics
            stats = graph.get_stats()

            # Create result
            result = DependencyAnalysisResult(
                graph=graph,
                execution_order=execution_order,
                cycles=cycles,
                conflicts=conflicts,
                recommendations=recommendations,
                stats=stats,
                analysis_duration=time.time() - start_time,
            )

            # Cache result
            self._analysis_cache[cache_key] = result

            self.logger.info(
                f"Dependency analysis completed in {result.analysis_duration:.2f}s"
            )
            return result

        except Exception as e:
            self.logger.error(f"Dependency analysis failed: {str(e)}")
            raise DependencyError(f"Dependency analysis failed: {str(e)}") from e

    def _build_dependency_graph(
        self,
        bronze_steps: Optional[Dict[str, BronzeStepProtocol]],
        silver_steps: Optional[Dict[str, SilverStepProtocol]],
        gold_steps: Optional[Dict[str, GoldStepProtocol]],
        creation_order: Optional[Dict[str, int]] = None,
    ) -> DependencyGraph:
        """Build the dependency graph from all step types."""
        graph = DependencyGraph()

        # Add bronze steps
        if bronze_steps:
            for name, step in bronze_steps.items():
                metadata = {"step": step}
                if creation_order and name in creation_order:
                    metadata["creation_order"] = creation_order[name]
                node = StepNode(
                    name=name, step_type=StepType.BRONZE, metadata=metadata
                )
                graph.add_node(node)

        # Add silver steps - first pass: add all nodes
        silver_step_info = {}  # Store step info for dependency processing
        if silver_steps:
            for name, silver_step in silver_steps.items():
                metadata = {"step": silver_step}
                if creation_order and name in creation_order:
                    metadata["creation_order"] = creation_order[name]
                node = StepNode(
                    name=name, step_type=StepType.SILVER, metadata=metadata
                )
                graph.add_node(node)
                # Store step info for second pass
                silver_step_info[name] = silver_step

        # Second pass: add dependencies (now all nodes exist in graph)
        for name, silver_step in silver_step_info.items():
            # Add dependencies
            # SilverStep always has source_bronze attribute
            source_bronze = getattr(silver_step, "source_bronze", None)
            if source_bronze:
                # Check if the source bronze step exists
                if source_bronze in graph.nodes:
                    graph.add_dependency(name, source_bronze)
                else:
                    # Log warning about missing dependency
                    self.logger.warning(
                        f"Silver step {name} references non-existent bronze step {source_bronze}"
                    )

            # Check for silver-to-silver dependencies via source_silvers
            # This allows silver steps to depend on other silver steps
            # IMPORTANT: source_silvers overrides creation order
            source_silvers = getattr(silver_step, "source_silvers", None)
            if source_silvers:
                if isinstance(source_silvers, (list, tuple)):
                    for dep in source_silvers:
                        if dep in graph.nodes:
                            graph.add_dependency(name, dep)
                        else:
                            self.logger.warning(
                                f"Silver step {name} references non-existent silver step {dep}"
                            )
                elif isinstance(source_silvers, str):
                    if source_silvers in graph.nodes:
                        graph.add_dependency(name, source_silvers)
                    else:
                        self.logger.warning(
                            f"Silver step {name} references non-existent silver step {source_silvers}"
                        )

            # Check for additional dependencies (backward compatibility)
            if hasattr(silver_step, "depends_on"):
                depends_on = getattr(silver_step, "depends_on", None)
                if depends_on and isinstance(depends_on, (list, tuple, set)):
                    for dep in depends_on:
                        if dep in graph.nodes:
                            graph.add_dependency(name, dep)
                        else:
                            self.logger.warning(
                                f"Silver step {name} references non-existent dependency {dep}"
                            )

        # Add gold steps - first pass: add all nodes
        gold_step_info = {}  # Store step info for dependency processing
        if gold_steps:
            for name, gold_step in gold_steps.items():
                metadata = {"step": gold_step}
                if creation_order and name in creation_order:
                    metadata["creation_order"] = creation_order[name]
                node = StepNode(
                    name=name, step_type=StepType.GOLD, metadata=metadata
                )
                graph.add_node(node)
                # Store step info for second pass
                gold_step_info[name] = gold_step

        # Second pass: add dependencies (now all nodes exist in graph)
        for name, gold_step in gold_step_info.items():
            # Add dependencies
            # GoldStep always has source_silvers attribute (can be None)
            # IMPORTANT: source_silvers overrides creation order
            source_silvers = getattr(gold_step, "source_silvers", None)
            if source_silvers:
                for dep in source_silvers:
                    if dep in graph.nodes:
                        graph.add_dependency(name, dep)
                    else:
                        self.logger.warning(
                            f"Gold step {name} references non-existent silver step {dep}"
                        )

        return graph

    def _resolve_cycles(
        self, graph: DependencyGraph, cycles: list[list[str]]
    ) -> DependencyGraph:
        """Resolve cycles in the dependency graph."""
        # Simple cycle resolution: break cycles by removing the last dependency
        for cycle in cycles:
            if len(cycle) > 1:
                # Remove the last dependency in the cycle
                from_step = cycle[-2]
                to_step = cycle[-1]

                self.logger.warning(
                    f"Breaking cycle by removing dependency: {from_step} -> {to_step}"
                )

                # Remove from adjacency lists
                if to_step in graph._adjacency_list[from_step]:
                    graph._adjacency_list[from_step].remove(to_step)
                if from_step in graph._reverse_adjacency_list[to_step]:
                    graph._reverse_adjacency_list[to_step].remove(from_step)

                # Update node dependencies
                if to_step in graph.nodes[from_step].dependencies:
                    graph.nodes[from_step].dependencies.remove(to_step)
                if from_step in graph.nodes[to_step].dependents:
                    graph.nodes[to_step].dependents.remove(from_step)

        return graph

    def _detect_conflicts(self, graph: DependencyGraph) -> list[str]:
        """Detect dependency conflicts."""
        conflicts = []

        # Check for conflicting step names
        step_names = list(graph.nodes.keys())
        seen_names = set()
        for node_name in step_names:
            if node_name in seen_names:
                conflicts.append(f"Conflicting step name: {node_name}")
            seen_names.add(node_name)

        # Check for missing dependencies
        for node_name, node in graph.nodes.items():
            for dep in node.dependencies:
                if dep not in graph.nodes:
                    conflicts.append(f"Node {node_name} depends on missing node {dep}")

        return conflicts

    def _generate_recommendations(
        self, graph: DependencyGraph, cycles: list[list[str]], conflicts: list[str]
    ) -> list[str]:
        """Generate optimization recommendations."""
        recommendations = []

        # Cycle recommendations
        if cycles:
            recommendations.append(
                "Consider refactoring to eliminate circular dependencies"
            )

        # Conflict recommendations
        if conflicts:
            recommendations.append("Resolve dependency conflicts before execution")

        # Performance recommendations
        stats = graph.get_stats()
        if stats["average_dependencies"] > 3:
            recommendations.append(
                "Consider reducing step dependencies for better performance"
            )

        if len(graph.nodes) > 10:
            recommendations.append(
                "Consider breaking large pipelines into smaller, focused pipelines"
            )

        return recommendations

    def _create_cache_key(
        self,
        bronze_steps: Optional[Dict[str, BronzeStepProtocol]],
        silver_steps: Optional[Dict[str, SilverStepProtocol]],
        gold_steps: Optional[Dict[str, GoldStepProtocol]],
    ) -> str:
        """Create a cache key for the analysis."""
        # Create a simple hash of the step configurations
        key_parts = []

        if bronze_steps:
            key_parts.extend(sorted(bronze_steps.keys()))
        if silver_steps:
            key_parts.extend(sorted(silver_steps.keys()))
        if gold_steps:
            key_parts.extend(sorted(gold_steps.keys()))

        key_string = f"{self.strategy.value}:{':'.join(key_parts)}"
        return hashlib.sha256(key_string.encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear the analysis cache."""
        self._analysis_cache.clear()
        self.logger.info("Dependency analysis cache cleared")
