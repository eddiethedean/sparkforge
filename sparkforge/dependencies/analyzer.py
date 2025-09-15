# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

"""
Unified dependency analyzer for SparkForge pipelines.

This module provides a single, consolidated dependency analyzer that replaces
both DependencyAnalyzer and UnifiedDependencyAnalyzer with a cleaner design.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..logger import PipelineLogger
from ..models import BronzeStep, GoldStep, SilverStep
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
    execution_groups: list[list[str]]
    cycles: list[list[str]]
    conflicts: list[str]
    recommendations: list[str]
    stats: dict[str, Any]
    analysis_duration: float


class DependencyAnalyzer:
    """
    Unified dependency analyzer for all pipeline step types.

    This analyzer consolidates the functionality of both DependencyAnalyzer
    and UnifiedDependencyAnalyzer into a single, more maintainable implementation.

    Features:
    - Single analyzer for all step types (Bronze, Silver, Gold)
    - Multiple analysis strategies
    - Cycle detection and resolution
    - Execution group optimization
    - Performance analysis and recommendations
    """

    def __init__(
        self,
        strategy: AnalysisStrategy = AnalysisStrategy.HYBRID,
        logger: PipelineLogger | None = None,
    ):
        self.strategy = strategy
        self.logger = logger or PipelineLogger()
        self._analysis_cache: dict[str, DependencyAnalysisResult] = {}

    def analyze_dependencies(
        self,
        bronze_steps: dict[str, BronzeStep] | None = None,
        silver_steps: dict[str, SilverStep] | None = None,
        gold_steps: dict[str, GoldStep] | None = None,
        force_refresh: bool = False,
    ) -> DependencyAnalysisResult:
        """
        Analyze dependencies across all step types.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps
            force_refresh: Whether to force refresh of cached results

        Returns:
            DependencyAnalysisResult containing analysis results
        """
        import time

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
            # Step 1: Build dependency graph
            graph = self._build_dependency_graph(bronze_steps, silver_steps, gold_steps)

            # Step 2: Detect cycles
            cycles = graph.detect_cycles()
            if cycles:
                self.logger.warning(f"Detected {len(cycles)} circular dependencies")
                graph = self._resolve_cycles(graph, cycles)

            # Step 3: Detect conflicts
            conflicts = self._detect_conflicts(graph)
            if conflicts:
                self.logger.warning(f"Detected {len(conflicts)} dependency conflicts")

            # Step 4: Generate execution groups
            execution_groups = graph.get_execution_groups()

            # Step 5: Generate recommendations
            recommendations = self._generate_recommendations(graph, cycles, conflicts)

            # Step 6: Calculate statistics
            stats = graph.get_stats()

            # Create result
            result = DependencyAnalysisResult(
                graph=graph,
                execution_groups=execution_groups,
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
            raise DependencyError(f"Dependency analysis failed: {str(e)}")

    def _build_dependency_graph(
        self,
        bronze_steps: dict[str, BronzeStep] | None,
        silver_steps: dict[str, SilverStep] | None,
        gold_steps: dict[str, GoldStep] | None,
    ) -> DependencyGraph:
        """Build the dependency graph from all step types."""
        graph = DependencyGraph()

        # Add bronze steps
        if bronze_steps:
            for name, step in bronze_steps.items():
                node = StepNode(
                    name=name, step_type=StepType.BRONZE, metadata={"step": step}
                )
                graph.add_node(node)

        # Add silver steps
        if silver_steps:
            for name, step in silver_steps.items():
                node = StepNode(
                    name=name, step_type=StepType.SILVER, metadata={"step": step}
                )
                graph.add_node(node)

                # Add dependencies
                if hasattr(step, "source_bronze") and step.source_bronze:
                    # Check if the source bronze step exists
                    if step.source_bronze in graph.nodes:
                        graph.add_dependency(name, step.source_bronze)
                    else:
                        # Log warning about missing dependency
                        self.logger.warning(
                            f"Silver step {name} references non-existent bronze step {step.source_bronze}"
                        )

                if hasattr(step, "depends_on") and step.depends_on:
                    for dep in step.depends_on:
                        if dep in graph.nodes:
                            graph.add_dependency(name, dep)
                        else:
                            self.logger.warning(
                                f"Silver step {name} references non-existent dependency {dep}"
                            )

        # Add gold steps
        if gold_steps:
            for name, step in gold_steps.items():
                node = StepNode(
                    name=name, step_type=StepType.GOLD, metadata={"step": step}
                )
                graph.add_node(node)

                # Add dependencies
                if hasattr(step, "source_silvers") and step.source_silvers:
                    for dep in step.source_silvers:
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
        step_names = set(graph.nodes.keys())
        for node_name in step_names:
            if node_name in step_names and node_name != node_name:
                conflicts.append(f"Conflicting step name: {node_name}")

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
                "Consider reducing step dependencies for better parallelization"
            )

        if len(graph.nodes) > 10:
            recommendations.append(
                "Consider breaking large pipelines into smaller, focused pipelines"
            )

        return recommendations

    def _create_cache_key(
        self,
        bronze_steps: dict[str, BronzeStep] | None,
        silver_steps: dict[str, SilverStep] | None,
        gold_steps: dict[str, GoldStep] | None,
    ) -> str:
        """Create a cache key for the analysis."""
        import hashlib

        # Create a simple hash of the step configurations
        key_parts = []

        if bronze_steps:
            key_parts.extend(sorted(bronze_steps.keys()))
        if silver_steps:
            key_parts.extend(sorted(silver_steps.keys()))
        if gold_steps:
            key_parts.extend(sorted(gold_steps.keys()))

        key_string = f"{self.strategy.value}:{':'.join(key_parts)}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear the analysis cache."""
        self._analysis_cache.clear()
        self.logger.info("Dependency analysis cache cleared")
