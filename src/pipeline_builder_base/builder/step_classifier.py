"""
Step classifier utility for identifying and grouping steps.

This module provides utilities for classifying steps by type,
extracting dependencies, and building dependency graphs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

from ..dependencies import DependencyGraph


class StepClassifier:
    """
    Utility class for classifying and analyzing pipeline steps.
    """

    @staticmethod
    def classify_step_type(step: Any) -> str:
        """
        Classify step type from step object.

        Args:
            step: Step object to classify

        Returns:
            Step type: 'bronze', 'silver', 'gold', or 'unknown'
        """
        # Check if step has type attribute
        if hasattr(step, "type") and step.type:
            step_type = str(step.type).lower()
            if step_type in ("bronze", "silver", "gold"):
                return step_type

        # Determine type from class name
        class_name = step.__class__.__name__
        if "Bronze" in class_name:
            return "bronze"
        elif "Silver" in class_name:
            return "silver"
        elif "Gold" in class_name:
            return "gold"

        return "unknown"

    @staticmethod
    def extract_step_dependencies(step: Any) -> List[str]:
        """
        Extract dependencies from a step.

        Args:
            step: Step object to analyze

        Returns:
            List of dependency step names
        """
        dependencies: List[str] = []

        # Check for source_bronze (silver steps)
        source_bronze = getattr(step, "source_bronze", None)
        if source_bronze:
            dependencies.append(source_bronze)

        # Check for source_silvers (gold steps)
        source_silvers = getattr(step, "source_silvers", None)
        if source_silvers:
            if isinstance(source_silvers, list):
                dependencies.extend(source_silvers)
            elif isinstance(source_silvers, str):
                dependencies.append(source_silvers)

        # Check for source attribute (backward compatibility)
        source = getattr(step, "source", None)
        if source and source not in dependencies:
            if isinstance(source, str):
                dependencies.append(source)
            elif isinstance(source, list):
                dependencies.extend(source)

        return dependencies

    @staticmethod
    def group_steps_by_type(
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Group steps by type into a single dictionary.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            Dictionary mapping step types to step dictionaries
        """
        return {
            "bronze": bronze_steps,
            "silver": silver_steps,
            "gold": gold_steps,
        }

    @staticmethod
    def get_all_step_names(
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> Set[str]:
        """
        Get all step names from all step types.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            Set of all step names
        """
        all_names: Set[str] = set()
        all_names.update(bronze_steps.keys())
        all_names.update(silver_steps.keys())
        all_names.update(gold_steps.keys())
        return all_names

    @staticmethod
    def build_dependency_graph(
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> DependencyGraph:
        """
        Build a dependency graph from pipeline steps.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            DependencyGraph instance
        """
        from ..dependencies.graph import StepNode, StepType

        graph = DependencyGraph()

        # Add all steps as nodes
        for step_name in StepClassifier.get_all_step_names(
            bronze_steps, silver_steps, gold_steps
        ):
            # Determine step type
            if step_name in bronze_steps:
                step_type = StepType.BRONZE
            elif step_name in silver_steps:
                step_type = StepType.SILVER
            elif step_name in gold_steps:
                step_type = StepType.GOLD
            else:
                step_type = StepType.BRONZE  # Default

            node = StepNode(name=step_name, step_type=step_type)
            graph.add_node(node)

        # Add dependencies for silver steps (depend on bronze)
        for step_name, step in silver_steps.items():
            source_bronze = getattr(step, "source_bronze", None)
            if source_bronze and source_bronze in graph.nodes:
                # add_dependency(from_step, to_step) means from_step depends on to_step
                # So silver depends on bronze: add_dependency(silver, bronze)
                graph.add_dependency(step_name, source_bronze)

        # Add dependencies for gold steps (depend on silver)
        for step_name, step in gold_steps.items():
            source_silvers = getattr(step, "source_silvers", None)
            if source_silvers:
                if isinstance(source_silvers, list):
                    for silver_name in source_silvers:
                        if silver_name in graph.nodes:
                            graph.add_dependency(step_name, silver_name)
                elif isinstance(source_silvers, str):
                    if source_silvers in graph.nodes:
                        graph.add_dependency(step_name, source_silvers)

        return graph

    @staticmethod
    def get_execution_order(
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Get execution order for all steps based on dependencies.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            List of step names in execution order
        """
        graph = StepClassifier.build_dependency_graph(
            bronze_steps, silver_steps, gold_steps
        )
        return graph.topological_sort()
