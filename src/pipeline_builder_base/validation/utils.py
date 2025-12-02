"""
Utility functions for the framework validation.

This module provides utility functions for data analysis and validation operations.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero or None.

    Args:
        numerator: The numerator
        denominator: The denominator
        default: Default value to return if denominator is zero or None

    Returns:
        The division result or default value
    """
    if denominator is None or numerator is None or denominator == 0:
        return default
    return numerator / denominator


def validate_step_name(name: str) -> bool:
    """
    Validate step name format.

    Args:
        name: Step name to validate

    Returns:
        True if valid, False otherwise
    """
    if not name:
        return False
    if not isinstance(name, str):
        return False  # type: ignore[unreachable]
    if not name.strip():
        return False
    if len(name) > 128:  # Reasonable limit
        return False
    return True


def validate_schema_name(schema: str) -> bool:
    """
    Validate schema name format.

    Args:
        schema: Schema name to validate

    Returns:
        True if valid, False otherwise
    """
    if not schema:
        return False
    if not isinstance(schema, str):
        return False  # type: ignore[unreachable]
    if not schema.strip():
        return False
    if len(schema) > 128:  # Reasonable limit
        return False
    return True


def check_duplicate_names(items: List[Any], name_attr: str = "name") -> List[str]:
    """
    Check for duplicate names in a list of items.

    Args:
        items: List of items to check
        name_attr: Attribute name to use for getting the name

    Returns:
        List of duplicate names found
    """
    seen: Dict[str, int] = {}
    duplicates: List[str] = []

    for item in items:
        name = getattr(item, name_attr, None)
        if name:
            if name in seen:
                seen[name] += 1
                if name not in duplicates:
                    duplicates.append(name)
            else:
                seen[name] = 1

    return duplicates


def validate_dependency_chain(steps: Dict[str, Any]) -> List[str]:
    """
    Validate dependency chain and detect circular dependencies.

    Args:
        steps: Dictionary of steps with their dependencies

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Build dependency graph
    dependencies: Dict[str, List[str]] = {}
    for step_name, step in steps.items():
        deps = []
        if hasattr(step, "source_bronze") and step.source_bronze:
            deps.append(step.source_bronze)
        if hasattr(step, "source_silvers") and step.source_silvers:
            if isinstance(step.source_silvers, list):
                deps.extend(step.source_silvers)
        if deps:
            dependencies[step_name] = deps

    # Check for circular dependencies using DFS
    visited: Set[str] = set()
    rec_stack: Set[str] = set()

    def has_cycle(node: str) -> bool:
        """Check if there's a cycle starting from node."""
        visited.add(node)
        rec_stack.add(node)

        for neighbor in dependencies.get(node, []):
            if neighbor not in visited:
                if has_cycle(neighbor):
                    return True
            elif neighbor in rec_stack:
                # Found a back edge, cycle exists
                return True

        rec_stack.remove(node)
        return False

    # Check all nodes for cycles
    all_nodes = set(dependencies.keys())
    for node in all_nodes:
        if node not in visited:
            if has_cycle(node):
                errors.append(f"Circular dependency detected involving step '{node}'")

    return errors
