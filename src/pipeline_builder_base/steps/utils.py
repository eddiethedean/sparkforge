"""
Step utility functions.

This module provides utility functions for working with pipeline steps.
"""

from __future__ import annotations

from typing import Any, List


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


def get_step_target(step: Any) -> str:
    """
    Get target table name from a step.

    Args:
        step: Step object

    Returns:
        Target table name, or empty string if not found
    """
    # Check for table_name attribute
    table_name = getattr(step, "table_name", None)
    if table_name:
        return str(table_name)

    # Check for target attribute
    target = getattr(step, "target", None)
    if target:
        return str(target)

    return ""


def normalize_step_name(name: str) -> str:
    """
    Normalize step name (trim whitespace, convert to lowercase).

    Args:
        name: Step name to normalize

    Returns:
        Normalized step name
    """
    if not name:
        return ""
    return name.strip().lower()
