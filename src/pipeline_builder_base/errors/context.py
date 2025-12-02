"""
Error context builders and suggestion generators.

This module provides utilities for building structured error context
and generating helpful error suggestions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:
    from ..errors import ErrorContext as ErrorContextType
else:
    # Type alias for runtime
    ErrorContextType = Dict[
        str, Union[str, int, float, bool, List[str], Dict[str, str], None]
    ]


class ErrorContext:
    """
    Structured error context for better error messages.
    """

    def __init__(self, **kwargs: Any):
        """
        Initialize error context with key-value pairs.

        Args:
            **kwargs: Context key-value pairs
        """
        self.context: ErrorContextType = dict(kwargs)

    def add(self, key: str, value: Any) -> None:
        """
        Add a context value.

        Args:
            key: Context key
            value: Context value
        """
        self.context[key] = value  # type: ignore[index]

    def to_dict(self) -> ErrorContextType:
        """
        Convert context to dictionary.

        Returns:
            Dictionary representation of context
        """
        from typing import cast
        return cast(ErrorContextType, self.context.copy())

    def __repr__(self) -> str:
        """Return string representation."""
        return f"ErrorContext({self.context})"


class SuggestionGenerator:
    """
    Generator for helpful error suggestions.
    """

    @staticmethod
    def suggest_fix_for_missing_dependency(
        step_name: str, missing: str, step_type: str = "step"
    ) -> List[str]:
        """
        Generate suggestions for missing dependency.

        Args:
            step_name: Name of the step with missing dependency
            missing: Name of the missing dependency
            step_type: Type of step (bronze/silver/gold)

        Returns:
            List of suggestion strings
        """
        suggestions = [
            f"Add {step_type} step '{missing}' before '{step_name}'",
            f"Check spelling of '{missing}'",
            f"Ensure '{missing}' is defined in the pipeline",
        ]

        if step_type == "silver":
            suggestions.append(
                f"Bronze step '{missing}' must be added with with_bronze_rules()"
            )
        elif step_type == "gold":
            suggestions.append(
                f"Silver step '{missing}' must be added with add_silver_transform()"
            )

        return suggestions

    @staticmethod
    def suggest_fix_for_duplicate_name(name: str, step_type: str = "step") -> List[str]:
        """
        Generate suggestions for duplicate step name.

        Args:
            name: Duplicate step name
            step_type: Type of step (bronze/silver/gold)

        Returns:
            List of suggestion strings
        """
        return [
            f"Use a different name for this {step_type} step",
            f"Remove the existing {step_type} step '{name}' first",
            f"Check if '{name}' was already added to the pipeline",
        ]

    @staticmethod
    def suggest_fix_for_invalid_schema(schema: str) -> List[str]:
        """
        Generate suggestions for invalid schema name.

        Args:
            schema: Invalid schema name

        Returns:
            List of suggestion strings
        """
        return [
            "Schema name must be a non-empty string",
            "Schema name must be 128 characters or less",
            "Schema name cannot contain only whitespace",
            f"Check the schema name: '{schema}'",
        ]

    @staticmethod
    def suggest_fix_for_missing_rules(
        step_name: str, step_type: str = "step"
    ) -> List[str]:
        """
        Generate suggestions for missing validation rules.

        Args:
            step_name: Name of step missing rules
            step_type: Type of step (bronze/silver/gold)

        Returns:
            List of suggestion strings
        """
        return [
            f"{step_type.capitalize()} step '{step_name}' requires validation rules",
            "Add rules dictionary with column validation rules",
            "Rules cannot be empty",
        ]


def build_validation_context(step: Any, step_type: str) -> ErrorContextType:
    """
    Build error context for validation errors.

    Args:
        step: Step object
        step_type: Type of step (bronze/silver/gold)

    Returns:
        Error context dictionary
    """
    context: ErrorContextType = {
        "step_type": step_type,
    }

    step_name = getattr(step, "name", None)
    if step_name:
        context["step_name"] = step_name  # type: ignore[index]

    # Add step-specific context
    if step_type == "silver":
        source_bronze = getattr(step, "source_bronze", None)
        if source_bronze:
            context["source_bronze"] = source_bronze  # type: ignore[index]
    elif step_type == "gold":
        source_silvers = getattr(step, "source_silvers", None)
        if source_silvers:
            context["source_silvers"] = (  # type: ignore[index]
                source_silvers if isinstance(source_silvers, list) else [source_silvers]
            )

    return context


def build_execution_context(step: Any, error: Exception) -> ErrorContextType:
    """
    Build error context for execution errors.

    Args:
        step: Step object that failed
        error: Exception that occurred

    Returns:
        Error context dictionary
    """
    context: ErrorContextType = {
        "error_type": error.__class__.__name__,
        "error_message": str(error),
    }

    step_name = getattr(step, "name", None)
    if step_name:
        context["step_name"] = step_name  # type: ignore[index]

    step_type = getattr(step, "type", None)
    if step_type:
        context["step_type"] = step_type  # type: ignore[index]

    return context
