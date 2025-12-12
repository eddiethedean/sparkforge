"""
Base step validator with common step validation patterns.

This module provides a base StepValidator class that can be used
by all pipeline builder implementations to validate individual steps.
"""

from __future__ import annotations

from typing import Any, List, Optional

from ..logging import PipelineLogger


class StepValidator:
    """
    Base step validator with common step validation patterns.

    This class provides shared validation patterns that can be used
    by all pipeline builder implementations.
    """

    def __init__(self, logger: Optional[PipelineLogger] = None):
        """
        Initialize the step validator.

        Args:
            logger: Optional logger instance for validation messages
        """
        self.logger = logger or PipelineLogger()

    def validate_step_name(self, name: Any, step_type: str = "step") -> List[str]:
        """
        Validate step name format.

        Args:
            name: Step name to validate (can be any type, will be validated)
            step_type: Type of step (bronze/silver/gold) for error messages

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        if not isinstance(name, str):
            errors.append(f"{step_type.capitalize()} step name must be a string")
        elif not name:
            errors.append(f"{step_type.capitalize()} step name cannot be empty")
        elif not name.strip():
            errors.append(
                f"{step_type.capitalize()} step name cannot be whitespace only"
            )
        elif len(name) > 128:  # Reasonable limit
            errors.append(
                f"{step_type.capitalize()} step name is too long (max 128 characters)"
            )

        return errors

    def validate_step_rules(self, step: Any, step_type: str = "step") -> List[str]:
        """
        Validate that step has rules.

        Args:
            step: Step object to validate
            step_type: Type of step (bronze/silver/gold) for error messages

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        if not hasattr(step, "rules"):
            errors.append(f"{step_type.capitalize()} step missing 'rules' attribute")
        elif not step.rules:
            step_name = getattr(step, "name", "unknown")
            errors.append(
                f"{step_type.capitalize()} step '{step_name}' has empty validation rules"
            )
        elif not isinstance(step.rules, dict):
            step_name = getattr(step, "name", "unknown")
            errors.append(
                f"{step_type.capitalize()} step '{step_name}' rules must be a dictionary"
            )

        return errors

    def classify_step_type(self, step: Any) -> str:
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

    def validate_step_dependencies(
        self, step: Any, available_sources: List[str], step_type: str = "step"
    ) -> List[str]:
        """
        Validate step dependencies exist.

        Args:
            step: Step object to validate
            available_sources: List of available source step names
            step_type: Type of step (bronze/silver/gold) for error messages

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []
        step_name = getattr(step, "name", "unknown")

        if step_type == "silver":
            # Silver steps depend on bronze steps
            source_bronze = getattr(step, "source_bronze", None)
            if source_bronze and source_bronze not in available_sources:
                errors.append(
                    f"Silver step '{step_name}' references unknown bronze source '{source_bronze}'"
                )
        elif step_type == "gold":
            # Gold steps depend on silver steps
            source_silvers = getattr(step, "source_silvers", None)
            if source_silvers:
                if not isinstance(source_silvers, list):
                    errors.append(
                        f"Gold step '{step_name}' source_silvers must be a list"
                    )
                else:
                    for silver_name in source_silvers:
                        if silver_name not in available_sources:
                            errors.append(
                                f"Gold step '{step_name}' references unknown silver source '{silver_name}'"
                            )

        return errors

    def validate_step(
        self, step: Any, available_sources: Optional[List[str]] = None
    ) -> List[str]:
        """
        Validate a single step.

        Args:
            step: Step object to validate
            available_sources: Optional list of available source step names for dependency validation

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        # Classify step type
        step_type = self.classify_step_type(step)

        # Validate step name
        step_name = getattr(step, "name", None)
        if step_name:
            name_errors = self.validate_step_name(step_name, step_type)
            errors.extend(name_errors)
        else:
            errors.append(f"{step_type.capitalize()} step missing 'name' attribute")

        # Validate step rules
        rules_errors = self.validate_step_rules(step, step_type)
        errors.extend(rules_errors)

        # Validate dependencies if sources provided
        if available_sources is not None:
            dep_errors = self.validate_step_dependencies(
                step, available_sources, step_type
            )
            errors.extend(dep_errors)

        return errors
