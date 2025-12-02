"""
Step manager for managing step collections.

This module provides a StepManager class for managing pipeline step collections
with validation and query capabilities.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..errors import ValidationError
from ..validation import StepValidator


class StepManager:
    """
    Manager for pipeline step collections.

    This class provides methods for managing step collections with
    validation and query capabilities.
    """

    def __init__(self) -> None:
        """Initialize the step manager."""
        self.bronze_steps: Dict[str, Any] = {}
        self.silver_steps: Dict[str, Any] = {}
        self.gold_steps: Dict[str, Any] = {}
        self.validator = StepValidator()

    def add_step(self, step: Any, step_type: str) -> None:
        """
        Add a step to the appropriate collection.

        Args:
            step: Step object to add
            step_type: Type of step (bronze/silver/gold)

        Raises:
            ValidationError: If step is invalid or name already exists
        """
        step_name = getattr(step, "name", None)
        if not step_name:
            raise ValidationError(
                f"{step_type.capitalize()} step missing 'name' attribute"
            )

        # Check for duplicate name
        step_dict = getattr(self, f"{step_type}_steps", {})
        if step_name in step_dict:
            raise ValidationError(
                f"{step_type.capitalize()} step '{step_name}' already exists"
            )

        # Validate step
        errors = self.validator.validate_step(step)
        if errors:
            raise ValidationError(
                f"Invalid {step_type} step '{step_name}': {errors[0]}",
                context={"step_name": step_name, "step_type": step_type},
            )

        # Add step
        step_dict[step_name] = step

    def get_step(self, name: str, step_type: Optional[str] = None) -> Optional[Any]:
        """
        Get a step by name and optional type.

        Args:
            name: Step name
            step_type: Optional step type (bronze/silver/gold). If None, searches all types.

        Returns:
            Step object if found, None otherwise
        """
        if step_type:
            step_dict = getattr(self, f"{step_type}_steps", {})
            return step_dict.get(name)
        else:
            # Search all step types
            for step_type in ["bronze", "silver", "gold"]:
                step_dict = getattr(self, f"{step_type}_steps", {})
                if name in step_dict:
                    return step_dict[name]
        return None

    def get_all_steps(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all steps grouped by type.

        Returns:
            Dictionary mapping step types to step dictionaries
        """
        return {
            "bronze": self.bronze_steps,
            "silver": self.silver_steps,
            "gold": self.gold_steps,
        }

    def get_steps_by_type(self, step_type: str) -> Dict[str, Any]:
        """
        Get all steps of a specific type.

        Args:
            step_type: Type of steps to get (bronze/silver/gold)

        Returns:
            Dictionary of steps of the specified type
        """
        step_dict = getattr(self, f"{step_type}_steps", {})
        return step_dict.copy()

    def validate_all_steps(self) -> List[str]:
        """
        Validate all steps in the manager.

        Returns:
            List of validation errors (empty if all valid)
        """
        errors: List[str] = []

        # Validate bronze steps
        for step_name, step in self.bronze_steps.items():
            step_errors = self.validator.validate_step(step)
            errors.extend([f"Bronze step '{step_name}': {e}" for e in step_errors])

        # Validate silver steps
        bronze_names = list(self.bronze_steps.keys())
        for step_name, step in self.silver_steps.items():
            step_errors = self.validator.validate_step(
                step, available_sources=bronze_names
            )
            errors.extend([f"Silver step '{step_name}': {e}" for e in step_errors])

        # Validate gold steps
        silver_names = list(self.silver_steps.keys())
        for step_name, step in self.gold_steps.items():
            step_errors = self.validator.validate_step(
                step, available_sources=silver_names
            )
            errors.extend([f"Gold step '{step_name}': {e}" for e in step_errors])

        return errors
