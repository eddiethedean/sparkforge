"""
Protocol definitions for validator interfaces.

This module defines Protocol classes that specify the expected interfaces
for validators, ensuring consistent return types and method signatures
across different validator implementations.
"""

from __future__ import annotations

from typing import Any, Dict, List, Protocol

from ..models import PipelineConfig


class PipelineValidatorProtocol(Protocol):
    """
    Protocol defining the interface for pipeline validators.

    This protocol ensures that all pipeline validators have consistent
    method signatures and return types, preventing type mismatch bugs.

    Note: Different implementations may return different types:
    - PipelineValidator returns List[str]
    - UnifiedValidator returns ValidationResult
    """

    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> List[str] | Any:
        """
        Validate entire pipeline configuration.

        Args:
            config: Pipeline configuration
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            List[str] or ValidationResult depending on implementation
        """
        ...

    def validate_schema(self, schema: str) -> List[str]:
        """
        Validate schema name.

        Args:
            schema: Schema name to validate

        Returns:
            List of validation errors (empty if valid)
        """
        ...


class ValidationResultProtocol(Protocol):
    """
    Protocol defining the interface for validation results.

    This protocol ensures that ValidationResult objects have consistent
    attributes that can be safely accessed.
    """

    @property
    def errors(self) -> List[str]:
        """List of validation error messages."""
        ...

    @property
    def warnings(self) -> List[str]:
        """List of validation warnings."""
        ...

    @property
    def is_valid(self) -> bool:
        """Whether validation passed."""
        ...

