"""
Base pipeline builder with common builder patterns.

This module provides a base BasePipelineBuilder class that can be used
by all pipeline builder implementations to reduce code duplication.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..errors import ValidationError
from ..logging import PipelineLogger
from ..models import PipelineConfig
from ..validation import PipelineValidator, StepValidator


class BasePipelineBuilder:
    """
    Base pipeline builder with common builder patterns.

    This class provides shared builder functionality that can be used
    by all pipeline builder implementations.
    """

    def __init__(
        self,
        config: PipelineConfig,
        logger: Optional[PipelineLogger] = None,
    ):
        """
        Initialize the base pipeline builder.

        Args:
            config: Pipeline configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or PipelineLogger()
        self.validator = PipelineValidator(self.logger)
        self.step_validator = StepValidator(self.logger)

        # Step storage - subclasses should initialize these
        self.bronze_steps: Dict[str, Any] = {}
        self.silver_steps: Dict[str, Any] = {}
        self.gold_steps: Dict[str, Any] = {}

    def _check_duplicate_step_name(self, name: str, step_type: str) -> None:
        """
        Check if step name already exists and raise error if duplicate.

        Args:
            name: Step name to check
            step_type: Type of step (bronze/silver/gold)

        Raises:
            ValidationError: If step name already exists
        """
        step_dict = getattr(self, f"{step_type}_steps", {})
        if name in step_dict:
            raise ValidationError(
                f"{step_type.capitalize()} step '{name}' already exists",
                context={"step_name": name, "step_type": step_type},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            )

    def _validate_step_dependencies(
        self,
        step: Any,
        step_type: str,
        available_sources: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Validate step dependencies exist.

        Args:
            step: Step object to validate
            step_type: Type of step (bronze/silver/gold)
            available_sources: Optional dictionary of available source steps

        Raises:
            ValidationError: If dependencies are invalid
        """
        if step_type == "silver":
            source_bronze = getattr(step, "source_bronze", None)
            if source_bronze:
                if available_sources is None:
                    available_sources = self.bronze_steps
                if source_bronze not in available_sources:
                    raise ValidationError(
                        f"Bronze step '{source_bronze}' not found",
                        context={
                            "step_name": getattr(step, "name", "unknown"),
                            "step_type": step_type,
                            "missing_dependency": source_bronze,
                        },
                        suggestions=[
                            f"Add bronze step '{source_bronze}' first",
                            f"Check spelling of '{source_bronze}'",
                        ],
                    )
        elif step_type == "gold":
            source_silvers = getattr(step, "source_silvers", None)
            if source_silvers:
                if not isinstance(source_silvers, list):
                    raise ValidationError(
                        "Gold step source_silvers must be a list",
                        context={
                            "step_name": getattr(step, "name", "unknown"),
                            "step_type": step_type,
                        },
                    )
                if available_sources is None:
                    available_sources = self.silver_steps
                for silver_name in source_silvers:
                    if silver_name not in available_sources:
                        raise ValidationError(
                            f"Silver step '{silver_name}' not found",
                            context={
                                "step_name": getattr(step, "name", "unknown"),
                                "step_type": step_type,
                                "missing_dependency": silver_name,
                            },
                            suggestions=[
                                f"Add silver step '{silver_name}' first",
                                f"Check spelling of '{silver_name}'",
                            ],
                        )

    def _validate_schema(self, schema: str) -> None:
        """
        Validate schema name format.

        Args:
            schema: Schema name to validate

        Raises:
            ValidationError: If schema is invalid
        """
        errors = self.validator.validate_schema(schema)
        if errors:
            raise ValidationError(
                errors[0],
                context={"schema": schema},
                suggestions=[
                    "Schema name must be a non-empty string",
                    "Schema name must be 128 characters or less",
                ],
            )

    def validate_pipeline(self) -> List[str]:
        """
        Validate entire pipeline configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        return self.validator.validate_pipeline(
            self.config, self.bronze_steps, self.silver_steps, self.gold_steps
        )
