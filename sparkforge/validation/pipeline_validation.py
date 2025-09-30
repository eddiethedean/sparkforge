"""
Pipeline validation functions for SparkForge.

This module provides functions and classes for validating pipeline configurations,
step dependencies, and overall pipeline structure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..logging import PipelineLogger
from ..models import BronzeStep, ExecutionContext, GoldStep, PipelineConfig, SilverStep

# Type alias for step names
StepName = str


class StepValidator:
    """Protocol for custom step validators."""

    def validate(self, step: Any, context: ExecutionContext) -> list[str]:
        """Validate a step and return any validation errors."""
        return []


@dataclass
class ValidationResult:
    """Result of validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    recommendations: list[str]

    def __bool__(self) -> bool:
        """Return whether validation passed."""
        return self.is_valid


class UnifiedValidator:
    """
    Unified validation system for both data and pipeline validation.

    This class provides a single interface for all validation needs,
    combining data validation and pipeline validation functionality.
    """

    def __init__(self, logger: PipelineLogger | None = None):
        """Initialize the unified validator."""
        if logger is None:
            self.logger = PipelineLogger()
        else:
            self.logger = logger
        self.custom_validators: list[StepValidator] = []

    def add_validator(self, validator: StepValidator) -> None:
        """Add a custom step validator."""
        self.custom_validators.append(validator)
        self.logger.info(f"Added custom validator: {validator.__class__.__name__}")

    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: dict[StepName, BronzeStep],
        silver_steps: dict[StepName, SilverStep],
        gold_steps: dict[StepName, GoldStep],
    ) -> ValidationResult:
        """Validate the entire pipeline configuration."""
        errors: list[str] = []
        warnings: list[str] = []
        recommendations: list[str] = []

        # Validate configuration
        config_errors = self._validate_config(config)
        errors.extend(config_errors)

        # Validate steps
        bronze_errors, bronze_warnings = self._validate_bronze_steps(bronze_steps)
        errors.extend(bronze_errors)
        warnings.extend(bronze_warnings)

        silver_errors, silver_warnings = self._validate_silver_steps(
            silver_steps, bronze_steps
        )
        errors.extend(silver_errors)
        warnings.extend(silver_warnings)

        gold_errors, gold_warnings = self._validate_gold_steps(gold_steps, silver_steps)
        errors.extend(gold_errors)
        warnings.extend(gold_warnings)

        # Validate dependencies
        dep_errors, dep_warnings = self._validate_dependencies(
            bronze_steps, silver_steps, gold_steps
        )
        errors.extend(dep_errors)
        warnings.extend(dep_warnings)

        is_valid = len(errors) == 0

        if is_valid:
            self.logger.info("Pipeline validation passed")
        else:
            self.logger.error(f"Pipeline validation failed with {len(errors)} errors")

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations,
        )

    def validate_step(
        self, step: Any, step_type: str, context: ExecutionContext
    ) -> ValidationResult:
        """Validate a single step."""
        errors: list[str] = []
        warnings: list[str] = []

        # Run custom validators
        for validator in self.custom_validators:
            try:
                validator_errors = validator.validate(step, context)
                errors.extend(validator_errors)
            except Exception as e:
                errors.append(
                    f"Custom validator {validator.__class__.__name__} failed: {e}"
                )

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            recommendations=[],
        )

    def _validate_config(self, config: PipelineConfig) -> list[str]:
        """Validate pipeline configuration."""
        errors = []

        if not config.schema:
            errors.append("Pipeline schema is required")

        # Table prefix is optional in simplified config
        # if not config.table_prefix:
        #     errors.append("Table prefix is required")

        return errors

    def _validate_bronze_steps(
        self, bronze_steps: dict[StepName, BronzeStep]
    ) -> tuple[list[str], list[str]]:
        """Validate bronze steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in bronze_steps.items():
            # Simplified validation - just check that step has required basic attributes
            if not step.name:
                errors.append(f"Bronze step {step_name} missing name")

            if not step.rules:
                errors.append(f"Bronze step {step_name} missing validation rules")

        return errors, warnings

    def _validate_silver_steps(
        self,
        silver_steps: dict[StepName, SilverStep],
        bronze_steps: dict[StepName, BronzeStep],
    ) -> tuple[list[str], list[str]]:
        """Validate silver steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in silver_steps.items():
            if not step.source_bronze:
                errors.append(f"Silver step {step_name} missing source_bronze")

            # Check source_bronze exists
            if step.source_bronze not in bronze_steps:
                errors.append(
                    f"Silver step {step_name} depends on non-existent bronze step {step.source_bronze}"
                )

        return errors, warnings

    def _validate_gold_steps(
        self,
        gold_steps: dict[StepName, GoldStep],
        silver_steps: dict[StepName, SilverStep],
    ) -> tuple[list[str], list[str]]:
        """Validate gold steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in gold_steps.items():
            # Check source_silvers exist (if specified)
            if step.source_silvers:
                for silver_name in step.source_silvers:
                    if silver_name not in silver_steps:
                        errors.append(
                            f"Gold step {step_name} depends on non-existent silver step {silver_name}"
                        )

        return errors, warnings

    def _validate_dependencies(
        self,
        bronze_steps: dict[StepName, BronzeStep],
        silver_steps: dict[StepName, SilverStep],
        gold_steps: dict[StepName, GoldStep],
    ) -> tuple[list[str], list[str]]:
        """Validate step dependencies."""
        errors = []
        warnings: list[str] = []

        # Check for circular dependencies
        all_steps = {**bronze_steps, **silver_steps, **gold_steps}

        for step_name, step in all_steps.items():
            # Check for circular dependencies in non-standard dependencies attribute
            # This is only for custom step types that might have a dependencies field
            if hasattr(step, "dependencies") and step.dependencies:
                for dep in step.dependencies:
                    if hasattr(dep, "step_name") and dep.step_name == step_name:
                        errors.append(
                            f"Step {step_name} has circular dependency on itself"
                        )

        return errors, warnings
