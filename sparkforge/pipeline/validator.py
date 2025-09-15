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
Pipeline validation system for SparkForge.

This module provides comprehensive validation for pipeline configurations,
step definitions, and execution contexts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from ..logger import PipelineLogger
from ..models import BronzeStep, ExecutionContext, GoldStep, PipelineConfig, SilverStep
from .models import StepExecutionContext


class StepValidator(Protocol):
    """Protocol for custom step validators."""

    def validate(self, step: Any, context: ExecutionContext) -> list[str]:
        """Validate a step and return any validation errors."""
        ...


@dataclass
class ValidationResult:
    """Result of pipeline validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    recommendations: list[str]

    def __bool__(self) -> bool:
        """Return whether validation passed."""
        return self.is_valid


class PipelineValidator:
    """
    Comprehensive pipeline validation system.

    This class provides validation for pipeline configurations, step definitions,
    and execution contexts, ensuring data quality and preventing runtime errors.

    Features:
    - Pipeline configuration validation
    - Step definition validation
    - Dependency validation
    - Data quality threshold validation
    - Custom validator support
    """

    def __init__(self, logger: PipelineLogger | None = None):
        self.logger = logger or PipelineLogger()
        self.custom_validators: list[StepValidator] = []

    def add_validator(self, validator: StepValidator) -> None:
        """Add a custom step validator."""
        self.custom_validators.append(validator)
        self.logger.info(f"Added custom validator: {validator.__class__.__name__}")

    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: dict[str, BronzeStep],
        silver_steps: dict[str, SilverStep],
        gold_steps: dict[str, GoldStep],
    ) -> ValidationResult:
        """
        Validate the entire pipeline configuration.

        Args:
            config: Pipeline configuration
            bronze_steps: Bronze step definitions
            silver_steps: Silver step definitions
            gold_steps: Gold step definitions

        Returns:
            ValidationResult containing validation status and issues
        """
        errors: list[str] = []
        warnings: list[str] = []
        recommendations: list[str] = []

        # Validate configuration
        config_errors = self._validate_config(config)
        errors.extend(config_errors)

        # Validate bronze steps
        bronze_errors, bronze_warnings = self._validate_bronze_steps(bronze_steps)
        errors.extend(bronze_errors)
        warnings.extend(bronze_warnings)

        # Validate silver steps
        silver_errors, silver_warnings = self._validate_silver_steps(
            silver_steps, bronze_steps
        )
        errors.extend(silver_errors)
        warnings.extend(silver_warnings)

        # Validate gold steps
        gold_errors, gold_warnings = self._validate_gold_steps(gold_steps, silver_steps)
        errors.extend(gold_errors)
        warnings.extend(gold_warnings)

        # Validate dependencies
        dep_errors, dep_warnings = self._validate_dependencies(
            bronze_steps, silver_steps, gold_steps
        )
        errors.extend(dep_errors)
        warnings.extend(dep_warnings)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            config, bronze_steps, silver_steps, gold_steps
        )

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
                    f"Validator {validator.__class__.__name__} failed: {str(e)}"
                )

        # Basic step validation
        if hasattr(step, "name") and not step.name:
            errors.append(f"{step_type} step must have a name")

        if hasattr(step, "rules") and not step.rules:
            warnings.append(f"{step_type} step has no validation rules")

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
            errors.append("Schema name cannot be empty")

        # Validate thresholds
        if config.thresholds.bronze < 0 or config.thresholds.bronze > 100:
            errors.append(
                f"bronze threshold must be between 0 and 100, got {config.thresholds.bronze}"
            )

        if config.thresholds.silver < 0 or config.thresholds.silver > 100:
            errors.append(
                f"silver threshold must be between 0 and 100, got {config.thresholds.silver}"
            )

        if config.thresholds.gold < 0 or config.thresholds.gold > 100:
            errors.append(
                f"gold threshold must be between 0 and 100, got {config.thresholds.gold}"
            )

        # Validate parallel config
        if config.parallel.max_workers < 1:
            errors.append(
                f"max_workers must be at least 1, got {config.parallel.max_workers}"
            )

        return errors

    def _validate_bronze_steps(
        self, bronze_steps: dict[str, BronzeStep]
    ) -> tuple[list[str], list[str]]:
        """Validate bronze steps."""
        errors = []
        warnings = []

        if not bronze_steps:
            warnings.append("No bronze steps defined")
            return errors, warnings

        for name, step in bronze_steps.items():
            # Use the validate_step method which includes custom validators
            from ..models import ExecutionMode

            context = StepExecutionContext(
                step_name=name,
                step_type="bronze",
                mode=ExecutionMode.INITIAL,
                start_time=datetime.now(),
            )
            validation_result = self.validate_step(step, "bronze", context)
            errors.extend(validation_result.errors)
            warnings.extend(validation_result.warnings)

        return errors, warnings

    def _validate_silver_steps(
        self, silver_steps: dict[str, SilverStep], bronze_steps: dict[str, BronzeStep]
    ) -> tuple[list[str], list[str]]:
        """Validate silver steps."""
        errors = []
        warnings = []

        for name, step in silver_steps.items():
            if not step.name:
                errors.append(f"Silver step {name} must have a name")

            if not step.rules:
                warnings.append(f"Silver step {name} has no validation rules")

            if hasattr(step, "source_bronze") and step.source_bronze:
                if step.source_bronze not in bronze_steps:
                    errors.append(
                        f"Silver step {name} source bronze '{step.source_bronze}' not found"
                    )

        return errors, warnings

    def _validate_gold_steps(
        self, gold_steps: dict[str, GoldStep], silver_steps: dict[str, SilverStep]
    ) -> tuple[list[str], list[str]]:
        """Validate gold steps."""
        errors = []
        warnings = []

        for name, step in gold_steps.items():
            if not step.name:
                errors.append(f"Gold step {name} must have a name")

            if not step.rules:
                warnings.append(f"Gold step {name} has no validation rules")

            if hasattr(step, "source_silvers") and step.source_silvers:
                for silver_name in step.source_silvers:
                    if silver_name not in silver_steps:
                        errors.append(
                            f"Gold step {name} references non-existent silver step: {silver_name}"
                        )

        return errors, warnings

    def _validate_dependencies(
        self,
        bronze_steps: dict[str, BronzeStep],
        silver_steps: dict[str, SilverStep],
        gold_steps: dict[str, GoldStep],
    ) -> tuple[list[str], list[str]]:
        """Validate step dependencies."""
        errors = []
        warnings = []

        # Check for circular dependencies
        # This is a simplified check - in practice, you'd use a proper graph algorithm
        all_steps = {**bronze_steps, **silver_steps, **gold_steps}

        # Check for missing dependencies
        for step_name, step in all_steps.items():
            if hasattr(step, "depends_on") and step.depends_on:
                for dep in step.depends_on:
                    if dep not in all_steps:
                        errors.append(
                            f"Step {step_name} depends on non-existent step: {dep}"
                        )

        return errors, warnings

    def _generate_recommendations(
        self,
        config: PipelineConfig,
        bronze_steps: dict[str, BronzeStep],
        silver_steps: dict[str, SilverStep],
        gold_steps: dict[str, GoldStep],
    ) -> list[str]:
        """Generate optimization recommendations."""
        recommendations = []

        # Performance recommendations
        if len(silver_steps) > 5 and not config.parallel.enabled:
            recommendations.append(
                "Consider enabling parallel silver execution for better performance"
            )

        if config.parallel.max_workers < 4 and len(silver_steps) > 3:
            recommendations.append(
                "Consider increasing max_workers for better parallelization"
            )

        # Data quality recommendations
        if config.thresholds.bronze < 90:
            recommendations.append("Consider increasing bronze data quality threshold")

        if config.thresholds.silver < 95:
            recommendations.append("Consider increasing silver data quality threshold")

        # Architecture recommendations
        if len(bronze_steps) == 0:
            recommendations.append("Consider adding bronze steps for data validation")

        if len(gold_steps) == 0:
            recommendations.append("Consider adding gold steps for business analytics")

        return recommendations
