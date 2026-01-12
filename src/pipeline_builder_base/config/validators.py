"""
Configuration validation functions.

This module provides validation functions for pipeline configurations.
"""

from __future__ import annotations

from typing import List

from ..models import PipelineConfig, ValidationThresholds


def validate_pipeline_config(config: PipelineConfig) -> List[str]:
    """
    Validate pipeline configuration.

    Args:
        config: Pipeline configuration to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Validate schema
    # Note: config.schema is typed as str in PipelineConfig, but we validate at runtime
    # The isinstance check is for runtime validation, even though mypy knows it's a str
    if not isinstance(config.schema, str):  # type: ignore[unreachable]
        errors.append("Pipeline schema must be a string")
        return errors  # Early return to avoid unreachable code
    # After isinstance check, mypy knows it's a str
    # The empty check is for runtime validation (empty strings are valid str types)
    if not config.schema.strip():  # type: ignore[unreachable]
        errors.append("Pipeline schema cannot be empty")
    if len(config.schema) > 128:
        errors.append("Pipeline schema name is too long (max 128 characters)")

    # Validate thresholds
    threshold_errors = validate_thresholds(config.thresholds)
    errors.extend(threshold_errors)


    return errors


def validate_thresholds(thresholds: ValidationThresholds) -> List[str]:
    """
    Validate validation thresholds.

    Args:
        thresholds: Validation thresholds to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Check threshold ranges (0-100)
    if not (0.0 <= thresholds.bronze <= 100.0):
        errors.append(
            f"Bronze threshold must be between 0 and 100, got {thresholds.bronze}"
        )

    if not (0.0 <= thresholds.silver <= 100.0):
        errors.append(
            f"Silver threshold must be between 0 and 100, got {thresholds.silver}"
        )

    if not (0.0 <= thresholds.gold <= 100.0):
        errors.append(
            f"Gold threshold must be between 0 and 100, got {thresholds.gold}"
        )

    # Check that thresholds are in ascending order (bronze <= silver <= gold)
    if thresholds.bronze > thresholds.silver:
        errors.append(
            f"Bronze threshold ({thresholds.bronze}) should not exceed silver threshold ({thresholds.silver})"
        )

    if thresholds.silver > thresholds.gold:
        errors.append(
            f"Silver threshold ({thresholds.silver}) should not exceed gold threshold ({thresholds.gold})"
        )

    return errors


