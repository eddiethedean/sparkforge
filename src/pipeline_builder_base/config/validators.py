"""
Configuration validation functions.

This module provides validation functions for pipeline configurations.
"""

from __future__ import annotations

from typing import List

from ..models import ParallelConfig, PipelineConfig, ValidationThresholds


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
    if not config.schema:
        errors.append("Pipeline schema cannot be empty")
    elif not isinstance(config.schema, str):
        errors.append("Pipeline schema must be a string")
    elif len(config.schema) > 128:
        errors.append("Pipeline schema name is too long (max 128 characters)")

    # Validate thresholds
    threshold_errors = validate_thresholds(config.thresholds)
    errors.extend(threshold_errors)

    # Validate parallel config
    # After __post_init__, parallel is always ParallelConfig, but check for type safety
    if isinstance(config.parallel, ParallelConfig):
        parallel_errors = validate_parallel_config(config.parallel)
        errors.extend(parallel_errors)

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


def validate_parallel_config(config: ParallelConfig) -> List[str]:
    """
    Validate parallel configuration.

    Args:
        config: Parallel configuration to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Check max_workers is positive
    if config.max_workers < 1:
        errors.append(f"max_workers must be at least 1, got {config.max_workers}")

    # Check max_workers is reasonable (e.g., not more than 100)
    if config.max_workers > 100:
        errors.append(
            f"max_workers is very large ({config.max_workers}), consider using a smaller value"
        )

    return errors
