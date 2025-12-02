"""
Execution helper functions for pipeline runners.

This module provides utility functions for execution mode determination,
source validation, and execution preparation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from ..models import ExecutionMode, PipelineConfig


def determine_execution_mode(
    config: PipelineConfig,
    bronze_sources: Optional[Dict[str, Any]] = None,
    last_run: Optional[datetime] = None,
) -> ExecutionMode:
    """
    Determine execution mode based on configuration and context.

    Args:
        config: Pipeline configuration
        bronze_sources: Optional dictionary of bronze sources
        last_run: Optional datetime of last pipeline run

    Returns:
        ExecutionMode enum value
    """
    # Check if this is an initial load (no bronze sources provided)
    if not bronze_sources or len(bronze_sources) == 0:
        return ExecutionMode.INITIAL

    # Check if incremental mode should be used
    if should_run_incremental(config, last_run):
        return ExecutionMode.INCREMENTAL

    # Default to initial if we can't determine
    return ExecutionMode.INITIAL


def should_run_incremental(
    config: PipelineConfig, last_run: Optional[datetime] = None
) -> bool:
    """
    Determine if pipeline should run in incremental mode.

    Args:
        config: Pipeline configuration
        last_run: Optional datetime of last pipeline run

    Returns:
        True if incremental mode should be used, False otherwise
    """
    # If no last run time, must be initial load
    if last_run is None:
        return False

    # Check if config has incremental settings
    # This is a placeholder - actual logic depends on implementation
    return True


def validate_bronze_sources(
    sources: Dict[str, Any],
    expected_bronze_steps: Dict[str, Any],
    source_validator: Optional[Any] = None,
) -> None:
    """
    Validate bronze sources match expected bronze steps.

    Args:
        sources: Dictionary of bronze sources
        expected_bronze_steps: Dictionary of expected bronze steps
        source_validator: Optional validator function to check source validity

    Raises:
        ValueError: If sources don't match expected steps or are invalid
    """
    # Check that all expected bronze steps have sources
    missing_sources = set(expected_bronze_steps.keys()) - set(sources.keys())
    if missing_sources:
        raise ValueError(
            f"Missing bronze sources for steps: {', '.join(missing_sources)}"
        )

    # Check that all sources have corresponding expected steps
    unexpected_sources = set(sources.keys()) - set(expected_bronze_steps.keys())
    if unexpected_sources:
        raise ValueError(
            f"Unexpected bronze sources (no corresponding step): {', '.join(unexpected_sources)}"
        )

    # Validate each source if validator provided
    if source_validator:
        for step_name, source in sources.items():
            if not source_validator(source):
                raise ValueError(
                    f"Invalid bronze source for step '{step_name}': {type(source)}"
                )


def prepare_sources_for_execution(
    sources: Dict[str, Any],
    step_type: str,
    step_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Prepare sources for step execution.

    Args:
        sources: Dictionary of sources
        step_type: Type of step (bronze/silver/gold)
        step_name: Optional step name for filtering

    Returns:
        Dictionary of prepared sources
    """
    if step_type == "bronze":
        # Bronze steps use all provided sources
        return sources
    elif step_type == "silver":
        # Silver steps use bronze sources
        return {k: v for k, v in sources.items() if k in sources}
    elif step_type == "gold":
        # Gold steps use silver sources
        return {k: v for k, v in sources.items() if k in sources}

    return sources
