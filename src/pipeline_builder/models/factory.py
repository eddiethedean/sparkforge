"""
Factory functions for creating and managing pipeline models.

This module provides factory functions for creating and validating pipeline
configuration objects, execution contexts, and step configurations. These
functions simplify object creation and ensure consistent initialization.

Key Functions:
    - **create_pipeline_config**: Create PipelineConfig with custom thresholds
    - **create_execution_context**: Create ExecutionContext for pipeline runs
    - **validate_pipeline_config**: Validate PipelineConfig instances
    - **validate_step_config**: Validate step configurations
    - **serialize_pipeline_config**: Serialize config to JSON
    - **deserialize_pipeline_config**: Deserialize config from JSON

Dependencies:
    - models.base: ValidationThresholds
    - models.enums: ExecutionMode
    - models.exceptions: PipelineConfigurationError, PipelineExecutionError
    - models.execution: ExecutionContext
    - models.pipeline: PipelineConfig
    - models.steps: BronzeStep, SilverStep, GoldStep

Example:
    >>> from pipeline_builder.models.factory import (
    ...     create_pipeline_config,
    ...     create_execution_context,
    ...     validate_pipeline_config
    ... )
    >>> from pipeline_builder.models.enums import ExecutionMode
    >>>
    >>> # Create pipeline configuration
    >>> config = create_pipeline_config(
    ...     schema="analytics",
    ...     bronze_threshold=95.0,
    ...     silver_threshold=98.0,
    ...     gold_threshold=99.0
    ... )
    >>> validate_pipeline_config(config)
    >>>
    >>> # Create execution context
    >>> context = create_execution_context(ExecutionMode.INITIAL)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Union

from .base import ValidationThresholds
from .enums import ExecutionMode
from .exceptions import PipelineConfigurationError, PipelineExecutionError
from .execution import ExecutionContext
from .pipeline import PipelineConfig
from .steps import BronzeStep, GoldStep, SilverStep


def create_pipeline_config(
    schema: str,
    bronze_threshold: float = 95.0,
    silver_threshold: float = 98.0,
    gold_threshold: float = 99.0,
    verbose: bool = True,
) -> PipelineConfig:
    """Factory function to create pipeline configuration.

    Creates a PipelineConfig instance with custom validation thresholds.
    This is a convenience function that simplifies configuration creation
    compared to manually constructing ValidationThresholds and PipelineConfig.

    Args:
        schema: Database schema name for pipeline tables. Must be a
            non-empty string.
        bronze_threshold: Minimum validation success rate for Bronze layer
            (0-100). Defaults to 95.0.
        silver_threshold: Minimum validation success rate for Silver layer
            (0-100). Defaults to 98.0.
        gold_threshold: Minimum validation success rate for Gold layer
            (0-100). Defaults to 99.0.
        verbose: Whether to enable verbose logging. Defaults to True.

    Returns:
        PipelineConfig instance with the specified settings.

    Raises:
        PipelineValidationError: If schema is invalid or thresholds are
            outside the valid range (0-100).

    Example:
        >>> config = create_pipeline_config(
        ...     schema="analytics",
        ...     bronze_threshold=90.0,
        ...     silver_threshold=95.0,
        ...     gold_threshold=99.0,
        ...     verbose=False
        ... )
        >>> print(config.schema)  # "analytics"
        >>> print(config.min_bronze_rate)  # 90.0
    """
    thresholds = ValidationThresholds(
        bronze=bronze_threshold, silver=silver_threshold, gold=gold_threshold
    )
    return PipelineConfig(schema=schema, thresholds=thresholds, verbose=verbose)


def create_execution_context(mode: ExecutionMode) -> ExecutionContext:
    """Factory function to create execution context.

    Creates an ExecutionContext instance with the specified execution mode
    and current timestamp. Automatically generates unique run_id and
    execution_id.

    Args:
        mode: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH,
            VALIDATION_ONLY).

    Returns:
        ExecutionContext instance initialized with the current timestamp
        and a unique run identifier.

    Example:
        >>> from pipeline_builder.models.enums import ExecutionMode
        >>> context = create_execution_context(ExecutionMode.INITIAL)
        >>> print(context.mode)  # ExecutionMode.INITIAL
        >>> print(context.run_id)  # Unique UUID string
    """
    return ExecutionContext(mode=mode, start_time=datetime.now(timezone.utc))


def validate_pipeline_config(config: PipelineConfig) -> None:
    """Validate a pipeline configuration.

    Validates a PipelineConfig instance and converts PipelineExecutionError
    to PipelineConfigurationError for clearer error semantics.

    Args:
        config: PipelineConfig instance to validate.

    Raises:
        PipelineConfigurationError: If the configuration is invalid.
            Wraps any PipelineExecutionError from the validation process.

    Example:
        >>> config = create_pipeline_config(schema="test")
        >>> validate_pipeline_config(config)  # Passes
        >>>
        >>> invalid = PipelineConfig(schema="", thresholds=ValidationThresholds.create_default())
        >>> validate_pipeline_config(invalid)  # Raises PipelineConfigurationError
    """
    try:
        config.validate()
    except PipelineExecutionError as e:
        raise PipelineConfigurationError(f"Invalid pipeline configuration: {e}") from e


def validate_step_config(step: Union[BronzeStep, SilverStep, GoldStep]) -> None:
    """Validate a step configuration.

    Validates a step configuration (Bronze, Silver, or Gold) and converts
    PipelineExecutionError to PipelineConfigurationError for clearer error
    semantics.

    Args:
        step: Step instance (BronzeStep, SilverStep, or GoldStep) to validate.

    Raises:
        PipelineConfigurationError: If the step configuration is invalid.
            Wraps any PipelineExecutionError from the validation process.

    Example:
        >>> from pipeline_builder.models.steps import BronzeStep
        >>> step = BronzeStep(name="test", rules={"id": [F.col("id").isNotNull()]})
        >>> validate_step_config(step)  # Passes
        >>>
        >>> invalid = BronzeStep(name="", rules={})
        >>> validate_step_config(invalid)  # Raises PipelineConfigurationError
    """
    try:
        step.validate()
    except PipelineExecutionError as e:
        raise PipelineConfigurationError(f"Invalid step configuration: {e}") from e


def serialize_pipeline_config(config: PipelineConfig) -> str:
    """Serialize pipeline configuration to JSON.

    Converts a PipelineConfig instance to a JSON string for storage or
    transmission. Uses the config's `to_json` method.

    Args:
        config: PipelineConfig instance to serialize.

    Returns:
        JSON string representation of the configuration.

    Example:
        >>> config = create_pipeline_config(schema="analytics")
        >>> json_str = serialize_pipeline_config(config)
        >>> print(json_str)  # {"schema": "analytics", ...}
    """
    return config.to_json()


def deserialize_pipeline_config(json_str: str) -> PipelineConfig:
    """Deserialize pipeline configuration from JSON.

    Converts a JSON string back to a PipelineConfig instance. This is the
    inverse operation of `serialize_pipeline_config`.

    Args:
        json_str: JSON string representation of a PipelineConfig.

    Returns:
        PipelineConfig instance reconstructed from the JSON string.

    Raises:
        json.JSONDecodeError: If the JSON string is invalid.
        KeyError: If required fields are missing from the JSON.

    Example:
        >>> config = create_pipeline_config(schema="analytics")
        >>> json_str = serialize_pipeline_config(config)
        >>> restored = deserialize_pipeline_config(json_str)
        >>> print(restored.schema)  # "analytics"
    """
    data = json.loads(json_str)
    return PipelineConfig(
        schema=data["schema"],
        thresholds=ValidationThresholds(
            bronze=data["thresholds"]["bronze"],
            silver=data["thresholds"]["silver"],
            gold=data["thresholds"]["gold"],
        ),
        verbose=data.get("verbose", True),
    )
