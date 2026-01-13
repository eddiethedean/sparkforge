"""
Enums for the Pipeline Builder models.

This module provides enumeration classes for pipeline phases, execution modes,
write modes, and validation results. These enums ensure type safety and
provide clear constants for use throughout the pipeline system.

Key Components:
    - **PipelinePhase**: Medallion Architecture layers (BRONZE, SILVER, GOLD)
    - **ExecutionMode**: Pipeline execution modes (INITIAL, INCREMENTAL, etc.)
    - **WriteMode**: Data write modes (OVERWRITE, APPEND)
    - **ValidationResult**: Validation outcomes (PASSED, FAILED, WARNING)

Example:
    >>> from pipeline_builder.models.enums import (
    ...     PipelinePhase,
    ...     ExecutionMode,
    ...     WriteMode,
    ...     ValidationResult
    ... )
    >>>
    >>> # Use pipeline phase
    >>> phase = PipelinePhase.BRONZE
    >>> print(phase.value)  # "bronze"
    >>>
    >>> # Use execution mode
    >>> mode = ExecutionMode.INITIAL
    >>> print(mode.value)  # "initial"
"""

from enum import Enum


class PipelinePhase(Enum):
    """Enumeration of pipeline phases.

    Represents the three layers of the Medallion Architecture:
    - BRONZE: Raw data ingestion and validation layer
    - SILVER: Cleaned and enriched data layer
    - GOLD: Business-ready analytics and reporting layer

    Example:
        >>> from pipeline_builder.models.enums import PipelinePhase
        >>> phase = PipelinePhase.BRONZE
        >>> print(phase.value)  # "bronze"
    """

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ExecutionMode(Enum):
    """Enumeration of execution modes.

    Defines how a pipeline should be executed:
    - INITIAL: First-time execution with full data processing
    - INCREMENTAL: Process only new data based on watermark columns
    - FULL_REFRESH: Reprocess all data, overwriting existing results
    - VALIDATION_ONLY: Validate data without writing results

    Example:
        >>> from pipeline_builder.models.enums import ExecutionMode
        >>> mode = ExecutionMode.INCREMENTAL
        >>> print(mode.value)  # "incremental"
    """

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    VALIDATION_ONLY = "validation_only"


class WriteMode(Enum):
    """Enumeration of write modes.

    Defines how data should be written to tables:
    - OVERWRITE: Replace all existing data in the table
    - APPEND: Add new data to existing table data

    Example:
        >>> from pipeline_builder.models.enums import WriteMode
        >>> mode = WriteMode.OVERWRITE
        >>> print(mode.value)  # "overwrite"
    """

    OVERWRITE = "overwrite"
    APPEND = "append"


class ValidationResult(Enum):
    """Enumeration of validation results.

    Represents the outcome of data validation:
    - PASSED: Validation succeeded, data meets quality requirements
    - FAILED: Validation failed, data does not meet quality requirements
    - WARNING: Validation passed but with warnings (e.g., low validation rate)

    Example:
        >>> from pipeline_builder.models.enums import ValidationResult
        >>> result = ValidationResult.PASSED
        >>> print(result.value)  # "passed"
    """

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
