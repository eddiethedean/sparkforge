"""
Custom exceptions for the Pipeline Builder models.

This module provides custom exception classes for pipeline configuration
and execution errors. These exceptions provide clearer error semantics
than generic Python exceptions.

Key Components:
    - **PipelineConfigurationError**: Raised when pipeline configuration
      is invalid (e.g., missing required fields, invalid values)
    - **PipelineExecutionError**: Raised when pipeline execution fails
      (e.g., step execution errors, validation failures)

Example:
    >>> from pipeline_builder.models.exceptions import (
    ...     PipelineConfigurationError,
    ...     PipelineExecutionError
    ... )
    >>>
    >>> # Raise configuration error
    >>> if not schema:
    ...     raise PipelineConfigurationError("Schema name is required")
    >>>
    >>> # Raise execution error
    >>> if step_failed:
    ...     raise PipelineExecutionError("Step execution failed")
"""


class PipelineConfigurationError(ValueError):
    """Raised when pipeline configuration is invalid.

    This exception is raised when pipeline configuration objects (e.g.,
    PipelineConfig, step configurations) are invalid. It indicates a
    problem with the configuration itself, not with execution.

    Common causes:
        - Missing required fields
        - Invalid field values (e.g., negative thresholds)
        - Inconsistent configuration (e.g., invalid schema name)

    Example:
        >>> from pipeline_builder.models.exceptions import PipelineConfigurationError
        >>> if not schema:
        ...     raise PipelineConfigurationError("Schema name is required")
    """

    pass


class PipelineExecutionError(RuntimeError):
    """Raised when pipeline execution fails.

    This exception is raised when pipeline execution encounters an error
    during runtime. It indicates a problem with execution, not with
    configuration.

    Common causes:
        - Step execution failures
        - Data validation failures
        - Write operation failures
        - Resource constraints (memory, disk space)

    Example:
        >>> from pipeline_builder.models.exceptions import PipelineExecutionError
        >>> if validation_rate < threshold:
        ...     raise PipelineExecutionError(
        ...         f"Validation rate {validation_rate}% below threshold {threshold}%"
        ...     )
    """

    pass
