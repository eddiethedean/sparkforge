"""
Error handling package.

This package provides error handling utilities for pipeline operations.
Re-exports error classes from pipeline_builder_base for backward compatibility.
"""

# Re-export error classes from pipeline_builder_base (from errors.py)
from pipeline_builder_base.errors import (
    ConfigurationError,
    DataError,
    ErrorCategory,
    ErrorContext,
    ErrorContextValue,
    ErrorSeverity,
    ErrorSuggestions,
    ExecutionError,
    PerformanceError,
    PipelineValidationError,
    ResourceError,
    SparkForgeError,
    SystemError,
    ValidationError,
)

# Export ErrorHandler from this package
from .error_handler import ErrorHandler

__all__ = [
    "SparkForgeError",
    "ValidationError",
    "PipelineValidationError",
    "ConfigurationError",
    "ExecutionError",
    "DataError",
    "SystemError",
    "PerformanceError",
    "ResourceError",
    "ErrorSeverity",
    "ErrorCategory",
    "ErrorContext",
    "ErrorContextValue",
    "ErrorSuggestions",
    "ErrorHandler",
    # Backward compatibility aliases
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "TableOperationError",
    "DependencyError",
    "StepError",
    "PipelineError",
]

# Backward compatibility aliases
PipelineConfigurationError = ConfigurationError
PipelineExecutionError = ExecutionError
TableOperationError = DataError
DependencyError = ValidationError
StepError = ExecutionError
PipelineError = ExecutionError
