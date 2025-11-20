"""
Simplified error handling system for the framework.

This module re-exports error classes from pipeline_builder_base
for backward compatibility.
"""

from __future__ import annotations

# Re-export from base for backward compatibility
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
]

# Backward compatibility aliases
PipelineValidationError = ValidationError
PipelineConfigurationError = ConfigurationError
PipelineExecutionError = ExecutionError
TableOperationError = DataError
DependencyError = ValidationError
StepError = ExecutionError
PipelineError = ExecutionError
