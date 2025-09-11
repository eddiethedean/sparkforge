# exceptions.py
"""
Custom exceptions for the SparkForge pipeline framework.

This module contains all custom exceptions used throughout the framework,
providing clear error handling and better debugging capabilities.
"""


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class TableOperationError(Exception):
    """Raised when table operations fail."""
    pass


class PerformanceError(Exception):
    """Raised when performance thresholds are exceeded."""
    pass


class PipelineValidationError(Exception):
    """Raised when pipeline configuration validation fails."""
    pass


class ExecutionError(Exception):
    """Raised when pipeline execution fails."""
    pass


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass
