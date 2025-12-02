"""
Error handling utilities and context builders.

This module re-exports error classes from the parent errors.py module
and adds context utilities.
"""

# Import from parent errors.py using importlib to avoid circular imports
import importlib.util
from pathlib import Path

# Load the parent errors.py module
_parent_dir = Path(__file__).parent.parent
_errors_file = _parent_dir / "errors.py"
if _errors_file.exists():
    spec = importlib.util.spec_from_file_location("_errors_module", _errors_file)
    if spec is not None and spec.loader is not None:
        _errors_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_errors_module)
    else:
        raise ImportError("Could not load errors.py module spec")

    # Re-export error classes and type definitions
    ConfigurationError = _errors_module.ConfigurationError
    DataError = _errors_module.DataError
    ErrorCategory = _errors_module.ErrorCategory
    ErrorContext = _errors_module.ErrorContext
    ErrorContextValue = _errors_module.ErrorContextValue
    ErrorSeverity = _errors_module.ErrorSeverity
    ErrorSuggestions = _errors_module.ErrorSuggestions
    ExecutionError = _errors_module.ExecutionError
    PerformanceError = _errors_module.PerformanceError
    PipelineValidationError = _errors_module.PipelineValidationError
    ResourceError = _errors_module.ResourceError
    SparkForgeError = _errors_module.SparkForgeError
    SystemError = _errors_module.SystemError
    ValidationError = _errors_module.ValidationError
else:
    # Fallback if errors.py doesn't exist (shouldn't happen)
    raise ImportError("Could not find errors.py module")

# Import context utilities (after parent module is loaded)
from .context import (  # noqa: E402
    ErrorContext,
    SuggestionGenerator,
    build_execution_context,
    build_validation_context,
)

__all__ = [
    "ConfigurationError",
    "DataError",
    "ErrorCategory",
    "ErrorContext",
    "ErrorContextValue",
    "ErrorSeverity",
    "ErrorSuggestions",
    "ExecutionError",
    "PerformanceError",
    "PipelineValidationError",
    "ResourceError",
    "SuggestionGenerator",
    "SparkForgeError",
    "SystemError",
    "ValidationError",
    "build_execution_context",
    "build_validation_context",
]
