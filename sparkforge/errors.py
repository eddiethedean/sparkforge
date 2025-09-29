"""
Simplified error handling system for SparkForge.

This module provides a clean, consolidated error handling system
with just the essential error types needed for the project.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, cast


class ErrorSeverity(Enum):
    """Severity levels for errors."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of errors."""

    CONFIGURATION = "configuration"
    VALIDATION = "validation"
    EXECUTION = "execution"
    DATA = "data"
    SYSTEM = "system"
    PERFORMANCE = "performance"
    RESOURCE = "resource"


# Type definitions for error context
ErrorContextValue = Union[str, int, float, bool, List[str], Dict[str, str], None]
ErrorContext = Dict[str, ErrorContextValue]
ErrorSuggestions = List[str]


class SparkForgeError(Exception):
    """
    Base exception for all SparkForge errors.

    This is the root exception class that all other SparkForge exceptions
    inherit from, providing consistent error handling patterns and rich context.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        category: ErrorCategory | None = None,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: ErrorContext | None = None,
        suggestions: ErrorSuggestions | None = None,
        timestamp: datetime | None = None,
        cause: Exception | None = None,
    ):
        """
        Initialize a SparkForge error.

        Args:
            message: Human-readable error message
            error_code: Optional error code for programmatic handling
            category: Error category for classification
            severity: Error severity level
            context: Additional context information
            suggestions: Suggested actions to resolve the error
            timestamp: When the error occurred (defaults to now)
            cause: The underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.category = category
        self.severity = severity
        self.context = context or {}
        self.suggestions = suggestions or []
        self.timestamp = timestamp or datetime.utcnow()
        self.cause = cause

    def __str__(self) -> str:
        """Return string representation of the error."""
        parts = [self.message]

        if self.error_code:
            parts.append(f"[{self.error_code}]")

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")

        if self.suggestions:
            parts.append(f"Suggestions: {'; '.join(self.suggestions)}")

        return " | ".join(parts)

    def to_dict(
        self,
    ) -> Dict[
        str,
        str
        | int
        | float
        | bool
        | list[str]
        | dict[str, str | int | float | bool | list[str] | dict[str, str] | None]
        | None,
    ]:
        """Convert error to dictionary for serialization."""
        return {
            "message": self.message,
            "error_code": self.error_code,
            "category": self.category.value if self.category else None,
            "severity": self.severity.value,
            "context": self.context,
            "suggestions": self.suggestions,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None,
        }


class ConfigurationError(SparkForgeError):
    """Raised when there's a configuration-related error."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class ValidationError(SparkForgeError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class ExecutionError(SparkForgeError):
    """Raised when pipeline execution fails."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class DataError(SparkForgeError):
    """Raised when there's a data-related error."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.DATA,
            severity=ErrorSeverity.MEDIUM,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class SystemError(SparkForgeError):
    """Raised when there's a system-level error."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class PerformanceError(SparkForgeError):
    """Raised when there's a performance-related error."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.PERFORMANCE,
            severity=ErrorSeverity.MEDIUM,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


class ResourceError(SparkForgeError):
    """Raised when there's a resource-related error."""

    def __init__(
        self,
        message: str,
        **kwargs: str | int | float | bool | list[str] | dict[str, str] | None,
    ):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.HIGH,
            error_code=(
                cast(Optional[str], kwargs.get("error_code"))
                if isinstance(kwargs.get("error_code"), str)
                else None
            ),
            context=(
                cast(
                    Dict[
                        str,
                        Union[str, int, float, bool, List[str], Dict[str, str], None],
                    ],
                    kwargs.get("context", {}),
                )
                if isinstance(kwargs.get("context"), dict)
                else {}
            ),
            suggestions=(
                cast(List[str], kwargs.get("suggestions", []))
                if isinstance(kwargs.get("suggestions"), list)
                else []
            ),
            cause=(
                cast(Optional[Exception], kwargs.get("cause"))
                if isinstance(kwargs.get("cause"), Exception)
                else None
            ),
        )


# Backward compatibility aliases
PipelineValidationError = ValidationError
PipelineConfigurationError = ConfigurationError
PipelineExecutionError = ExecutionError
TableOperationError = DataError
DependencyError = ValidationError
StepError = ExecutionError
PipelineError = ExecutionError
