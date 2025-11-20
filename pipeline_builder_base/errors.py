"""
Simplified error handling system for the framework.

This module provides a clean, consolidated error handling system
with just the essential error types needed for the project.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Dict, List, Optional, Union, cast


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
    Base exception for all framework errors.

    This is the root exception class that all other framework exceptions
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
        Initialize a framework error.

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
        self.timestamp = timestamp or datetime.now(timezone.utc)
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

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for serialization."""
        return {
            "message": self.message,
            "error_code": self.error_code,
            "category": self.category.value if self.category else None,
            "severity": self.severity.value if self.severity else None,
            "context": self.context,
            "suggestions": self.suggestions,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "cause": str(self.cause) if self.cause else None,
        }


class ValidationError(SparkForgeError):
    """Raised when validation fails."""

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        value: Any = None,
        **kwargs: Any,
    ):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )
        self.field = field
        self.value = value
        if field:
            self.context["field"] = field
        if value is not None:
            self.context["value"] = str(value)


class PipelineValidationError(ValidationError):
    """Raised when pipeline validation fails."""

    def __init__(
        self,
        message: str,
        *,
        step_name: str | None = None,
        phase: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, **kwargs)
        self.step_name = step_name
        self.phase = phase
        if step_name:
            self.context["step_name"] = step_name
        if phase:
            self.context["phase"] = phase


class ConfigurationError(SparkForgeError):
    """Raised when configuration is invalid."""

    def __init__(self, message: str, **kwargs: Any):
        # Only set default severity if not provided in kwargs
        if "severity" not in kwargs:
            kwargs["severity"] = ErrorSeverity.MEDIUM
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            **kwargs,
        )


class ExecutionError(SparkForgeError):
    """Raised when execution fails."""

    def __init__(
        self,
        message: str,
        *,
        step_name: str | None = None,
        phase: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.step_name = step_name
        self.phase = phase
        if step_name:
            self.context["step_name"] = step_name
        if phase:
            self.context["phase"] = phase


class DataError(SparkForgeError):
    """Raised when data operations fail."""

    def __init__(self, message: str, **kwargs: Any):
        super().__init__(
            message,
            category=ErrorCategory.DATA,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )


class SystemError(SparkForgeError):
    """Raised when system operations fail."""

    def __init__(self, message: str, **kwargs: Any):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.CRITICAL,
            **kwargs,
        )


class PerformanceError(SparkForgeError):
    """Raised when performance issues are detected."""

    def __init__(self, message: str, **kwargs: Any):
        super().__init__(
            message,
            category=ErrorCategory.PERFORMANCE,
            severity=ErrorSeverity.LOW,
            **kwargs,
        )


class ResourceError(SparkForgeError):
    """Raised when resource operations fail."""

    def __init__(self, message: str, **kwargs: Any):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )

