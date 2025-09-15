# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

"""
Base exception classes for SparkForge.

This module defines the foundational exception hierarchy that all other
SparkForge exceptions inherit from, providing consistent error handling patterns.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any


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
    DATA_QUALITY = "data_quality"
    RESOURCE = "resource"
    SYSTEM = "system"
    NETWORK = "network"
    STORAGE = "storage"
    PERFORMANCE = "performance"


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
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
        timestamp: datetime | None = None,
        cause: Exception | None = None,
    ):
        """
        Initialize a SparkForge error.

        Args:
            message: Human-readable error message
            error_code: Unique error code for programmatic handling
            category: Error category for classification
            severity: Error severity level
            context: Additional context information
            suggestions: List of suggested fixes or actions
            timestamp: When the error occurred (defaults to now)
            cause: The underlying exception that caused this error
        """
        super().__init__(message)

        self.message = message
        self.error_code = error_code or self.__class__.__name__.upper()
        self.category = category
        self.severity = severity
        self.context = context or {}
        self.suggestions = suggestions or []
        self.timestamp = timestamp or datetime.now()
        self.cause = cause

    def __str__(self) -> str:
        """Return formatted error message."""
        parts = [self.message]

        if self.error_code:
            parts.append(f"(Code: {self.error_code})")

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")

        return " | ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary for serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "category": self.category.value if self.category else None,
            "severity": self.severity.value,
            "context": self.context,
            "suggestions": self.suggestions,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None,
        }

    def add_context(self, key: str, value: Any) -> None:
        """Add context information to the error."""
        self.context[key] = value

    def add_suggestion(self, suggestion: str) -> None:
        """Add a suggestion for fixing the error."""
        self.suggestions.append(suggestion)


class ConfigurationError(SparkForgeError):
    """Raised when configuration is invalid or missing."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )


class ValidationError(SparkForgeError):
    """Raised when data validation fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )


class ExecutionError(SparkForgeError):
    """Raised when execution fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )


class DataQualityError(SparkForgeError):
    """Raised when data quality issues are detected."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATA_QUALITY,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )


class ResourceError(SparkForgeError):
    """Raised when resource-related errors occur."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
