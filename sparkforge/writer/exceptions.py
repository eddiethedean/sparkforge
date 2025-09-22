"""
Writer-specific exceptions.

This module contains all the custom exceptions used by the writer module,
providing clear error handling and debugging information.
"""

from __future__ import annotations

from typing import Any


class WriterError(Exception):
    """
    Base exception for all writer-related errors.

    Provides a common base class for all writer exceptions with
    enhanced error context and suggestions.
    """

    def __init__(
        self,
        message: str,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """
        Initialize the writer error.

        Args:
            message: Error message
            context: Additional context information
            suggestions: List of suggestions to resolve the error
            cause: The underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.suggestions = suggestions or []
        self.cause = cause

    def __str__(self) -> str:
        """Return formatted error message."""
        msg = self.message
        if self.context:
            msg += f"\nContext: {self.context}"
        if self.suggestions:
            msg += f"\nSuggestions: {'; '.join(self.suggestions)}"
        return msg


class WriterValidationError(WriterError):
    """
    Raised when writer validation fails.

    This exception is raised when data validation fails during
    the writing process, such as invalid log rows or schema mismatches.
    """

    def __init__(
        self,
        message: str,
        validation_errors: list[str] | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """
        Initialize validation error.

        Args:
            message: Error message
            validation_errors: List of specific validation errors
            context: Additional context information
            suggestions: List of suggestions to resolve the error
        """
        super().__init__(message, context, suggestions)
        self.validation_errors = validation_errors or []


class WriterConfigurationError(WriterError):
    """
    Raised when writer configuration is invalid.

    This exception is raised when the WriterConfig contains
    invalid values or conflicting settings.
    """

    def __init__(
        self,
        message: str,
        config_errors: list[str] | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """
        Initialize configuration error.

        Args:
            message: Error message
            config_errors: List of specific configuration errors
            context: Additional context information
            suggestions: List of suggestions to resolve the error
        """
        super().__init__(message, context, suggestions)
        self.config_errors = config_errors or []


class WriterTableError(WriterError):
    """
    Raised when table operations fail.

    This exception is raised when there are issues with Delta table
    operations, such as table creation, writing, or schema evolution.
    """

    def __init__(
        self,
        message: str,
        table_name: str | None = None,
        operation: str | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """
        Initialize table error.

        Args:
            message: Error message
            table_name: Name of the table that caused the error
            operation: The operation that failed
            context: Additional context information
            suggestions: List of suggestions to resolve the error
            cause: The underlying exception that caused this error
        """
        super().__init__(message, context, suggestions, cause)
        self.table_name = table_name
        self.operation = operation


class WriterPerformanceError(WriterError):
    """
    Raised when performance thresholds are exceeded.

    This exception is raised when operations take longer than expected
    or consume more resources than configured limits.
    """

    def __init__(
        self,
        message: str,
        actual_duration: float | None = None,
        expected_duration: float | None = None,
        actual_memory: float | None = None,
        expected_memory: float | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """
        Initialize performance error.

        Args:
            message: Error message
            actual_duration: Actual duration in seconds
            expected_duration: Expected duration in seconds
            actual_memory: Actual memory usage in MB
            expected_memory: Expected memory usage in MB
            context: Additional context information
            suggestions: List of suggestions to resolve the error
        """
        super().__init__(message, context, suggestions)
        self.actual_duration = actual_duration
        self.expected_duration = expected_duration
        self.actual_memory = actual_memory
        self.expected_memory = expected_memory


class WriterSchemaError(WriterError):
    """
    Raised when schema operations fail.

    This exception is raised when there are issues with schema
    validation, evolution, or compatibility.
    """

    def __init__(
        self,
        message: str,
        schema_errors: list[str] | None = None,
        expected_schema: str | None = None,
        actual_schema: str | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """
        Initialize schema error.

        Args:
            message: Error message
            schema_errors: List of specific schema errors
            expected_schema: Expected schema definition
            actual_schema: Actual schema definition
            context: Additional context information
            suggestions: List of suggestions to resolve the error
        """
        super().__init__(message, context, suggestions)
        self.schema_errors = schema_errors or []
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema


class WriterDataQualityError(WriterError):
    """
    Raised when data quality checks fail.

    This exception is raised when data quality validation fails,
    such as when validation rates are too low or data anomalies are detected.
    """

    def __init__(
        self,
        message: str,
        quality_issues: list[str] | None = None,
        validation_rate: float | None = None,
        threshold: float | None = None,
        context: dict[str, Any] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """
        Initialize data quality error.

        Args:
            message: Error message
            quality_issues: List of specific quality issues
            validation_rate: Actual validation rate
            threshold: Expected validation threshold
            context: Additional context information
            suggestions: List of suggestions to resolve the error
        """
        super().__init__(message, context, suggestions)
        self.quality_issues = quality_issues or []
        self.validation_rate = validation_rate
        self.threshold = threshold
