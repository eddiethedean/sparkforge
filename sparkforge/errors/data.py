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
Data-specific exceptions for SparkForge.

This module defines exceptions specific to data operations,
providing detailed error context for data-related issues.
"""

from __future__ import annotations

from typing import Any

from .base import ErrorCategory, ErrorSeverity, SparkForgeError


class DataError(SparkForgeError):
    """Base exception for all data-related errors."""

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.table_name = table_name
        self.column_name = column_name

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.table_name and self.column_name:
            return f"[{self.table_name}.{self.column_name}] {base_msg}"
        elif self.table_name:
            return f"[{self.table_name}] {base_msg}"
        return base_msg


class DataQualityError(DataError):
    """Raised when data quality issues are detected."""

    def __init__(
        self,
        message: str,
        *,
        quality_rate: float | None = None,
        threshold: float | None = None,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.DATA_QUALITY,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )
        self.quality_rate = quality_rate
        self.threshold = threshold

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.quality_rate is not None and self.threshold is not None:
            return f"{base_msg} (Quality: {self.quality_rate:.2f}% < {self.threshold:.2f}%)"
        return base_msg


class SchemaError(DataError):
    """Raised when schema-related errors occur."""

    def __init__(
        self,
        message: str,
        *,
        expected_schema: dict[str, Any] | None = None,
        actual_schema: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.expected_schema and self.actual_schema:
            return f"{base_msg} | Expected: {self.expected_schema} | Actual: {self.actual_schema}"
        return base_msg


class ValidationError(DataError):
    """Raised when data validation fails."""

    def __init__(
        self, message: str, *, validation_errors: list[str] | None = None, **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )
        self.validation_errors = validation_errors or []

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.validation_errors:
            errors_str = "; ".join(self.validation_errors)
            return f"{base_msg} | Validation errors: {errors_str}"
        return base_msg


class TableOperationError(DataError):
    """Raised when table operations fail."""

    def __init__(self, message: str, *, operation: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.operation = operation

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.operation:
            return f"[{self.operation}] {base_msg}"
        return base_msg
