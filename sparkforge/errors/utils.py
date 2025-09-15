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
Error handling utilities for SparkForge.

This module provides utility functions for consistent error handling,
error recovery, and error reporting across all SparkForge modules.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from functools import wraps
from typing import Any, Callable

from .base import ErrorCategory, ErrorSeverity, SparkForgeError


def handle_errors(
    *,
    error_type: type[SparkForgeError],
    message: str = "An error occurred",
    error_code: str | None = None,
    category: ErrorCategory | None = None,
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    context: dict[str, Any] | None = None,
    suggestions: list[str] | None = None,
    reraise: bool = True,
):
    """
    Decorator for consistent error handling.

    Args:
        error_type: The exception type to raise
        message: Base error message
        error_code: Error code for programmatic handling
        category: Error category
        severity: Error severity level
        context: Additional context information
        suggestions: List of suggested fixes
        reraise: Whether to reraise the exception
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except SparkForgeError:
                # Re-raise SparkForge errors as-is
                raise
            except Exception as e:
                # Convert other exceptions to SparkForge errors
                error_context = context or {}
                error_context.update(
                    {
                        "function": func.__name__,
                        "module": func.__module__,
                        "original_error": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )

                error = error_type(
                    message=f"{message}: {str(e)}",
                    error_code=error_code,
                    category=category,
                    severity=severity,
                    context=error_context,
                    suggestions=suggestions,
                    cause=e,
                )

                if reraise:
                    raise error
                else:
                    logging.error(f"Error in {func.__name__}: {error}")
                    return None

        return wrapper

    return decorator


def create_error_context(
    *,
    step_name: str | None = None,
    step_type: str | None = None,
    pipeline_id: str | None = None,
    execution_id: str | None = None,
    table_name: str | None = None,
    column_name: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    """
    Create standardized error context.

    Args:
        step_name: Name of the step where error occurred
        step_type: Type of the step (bronze, silver, gold)
        pipeline_id: Pipeline identifier
        execution_id: Execution identifier
        table_name: Table name if applicable
        column_name: Column name if applicable
        **kwargs: Additional context information

    Returns:
        Dictionary containing error context
    """
    context = {"timestamp": datetime.now().isoformat(), **kwargs}

    if step_name:
        context["step_name"] = step_name
    if step_type:
        context["step_type"] = step_type
    if pipeline_id:
        context["pipeline_id"] = pipeline_id
    if execution_id:
        context["execution_id"] = execution_id
    if table_name:
        context["table_name"] = table_name
    if column_name:
        context["column_name"] = column_name

    return context


def get_error_suggestions(error: SparkForgeError) -> list[str]:
    """
    Get suggestions for fixing an error.

    Args:
        error: The error to get suggestions for

    Returns:
        List of suggested fixes
    """
    suggestions = error.suggestions.copy()

    # Add category-specific suggestions
    if error.category == ErrorCategory.CONFIGURATION:
        suggestions.extend(
            [
                "Check configuration file syntax",
                "Verify all required configuration parameters are set",
                "Ensure configuration values are within valid ranges",
            ]
        )
    elif error.category == ErrorCategory.VALIDATION:
        suggestions.extend(
            [
                "Review data quality rules",
                "Check data schema compatibility",
                "Verify input data format and types",
            ]
        )
    elif error.category == ErrorCategory.EXECUTION:
        suggestions.extend(
            [
                "Check step dependencies",
                "Verify resource availability",
                "Review execution logs for detailed error information",
            ]
        )
    elif error.category == ErrorCategory.DATA_QUALITY:
        suggestions.extend(
            [
                "Review data quality thresholds",
                "Check data source for quality issues",
                "Consider data cleaning or transformation steps",
            ]
        )
    elif error.category == ErrorCategory.RESOURCE:
        suggestions.extend(
            [
                "Check resource availability",
                "Verify resource permissions",
                "Consider increasing resource limits",
            ]
        )

    return suggestions


def format_error_message(error: SparkForgeError) -> str:
    """
    Format an error message with full context.

    Args:
        error: The error to format

    Returns:
        Formatted error message
    """
    parts = [str(error)]

    if error.context:
        context_parts = []
        for key, value in error.context.items():
            if key not in ["traceback", "original_error"]:
                context_parts.append(f"{key}={value}")

        if context_parts:
            parts.append(f"Context: {', '.join(context_parts)}")

    if error.suggestions:
        suggestions_str = "; ".join(error.suggestions[:3])  # Limit to first 3
        parts.append(f"Suggestions: {suggestions_str}")

    return " | ".join(parts)


def log_error(error: SparkForgeError, logger: logging.Logger) -> None:
    """
    Log an error with appropriate level and context.

    Args:
        error: The error to log
        logger: Logger instance
    """
    message = format_error_message(error)

    if error.severity == ErrorSeverity.CRITICAL:
        logger.critical(message)
    elif error.severity == ErrorSeverity.HIGH:
        logger.error(message)
    elif error.severity == ErrorSeverity.MEDIUM:
        logger.warning(message)
    else:
        logger.info(message)

    # Log full context if available
    if error.context and "traceback" in error.context:
        logger.debug(f"Full traceback: {error.context['traceback']}")


def is_recoverable_error(error: SparkForgeError) -> bool:
    """
    Determine if an error is recoverable.

    Args:
        error: The error to check

    Returns:
        True if the error is recoverable, False otherwise
    """
    # Configuration errors are usually not recoverable
    if error.category == ErrorCategory.CONFIGURATION:
        return False

    # Resource errors might be recoverable with retry
    if error.category == ErrorCategory.RESOURCE:
        return True

    # Network errors are often recoverable
    if error.category == ErrorCategory.NETWORK:
        return True

    # Data quality errors are usually not recoverable without data changes
    if error.category == ErrorCategory.DATA_QUALITY:
        return False

    # Default to not recoverable for safety
    return False


def should_retry_error(
    error: SparkForgeError, retry_count: int, max_retries: int = 3
) -> bool:
    """
    Determine if an error should be retried.

    Args:
        error: The error to check
        retry_count: Current retry count
        max_retries: Maximum number of retries

    Returns:
        True if the error should be retried, False otherwise
    """
    if retry_count >= max_retries:
        return False

    if not is_recoverable_error(error):
        return False

    # Don't retry critical errors
    if error.severity == ErrorSeverity.CRITICAL:
        return False

    return True
