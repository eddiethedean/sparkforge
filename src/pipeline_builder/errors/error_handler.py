"""Error handling utilities for pipeline operations.

This module provides centralized error handling with consistent error wrapping
and context addition. The ErrorHandler ensures all errors are wrapped in
ExecutionError with appropriate context and suggestions.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Dict, Generator, List, Optional, TypeVar, Union

from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger

F = TypeVar("F", bound=Callable[..., Any])
ContextType = Union[Optional[Dict[str, Any]], Callable[..., Dict[str, Any]]]
SuggestionsType = Union[Optional[List[str]], Callable[..., List[str]]]


class ErrorHandler:
    """Centralized error handler for pipeline operations.

    Provides consistent error wrapping and context addition. Ensures all
    errors are wrapped in ExecutionError with appropriate context and
    suggestions for debugging.

    Attributes:
        logger: PipelineLogger instance for logging.

    Example:
        Using as context manager:

        >>> from pipeline_builder.errors.error_handler import ErrorHandler
        >>>
        >>> handler = ErrorHandler()
        >>> with handler.handle_errors(
        ...     "table write",
        ...     context={"table": "analytics.events"},
        ...     suggestions=["Check table permissions", "Verify schema"]
        ... ):
        ...     df.write.saveAsTable("analytics.events")

        Using as decorator:

        >>> @handler.wrap_error("data validation")
        >>> def validate_data(df):
        ...     # validation logic
        ...     pass
    """

    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the error handler.

        Args:
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.logger = logger or PipelineLogger()

    @contextmanager
    def handle_errors(
        self,
        operation: str,
        context: Optional[Dict[str, Any]] = None,
        suggestions: Optional[List[str]] = None,
    ) -> Generator[None, None, None]:
        """Context manager for error handling.

        Wraps code in a context manager that catches exceptions and wraps
        them in ExecutionError with context and suggestions. ExecutionError
        exceptions are re-raised as-is.

        Args:
            operation: Description of the operation being performed (used in
                error messages).
            context: Optional dictionary with additional context about the
                operation (e.g., table name, step name).
            suggestions: Optional list of suggestions for fixing errors.

        Yields:
            None (context manager yields control to the wrapped code).

        Raises:
            ExecutionError: Wrapped error with context and suggestions.
                ExecutionError exceptions are re-raised as-is without wrapping.

        Example:
            >>> with handler.handle_errors(
            ...     "table write",
            ...     context={"table": "analytics.events"},
            ...     suggestions=["Check permissions", "Verify schema"]
            ... ):
            ...     df.write.saveAsTable("analytics.events")
        """
        try:
            yield
        except ExecutionError:
            # Re-raise ExecutionError as-is (already has context)
            raise
        except Exception as e:
            # Wrap other exceptions
            raise ExecutionError(
                f"Error during {operation}: {str(e)}",
                context=context or {},
                suggestions=suggestions or [],
            ) from e

    def wrap_error(
        self,
        operation: str,
        context: ContextType = None,
        suggestions: SuggestionsType = None,
    ) -> Callable[[F], F]:
        """Decorator for wrapping function errors.

        Decorator that wraps function exceptions in ExecutionError with context
        and suggestions. Context and suggestions can be callables that receive
        function arguments for dynamic error messages.

        Args:
            operation: Description of the operation being performed (used in
                error messages).
            context: Optional dictionary with additional context, or a callable
                that receives function args and returns a context dictionary.
            suggestions: Optional list of suggestions, or a callable that
                receives function args and returns a list of suggestions.

        Returns:
            Decorator function that wraps the target function.

        Example:
            >>> @handler.wrap_error(
            ...     "data validation",
            ...     context=lambda df, rules: {"df_rows": df.count(), "rules_count": len(rules)},
            ...     suggestions=["Check data quality", "Review validation rules"]
            ... )
            >>> def validate_data(df, rules):
            ...     # validation logic
            ...     pass
        """

        def decorator(func: F) -> F:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    # Build context if it's a callable
                    if callable(context):
                        ctx = context(*args, **kwargs)
                    else:
                        ctx = context or {}

                    # Build suggestions if it's a callable
                    if callable(suggestions):
                        sugg = suggestions(*args, **kwargs)
                    else:
                        sugg = suggestions or []

                    return func(*args, **kwargs)
                except ExecutionError:
                    # Re-raise ExecutionError as-is
                    raise
                except Exception as e:
                    # Wrap other exceptions
                    raise ExecutionError(
                        f"Error during {operation}: {str(e)}",
                        context=ctx,
                        suggestions=sugg,
                    ) from e

            return wrapper  # type: ignore[return-value]

        return decorator
