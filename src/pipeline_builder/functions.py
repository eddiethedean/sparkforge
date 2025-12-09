"""
Functions interface for the framework.

This module provides a protocol for PySpark functions that can be injected
into framework components, allowing for better testability and flexibility.

# Depends on:
#   compat
"""

from __future__ import annotations

from typing import Protocol, cast

from .compat import Column


class FunctionsProtocol(Protocol):
    """Protocol for PySpark functions interface."""

    def col(self, col_name: str) -> Column:  # type: ignore[valid-type]
        """Create a column reference."""
        ...

    def expr(self, expr: str) -> Column:  # type: ignore[valid-type]
        """Create an expression from a string."""
        ...

    def lit(self, value: str | int | float | bool | None) -> Column:  # type: ignore[valid-type]
        """Create a literal column."""
        ...

    def when(
        self,
        condition: Column,
        value: str | int | float | bool | None,
    ) -> Column:  # type: ignore[valid-type]
        """Create a conditional expression."""
        ...

    def count(self, col: str | Column = "*") -> Column:  # type: ignore[valid-type]
        """Create a count aggregation."""
        ...

    def countDistinct(self, *cols: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a count distinct aggregation."""
        ...

    def sum(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a sum aggregation."""
        ...

    def max(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a max aggregation."""
        ...

    def min(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a min aggregation."""
        ...

    def avg(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create an average aggregation."""
        ...

    def length(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a length function."""
        ...

    def date_trunc(self, format: str, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a date truncation function."""
        ...

    def dayofweek(self, col: str | Column) -> Column:  # type: ignore[valid-type]
        """Create a day of week function."""
        ...

    def current_timestamp(self) -> Column:  # type: ignore[valid-type]
        """Create a current timestamp function."""
        ...


def get_default_functions() -> FunctionsProtocol:
    """Get the default PySpark functions implementation.

    Returns the functions from the current compatibility layer.
    """
    from .compat import F

    # F is dynamically selected at runtime, so we cast to FunctionsProtocol
    return cast(FunctionsProtocol, F)
