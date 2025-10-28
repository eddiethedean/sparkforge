"""
Functions interface for the framework.

This module provides a protocol for PySpark functions that can be injected
into framework components, allowing for better testability and flexibility.

# Depends on:
#   compat
"""

from __future__ import annotations

from typing import Protocol

from .compat import Column


class FunctionsProtocol(Protocol):
    """Protocol for PySpark functions interface."""

    def col(self, col_name: str) -> Column:
        """Create a column reference."""
        ...

    def expr(self, expr: str) -> Column:
        """Create an expression from a string."""
        ...

    def lit(self, value: str | int | float | bool | None) -> Column:
        """Create a literal column."""
        ...

    def when(self, condition: Column, value: str | int | float | bool | None) -> Column:
        """Create a conditional expression."""
        ...

    def count(self, col: str | Column = "*") -> Column:
        """Create a count aggregation."""
        ...

    def countDistinct(self, *cols: str | Column) -> Column:
        """Create a count distinct aggregation."""
        ...

    def sum(self, col: str | Column) -> Column:
        """Create a sum aggregation."""
        ...

    def max(self, col: str | Column) -> Column:
        """Create a max aggregation."""
        ...

    def min(self, col: str | Column) -> Column:
        """Create a min aggregation."""
        ...

    def avg(self, col: str | Column) -> Column:
        """Create an average aggregation."""
        ...

    def length(self, col: str | Column) -> Column:
        """Create a length function."""
        ...

    def date_trunc(self, format: str, col: str | Column) -> Column:
        """Create a date truncation function."""
        ...

    def dayofweek(self, col: str | Column) -> Column:
        """Create a day of week function."""
        ...

    def current_timestamp(self) -> Column:
        """Create a current timestamp function."""
        ...


def get_default_functions() -> FunctionsProtocol:
    """Get the default PySpark functions implementation.

    Returns the functions from the current compatibility layer.
    """
    from .compat import F

    return F
