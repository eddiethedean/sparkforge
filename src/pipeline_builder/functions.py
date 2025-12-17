"""
Functions interface for the framework.

This module provides a protocol for PySpark functions that can be injected
into framework components, allowing for better testability and flexibility.

# Depends on:
#   compat
"""

from __future__ import annotations

from typing import Optional, Protocol, Union, cast

from .protocols import ColumnProtocol


class FunctionsProtocol(Protocol):
    """Protocol for functions interface."""

    def col(self, col_name: str) -> ColumnProtocol:
        """Create a column reference."""
        ...

    def expr(self, expr: str) -> ColumnProtocol:
        """Create an expression from a string."""
        ...

    def lit(
        self, value: Union[str, int] | Union[float, Optional[bool]]
    ) -> ColumnProtocol:
        """Create a literal column."""
        ...

    def when(
        self,
        condition: ColumnProtocol,
        value: Union[str, int] | Union[float, Optional[bool]],
    ) -> ColumnProtocol:
        """Create a conditional expression."""
        ...

    def count(self, col: Union[str, ColumnProtocol] = "*") -> ColumnProtocol:
        """Create a count aggregation."""
        ...

    def countDistinct(self, *cols: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a count distinct aggregation."""
        ...

    def sum(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a sum aggregation."""
        ...

    def max(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a max aggregation."""
        ...

    def min(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a min aggregation."""
        ...

    def avg(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create an average aggregation."""
        ...

    def length(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a length function."""
        ...

    def date_trunc(
        self, format: str, col: Union[str, ColumnProtocol]
    ) -> ColumnProtocol:
        """Create a date truncation function."""
        ...

    def dayofweek(self, col: Union[str, ColumnProtocol]) -> ColumnProtocol:
        """Create a day of week function."""
        ...

    def current_timestamp(self) -> ColumnProtocol:
        """Create a current timestamp function."""
        ...


def get_default_functions() -> FunctionsProtocol:
    """Get the injected functions implementation."""

    from .compat import F

    return cast(FunctionsProtocol, F)
