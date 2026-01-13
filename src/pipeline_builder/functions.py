"""
Functions interface protocol for the framework.

This module provides a protocol-based interface for PySpark functions that can
be injected into framework components. This design allows for better testability,
flexibility, and engine abstraction.

**Key Features:**
    - **Protocol-Based**: Uses Python Protocol for type safety and duck typing
    - **Engine Abstraction**: Works with both real PySpark and mock implementations
    - **Injection Support**: Functions can be injected via engine configuration
    - **Type Safety**: Provides type hints for all function signatures

**Usage:**
    The functions protocol is typically accessed through the compat module:

    >>> from pipeline_builder.compat import F
    >>> df.select(F.col("id"), F.lit("test"))

    Or via the get_default_functions() helper:

    >>> from pipeline_builder.functions import get_default_functions
    >>> F = get_default_functions()
    >>> df.select(F.col("id"))

**Supported Functions:**
    The protocol defines common PySpark functions including:
    - Column operations: col, expr, lit, when
    - Aggregations: count, countDistinct, sum, max, min, avg
    - String functions: length
    - Date functions: date_trunc, dayofweek, current_timestamp

Dependencies:
    - compat: Compatibility layer for engine detection

Example:
    >>> from pipeline_builder.functions import FunctionsProtocol, get_default_functions
    >>> from pipeline_builder.compat import F
    >>>
    >>> # Get functions from compat module
    >>> functions = get_default_functions()
    >>> # Use functions for DataFrame operations
    >>> df.select(functions.col("id"), functions.lit("value"))
"""

from __future__ import annotations

from typing import Optional, Protocol, Union, cast

from .protocols import ColumnProtocol


class FunctionsProtocol(Protocol):
    """Protocol for PySpark functions interface.

    This protocol defines the interface that all functions implementations
    must satisfy. It includes common PySpark functions for column operations,
    aggregations, and transformations.

    **Implementation Requirements:**
        Any class or module implementing this protocol must provide all
        the methods defined here with matching signatures. The protocol
        supports both real PySpark functions and mock implementations
        for testing.

    **Common Implementations:**
        - PySpark `pyspark.sql.functions` module
        - Mock functions for testing (see test utilities)
        - Custom function wrappers for specific engines

    Example:
        >>> from pipeline_builder.functions import FunctionsProtocol
        >>> from pipeline_builder.compat import F
        >>>
        >>> # F implements FunctionsProtocol
        >>> def use_functions(f: FunctionsProtocol):
        ...     return f.col("id")
        >>>
        >>> result = use_functions(F)
    """

    def col(self, col_name: str) -> ColumnProtocol:
        """Create a column reference.

        Args:
            col_name: Name of the column to reference.

        Returns:
            Column expression representing the column reference.

        Example:
            >>> F.col("user_id")
        """
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
    """Get the injected functions implementation.

    Returns the functions module (F) from the configured engine. This is
    the same as accessing `F` directly from the compat module, but provides
    a typed interface for dependency injection.

    Returns:
        FunctionsProtocol instance from the configured engine. This is
        typically the PySpark functions module or a mock equivalent.

    Example:
        >>> from pipeline_builder.functions import get_default_functions
        >>> F = get_default_functions()
        >>> # Use F for DataFrame operations
        >>> df.select(F.col("id"), F.count("*"))
    """

    from .compat import F

    return cast(FunctionsProtocol, F)
