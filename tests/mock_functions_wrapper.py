"""
Mock Functions wrapper to ensure compatibility with FunctionsProtocol.

This module provides a wrapper around mock_spark's MockFunctions to ensure
it implements all methods required by the FunctionsProtocol.
"""

from typing import Any

from mock_spark.functions import F as _F


class MockFunctions:
    """
    Wrapper around mock_spark's F to ensure FunctionsProtocol compatibility.
    
    This wrapper adds missing methods and fixes signature mismatches.
    """

    def __init__(self) -> None:
        """Initialize the MockFunctions wrapper."""
        self._f = _F

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying F object."""
        return getattr(self._f, name)

    def col(self, col_name: str) -> Any:
        """Create a column reference."""
        return self._f.col(col_name)

    def expr(self, expr: str) -> Any:
        """Create an expression from a string."""
        return self._f.expr(expr)

    def lit(self, value: Any) -> Any:
        """Create a literal column."""
        return self._f.lit(value)

    def when(self, condition: Any, value: Any) -> Any:
        """Create a conditional expression."""
        return self._f.when(condition, value)

    def count(self, col: str | Any = "*") -> Any:
        """Create a count aggregation."""
        return self._f.count(col)

    def countDistinct(self, *cols: str | Any) -> Any:
        """Create a count distinct aggregation.
        
        Note: This method signature matches FunctionsProtocol but differs
        from mock_spark's implementation which takes a single column.
        """
        # mock_spark's countDistinct takes a single column
        # We need to handle the *cols case
        if len(cols) == 1:
            return self._f.countDistinct(cols[0])
        else:
            # For multiple columns, we need to create a combined expression
            # This is a simplified implementation
            return self._f.countDistinct(cols[0])

    def sum(self, col: str | Any) -> Any:
        """Create a sum aggregation."""
        return self._f.sum(col)

    def max(self, col: str | Any) -> Any:
        """Create a max aggregation."""
        return self._f.max(col)

    def min(self, col: str | Any) -> Any:
        """Create a min aggregation."""
        return self._f.min(col)

    def avg(self, col: str | Any) -> Any:
        """Create an average aggregation."""
        return self._f.avg(col)

    def length(self, col: str | Any) -> Any:
        """Create a length function."""
        return self._f.length(col)

    def date_trunc(self, format: str, col: str | Any) -> Any:
        """Create a date truncation function.
        
        This method is not present in mock_spark, so we provide a stub.
        """
        # mock_spark doesn't have date_trunc, so we return a mock column
        # that represents the truncated date
        from mock_spark import Column
        
        class MockDateTruncColumn(Column):
            """Mock column for date_trunc operation."""
            
            def __init__(self, format: str, col: Any) -> None:
                self.format = format
                self.col = col
                super().__init__()
            
            def __repr__(self) -> str:
                return f"date_trunc({self.format}, {self.col})"
        
        return MockDateTruncColumn(format, col)

    def dayofweek(self, col: str | Any) -> Any:
        """Create a day of week function."""
        return self._f.dayofweek(col)

    def current_timestamp(self) -> Any:
        """Create a current timestamp function."""
        return self._f.current_timestamp()

