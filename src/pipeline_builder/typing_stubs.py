"""
Type stubs and Protocol classes for dynamic PySpark/mock-spark attributes.

This module provides Protocol classes that define the expected interface
for DataFrame, Column, and SparkSession objects, allowing mypy to understand
dynamic attributes that are added at runtime.
"""

from typing import Any, Protocol


class DataFrameProtocol(Protocol):
    """Protocol for DataFrame-like objects with dynamic attributes."""

    def write(self) -> Any:
        """Get DataFrameWriter for writing data."""
        ...

    def filter(self, condition: Any) -> Any:
        """Filter rows based on condition."""
        ...

    def select(self, *cols: Any) -> Any:
        """Select columns."""
        ...

    def withColumn(self, colName: str, col: Any) -> Any:
        """Add or replace a column."""
        ...

    def withColumnRenamed(self, existing: str, new: str) -> Any:
        """Rename a column."""
        ...

    def count(self) -> int:
        """Count rows."""
        ...

    def collect(self) -> list[Any]:
        """Collect rows to driver."""
        ...

    def cache(self) -> Any:
        """Cache the DataFrame."""
        ...

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Show DataFrame contents."""
        ...


class ColumnProtocol(Protocol):
    """Protocol for Column-like objects with dynamic attributes."""

    def isNotNull(self) -> Any:
        """Check if column is not null."""
        ...

    def isNull(self) -> Any:
        """Check if column is null."""
        ...

    def __eq__(self, other: Any) -> Any:
        """Equality comparison."""
        ...

    def __ne__(self, other: Any) -> Any:
        """Inequality comparison."""
        ...

    def __gt__(self, other: Any) -> Any:
        """Greater than comparison."""
        ...

    def __lt__(self, other: Any) -> Any:
        """Less than comparison."""
        ...

    def __ge__(self, other: Any) -> Any:
        """Greater than or equal comparison."""
        ...

    def __le__(self, other: Any) -> Any:
        """Less than or equal comparison."""
        ...

    def __add__(self, other: Any) -> Any:
        """Addition operator."""
        ...

    def __sub__(self, other: Any) -> Any:
        """Subtraction operator."""
        ...

    def __mul__(self, other: Any) -> Any:
        """Multiplication operator."""
        ...

    def __div__(self, other: Any) -> Any:
        """Division operator."""
        ...

    def cast(self, dataType: Any) -> Any:
        """Cast column to different type."""
        ...


class SparkSessionProtocol(Protocol):
    """Protocol for SparkSession-like objects with dynamic attributes."""

    catalog: Any
    """Catalog for accessing databases and tables."""

    def table(self, name: str) -> Any:
        """Get a table as DataFrame."""
        ...

    def createDataFrame(
        self, data: Any, schema: Any = None, samplingRatio: Any = None
    ) -> Any:
        """Create DataFrame from data."""
        ...

    def sql(self, sqlQuery: str) -> Any:
        """Execute SQL query and return DataFrame."""
        ...

    def stop(self) -> None:
        """Stop the SparkSession."""
        ...

    def sparkContext(self) -> Any:
        """Get SparkContext."""
        ...

    def conf(self) -> Any:
        """Get SparkConf."""
        ...
