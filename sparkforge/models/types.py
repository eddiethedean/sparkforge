"""
Type definitions and protocols for the Pipeline Builder models.
"""

from typing import Any, Callable, Dict, List, Protocol, TypeVar, Union

from pyspark.sql import Column, DataFrame, SparkSession

# Specific types for model values instead of Any
ModelValue = Union[str, int, float, bool, List[str], Dict[str, str], None]
ColumnRule = Union[DataFrame, str, bool]  # PySpark Column, string, or boolean
ResourceValue = Union[str, int, float, bool, List[str], Dict[str, str]]

# Type aliases for better readability
ColumnRules = Dict[str, List[Union[str, Column]]]
TransformFunction = Callable[[DataFrame], DataFrame]
SilverTransformFunction = Callable[
    [SparkSession, DataFrame, Dict[str, DataFrame]], DataFrame
]
GoldTransformFunction = Callable[[SparkSession, Dict[str, DataFrame]], DataFrame]

# Generic type for pipeline results
T = TypeVar("T")


class Validatable(Protocol):
    """Protocol for objects that can be validated."""

    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid."""
        ...


class Serializable(Protocol):
    """Protocol for objects that can be serialized."""

    def to_dict(self) -> Dict[str, ModelValue]:
        """Convert object to dictionary."""
        ...

    def to_json(self) -> str:
        """Convert object to JSON string."""
        ...
