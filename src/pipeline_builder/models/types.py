"""
Type definitions and protocols for the Pipeline Builder models.

This module provides type aliases, protocols, and type definitions used
throughout the pipeline system. It defines the structure of validation rules,
transform functions, and model values.

Key Components:
    - **Type Aliases**: ColumnRules, TransformFunction, SilverTransformFunction,
      GoldTransformFunction for better code readability
    - **Protocols**: Validatable, Serializable for type checking and duck typing
    - **Model Types**: ModelValue, ColumnRule, ResourceValue for type safety

Dependencies:
    - compat: Compatibility layer for Spark/PySpark types

Example:
    >>> from pipeline_builder.models.types import ColumnRules, SilverTransformFunction
    >>> from pipeline_builder.compat import SparkSession, DataFrame
    >>>
    >>> # Define validation rules
    >>> rules: ColumnRules = {
    ...     "user_id": [F.col("user_id").isNotNull()],
    ...     "email": [F.col("email").contains("@")]
    ... }
    >>>
    >>> # Define transform function
    >>> def clean_data(spark: SparkSession, bronze_df: DataFrame, prior_silvers: dict) -> DataFrame:
    ...     return bronze_df.filter(F.col("user_id").isNotNull())
    >>>
    >>> transform: SilverTransformFunction = clean_data
"""

from typing import Callable, Dict, List, Optional, Protocol, TypeVar, Union

from ..compat import Column, DataFrame, SparkSession

# Specific types for model values instead of Any
ModelValue = Union[str, int, float, bool, List[str], Dict[str, str], None]
ColumnRule = Union[DataFrame, str, bool]  # PySpark Column, string, or boolean
ResourceValue = Union[str, int, float, bool, List[str], Dict[str, str]]

# Type aliases for better readability
ColumnRules = Dict[str, List[Union[str, Column]]]
TransformFunction = Callable[[DataFrame], DataFrame]
SilverTransformFunction = Callable[
    [SparkSession, DataFrame, Dict[str, DataFrame], Optional[Dict[str, DataFrame]]],
    DataFrame,
]
GoldTransformFunction = Callable[
    [SparkSession, Dict[str, DataFrame], Optional[Dict[str, DataFrame]]], DataFrame
]

# Generic type for pipeline results
T = TypeVar("T")


class Validatable(Protocol):
    """Protocol for objects that can be validated.

    This protocol defines the interface for objects that support validation.
    Any class implementing this protocol must provide a `validate` method
    that checks the object's state and raises an exception if invalid.

    Example:
        >>> class MyModel:
        ...     def validate(self) -> None:
        ...         if not self.name:
        ...             raise ValueError("Name required")
        >>>
        >>> def check_valid(obj: Validatable) -> None:
        ...     obj.validate()
        >>>
        >>> model = MyModel()
        >>> check_valid(model)  # Type checker accepts this
    """

    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid.

        Raises:
            ValidationError: If the object is invalid. Subclasses should
                raise specific error types (e.g., PipelineValidationError).
        """
        ...


class Serializable(Protocol):
    """Protocol for objects that can be serialized.

    This protocol defines the interface for objects that support serialization
    to dictionaries and JSON strings. Any class implementing this protocol
    must provide `to_dict` and `to_json` methods.

    Example:
        >>> class MyModel:
        ...     def to_dict(self) -> Dict[str, ModelValue]:
        ...         return {"name": self.name}
        ...
        ...     def to_json(self) -> str:
        ...         return json.dumps(self.to_dict())
        >>>
        >>> def serialize(obj: Serializable) -> str:
        ...     return obj.to_json()
        >>>
        >>> model = MyModel()
        >>> serialize(model)  # Type checker accepts this
    """

    def to_dict(self) -> Dict[str, ModelValue]:
        """Convert object to dictionary.

        Returns:
            Dictionary representation of the object with all fields
            converted to primitive types or dictionaries.
        """
        ...

    def to_json(self) -> str:
        """Convert object to JSON string.

        Returns:
            JSON string representation of the object.
        """
        ...
