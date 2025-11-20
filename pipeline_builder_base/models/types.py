"""
Type definitions and protocols for the Pipeline Builder models.
"""

from typing import Dict, List, Protocol, TypeVar, Union

# Specific types for model values instead of Any
ModelValue = Union[str, int, float, bool, List[str], Dict[str, str], None]
ResourceValue = Union[str, int, float, bool, List[str], Dict[str, str]]

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

