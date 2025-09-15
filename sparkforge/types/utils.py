# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

"""
Type utilities for SparkForge.

This module provides utility functions for type checking, validation,
and manipulation across all SparkForge modules.
"""

from __future__ import annotations

import functools
import inspect
from typing import (
    Any,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

T = TypeVar("T")

# ============================================================================
# Type Checking Utilities
# ============================================================================


def is_valid_type(value: Any, expected_type: type[T]) -> bool:
    """Check if value is valid for expected type."""
    try:
        if expected_type == Any:
            return True
        if expected_type == Optional[Any]:
            return True
        if get_origin(expected_type) == Union:
            return any(isinstance(value, arg) for arg in get_args(expected_type))
        if get_origin(expected_type) == Optional:
            return value is None or isinstance(value, get_args(expected_type)[0])
        return isinstance(value, expected_type)
    except Exception:
        return False


def get_type_hints(obj: Any) -> dict[str, Any]:
    """Get type hints for object."""
    try:
        return inspect.get_type_hints(obj)
    except Exception:
        return {}


def validate_type(value: Any, expected_type: type[T], name: str = "value") -> T:
    """Validate value against expected type."""
    if not is_valid_type(value, expected_type):
        raise TypeError(f"{name} must be of type {expected_type}, got {type(value)}")
    return value


def validate_types(**kwargs: Any) -> None:
    """Validate multiple values against their expected types."""
    for name, (value, expected_type) in kwargs.items():
        validate_type(value, expected_type, name)


# ============================================================================
# Type Conversion Utilities
# ============================================================================


def cast_safe(value: Any, target_type: type[T]) -> T | None:
    """Safely cast value to target type."""
    try:
        if target_type == Any:
            return value
        if target_type == Optional[Any]:
            return value
        if get_origin(target_type) == Union:
            for arg in get_args(target_type):
                if isinstance(value, arg):
                    return value
            return None
        if get_origin(target_type) == Optional:
            if value is None:
                return None
            return cast_safe(value, get_args(target_type)[0])
        return target_type(value)
    except Exception:
        return None


def convert_type(value: Any, target_type: type[T]) -> T:
    """Convert value to target type, raising error if fails."""
    try:
        if target_type == Any:
            return value
        if target_type == Optional[Any]:
            return value
        if get_origin(target_type) == Union:
            for arg in get_args(target_type):
                if isinstance(value, arg):
                    return value
            raise TypeError(f"Cannot convert {type(value)} to {target_type}")
        if get_origin(target_type) == Optional:
            if value is None:
                return None
            return convert_type(value, get_args(target_type)[0])
        return target_type(value)
    except Exception as e:
        raise TypeError(f"Cannot convert {type(value)} to {target_type}: {e}")


def normalize_type(value: Any, target_type: type[T]) -> T:
    """Normalize value to target type with fallback."""
    try:
        return convert_type(value, target_type)
    except TypeError:
        if target_type == str:
            return str(value)
        if target_type == int:
            return int(float(value))
        if target_type == float:
            return float(value)
        if target_type == bool:
            return bool(value)
        raise


# ============================================================================
# Type Inference Utilities
# ============================================================================


def infer_type(value: Any) -> type:
    """Infer type of value."""
    if value is None:
        return type(None)
    if isinstance(value, bool):
        return bool
    if isinstance(value, int):
        return int
    if isinstance(value, float):
        return float
    if isinstance(value, str):
        return str
    if isinstance(value, list):
        return List[Any]
    if isinstance(value, dict):
        return Dict[str, Any]
    if isinstance(value, tuple):
        return tuple
    if isinstance(value, set):
        return set
    return type(value)


def infer_return_type(func: callable) -> type:
    """Infer return type of function."""
    try:
        hints = get_type_hints(func)
        return hints.get("return", Any)
    except Exception:
        return Any


def infer_parameter_types(func: callable) -> dict[str, type]:
    """Infer parameter types of function."""
    try:
        hints = get_type_hints(func)
        return {name: hint for name, hint in hints.items() if name != "return"}
    except Exception:
        return {}


# ============================================================================
# Type Validation Decorators
# ============================================================================


def typed(func: callable) -> callable:
    """Decorator to add type checking to function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get type hints
        hints = get_type_hints(func)

        # Check positional arguments
        for i, (name, expected_type) in enumerate(hints.items()):
            if name == "return":
                continue
            if i < len(args):
                validate_type(args[i], expected_type, name)

        # Check keyword arguments
        for name, value in kwargs.items():
            if name in hints:
                expected_type = hints[name]
                validate_type(value, expected_type, name)

        # Execute function
        result = func(*args, **kwargs)

        # Check return type
        if "return" in hints:
            expected_return_type = hints["return"]
            validate_type(result, expected_return_type, "return")

        return result

    return wrapper


def typed_method(func: callable) -> callable:
    """Decorator to add type checking to method."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Get type hints
        hints = get_type_hints(func)

        # Check positional arguments (skip self)
        for i, (name, expected_type) in enumerate(hints.items()):
            if name == "return" or name == "self":
                continue
            if i < len(args):
                validate_type(args[i], expected_type, name)

        # Check keyword arguments
        for name, value in kwargs.items():
            if name in hints:
                expected_type = hints[name]
                validate_type(value, expected_type, name)

        # Execute method
        result = func(self, *args, **kwargs)

        # Check return type
        if "return" in hints:
            expected_return_type = hints["return"]
            validate_type(result, expected_return_type, "return")

        return result

    return wrapper


# ============================================================================
# Type-Safe Collections
# ============================================================================


class TypedDict(dict):
    """Type-safe dictionary with validation."""

    def __init__(self, value_type: type[T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value_type = value_type

    def __setitem__(self, key: str, value: T) -> None:
        validate_type(value, self.value_type, f"dict[{key}]")
        super().__setitem__(key, value)

    def update(self, other: dict[str, T]) -> None:
        for key, value in other.items():
            self[key] = value


class TypedList(list):
    """Type-safe list with validation."""

    def __init__(self, item_type: type[T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item_type = item_type

    def append(self, item: T) -> None:
        validate_type(item, self.item_type, "list item")
        super().append(item)

    def extend(self, items: list[T]) -> None:
        for item in items:
            self.append(item)

    def insert(self, index: int, item: T) -> None:
        validate_type(item, self.item_type, "list item")
        super().insert(index, item)


# ============================================================================
# Type-Safe Function Wrappers
# ============================================================================


def safe_call(func: callable, *args, **kwargs) -> Any | None:
    """Safely call function with type checking."""
    try:
        return func(*args, **kwargs)
    except TypeError as e:
        if "type" in str(e).lower():
            return None
        raise


def typed_call(func: callable, *args, **kwargs) -> Any:
    """Call function with type checking."""
    # Get type hints
    hints = get_type_hints(func)

    # Validate arguments
    for i, (name, expected_type) in enumerate(hints.items()):
        if name == "return":
            continue
        if i < len(args):
            validate_type(args[i], expected_type, name)

    # Validate keyword arguments
    for name, value in kwargs.items():
        if name in hints:
            expected_type = hints[name]
            validate_type(value, expected_type, name)

    # Call function
    result = func(*args, **kwargs)

    # Validate return type
    if "return" in hints:
        expected_return_type = hints["return"]
        validate_type(result, expected_return_type, "return")

    return result


# ============================================================================
# Type Information Utilities
# ============================================================================


def get_type_info(obj: Any) -> dict[str, Any]:
    """Get comprehensive type information for object."""
    return {
        "type": type(obj),
        "type_name": type(obj).__name__,
        "module": type(obj).__module__,
        "is_generic": hasattr(type(obj), "__args__"),
        "generic_args": getattr(type(obj), "__args__", ()),
        "is_optional": get_origin(type(obj)) == Optional,
        "is_union": get_origin(type(obj)) == Union,
        "is_list": isinstance(obj, list),
        "is_dict": isinstance(obj, dict),
        "is_callable": callable(obj),
        "is_none": obj is None,
    }


def get_function_info(func: callable) -> dict[str, Any]:
    """Get comprehensive function information."""
    hints = get_type_hints(func)
    return {
        "name": func.__name__,
        "module": func.__module__,
        "docstring": func.__doc__,
        "annotations": hints,
        "parameters": list(inspect.signature(func).parameters.keys()),
        "return_type": hints.get("return", Any),
        "is_generic": hasattr(func, "__type_params__"),
        "is_typed": hasattr(func, "__typed__"),
        "is_async": inspect.iscoroutinefunction(func),
    }


# ============================================================================
# Type Validation Helpers
# ============================================================================


def validate_not_none(value: Any, name: str = "value") -> Any:
    """Validate value is not None."""
    if value is None:
        raise ValueError(f"{name} cannot be None")
    return value


def validate_not_empty(value: Any, name: str = "value") -> Any:
    """Validate value is not empty."""
    if not value:
        raise ValueError(f"{name} cannot be empty")
    return value


def validate_positive(value: int | float, name: str = "value") -> int | float:
    """Validate value is positive."""
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def validate_range(
    value: int | float,
    min_val: int | float,
    max_val: int | float,
    name: str = "value",
) -> int | float:
    """Validate value is in range."""
    if not (min_val <= value <= max_val):
        raise ValueError(f"{name} must be between {min_val} and {max_val}, got {value}")
    return value
