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
Generic type definitions for SparkForge.

This module provides generic types and type variables for better
type safety and reusability across all SparkForge modules.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, List, Protocol, TypeVar, Union

# ============================================================================
# Type Variables
# ============================================================================

# Basic type variables
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")
L = TypeVar("L")

# Bounded type variables
Numeric = TypeVar("Numeric", bound=Union[int, float])
String = TypeVar("String", bound=str)
DictLike = TypeVar("DictLike", bound=Dict[str, Any])
ListLike = TypeVar("ListLike", bound=List[Any])
CallableLike = TypeVar("CallableLike", bound=Callable[..., Any])

# Specific type variables
DataFrame = TypeVar("DataFrame")
SparkSession = TypeVar("SparkSession")
StepName = TypeVar("StepName", bound=str)
PipelineId = TypeVar("PipelineId", bound=str)
ExecutionId = TypeVar("ExecutionId", bound=str)

# ============================================================================
# Generic Classes
# ============================================================================


class Result(Generic[T]):
    """Generic result container."""

    def __init__(self, value: T, success: bool = True, error: str | None = None):
        self.value = value
        self.success = success
        self.error = error

    def __bool__(self) -> bool:
        return self.success

    def __str__(self) -> str:
        if self.success:
            return f"Result(success=True, value={self.value})"
        else:
            return f"Result(success=False, error={self.error})"


class OptionalResult(Generic[T]):
    """Generic optional result container."""

    def __init__(self, value: T | None = None, error: str | None = None):
        self.value = value
        self.error = error

    @property
    def is_some(self) -> bool:
        return self.value is not None

    @property
    def is_none(self) -> bool:
        return self.value is None

    def unwrap(self) -> T:
        """Unwrap the value, raising error if None."""
        if self.value is None:
            raise ValueError(f"Attempted to unwrap None value: {self.error}")
        return self.value

    def unwrap_or(self, default: T) -> T:
        """Unwrap the value or return default."""
        return self.value if self.value is not None else default


class Either(Generic[L, R]):
    """Generic either type for error handling."""

    def __init__(self, left: L | None = None, right: R | None = None):
        if (left is None) == (right is None):
            raise ValueError("Either must have exactly one of left or right")
        self.left = left
        self.right = right

    @property
    def is_left(self) -> bool:
        return self.left is not None

    @property
    def is_right(self) -> bool:
        return self.right is not None

    def map_left(self, func: Callable[[L], T]) -> Either[T, R]:
        """Map over left value."""
        if self.is_left:
            return Either(left=func(self.left))
        return Either(right=self.right)

    def map_right(self, func: Callable[[R], T]) -> Either[L, T]:
        """Map over right value."""
        if self.is_right:
            return Either(right=func(self.right))
        return Either(left=self.left)


class Try(Generic[T]):
    """Generic try type for exception handling."""

    def __init__(self, value: T | None = None, exception: Exception | None = None):
        if (value is None) == (exception is None):
            raise ValueError("Try must have exactly one of value or exception")
        self.value = value
        self.exception = exception

    @property
    def is_success(self) -> bool:
        return self.value is not None

    @property
    def is_failure(self) -> bool:
        return self.exception is not None

    def get(self) -> T:
        """Get the value, raising exception if failed."""
        if self.is_failure:
            raise self.exception
        return self.value

    def get_or_else(self, default: T) -> T:
        """Get the value or return default."""
        return self.value if self.is_success else default

    def map(self, func: Callable[[T], R]) -> Try[R]:
        """Map over the value."""
        if self.is_success:
            try:
                return Try(value=func(self.value))
            except Exception as e:
                return Try(exception=e)
        return Try(exception=self.exception)

    def flat_map(self, func: Callable[[T], Try[R]]) -> Try[R]:
        """Flat map over the value."""
        if self.is_success:
            return func(self.value)
        return Try(exception=self.exception)


# ============================================================================
# Generic Protocols
# ============================================================================


class Functor(Protocol[T]):
    """Protocol for functor types."""

    def map(self, func: Callable[[T], R]) -> Functor[R]:
        """Map function over the value."""
        ...


class Monad(Protocol[T]):
    """Protocol for monad types."""

    def flat_map(self, func: Callable[[T], Monad[R]]) -> Monad[R]:
        """Flat map function over the value."""
        ...

    def unit(self, value: T) -> Monad[T]:
        """Wrap value in monad."""
        ...


class Foldable(Protocol[T]):
    """Protocol for foldable types."""

    def fold_left(self, initial: R, func: Callable[[R, T], R]) -> R:
        """Fold left over the values."""
        ...

    def fold_right(self, initial: R, func: Callable[[T, R], R]) -> R:
        """Fold right over the values."""
        ...


# ============================================================================
# Generic Utilities
# ============================================================================


def identity(value: T) -> T:
    """Identity function."""
    return value


def constant(value: T) -> Callable[[Any], T]:
    """Return function that always returns the given value."""
    return lambda _: value


def compose(f: Callable[[B], C], g: Callable[[A], B]) -> Callable[[A], C]:
    """Compose two functions."""
    return lambda x: f(g(x))


def pipe(*funcs: Callable[[T], T]) -> Callable[[T], T]:
    """Pipe multiple functions together."""

    def piped(value: T) -> T:
        result = value
        for func in funcs:
            result = func(result)
        return result

    return piped


# ============================================================================
# Type-Safe Collections
# ============================================================================


class TypedDict(Generic[K, V], Dict[K, V]):
    """Type-safe dictionary."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_typed(self, key: K, default: V = None) -> V:
        """Get value with type safety."""
        return super().get(key, default)

    def set_typed(self, key: K, value: V) -> None:
        """Set value with type safety."""
        super().__setitem__(key, value)


class TypedList(Generic[T], List[T]):
    """Type-safe list."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def append_typed(self, item: T) -> None:
        """Append item with type safety."""
        super().append(item)

    def get_typed(self, index: int) -> T:
        """Get item with type safety."""
        return super().__getitem__(index)


# ============================================================================
# Generic Decorators
# ============================================================================


def typed_function(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to mark function as typed."""
    func.__typed__ = True
    return func


def generic_method(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to mark method as generic."""
    func.__generic__ = True
    return func


# ============================================================================
# Type Conversion Utilities
# ============================================================================


def safe_cast(value: Any, target_type: type[T]) -> T | None:
    """Safely cast value to target type."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return None


def force_cast(value: Any, target_type: type[T]) -> T:
    """Force cast value to target type, raising error if fails."""
    try:
        return target_type(value)
    except (ValueError, TypeError) as e:
        raise TypeError(f"Cannot cast {type(value)} to {target_type}: {e}")


def is_instance_of(value: Any, target_type: type[T]) -> bool:
    """Check if value is instance of target type."""
    return isinstance(value, target_type)


# ============================================================================
# Generic Type Checking
# ============================================================================


def get_type_name(value: Any) -> str:
    """Get type name of value."""
    return type(value).__name__


def get_generic_args(cls: type) -> tuple:
    """Get generic arguments of class."""
    return getattr(cls, "__args__", ())


def is_generic_type(cls: type) -> bool:
    """Check if class is generic type."""
    return hasattr(cls, "__args__")


def get_type_vars(cls: type) -> tuple:
    """Get type variables of class."""
    return getattr(cls, "__parameters__", ())
