"""
Type definitions for SQL pipeline builder.

This module defines the types used for SQL transform functions and validation rules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol, Union

# TypeAlias is available in Python 3.10+, use typing_extensions for 3.8/3.9
if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    try:
        from sqlalchemy.orm import Query, Session
        from sqlalchemy.sql import ColumnElement

        HAS_SQLALCHEMY = True
    except ImportError:
        HAS_SQLALCHEMY = False
        Query = Any  # type: ignore[misc, assignment]
        Session = Any  # type: ignore[misc, assignment]
        ColumnElement = Any  # type: ignore[misc, assignment]

    # Type for SQLAlchemy validation rules
    # Can be ColumnElement expressions like User.email.is_not(None) or column('age').between(18, 65)
    # Always use Union[ColumnElement, Any] to cover both cases - if SQLAlchemy not available, ColumnElement = Any
    SqlValidationRule: TypeAlias = Union[ColumnElement, Any]
    SilverTransformFunction: TypeAlias = Union[
        Callable[[Session, Query, Dict[str, Query]], Query],
        Callable[[Any, Any, Dict[str, Any]], Any],
    ]
    GoldTransformFunction: TypeAlias = Union[
        Callable[[Session, Dict[str, Query]], Query],
        Callable[[Any, Dict[str, Any]], Any],
    ]
# Note: These types are only used in type annotations, not at runtime
# With `from __future__ import annotations`, type annotations are strings and don't need runtime values

# Runtime check for SQLAlchemy availability
try:
    import importlib.util

    HAS_SQLALCHEMY = importlib.util.find_spec("sqlalchemy") is not None
except ImportError:
    HAS_SQLALCHEMY = False

# Define types at runtime (needed for imports and type annotations)
# At runtime, they're just Any/Callable since we can't know the actual types without SQLAlchemy
# At type checking time, the TypeAlias from TYPE_CHECKING block is used
SqlValidationRule = Any  # type: ignore[misc, assignment]
SilverTransformFunction = Callable[[Any, Any, Dict[str, Any]], Any]  # type: ignore[misc, assignment]
GoldTransformFunction = Callable[[Any, Dict[str, Any]], Any]  # type: ignore[misc, assignment]

# This uses SqlValidationRule which is defined above
# At type checking time, it uses the TypeAlias from TYPE_CHECKING
# At runtime, it uses the Any definition above
SqlColumnRules = Dict[str, List[SqlValidationRule]]


# Protocol for SQL step objects
class SqlStepProtocol(Protocol):
    """Protocol for SQL steps."""

    name: str
    rules: SqlColumnRules

    def validate(self) -> None:
        """Validate the step."""
        ...


class SqlBronzeStepProtocol(SqlStepProtocol, Protocol):
    """Protocol for SQL bronze steps."""

    incremental_col: Optional[str]


class SqlSilverStepProtocol(SqlStepProtocol, Protocol):
    """Protocol for SQL silver steps."""

    source_bronze: str
    table_name: str
    transform: SilverTransformFunction


class SqlGoldStepProtocol(SqlStepProtocol, Protocol):
    """Protocol for SQL gold steps."""

    source_silvers: Optional[list[str]]
    table_name: str
    transform: GoldTransformFunction
