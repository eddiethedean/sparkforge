"""
Type definitions for SQL pipeline builder.

This module defines the types used for SQL transform functions and validation rules.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Protocol, Union

try:
    from sqlalchemy.orm import Query, Session
    from sqlalchemy.sql import ColumnElement

    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    Query = None  # type: ignore[misc, assignment]
    Session = None  # type: ignore[misc, assignment]
    ColumnElement = None  # type: ignore[misc, assignment]


# Type for SQLAlchemy validation rules
# Can be ColumnElement expressions like User.email.is_not(None) or column('age').between(18, 65)
if HAS_SQLALCHEMY:
    SqlValidationRule = Union[ColumnElement, Any]
else:
    SqlValidationRule = Any
SqlColumnRules = Dict[str, List[SqlValidationRule]]

# Type for SQL transform functions
# Silver: (session, bronze_query, silvers_dict) -> Query
# Gold: (session, silvers_dict) -> Query
if HAS_SQLALCHEMY:
    SilverTransformFunction = Callable[[Session, Query, Dict[str, Query]], Query]
    GoldTransformFunction = Callable[[Session, Dict[str, Query]], Query]
else:
    SilverTransformFunction = Callable[[Any, Any, Dict[str, Any]], Any]
    GoldTransformFunction = Callable[[Any, Dict[str, Any]], Any]


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
