"""
Moltres integration helpers for sql_pipeline_builder.

This module keeps Moltres usage localized so the rest of the package can
interoperate via SQLAlchemy (Core Select / ORM Query) where needed.
"""

from __future__ import annotations

from typing import Any, Optional


def is_moltres_dataframe(obj: Any) -> bool:
    """
    Best-effort duck-typing check for a Moltres DataFrame-like object.

    We avoid strict imports/types here to keep runtime coupling minimal.
    """

    if obj is None:
        return False
    # Moltres DataFrames support: where(), collect(), to_sqlalchemy()
    return all(hasattr(obj, attr) for attr in ("where", "collect", "to_sqlalchemy"))


def moltres_database_from_session(session: Any) -> Any:
    """
    Create a Moltres Database from a SQLAlchemy Session.

    Moltres docs: Database.from_session(session)
    """

    from moltres import Database

    # Moltres supports Database.from_session(session) per docs, but some versions
    # may have issues. Fall back to engine/connection-based factories.
    try:
        return Database.from_session(session)
    except Exception:
        bind = getattr(session, "bind", None) or getattr(session, "get_bind", lambda: None)()
        if bind is not None:
            return Database.from_engine(bind)
        conn = getattr(session, "connection", lambda: None)()
        if conn is not None:
            return Database.from_connection(conn)
        raise


def to_sqlalchemy_select(obj: Any, *, dialect: Optional[str] = None) -> Any:
    """
    Convert a Moltres DataFrame (or pass-through SQLAlchemy objects) into a SQLAlchemy Select/Query.
    """

    if is_moltres_dataframe(obj):
        # Moltres DataFrame API: df.to_sqlalchemy(dialect=...)
        if dialect is None:
            return obj.to_sqlalchemy()
        return obj.to_sqlalchemy(dialect=dialect)
    return obj

