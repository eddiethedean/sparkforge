"""
SQL compatibility layer for detecting sync vs async SQLAlchemy engines.

This module provides utilities for detecting whether a SQLAlchemy engine
supports async operations and for working with both sync and async sessions.
"""

from __future__ import annotations

from typing import Any, Type, Union

try:
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
    from sqlalchemy.orm import Session

    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    Engine = None  # type: ignore[misc, assignment]
    AsyncEngine = None  # type: ignore[misc, assignment]
    Session = None  # type: ignore[misc, assignment]
    AsyncSession = None  # type: ignore[misc, assignment]


def is_async_engine(engine: Any) -> bool:
    """
    Check if a SQLAlchemy engine supports async operations.

    Args:
        engine: SQLAlchemy engine instance

    Returns:
        True if engine is async, False if sync
    """
    if not HAS_SQLALCHEMY:
        return False
    
    try:
        from sqlalchemy.ext.asyncio import AsyncEngine
        return isinstance(engine, AsyncEngine)
    except ImportError:
        return False


def get_session_type(engine: Any) -> Type[Union[Session, AsyncSession]]:
    """
    Get the appropriate session type for an engine.

    Args:
        engine: SQLAlchemy engine instance

    Returns:
        Session class (Session or AsyncSession)
    """
    if not HAS_SQLALCHEMY:
        raise ImportError("SQLAlchemy is not installed")
    
    if is_async_engine(engine):
        from sqlalchemy.ext.asyncio import AsyncSession
        return AsyncSession
    else:
        from sqlalchemy.orm import Session
        return Session


def create_session(engine: Any) -> Union[Session, AsyncSession]:
    """
    Create a session from an engine.

    Args:
        engine: SQLAlchemy engine instance

    Returns:
        Session instance (sync or async)
    """
    if not HAS_SQLALCHEMY:
        raise ImportError("SQLAlchemy is not installed")
    
    if is_async_engine(engine):
        from sqlalchemy.ext.asyncio import AsyncSession
        return AsyncSession(engine)
    else:
        from sqlalchemy.orm import Session
        return Session(engine)

