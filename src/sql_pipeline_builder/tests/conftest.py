"""
Shared pytest fixtures for sql_pipeline_builder tests.
"""

from __future__ import annotations

from typing import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture
def sqlite_engine():
    """Create an isolated in-memory SQLite engine per test."""
    return create_engine("sqlite:///:memory:", echo=False)


@pytest.fixture
def sqlite_session(sqlite_engine) -> Generator[Session, None, None]:
    """
    Create an isolated SQLAlchemy session with explicit transaction boundaries.
    """
    SessionLocal = sessionmaker(bind=sqlite_engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
