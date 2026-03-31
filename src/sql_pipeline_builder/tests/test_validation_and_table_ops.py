"""
Unit tests for SQL validation utilities and table operations.
"""

from __future__ import annotations

from typing import Any

import pytest
from pipeline_builder_base.errors import DataError
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import select

from sql_pipeline_builder.table_operations import (
    create_schema_if_not_exists,
    fqn,
    read_table,
    table_exists,
    write_table,
)
from sql_pipeline_builder.validation.sql_validation import apply_sql_validation_rules

Base: Any = declarative_base()


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    category = Column(String)
    value = Column(Integer)


class TargetItem(Base):
    __tablename__ = "target_items"
    id = Column(Integer, primary_key=True)
    category = Column(String)
    value = Column(Integer)


@pytest.fixture
def sqlite_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session = Session(engine)
    session.add_all(
        [
            Item(id=1, category="valid", value=10),
            Item(id=2, category="invalid", value=-5),
            Item(id=3, category="valid", value=30),
        ]
    )
    session.commit()
    yield session
    session.close()


def test_apply_sql_validation_rules_filters_invalid_rows(sqlite_session):
    """Valid rows should satisfy all SQLAlchemy rule expressions."""
    rules = {
        "value": [Item.value > 0],
        "category": [Item.category == "valid"],
    }

    valid_query, invalid_query, stats = apply_sql_validation_rules(
        sqlite_session.query(Item),
        rules,
        step_name="items",
        session=sqlite_session,
    )

    assert stats.total_rows == 3
    assert stats.valid_rows == 2
    assert stats.invalid_rows == 1
    assert valid_query.count() == 2
    assert invalid_query.count() == 1


def test_write_table_handles_overwrite_and_append(sqlite_session):
    """write_table should overwrite existing data and append incremental rows."""
    source_query = sqlite_session.query(Item)

    rows_written = write_table(
        sqlite_session,
        source_query,
        schema="analytics",
        table="target_items",
        mode="overwrite",
        model_class=TargetItem,
        drop_existing_table=True,
    )
    assert rows_written == 3
    assert sqlite_session.query(TargetItem).count() == 3

    # Append only the new record
    new_item = Item(id=4, category="valid", value=40)
    sqlite_session.add(new_item)
    sqlite_session.commit()

    append_query = sqlite_session.query(Item).filter(Item.id == 4)
    rows_written = write_table(
        sqlite_session,
        append_query,
        schema="analytics",
        table="target_items",
        mode="append",
        model_class=TargetItem,
        drop_existing_table=False,
    )
    assert rows_written == 1
    assert sqlite_session.query(TargetItem).count() == 4

    # Overwrite with a subset, ensuring previous rows are removed
    subset_query = sqlite_session.query(Item).filter(Item.value > 20)
    rows_written = write_table(
        sqlite_session,
        subset_query,
        schema="analytics",
        table="target_items",
        mode="overwrite",
        model_class=TargetItem,
        drop_existing_table=False,
    )
    assert rows_written == 2
    assert sqlite_session.query(TargetItem).count() == 2


def test_write_table_accepts_core_select(sqlite_session):
    """write_table should accept SQLAlchemy Core Select (e.g. from Moltres .to_sqlalchemy())."""
    stmt = select(Item.id, Item.category, Item.value)
    rows_written = write_table(
        sqlite_session,
        stmt,
        schema="analytics",
        table="target_items",
        mode="overwrite",
        model_class=TargetItem,
        drop_existing_table=True,
    )
    assert rows_written == 3
    assert sqlite_session.query(TargetItem).count() == 3


def test_create_schema_if_not_exists_without_engine_raises_data_error():
    """create_schema_if_not_exists should raise DataError when no engine is bound."""

    class DummySession:
        """Session without bind or get_bind."""

        pass

    session = DummySession()

    with pytest.raises(DataError) as exc_info:
        create_schema_if_not_exists(session, schema="analytics")

    assert "Session has no bound engine" in str(exc_info.value)


def test_read_table_failure_raises_data_error(sqlite_session):
    """read_table should wrap underlying errors in DataError."""

    with pytest.raises(DataError) as exc_info:
        # Non-existent table triggers reflection error wrapped as DataError
        read_table(sqlite_session, schema="analytics", table="non_existent_table")

    message = str(exc_info.value)
    assert "Failed to read table analytics.non_existent_table" in message


def test_write_table_missing_model_class_raises_data_error(sqlite_session):
    """write_table should raise DataError when model_class is missing."""
    query = sqlite_session.query(Item)

    with pytest.raises(DataError) as exc_info:
        write_table(
            sqlite_session,
            query,
            schema="analytics",
            table="target_items",
            mode="overwrite",
            model_class=None,
        )

    message = str(exc_info.value)
    assert "model_class is required to write table 'target_items'" in message


def test_fqn_rejects_empty_parts():
    """fqn should reject empty schema/table values."""
    with pytest.raises(ValueError, match="cannot be empty"):
        fqn("", "table")
    with pytest.raises(ValueError, match="cannot be empty"):
        fqn("schema", "")


def test_write_table_rolls_back_on_bulk_insert_error(sqlite_session, monkeypatch):
    """write_table should rollback and wrap insertion failures in DataError."""
    query = sqlite_session.query(Item)

    def fail_bulk_insert(*args, **kwargs):
        raise RuntimeError("forced insert failure")

    monkeypatch.setattr(sqlite_session, "bulk_insert_mappings", fail_bulk_insert)

    with pytest.raises(DataError, match="forced insert failure"):
        write_table(
            sqlite_session,
            query,
            schema="analytics",
            table="target_items",
            mode="append",
            model_class=TargetItem,
            drop_existing_table=True,
        )

    assert sqlite_session.query(TargetItem).count() == 0


def test_table_exists_uses_fallback_when_inspector_errors(sqlite_session, monkeypatch):
    """table_exists should fall back to SQL queries if SQLAlchemy inspect fails."""
    import sqlalchemy

    # Ensure the table exists.
    assert sqlite_session.query(Item).count() == 3

    def fail_inspect(*args, **kwargs):
        raise RuntimeError("inspect failure")

    monkeypatch.setattr(sqlalchemy, "inspect", fail_inspect)
    assert table_exists(sqlite_session, schema=None, table="items") is True
