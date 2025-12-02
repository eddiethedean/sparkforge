"""
Unit tests for SQL validation utilities and table operations.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from sql_pipeline_builder.table_operations import write_table
from sql_pipeline_builder.validation.sql_validation import apply_sql_validation_rules

Base = declarative_base()


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
