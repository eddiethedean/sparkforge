"""
SQL table operations for the pipeline framework.

This module contains functions for reading, writing, and managing tables
in SQL databases using SQLAlchemy.
"""

from __future__ import annotations

from typing import Any

from pipeline_builder_base.errors import DataError
from pipeline_builder_base.logging import PipelineLogger

logger = PipelineLogger("TableOperations")


def fqn(schema: str, table: str) -> str:
    """
    Create a fully qualified table name.

    Args:
        schema: Database schema name
        table: Table name

    Returns:
        Fully qualified table name

    Raises:
        ValueError: If schema or table is empty
    """
    if not schema or not table:
        raise ValueError("Schema and table names cannot be empty")
    return f"{schema}.{table}"


def create_schema_if_not_exists(session: Any, schema: str) -> None:
    """
    Create a database schema if it does not exist.

    Args:
        session: SQLAlchemy session (sync or async)
        schema: Schema name to create

    Raises:
        TableOperationError: If schema creation fails
    """
    try:
        from sqlalchemy import text

        # Check if schema exists
        if hasattr(session, "execute"):
            # Sync session
            result = session.execute(text(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema}'"))
            if result.scalar() is None:
                session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                session.commit()
                logger.info(f"Created schema: {schema}")
        else:
            # Async session - would need await
            logger.warning("Async schema creation not yet implemented")
    except Exception as e:
        raise DataError(f"Failed to create schema '{schema}': {e}") from e


def read_table(session: Any, schema: str, table: str, model_class: Any | None = None) -> Any:
    """
    Read a table and return a SQLAlchemy Query.

    Args:
        session: SQLAlchemy session (sync or async)
        schema: Schema name
        table: Table name
        model_class: Optional SQLAlchemy ORM model class

    Returns:
        SQLAlchemy Query object

    Raises:
        TableOperationError: If read operation fails
    """
    try:
        if model_class is not None:
            # Use ORM model
            return session.query(model_class)
        else:
            # Use table reflection
            from sqlalchemy import Table, MetaData
            metadata = MetaData()
            table_obj = Table(table, metadata, schema=schema, autoload_with=session.bind)
            return session.query(table_obj)
    except Exception as e:
        raise DataError(f"Failed to read table {schema}.{table}: {e}") from e


def write_table(
    session: Any,
    query: Any,
    schema: str,
    table: str,
    mode: str = "overwrite",
    model_class: Any | None = None,
) -> int:
    """
    Write query results to a SQL table.

    Args:
        session: SQLAlchemy session (sync or async)
        query: SQLAlchemy Query object to write
        schema: Schema name
        table: Table name
        mode: Write mode ("overwrite" or "append")
        model_class: Optional SQLAlchemy ORM model class

    Returns:
        Number of rows written

    Raises:
        TableOperationError: If write operation fails
    """
    try:
        from sqlalchemy import text, Table, MetaData, select
        from sqlalchemy.dialects import postgresql, mysql, sqlite

        table_fqn = fqn(schema, table)

        # Get data from query
        if hasattr(query, "all"):
            # Sync query
            rows = query.all()
        else:
            # Async query - would need await
            logger.warning("Async query execution not yet implemented")
            rows = []

        if not rows:
            logger.warning(f"No rows to write to {table_fqn}")
            return 0

        # Determine write mode
        if mode == "overwrite":
            # Delete existing data
            if model_class is not None:
                session.query(model_class).delete()
            else:
                table_obj = Table(table, MetaData(), schema=schema, autoload_with=session.bind)
                session.execute(table_obj.delete())
            session.commit()

        # Convert rows to dictionaries for insertion
        # Handle both ORM objects and query result tuples/Row objects
        row_dicts = []
        for row in rows:
            if hasattr(row, "__dict__") and not hasattr(row, "_fields"):
                # ORM object - use __dict__ but filter out SQLAlchemy internal attributes
                row_dict = {k: v for k, v in row.__dict__.items() if not k.startswith("_")}
            elif hasattr(row, "_asdict"):
                # Named tuple or Row object
                row_dict = row._asdict()
            elif hasattr(row, "_mapping"):
                # SQLAlchemy Row object (2.0 style)
                row_dict = dict(row._mapping)
            elif isinstance(row, (tuple, list)):
                # Tuple/list - need column names from query
                # This is tricky - we'd need to get column names from the query
                # For now, try to infer from model_class if available
                if model_class is not None:
                    # Get column names from model
                    column_names = [col.key for col in model_class.__table__.columns]
                    row_dict = dict(zip(column_names, row))
                else:
                    # Fallback: try to get from query column_descriptions
                    if hasattr(query, "column_descriptions"):
                        column_names = [desc["name"] for desc in query.column_descriptions]
                        row_dict = dict(zip(column_names, row))
                    else:
                        # Last resort: use positional keys
                        row_dict = {f"col_{i}": val for i, val in enumerate(row)}
            else:
                # Try dict() conversion
                try:
                    row_dict = dict(row)
                except (TypeError, ValueError):
                    # If all else fails, try to convert to dict using attributes
                    row_dict = {k: getattr(row, k) for k in dir(row) if not k.startswith("_")}
            row_dicts.append(row_dict)

        # Insert new data
        if model_class is not None:
            # Use ORM bulk insert
            session.bulk_insert_mappings(model_class, row_dicts)
        else:
            # Use raw insert - need to get table object first
            from sqlalchemy import Table, MetaData
            metadata = MetaData()
            table_obj = Table(table, metadata, schema=schema, autoload_with=session.bind)
            session.execute(table_obj.insert(), row_dicts)

        session.commit()
        rows_written = len(rows)
        logger.info(f"Successfully wrote {rows_written} rows to {table_fqn} in {mode} mode")
        return rows_written

    except Exception as e:
        session.rollback()
        raise DataError(f"Failed to write table {table_fqn}: {e}") from e


def table_exists(session: Any, schema: str, table: str) -> bool:
    """
    Check if a table exists.

    Args:
        session: SQLAlchemy session (sync or async)
        schema: Schema name
        table: Table name

    Returns:
        True if table exists, False otherwise
    """
    try:
        from sqlalchemy import inspect, text

        inspector = inspect(session.bind)
        return inspector.has_table(table, schema=schema)
    except Exception:
        # Fallback: try querying information_schema
        try:
            from sqlalchemy import text
            result = session.execute(
                text(f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'")
            )
            return result.scalar() is not None
        except Exception:
            return False

