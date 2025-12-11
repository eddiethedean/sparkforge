"""
SQL table operations for the pipeline framework.

This module contains functions for reading, writing, and managing tables
in SQL databases using SQLAlchemy.
"""

from __future__ import annotations

from typing import Any, Optional

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


def create_schema_if_not_exists(session: Any, schema: Optional[str]) -> None:
    """
    Create a database schema if it does not exist.

    Args:
        session: SQLAlchemy session (sync or async)
        schema: Schema name to create

    Raises:
        TableOperationError: If schema creation fails
    """
    if not schema:
        # Some databases (like SQLite) don't support schemas
        return

    try:
        from sqlalchemy import inspect, text

        engine = (
            getattr(session, "bind", None)
            or getattr(session, "get_bind", lambda: None)()
        )
        if engine is None:
            raise DataError("Session has no bound engine for schema creation")

        dialect_name = getattr(engine.dialect, "name", "")
        if dialect_name == "sqlite":
            logger.debug("SQLite dialect detected — skipping schema creation")
            return

        inspector = inspect(engine)
        if schema in inspector.get_schema_names():
            return

        if hasattr(session, "execute"):
            session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            session.commit()
            logger.info(f"Created schema: {schema}")
        else:
            logger.warning("Async schema creation not yet implemented")
    except Exception as e:
        raise DataError(f"Failed to create schema '{schema}': {e}") from e


def read_table(
    session: Any, schema: str, table: str, model_class: Optional[Any] = None
) -> Any:
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
            from sqlalchemy import MetaData, Table

            metadata = MetaData()
            table_obj = Table(
                table, metadata, schema=schema, autoload_with=session.bind
            )
            return session.query(table_obj)
    except Exception as e:
        raise DataError(f"Failed to read table {schema}.{table}: {e}") from e


def _drop_table(session: Any, schema: Optional[str], table: str) -> None:
    """Drop a table if it exists."""
    from sqlalchemy import MetaData, Table

    metadata = MetaData()
    table_obj = Table(table, metadata, schema=schema, autoload_with=session.bind)
    table_obj.drop(bind=session.bind, checkfirst=True)
    if schema:
        logger.info(f"Dropped existing table '{schema}.{table}'")
    else:
        logger.info(f"Dropped existing table '{table}'")


def _ensure_table_from_model(
    session: Any,
    schema: Optional[str],
    table: str,
    model_class: Optional[Any],
    drop_existing: bool = False,
) -> None:
    """
    Ensure a SQL table exists by creating it from the provided SQLAlchemy model.

    Args:
        session: SQLAlchemy session (sync or async)
        schema: Schema name
        table: Table name
        model_class: SQLAlchemy ORM model class

    Raises:
        DataError: If model_class is missing or table creation fails
    """
    from sqlalchemy import MetaData

    if model_class is None or not hasattr(model_class, "__table__"):
        raise DataError(
            f"model_class with __table__ metadata is required to create table '{table}'"
        )

    engine = getattr(session, "bind", None)
    dialect_name = getattr(engine.dialect, "name", "") if engine else ""

    target_schema = schema or getattr(model_class.__table__, "schema", None)
    supports_schema = target_schema is not None and dialect_name not in {"sqlite"}
    ddl_schema = target_schema if supports_schema else None

    if table_exists(session, ddl_schema, table):
        if drop_existing:
            _drop_table(session, ddl_schema, table)
        else:
            return

    create_schema_if_not_exists(session, ddl_schema)

    metadata = MetaData()
    table_copy = model_class.__table__.tometadata(metadata, schema=ddl_schema)
    try:
        table_copy.create(bind=session.bind, checkfirst=True)
        if target_schema:
            logger.info(f"Created table '{target_schema}.{table}' from model")
        else:
            logger.info(f"Created table '{table}' from model")
    except Exception as exc:
        raise DataError(f"Failed to create table '{table}': {exc}") from exc


def write_table(
    session: Any,
    query: Any,
    schema: str,
    table: str,
    mode: str = "overwrite",
    model_class: Optional[Any] = None,
    *,
    drop_existing_table: bool = False,
) -> int:
    """
    Write query results to a SQL table.

    Args:
        session: SQLAlchemy session (sync or async)
        query: SQLAlchemy Query object to write
        schema: Schema name
        table: Table name
        mode: Write mode ("overwrite" or "append")
        model_class: SQLAlchemy ORM model class

    Returns:
        Number of rows written

    Raises:
        TableOperationError: If write operation fails
    """
    try:
        if model_class is None:
            raise DataError(
                f"model_class is required to write table '{table}'. "
                "Provide a SQLAlchemy ORM model when defining the step."
            )

        effective_schema = schema or getattr(model_class.__table__, "schema", None)
        _ensure_table_from_model(
            session,
            effective_schema,
            table,
            model_class,
            drop_existing=drop_existing_table,
        )

        if effective_schema:
            table_identifier = f"{effective_schema}.{table}"
        else:
            table_identifier = table

        # table_fqn = (
        #     fqn(effective_schema, table) if effective_schema else table_identifier
        # )

        # Get data from query
        if hasattr(query, "all"):
            rows = query.all()
        else:
            logger.warning("Async query execution not yet implemented")
            rows = []

        # Determine write mode
        if mode == "overwrite":
            session.query(model_class).delete()
            session.commit()

        if not rows:
            logger.warning(f"No rows to write to {table_identifier}")
            return 0

        # Convert rows to dictionaries for insertion
        row_dicts = []
        columns = [col.key for col in model_class.__table__.columns]
        table_name = getattr(getattr(model_class, "__table__", None), "name", None)

        def _normalize_mapping(mapping: dict[Any, Any]) -> dict[str, Any]:
            cleaned: dict[str, Any] = {}
            for key, value in mapping.items():
                if isinstance(key, str):
                    cleaned[key] = value
                elif hasattr(key, "key"):
                    cleaned[key.key] = value
                elif hasattr(key, "name"):
                    cleaned[key.name] = value
                else:
                    cleaned[str(key)] = value
            normalized: dict[str, Any] = {}
            for col in columns:
                if col in cleaned:
                    normalized[col] = cleaned[col]
                    continue
                candidates = []
                if table_name:
                    candidates.extend(
                        [
                            f"{table_name}_{col}",
                            f"{table_name}.{col}",
                        ]
                    )
                for key in cleaned.keys():
                    if key.endswith(f".{col}") or key.endswith(f"_{col}"):
                        candidates.append(key)
                value = None
                for candidate in candidates:
                    if candidate in cleaned:
                        value = cleaned[candidate]
                        break
                normalized[col] = value
            return normalized

        for row in rows:
            if hasattr(row, "_mapping"):
                mapping = dict(row._mapping)
                row_dict = _normalize_mapping(mapping)
                if any(row_dict[col] is None for col in columns):
                    try:
                        fallback_values = list(row)
                        fallback_dict = dict(zip(columns, fallback_values))
                        for col in columns:
                            if row_dict[col] is None and col in fallback_dict:
                                row_dict[col] = fallback_dict[col]
                    except TypeError:
                        pass
            elif hasattr(row, "__class__") and hasattr(row.__class__, "__mapper__"):
                mapper = row.__class__.__mapper__
                row_dict = {
                    column.key: getattr(row, column.key, None)
                    for column in mapper.columns
                }
            elif hasattr(row, "_asdict"):
                row_dict = row._asdict()
            elif isinstance(row, (tuple, list)):
                row_dict = dict(zip(columns, row))
            else:
                try:
                    row_dict = dict(row)
                except (TypeError, ValueError):
                    row_dict = {
                        k: getattr(row, k) for k in dir(row) if not k.startswith("_")
                    }
            normalized_row = {col: row_dict.get(col) for col in columns}
            row_dicts.append(normalized_row)

        session.bulk_insert_mappings(model_class, row_dicts)
        session.commit()
        rows_written = len(rows)
        logger.info(
            f"Successfully wrote {rows_written} rows to {table_identifier} in {mode} mode"
        )
        return rows_written

    except Exception as e:
        session.rollback()
        identifier = f"{schema}.{table}" if schema else table
        raise DataError(f"Failed to write table {identifier}: {e}") from e


def table_exists(session: Any, schema: Optional[str], table: str) -> bool:
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
        if schema:
            return bool(inspector.has_table(table, schema=schema))
        return bool(inspector.has_table(table))
    except Exception:
        # Fallback: try querying database metadata manually
        try:
            from sqlalchemy import text

            if schema:
                result = session.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :schema AND table_name = :table"
                    ),
                    {"schema": schema, "table": table},
                )
                return result.scalar() is not None
            else:
                # SQLite fallback
                result = session.execute(
                    text(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name = :table"
                    ),
                    {"table": table},
                )
                return result.scalar() is not None
        except Exception:
            return False
