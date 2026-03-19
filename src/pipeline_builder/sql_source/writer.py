"""
JDBC and SQLAlchemy write functionality for pipeline_builder.

Provides functions to write Spark DataFrames to relational databases
via JDBC or SQLAlchemy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Union, cast

from pipeline_builder_base.errors import ValidationError


@dataclass
class JdbcWriteConfig:
    """
    Configuration for writing a DataFrame via JDBC.

    Args:
        url: JDBC connection URL (e.g., "jdbc:postgresql://host:5432/db")
        table: Target table name (can include schema, e.g., "public.orders")
        properties: Connection properties dict (user, password, etc.)
        mode: Write mode - "overwrite", "append", "ignore", or "error"
        driver: Optional JDBC driver class name
    """

    url: str
    table: str
    properties: Dict[str, str]
    mode: Literal["overwrite", "append", "ignore", "error"] = "append"
    driver: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.url or not isinstance(self.url, str):
            raise ValidationError("JdbcWriteConfig requires a non-empty 'url'.")
        if not self.table or not isinstance(self.table, str):
            raise ValidationError("JdbcWriteConfig requires a non-empty 'table'.")
        if not isinstance(self.properties, dict):
            raise ValidationError("JdbcWriteConfig 'properties' must be a dict.")
        if self.mode not in ("overwrite", "append", "ignore", "error"):
            raise ValidationError(
                f"JdbcWriteConfig 'mode' must be one of 'overwrite', 'append', 'ignore', 'error'; got '{self.mode}'."
            )


@dataclass
class SqlAlchemyWriteConfig:
    """
    Configuration for writing a DataFrame via SQLAlchemy.

    Args:
        url: SQLAlchemy connection URL (e.g., "postgresql://user:pass@host/db")
        table: Target table name
        schema: Optional database schema (e.g., "public")
        if_exists: Behavior if table exists - "fail", "replace", or "append"
        index: Whether to write DataFrame index as a column
        engine: Optional pre-created SQLAlchemy engine (alternative to url)
    """

    table: str
    url: Optional[str] = None
    engine: Any = None
    schema: Optional[str] = None
    if_exists: Literal["fail", "replace", "append"] = "append"
    index: bool = False

    def __post_init__(self) -> None:
        if not self.table or not isinstance(self.table, str):
            raise ValidationError("SqlAlchemyWriteConfig requires a non-empty 'table'.")

        has_url = self.url is not None and (
            isinstance(self.url, str) and self.url.strip() != ""
        )
        has_engine = self.engine is not None
        if has_url and has_engine:
            raise ValidationError(
                "SqlAlchemyWriteConfig requires exactly one of 'url' or 'engine'; got both."
            )
        if not has_url and not has_engine:
            raise ValidationError(
                "SqlAlchemyWriteConfig requires exactly one of 'url' or 'engine'; got neither."
            )
        if self.if_exists not in ("fail", "replace", "append"):
            raise ValidationError(
                f"SqlAlchemyWriteConfig 'if_exists' must be one of 'fail', 'replace', 'append'; got '{self.if_exists}'."
            )


def write_jdbc(
    df: Any,
    config: JdbcWriteConfig,
) -> int:
    """
    Write a Spark DataFrame to a database table via JDBC.

    Args:
        df: Spark DataFrame to write.
        config: JdbcWriteConfig with connection details and write options.

    Returns:
        Number of rows written.

    Raises:
        ValidationError: If config is invalid.
        Exception: If the JDBC write fails.

    Example:
        >>> config = JdbcWriteConfig(
        ...     url="jdbc:postgresql://localhost:5432/mydb",
        ...     table="public.orders",
        ...     properties={"user": "postgres", "password": "secret"},
        ...     mode="append",
        ...     driver="org.postgresql.Driver",
        ... )
        >>> rows_written = write_jdbc(df, config)
    """
    props: Dict[str, str] = dict(config.properties)
    if config.driver:
        props["driver"] = config.driver

    row_count = cast(int, df.count())

    df.write.jdbc(
        url=config.url,
        table=config.table,
        mode=config.mode,
        properties=props,
    )

    return row_count


def write_sqlalchemy(
    df: Any,
    config: SqlAlchemyWriteConfig,
) -> int:
    """
    Write a Spark DataFrame to a database table via SQLAlchemy + pandas.

    Converts the Spark DataFrame to a pandas DataFrame and uses
    pandas.DataFrame.to_sql() to write to the database.

    Args:
        df: Spark DataFrame to write.
        config: SqlAlchemyWriteConfig with connection details and write options.

    Returns:
        Number of rows written.

    Raises:
        RuntimeError: If pandas or sqlalchemy are not installed.
        ValidationError: If config is invalid.
        Exception: If the write fails.

    Example:
        >>> config = SqlAlchemyWriteConfig(
        ...     url="postgresql://user:pass@localhost/mydb",
        ...     table="orders",
        ...     schema="public",
        ...     if_exists="append",
        ... )
        >>> rows_written = write_sqlalchemy(df, config)
    """
    try:
        import pandas as pd  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "SqlAlchemyWriteConfig requires pandas. Install with: pip install pipeline_builder[sql]"
        ) from e

    try:
        from sqlalchemy import create_engine
    except ImportError as e:
        raise RuntimeError(
            "SqlAlchemyWriteConfig requires sqlalchemy. Install with: pip install pipeline_builder[sql]"
        ) from e

    if config.engine is not None:
        engine = config.engine
    else:
        assert config.url is not None
        engine = create_engine(config.url)

    pdf = df.toPandas()
    row_count = len(pdf)

    pdf.to_sql(
        name=config.table,
        con=engine,
        schema=config.schema,
        if_exists=config.if_exists,
        index=config.index,
    )

    return row_count


def write_sql_target(
    df: Any,
    config: Union[JdbcWriteConfig, SqlAlchemyWriteConfig],
) -> int:
    """
    Write a Spark DataFrame to a SQL target (JDBC or SQLAlchemy).

    Dispatches to write_jdbc or write_sqlalchemy based on config type.

    Args:
        df: Spark DataFrame to write.
        config: JdbcWriteConfig or SqlAlchemyWriteConfig.

    Returns:
        Number of rows written.

    Raises:
        TypeError: If config is not a recognized type.
    """
    if isinstance(config, JdbcWriteConfig):
        return write_jdbc(df, config)
    if isinstance(config, SqlAlchemyWriteConfig):
        return write_sqlalchemy(df, config)
    raise TypeError(
        f"config must be JdbcWriteConfig or SqlAlchemyWriteConfig, got {type(config).__name__}"
    )
