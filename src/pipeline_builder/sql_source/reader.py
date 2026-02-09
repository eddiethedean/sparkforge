"""
Unified reader for JdbcSource and SqlAlchemySource.

Dispatches on source type and returns a Spark DataFrame.
"""

from __future__ import annotations

from typing import Any, Dict, Union, cast

from pipeline_builder.sql_source.models import JdbcSource, SqlAlchemySource


def read_sql_source(
    source: Union[JdbcSource, SqlAlchemySource],
    spark: Any,
) -> Any:
    """
    Read a SQL source into a Spark DataFrame.

    Args:
        source: JdbcSource or SqlAlchemySource configuration.
        spark: SparkSession (from pipeline_builder.compat or pyspark).

    Returns:
        Spark DataFrame.

    Raises:
        ValidationError: If source config is invalid.
        RuntimeError: If SqlAlchemySource is used but sqlalchemy/pandas not installed.
    """
    if isinstance(source, JdbcSource):
        return _read_jdbc(source, spark)
    if isinstance(source, SqlAlchemySource):
        return _read_sqlalchemy(source, spark)
    raise TypeError(
        f"source must be JdbcSource or SqlAlchemySource, got {type(source).__name__}"
    )


def _read_jdbc(source: JdbcSource, spark: Any) -> Any:
    table_or_query = source.table if source.table else source.query
    if not table_or_query:
        raise ValueError("JdbcSource must have table or query set")

    # Copy properties to a mutable dict so type checkers know we can mutate it
    props: Dict[str, str] = dict(source.properties)

    if source.driver:
        props["driver"] = source.driver

    kwargs = {
        "url": source.url,
        "table": table_or_query,
        "properties": props,
    }

    return spark.read.jdbc(**kwargs)


def _read_sqlalchemy(source: SqlAlchemySource, spark: Any) -> Any:
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError(
            "SqlAlchemySource requires pandas. Install with: pip install pipeline_builder[sql]"
        ) from e
    try:
        from sqlalchemy import create_engine
    except ImportError as e:
        raise RuntimeError(
            "SqlAlchemySource requires sqlalchemy. Install with: pip install pipeline_builder[sql]"
        ) from e

    if source.engine is not None:
        engine = source.engine
    else:
        # __post_init__ guarantees that url is a non-empty string when engine is None,
        # but type checkers cannot deduce this, so we cast explicitly.
        engine = create_engine(cast(str, source.url))

    if source.query is not None:
        pdf = pd.read_sql(source.query, engine)
    else:
        # SqlAlchemySource validation guarantees that table is set when query is None,
        # but we add a defensive assertion for type checkers and runtime safety.
        assert source.table is not None, (
            "SqlAlchemySource.table must be set when query is None"
        )

        if source.schema:
            pdf = pd.read_sql_table(source.table, engine, schema=source.schema)
        else:
            pdf = pd.read_sql_table(source.table, engine)

    return spark.createDataFrame(pdf)
