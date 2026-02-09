"""
SQL source configuration models for pipeline_builder.

JdbcSource: read via PySpark spark.read.jdbc() (no extra deps; JDBC driver JAR on classpath).
SqlAlchemySource: read via SQLAlchemy + pandas then spark.createDataFrame (optional [sql] extra).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pipeline_builder_base.errors import ValidationError


def _validate_table_or_query(
    table: Optional[str],
    query: Optional[str],
    source_type: str,
) -> None:
    """Require exactly one of table or query."""
    has_table = table is not None and table.strip() != ""
    has_query = query is not None and query.strip() != ""
    if has_table and has_query:
        raise ValidationError(
            f"{source_type} requires exactly one of 'table' or 'query'; got both."
        )
    if not has_table and not has_query:
        raise ValidationError(
            f"{source_type} requires exactly one of 'table' or 'query'; got neither."
        )


@dataclass
class JdbcSource:
    """
    JDBC-backed SQL source for pipeline steps.

    Data is read via spark.read.jdbc(). Requires the JDBC driver JAR
    for the database to be on the Spark application classpath.

    Exactly one of `table` or `query` must be set. For `query`, Spark
    expects a subquery alias, e.g. "(SELECT * FROM t WHERE x > 1) AS q".
    """

    url: str
    properties: Dict[str, str]
    table: Optional[str] = None
    query: Optional[str] = None
    driver: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.url or not isinstance(self.url, str):
            raise ValidationError("JdbcSource requires a non-empty 'url'.")
        if not isinstance(self.properties, dict):
            raise ValidationError("JdbcSource 'properties' must be a dict.")
        _validate_table_or_query(self.table, self.query, "JdbcSource")


@dataclass
class SqlAlchemySource:
    """
    SQLAlchemy-backed SQL source for pipeline steps.

    Data is read via SQLAlchemy + pandas (read_sql_table or read_sql)
    then spark.createDataFrame(). Requires pip install pipeline_builder[sql].

    Exactly one of `table` or `query` must be set. Optionally set `schema`
    for table reads (e.g. 'public' for PostgreSQL).
    """

    url: Optional[str] = None
    engine: Any = None
    table: Optional[str] = None
    query: Optional[str] = None
    schema: Optional[str] = None

    def __post_init__(self) -> None:
        has_url = self.url is not None and (
            isinstance(self.url, str) and self.url.strip() != ""
        )
        has_engine = self.engine is not None
        if has_url and has_engine:
            raise ValidationError(
                "SqlAlchemySource requires exactly one of 'url' or 'engine'; got both."
            )
        if not has_url and not has_engine:
            raise ValidationError(
                "SqlAlchemySource requires exactly one of 'url' or 'engine'; got neither."
            )
        _validate_table_or_query(self.table, self.query, "SqlAlchemySource")
