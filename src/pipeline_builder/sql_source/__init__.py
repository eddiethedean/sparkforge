"""
SQL source support for pipeline_builder.

Provides JdbcSource and SqlAlchemySource for reading from SQL databases
into Spark DataFrames, and read_sql_source() to resolve either type.

Also provides JdbcWriteConfig and SqlAlchemyWriteConfig for writing
Spark DataFrames to SQL databases, with write_jdbc(), write_sqlalchemy(),
and write_sql_target() functions.
"""

from pipeline_builder.sql_source.models import JdbcSource, SqlAlchemySource
from pipeline_builder.sql_source.reader import read_sql_source
from pipeline_builder.sql_source.writer import (
    JdbcWriteConfig,
    SqlAlchemyWriteConfig,
    write_jdbc,
    write_sql_target,
    write_sqlalchemy,
)

__all__ = [
    "JdbcSource",
    "SqlAlchemySource",
    "read_sql_source",
    "JdbcWriteConfig",
    "SqlAlchemyWriteConfig",
    "write_jdbc",
    "write_sqlalchemy",
    "write_sql_target",
]
