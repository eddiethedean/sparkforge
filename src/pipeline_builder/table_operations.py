"""
Table operations utilities for the pipeline framework.

This module contains functions for reading, writing, and managing tables
in the data lake.

# Depends on:
#   compat
#   errors
#   performance
"""

from __future__ import annotations

import logging
from typing import Union

from .compat import AnalysisException, DataFrame, SparkSession
from .errors import TableOperationError
from .performance import time_operation

logger = logging.getLogger(__name__)


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


@time_operation("table write (overwrite)")
def write_overwrite_table(
    df: DataFrame,
    fqn: str,
    **options: Union[str, int] | Union[float, bool],  # type: ignore[valid-type]
) -> int:
    """
    Write DataFrame to table in overwrite mode using DELETE + INSERT pattern.

    Args:
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options

    Returns:
        Number of rows written

    Raises:
        TableOperationError: If write operation fails
    """
    try:
        # Cache DataFrame for potential multiple operations
        df.cache()  # type: ignore[attr-defined]
        cnt: int = df.count()  # type: ignore[attr-defined]

        # Get SparkSession from DataFrame for DELETE operation
        spark = df.sql_ctx.sparkSession  # type: ignore[attr-defined]

        # For overwrite mode, use DELETE + INSERT pattern
        # Delete existing data if table exists
        if table_exists(spark, fqn):
            try:
                spark.sql(f"DELETE FROM {fqn}")  # type: ignore[attr-defined]
            except Exception as e:
                logger.warning(
                    f"Could not delete from table '{fqn}' before overwrite: {e}"
                )

        # Write with append mode and overwriteSchema option
        writer = (  # type: ignore[attr-defined]
            df.write.format("parquet").mode("append").option("overwriteSchema", "true")
        )

        # Apply additional options
        for key, value in options.items():
            writer = writer.option(key, value)

        writer.saveAsTable(fqn)
        logger.info(f"Successfully wrote {cnt} rows to {fqn} in overwrite mode")
        return cnt

    except Exception as e:
        raise TableOperationError(f"Failed to write table {fqn}: {e}") from e


@time_operation("table write (append)")
def write_append_table(
    df: DataFrame,
    fqn: str,
    **options: Union[str, int] | Union[float, bool],  # type: ignore[valid-type]
) -> int:
    """
    Write DataFrame to table in append mode.

    Args:
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options

    Returns:
        Number of rows written

    Raises:
        TableOperationError: If write operation fails
    """
    try:
        # Cache DataFrame for potential multiple operations
        df.cache()  # type: ignore[attr-defined]
        cnt: int = df.count()  # type: ignore[attr-defined]
        writer = df.write.format("parquet").mode("append")  # type: ignore[attr-defined]

        # Apply additional options
        for key, value in options.items():
            writer = writer.option(key, value)

        writer.saveAsTable(fqn)
        logger.info(f"Successfully wrote {cnt} rows to {fqn} in append mode")
        return cnt

    except Exception as e:
        raise TableOperationError(f"Failed to write table {fqn}: {e}") from e


def read_table(
    spark: SparkSession,
    fqn: str,  # type: ignore[valid-type]
) -> DataFrame:  # type: ignore[valid-type]
    """
    Read data from a table.

    Args:
        spark: Spark session
        fqn: Fully qualified table name

    Returns:
        DataFrame with table data

    Raises:
        TableOperationError: If read operation fails
    """
    try:
        df = spark.table(fqn)  # type: ignore[attr-defined]
        logger.info(f"Successfully read table {fqn}")
        return df
    except AnalysisException as e:
        raise TableOperationError(f"Table {fqn} does not exist: {e}") from e
    except Exception as e:
        raise TableOperationError(f"Failed to read table {fqn}: {e}") from e


def table_exists(
    spark: SparkSession,
    fqn: str,  # type: ignore[valid-type]
) -> bool:  # type: ignore[valid-type]
    """
    Check if a table exists.

    Args:
        spark: Spark session
        fqn: Fully qualified table name

    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.table(fqn).count()  # type: ignore[attr-defined]
        return True
    except AnalysisException:
        logger.debug(f"Table {fqn} does not exist (AnalysisException)")
        return False
    except Exception as e:
        logger.warning(f"Error checking if table {fqn} exists: {e}")
        return False


def drop_table(
    spark: SparkSession,
    fqn: str,  # type: ignore[valid-type]
) -> bool:  # type: ignore[valid-type]
    """
    Drop a table if it exists.

    Args:
        spark: Spark session
        fqn: Fully qualified table name

    Returns:
        True if table was dropped, False if it didn't exist
    """
    try:
        if table_exists(spark, fqn):
            # Use Java SparkSession to access external catalog
            jspark_session = spark._jsparkSession  # type: ignore[attr-defined]
            external_catalog = jspark_session.sharedState().externalCatalog()

            # Parse fully qualified name
            if "." in fqn:
                database_name, table_name = fqn.split(".", 1)
            else:
                database_name = "default"
                table_name = fqn

            # Drop the table using external catalog
            # Parameters: db, table, ignoreIfNotExists, purge
            external_catalog.dropTable(database_name, table_name, True, True)
            logger.info(f"Dropped table {fqn}")
            return True
        return False
    except Exception as e:
        logger.warning(f"Failed to drop table {fqn}: {e}")
        return False
