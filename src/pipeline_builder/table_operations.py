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

# Handle optional Delta Lake dependency
try:
    from delta.tables import DeltaTable

    HAS_DELTA = True
except (ImportError, AttributeError, RuntimeError):
    DeltaTable = None  # type: ignore[misc, assignment]
    HAS_DELTA = False

logger = logging.getLogger(__name__)


def prepare_delta_overwrite(
    spark: SparkSession,  # type: ignore[valid-type]
    table_name: str,
) -> None:
    """
    Prepare for Delta table overwrite by dropping existing Delta table if it exists.
    
    Delta tables don't support truncate in batch mode, so we must drop the table
    before overwriting it. This function safely handles this preparation.
    
    This is a public utility function that should be used before any Delta overwrite
    operation to avoid "Table does not support truncate in batch mode" errors.
    
    Args:
        spark: Spark session
        table_name: Fully qualified table name (e.g., "schema.table") or table path
        
    Example:
        >>> prepare_delta_overwrite(spark, "my_schema.my_table")
        >>> df.write.format("delta").mode("overwrite").saveAsTable("my_schema.my_table")
    """
    if not HAS_DELTA:
        return
    
    # Check if it's a table name (contains dot) or a path
    is_table_name = "." in table_name and not table_name.startswith("/")
    
    if is_table_name:
        # It's a table name - check if table exists
        if not table_exists(spark, table_name):
            return
        
        try:
            # Get table location from catalog
            table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()  # type: ignore[attr-defined]
            table_location = None
            provider = None
            for row in table_info:
                col_name = str(row[0]).strip()  # type: ignore[index]
                if col_name == "Location":
                    table_location = str(row[1]).strip()  # type: ignore[index]
                elif col_name == "Provider":
                    provider = str(row[1]).strip()  # type: ignore[index]
            
            # Check if it's a Delta table - either by provider or by location
            is_delta = False
            if provider and "delta" in provider.lower():
                is_delta = True
            elif table_location:
                try:
                    is_delta = DeltaTable.isDeltaTable(spark, table_location)  # type: ignore[attr-defined]
                except Exception:
                    # If we can't check, assume it might be Delta and drop it
                    is_delta = True
            
            if is_delta:
                # Drop the table (metadata only, data remains but will be overwritten)
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # type: ignore[attr-defined]
                logger.debug(f"Dropped Delta table {table_name} before overwrite")
        except Exception as e:
            # If we can't determine or drop, try to drop anyway if table exists
            # This is safer than failing later with truncate error
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # type: ignore[attr-defined]
                logger.debug(f"Dropped table {table_name} before overwrite (fallback)")
            except Exception:
                # If drop fails, let the write attempt proceed
                # The error will propagate if Delta overwrite fails
                logger.warning(f"Could not drop table {table_name} before overwrite: {e}")
    else:
        # It's a path - check if Delta table exists at that path
        try:
            if DeltaTable.isDeltaTable(spark, table_name):  # type: ignore[attr-defined]
                # For path-based Delta tables, we can't drop via SQL
                # The overwrite will handle it, but we log a warning
                logger.debug(f"Delta table exists at path {table_name}, overwrite will replace it")
        except Exception:
            # If we can't check, assume it might be Delta and proceed
            pass


# Keep the old function name for backward compatibility, but it now calls the public function
def _prepare_delta_overwrite_table_ops(
    spark: SparkSession,  # type: ignore[valid-type]
    table_name: str,
) -> None:
    """
    Legacy function name - use prepare_delta_overwrite() instead.
    
    This function is kept for backward compatibility but now delegates to
    the public prepare_delta_overwrite() function.
    """
    prepare_delta_overwrite(spark, table_name)


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
    Write DataFrame to table in overwrite mode using Delta overwrite pattern.

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
        df.cache()  # type: ignore[attr-defined]
        cnt: int = df.count()  # type: ignore[attr-defined]

        # Get SparkSession from DataFrame to prepare Delta overwrite
        spark = df.sql_ctx.sparkSession  # type: ignore[attr-defined]
        
        # Prepare for Delta overwrite by dropping existing Delta table if it exists
        prepare_delta_overwrite(spark, fqn)

        # Use standardized Delta overwrite pattern - always use Delta format
        # Failures will propagate if Delta is not available
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )  # type: ignore[attr-defined]

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
    except Exception as e:
        # Check if it's an AnalysisException (table doesn't exist)
        if isinstance(e, AnalysisException):
            raise TableOperationError(f"Table {fqn} does not exist: {e}") from e
        else:
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
        # If catalog has a fast check, use it first
        try:
            if hasattr(spark, "catalog") and spark.catalog.tableExists(fqn):  # type: ignore[attr-defined]
                # Run a lightweight count to mirror legacy behavior/side effects
                spark.table(fqn).count()  # type: ignore[attr-defined]
                return True
        except Exception:
            # Fall back to direct table access below
            pass

        spark.table(fqn).count()  # type: ignore[attr-defined]
        return True
    except AnalysisException:
        logger.debug(f"Table {fqn} does not exist (AnalysisException)")
        return False
    except Exception as e:
        logger.warning(f"Error checking if table {fqn} exists: {e}")
        return False


def table_schema_is_empty(spark: SparkSession, fqn: str) -> bool:
    """
    Check if a table exists but reports an empty schema (struct<>).

    This detects catalog sync issues where the metastore has a placeholder
    entry without columns. Callers can drop/recreate to heal.
    """
    try:
        if not table_exists(spark, fqn):
            return False
        schema = spark.table(fqn).schema  # type: ignore[attr-defined]
        if hasattr(schema, "fields"):
            return len(schema.fields) == 0
        return False
    except Exception as e:
        logger.debug(f"Could not inspect schema for {fqn}: {e}")
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
