"""
Table operations utilities for the pipeline framework.

This module provides comprehensive utilities for reading, writing, and managing
tables in the data lake. It includes Delta Lake support, table existence checks,
schema validation, and standardized write patterns.

**Key Features:**
    - **Delta Lake Integration**: Automatic detection and support for Delta tables
    - **Standardized Write Patterns**: Consistent write operations across the framework
    - **Table Management**: Existence checks, schema validation, and table operations
    - **Error Handling**: Comprehensive error handling with custom exceptions
    - **Performance Monitoring**: Built-in timing and performance tracking

**Common Patterns:**
    - Check if table exists before writing
    - Prepare Delta tables for overwrite operations
    - Create fully qualified table names (FQN)
    - Write DataFrames with standardized patterns

Dependencies:
    - compat: Compatibility layer for Spark/PySpark types
    - errors: Custom exception classes
    - performance: Performance monitoring decorators

Example:
    >>> from pipeline_builder.table_operations import (
    ...     table_exists,
    ...     write_overwrite_table,
    ...     fqn
    ... )
    >>> from pipeline_builder.compat import SparkSession
    >>>
    >>> # Create fully qualified name
    >>> table_name = fqn("analytics", "user_events")
    >>>
    >>> # Check if table exists
    >>> if table_exists(spark, table_name):
    ...     print(f"Table {table_name} exists")
    >>>
    >>> # Write DataFrame
    >>> rows = write_overwrite_table(df, table_name)
    >>> print(f"Wrote {rows} rows")
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any, Dict, Optional, Union

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

# Cache for Delta Lake availability checks
_delta_availability_cache: Dict[str, bool] = {}


def is_delta_lake_available(spark: SparkSession) -> bool:  # type: ignore[valid-type]
    """Check if Delta Lake is available in the Spark session.

    Checks whether Delta Lake extensions and catalog are configured in the
    Spark session. Uses caching to avoid repeated checks for the same session.

    Args:
        spark: SparkSession instance to check.

    Returns:
        True if Delta Lake is available (extensions and catalog configured),
        False otherwise.

    Example:
        >>> from pipeline_builder.table_operations import is_delta_lake_available
        >>> if is_delta_lake_available(spark):
        ...     print("Delta Lake is available")
        ...     # Use Delta-specific operations
        ... else:
        ...     print("Delta Lake is not available")
    """
    # Use session ID as cache key
    spark_id = str(id(spark))
    if spark_id in _delta_availability_cache:
        return _delta_availability_cache[spark_id]

    # Check if Delta extensions are configured
    try:
        extensions = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]
        if (
            extensions
            and catalog
            and "DeltaSparkSessionExtension" in extensions
            and "DeltaCatalog" in catalog
        ):
            _delta_availability_cache[spark_id] = True
            return True
    except Exception:
        pass  # Config check failed; proceed to lightweight test

    # If only extensions are configured, do a lightweight test
    try:
        extensions = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        if extensions and "DeltaSparkSessionExtension" in extensions:
            # Try a simple test - create a minimal DataFrame and try to write it
            test_df = spark.createDataFrame([(1, "test")], ["id", "name"])
            # Use a unique temp directory to avoid conflicts
            with tempfile.TemporaryDirectory() as temp_dir:
                test_path = os.path.join(temp_dir, "delta_test")
                try:
                    test_df.write.format("delta").mode("overwrite").save(test_path)
                    _delta_availability_cache[spark_id] = True
                    return True
                except Exception:
                    # Delta format failed - not available
                    pass
    except Exception:
        pass  # Lightweight Delta test failed or config unavailable

    # Delta is not available in this Spark session
    _delta_availability_cache[spark_id] = False
    return False


def create_dataframe_writer(
    df: DataFrame,
    spark: SparkSession,  # type: ignore[valid-type]
    mode: str,
    table_name: Optional[str] = None,
    **options: Any,
) -> Any:
    """Create a DataFrameWriter using the standardized Delta overwrite pattern.

    Creates a DataFrameWriter configured with Delta format and appropriate
    options. For overwrite mode, uses `overwriteSchema=true` to allow schema
    evolution. Always uses Delta format - failures will propagate if Delta
    is not available.

    Args:
        df: DataFrame to write.
        spark: SparkSession instance (used for Delta table preparation if needed).
        mode: Write mode string. Common values:
            - "overwrite": Replace existing data
            - "append": Add to existing data
            - "ignore": Skip if table exists
            - "error": Fail if table exists
        table_name: Optional fully qualified table name. If provided and mode
            is "overwrite", prepares the Delta table for overwrite.
        **options: Additional write options to pass to the writer (e.g.,
            partitionBy, mergeSchema, etc.).

    Returns:
        DataFrameWriter instance configured with Delta format and options.

    Example:
        >>> from pipeline_builder.table_operations import create_dataframe_writer
        >>> writer = create_dataframe_writer(
        ...     df,
        ...     spark,
        ...     mode="overwrite",
        ...     table_name="analytics.events",
        ...     partitionBy="date"
        ... )
        >>> writer.saveAsTable("analytics.events")
    """
    # Use standardized overwrite pattern: overwrite + overwriteSchema
    if mode == "overwrite":
        # Prepare Delta table for overwrite by dropping it if it exists
        # This avoids "Table does not support truncate in batch mode" errors
        if table_name is not None:
            prepare_delta_overwrite(spark, table_name)
        writer = (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        )
    else:
        # Append or other modes - always use Delta
        writer = df.write.format("delta").mode(mode)

    for key, value in options.items():
        writer = writer.option(key, value)

    return writer


def prepare_delta_overwrite(
    spark: SparkSession,  # type: ignore[valid-type]
    table_name: str,
) -> None:
    """Prepare for Delta table overwrite by dropping existing table if it exists.

    Delta tables don't support truncate in batch mode, so we must drop the table
    before overwriting it. This function safely handles this preparation by
    dropping the table if it exists, avoiding "Table does not support truncate
    in batch mode" errors.

    **Important:** This function should be called before any Delta overwrite
    operation to ensure compatibility with Delta Lake's limitations.

    Args:
        spark: SparkSession instance for executing SQL commands.
        table_name: Fully qualified table name (e.g., "schema.table") or
            table path. If it's a table name (contains dot and doesn't start
            with "/"), it will be dropped via SQL. If it's a path, the function
            checks if it's a Delta table but cannot drop it (overwrite will
            handle it).

    Example:
        >>> from pipeline_builder.table_operations import prepare_delta_overwrite
        >>> prepare_delta_overwrite(spark, "analytics.user_events")
        >>> df.write.format("delta").mode("overwrite").saveAsTable("analytics.user_events")

    Note:
        This function is safe to call even if the table doesn't exist. It uses
        `DROP TABLE IF EXISTS` to avoid errors. If the drop fails for any reason,
        a warning is logged but execution continues (the write operation may
        still succeed or fail with a more specific error).
    """
    if not HAS_DELTA:
        return

    # Check if it's a table name (contains dot) or a path
    is_table_name = "." in table_name and not table_name.startswith("/")

    if is_table_name:
        # Always try to drop the table if it exists, since we're about to overwrite it
        # This is safer than failing later with truncate error for Delta tables
        # Delta tables don't support truncate, so we must drop before overwrite
        # Use DROP TABLE IF EXISTS to avoid errors if table doesn't exist
        try:
            # First try using SQL DROP TABLE
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # type: ignore[attr-defined]
            logger.debug(f"Dropped table {table_name} before overwrite (if it existed)")

            # For Delta tables, also try using DeltaTable API if available
            # This ensures the table is fully removed, including metadata
            if HAS_DELTA:
                try:
                    # Try to get the table path and delete using DeltaTable
                    # This is a more thorough cleanup for Delta tables
                    spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()  # type: ignore[attr-defined]
                    # If we get here, table still exists - try DeltaTable.delete()
                    delta_table = DeltaTable.forName(spark, table_name)  # type: ignore[attr-defined,assignment,arg-type]
                    delta_table.delete()  # type: ignore[attr-defined]
                    logger.debug(
                        f"Deleted Delta table {table_name} using DeltaTable API"
                    )
                except Exception:
                    # DeltaTable API might not work or table might not be Delta
                    # This is fine - SQL DROP should have worked
                    pass
        except Exception as e:
            # If drop fails, log warning but continue
            # The write might still work if table doesn't exist
            logger.warning(f"Could not drop table {table_name} before overwrite: {e}")
    else:
        # It's a path - check if Delta table exists at that path
        try:
            if DeltaTable.isDeltaTable(spark, table_name):  # type: ignore[attr-defined,arg-type]
                # For path-based Delta tables, we can't drop via SQL
                # The overwrite will handle it, but we log a warning
                logger.debug(
                    f"Delta table exists at path {table_name}, overwrite will replace it"
                )
        except Exception:
            pass  # If we can't check Delta at path, assume and proceed with overwrite


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
    """Create a fully qualified table name (FQN).

    Combines schema and table names into a fully qualified table name in the
    format "schema.table". This is the standard format used throughout the
    framework for table references.

    Args:
        schema: Database schema name. Must be non-empty.
        table: Table name. Must be non-empty.

    Returns:
        Fully qualified table name in the format "schema.table".

    Raises:
        ValueError: If schema or table is empty.

    Example:
        >>> from pipeline_builder.table_operations import fqn
        >>> table_name = fqn("analytics", "user_events")
        >>> print(table_name)  # "analytics.user_events"
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
    """Write DataFrame to table in overwrite mode using Delta overwrite pattern.

    Writes a DataFrame to a table, replacing all existing data. Uses Delta
    format with `overwriteSchema=true` to allow schema evolution. Automatically
    prepares the Delta table for overwrite by dropping it if it exists (to
    avoid truncate errors).

    Args:
        df: DataFrame to write. Will be cached before writing.
        fqn: Fully qualified table name (e.g., "schema.table").
        **options: Additional write options to pass to the writer. Common
            options include:
            - partitionBy: Column(s) to partition by
            - mergeSchema: Whether to merge schemas (default: true via overwriteSchema)

    Returns:
        Number of rows written to the table.

    Raises:
        TableOperationError: If write operation fails (e.g., table creation
            fails, write fails, or Delta Lake is not available).

    Example:
        >>> from pipeline_builder.table_operations import write_overwrite_table
        >>> rows = write_overwrite_table(
        ...     df,
        ...     "analytics.user_events",
        ...     partitionBy="date"
        ... )
        >>> print(f"Wrote {rows} rows")
    """
    try:
        df.cache()  # type: ignore[attr-defined]
        cnt: int = df.count()  # type: ignore[attr-defined]

        # Get SparkSession from DataFrame to prepare Delta overwrite
        spark = df.sql_ctx.sparkSession  # type: ignore[attr-defined]

        # Prepare for Delta overwrite by dropping existing Delta table if it exists
        prepare_delta_overwrite(spark, fqn)

        # Delta Lake doesn't support append in batch mode
        # Always use overwrite mode for Delta tables
        # This is safe because we've already dropped the table if it existed
        write_mode = "overwrite"
        writer = (
            df.write.format("delta").mode(write_mode).option("overwriteSchema", "true")
        )  # type: ignore[attr-defined]

        for key, value in options.items():
            writer = writer.option(key, value)

        writer.saveAsTable(fqn)
        logger.info(f"Successfully wrote {cnt} rows to {fqn} in {write_mode} mode")
        return cnt

    except Exception as e:
        raise TableOperationError(f"Failed to write table {fqn}: {e}") from e


@time_operation("table write (append)")
def write_append_table(
    df: DataFrame,
    fqn: str,
    **options: Union[str, int] | Union[float, bool],  # type: ignore[valid-type]
) -> int:
    """Write DataFrame to table in append mode.

    Writes a DataFrame to a table, adding new data to existing data. Uses
    Parquet format for append operations. The table will be created if it
    doesn't exist.

    Args:
        df: DataFrame to write. Will be cached before writing.
        fqn: Fully qualified table name (e.g., "schema.table").
        **options: Additional write options to pass to the writer. Common
            options include:
            - partitionBy: Column(s) to partition by
            - compression: Compression codec (e.g., "snappy", "gzip")

    Returns:
        Number of rows written to the table.

    Raises:
        TableOperationError: If write operation fails (e.g., table creation
            fails or write fails).

    Example:
        >>> from pipeline_builder.table_operations import write_append_table
        >>> rows = write_append_table(
        ...     df,
        ...     "analytics.user_events",
        ...     partitionBy="date"
        ... )
        >>> print(f"Appended {rows} rows")
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
    """Read data from a table.

    Reads data from a table using Spark's `table()` method. Supports both
    regular tables and Delta tables.

    Args:
        spark: SparkSession instance for reading the table.
        fqn: Fully qualified table name (e.g., "schema.table").

    Returns:
        DataFrame containing the table data.

    Raises:
        TableOperationError: If read operation fails. Common causes:
            - Table does not exist (wrapped AnalysisException)
            - Permission errors
            - Table corruption

    Example:
        >>> from pipeline_builder.table_operations import read_table
        >>> df = read_table(spark, "analytics.user_events")
        >>> print(f"Read {df.count()} rows")
    """
    try:
        df = spark.table(fqn)  # type: ignore[attr-defined]
        logger.info(f"Successfully read table {fqn}")
        return df
    except Exception as e:
        # Check if it's an AnalysisException (table doesn't exist) - use type name check for Python 3.8 compatibility
        error_type_name = type(e).__name__
        if "AnalysisException" in error_type_name:
            raise TableOperationError(f"Table {fqn} does not exist: {e}") from e
        else:
            raise TableOperationError(f"Failed to read table {fqn}: {e}") from e


def table_exists(
    spark: SparkSession,
    fqn: str,  # type: ignore[valid-type]
) -> bool:  # type: ignore[valid-type]
    """Check if a table exists.

    Checks whether a table exists in the Spark catalog. Uses multiple methods
    for reliability: first tries the catalog's `tableExists()` method, then
    falls back to attempting to read the table.

    Args:
        spark: SparkSession instance for checking table existence.
        fqn: Fully qualified table name (e.g., "schema.table").

    Returns:
        True if the table exists and is accessible, False otherwise.
        Returns False if the table doesn't exist or an error occurs.

    Example:
        >>> from pipeline_builder.table_operations import table_exists
        >>> if table_exists(spark, "analytics.user_events"):
        ...     print("Table exists")
        ... else:
        ...     print("Table does not exist")
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
    except AnalysisException:  # type: ignore[misc]
        logger.debug(f"Table {fqn} does not exist (AnalysisException)")
        return False
    except Exception as e:
        logger.warning(f"Error checking if table {fqn} exists: {e}")
        return False


def table_schema_is_empty(spark: SparkSession, fqn: str) -> bool:
    """Check if a table exists but reports an empty schema (struct<>).

    Detects catalog synchronization issues where the metastore has a placeholder
    entry for a table but the table has no columns (empty schema). This can
    happen when table creation is interrupted or when there are catalog sync
    issues. Callers can drop and recreate the table to fix this.

    Args:
        spark: SparkSession instance for checking the table schema.
        fqn: Fully qualified table name (e.g., "schema.table").

    Returns:
        True if the table exists but has an empty schema (no fields),
        False otherwise. Returns False if the table doesn't exist or
        an error occurs.

    Example:
        >>> from pipeline_builder.table_operations import table_schema_is_empty
        >>> if table_schema_is_empty(spark, "analytics.user_events"):
        ...     print("Table has empty schema - needs recreation")
        ...     drop_table(spark, "analytics.user_events")
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
    """Drop a table if it exists.

    Drops a table from the Spark catalog using the external catalog API.
    This is a safe operation that only drops the table if it exists.

    Args:
        spark: SparkSession instance for dropping the table.
        fqn: Fully qualified table name (e.g., "schema.table"). If the name
            doesn't contain a dot, it's assumed to be in the "default" schema.

    Returns:
        True if the table was dropped, False if it didn't exist or an
        error occurred (logged as warning).

    Example:
        >>> from pipeline_builder.table_operations import drop_table
        >>> if drop_table(spark, "analytics.user_events"):
        ...     print("Table dropped successfully")
        ... else:
        ...     print("Table did not exist or could not be dropped")
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
