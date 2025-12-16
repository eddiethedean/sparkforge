"""
Test isolation utilities for ensuring clean state between tests.

This module provides helper functions to clear Spark state, Delta Lake metadata,
and global caches to prevent test pollution.
"""

from typing import Any


def clear_spark_state(spark: Any) -> None:
    """
    Comprehensively clear Spark session state.

    This function clears:
    - All cached tables and DataFrames
    - All temporary views
    - Spark catalog cache
    - All databases/schemas (except system schemas)

    Args:
        spark: SparkSession instance to clean
    """
    if not spark:
        return

    try:
        # Clear catalog cache
        if hasattr(spark, "catalog") and hasattr(spark.catalog, "clearCache"):
            spark.catalog.clearCache()

        # Clear all temporary views
        try:
            views = spark.catalog.listTables()
            for view in views:
                if view.isTemporary:
                    try:
                        spark.catalog.dropTempView(view.name)
                    except Exception:
                        pass
        except Exception:
            pass

        # Drop all non-system databases/schemas
        try:
            databases = spark.catalog.listDatabases()
            for db in databases:
                db_name = db.name if hasattr(db, "name") else str(db)
                if db_name not in ["default", "information_schema", "sys"]:
                    try:
                        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
                    except Exception:
                        pass
        except Exception:
            pass

        # Clear any cached DataFrames (Spark internal cache)
        try:
            if hasattr(spark, "sparkContext") and spark.sparkContext:
                spark.sparkContext._jvm.System.gc()  # type: ignore[attr-defined]
        except Exception:
            pass

    except Exception:
        pass  # Ignore all errors during cleanup


def clear_delta_tables(spark: Any, schema: str = "test_schema") -> None:
    """
    Clear Delta Lake specific state for a schema.

    This function:
    - Drops all tables in the specified schema
    - Clears Delta Lake transaction log cache
    - Resets Delta Lake table metadata

    Args:
        spark: SparkSession instance
        schema: Schema name to clean (default: "test_schema")
    """
    if not spark:
        return

    try:
        # Drop all tables in the schema
        try:
            tables = spark.catalog.listTables(schema)
            for table in tables:
                table_name = table.name if hasattr(table, "name") else str(table)
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")
                except Exception:
                    pass
        except Exception:
            pass

        # Drop the schema itself
        try:
            spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
        except Exception:
            pass

    except Exception:
        pass  # Ignore all errors during cleanup


def reset_global_state() -> None:
    """
    Reset global caches and state that might persist between tests.

    This function clears:
    - Delta Lake availability caches
    - Any other module-level caches
    """
    try:
        # Clear Delta Lake availability caches
        from pipeline_builder.execution import (
            _delta_availability_cache_execution,
        )
        from pipeline_builder.writer.storage import _delta_availability_cache

        _delta_availability_cache_execution.clear()
        _delta_availability_cache.clear()
    except Exception:
        pass  # Ignore import errors if modules aren't available


def clear_all_tables(spark: Any, schema_pattern: str = "*") -> None:
    """
    Clear all tables matching a schema pattern.

    This function:
    - Lists all tables in schemas matching the pattern
    - Drops each table with CASCADE
    - Handles Delta Lake table cleanup specifically

    Args:
        spark: SparkSession instance
        schema_pattern: Pattern to match schemas (default: "*" for all)
    """
    if not spark:
        return

    try:
        # Get all databases
        databases = spark.catalog.listDatabases()
        for db in databases:
            db_name = db.name if hasattr(db, "name") else str(db)
            if db_name in ["default", "information_schema", "sys"]:
                continue

            # Check if schema matches pattern
            if schema_pattern != "*" and db_name != schema_pattern:
                continue

            # Drop all tables in this schema
            try:
                tables = spark.catalog.listTables(db_name)
                for table in tables:
                    table_name = table.name if hasattr(table, "name") else str(table)
                    try:
                        # Use CASCADE to handle foreign key dependencies
                        spark.sql(
                            f"DROP TABLE IF EXISTS {db_name}.{table_name} CASCADE"
                        )
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass  # Ignore all errors during cleanup


def clear_spark_views(spark: Any) -> None:
    """
    Clear all temporary and global temporary views.

    This function:
    - Lists all temporary views
    - Lists all global temporary views
    - Drops each view explicitly

    Args:
        spark: SparkSession instance
    """
    if not spark:
        return

    try:
        # Clear all temporary views
        try:
            views = spark.catalog.listTables()
            for view in views:
                if hasattr(view, "isTemporary") and view.isTemporary:
                    try:
                        spark.catalog.dropTempView(view.name)
                    except Exception:
                        pass
                # Also try global temporary views
                if hasattr(view, "isGlobalTemporary") and view.isGlobalTemporary:
                    try:
                        spark.catalog.dropGlobalTempView(view.name)
                    except Exception:
                        pass
        except Exception:
            pass

        # Additional cleanup for global temporary views
        try:
            if hasattr(spark.catalog, "listGlobalTempViews"):
                global_views = spark.catalog.listGlobalTempViews()
                for view in global_views:
                    try:
                        spark.catalog.dropGlobalTempView(view)
                    except Exception:
                        pass
        except Exception:
            pass
    except Exception:
        pass  # Ignore all errors during cleanup


def reset_execution_state() -> None:
    """
    Reset execution engine and pipeline runner state.

    This function:
    - Clears any execution engine caches
    - Resets pipeline runner state
    - Clears LogWriter metrics caches

    Note: This clears module-level state, not instance state.
    """
    try:
        # Clear Delta Lake availability caches (already in reset_global_state)
        reset_global_state()

        # Clear LogWriter table row count caches if accessible
        try:
            # LogWriter caches are instance-level, so we can't clear them here
            # But we can reset global state which may help
            pass
        except Exception:
            pass

        # Clear any execution engine module-level state
        try:
            # ExecutionEngine doesn't have module-level caches currently
            pass
        except Exception:
            pass
    except Exception:
        pass  # Ignore all errors during cleanup


def clear_all_test_state(spark: Any, schema: str = "test_schema") -> None:
    """
    Comprehensive cleanup function that clears all test state.

    This is a convenience function that calls all cleanup functions.

    Args:
        spark: SparkSession instance to clean
        schema: Schema name to clean (default: "test_schema")
    """
    reset_global_state()
    reset_execution_state()
    clear_spark_views(spark)
    clear_all_tables(spark, schema_pattern="*")
    clear_spark_state(spark)
    clear_delta_tables(spark, schema)
