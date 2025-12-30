"""
Test isolation utilities for ensuring clean state between tests.

This module provides helper functions to clear Spark state, Delta Lake metadata,
and global caches to prevent test pollution. It also provides utilities for
generating unique test identifiers for parallel execution isolation.
"""

import os
import tempfile
import threading
from contextlib import contextmanager
from typing import Any, Optional
from uuid import uuid4


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


# ============================================================================
# Parallel Execution Isolation Utilities
# ============================================================================


def get_worker_id() -> str:
    """
    Get pytest-xdist worker ID or process ID for isolation.

    Returns:
        Worker ID string (e.g., "0", "1") or process ID if not using xdist
    """
    try:
        # pytest-xdist sets PYTEST_XDIST_WORKER environment variable
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
        if worker_id:
            # Format is usually "gw0", "gw1", etc. - extract number
            return worker_id.replace("gw", "").replace("master", "0")
        # Fall back to process ID if not using xdist
        return str(os.getpid())
    except Exception:
        # If anything fails, use process ID
        return str(os.getpid())


def get_test_identifier() -> str:
    """
    Generate unique test identifier for isolation.

    Combines worker ID, process ID, and UUID for maximum uniqueness.

    Returns:
        Unique identifier string: "{worker_id}_{pid}_{uuid}"
    """
    worker_id = get_worker_id()
    pid = os.getpid()
    uuid_part = uuid4().hex[:8]
    return f"{worker_id}_{pid}_{uuid_part}"


def get_unique_schema(base_name: str) -> str:
    """
    Generate unique schema name for test isolation.

    Args:
        base_name: Base schema name (e.g., "bronze", "silver", "gold")

    Returns:
        Unique schema name: "{base_name}_{worker_id}_{pid}_{uuid}"

    Example:
        >>> schema = get_unique_schema("bronze")
        >>> # Returns: "bronze_0_12345_a1b2c3d4"
    """
    identifier = get_test_identifier()
    return f"{base_name}_{identifier}"


def get_unique_warehouse_dir(base_name: str = "spark-warehouse") -> str:
    """
    Generate unique warehouse directory path for test isolation.

    Args:
        base_name: Base directory name (default: "spark-warehouse")

    Returns:
        Unique temporary directory path

    Example:
        >>> dir_path = get_unique_warehouse_dir()
        >>> # Returns: "/tmp/spark-warehouse_0_12345_a1b2c3d4"
    """
    identifier = get_test_identifier()
    return tempfile.mkdtemp(prefix=f"{base_name}_{identifier}_")


def get_unique_app_name(base_name: str = "SparkForgeTest") -> str:
    """
    Generate unique Spark application name for test isolation.

    Args:
        base_name: Base application name (default: "SparkForgeTest")

    Returns:
        Unique application name: "{base_name}_{worker_id}_{pid}_{uuid}"

    Example:
        >>> app_name = get_unique_app_name("MyTest")
        >>> # Returns: "MyTest_0_12345_a1b2c3d4"
    """
    identifier = get_test_identifier()
    return f"{base_name}_{identifier}"


@contextmanager
def isolate_env_var(var_name: str):
    """
    Context manager to isolate environment variable changes.

    Saves the original value, allows modification within context,
    and restores it after.

    Args:
        var_name: Name of environment variable to isolate

    Example:
        >>> with isolate_env_var("SPARKFORGE_ENGINE"):
        ...     os.environ["SPARKFORGE_ENGINE"] = "pyspark"
        ...     # Do test work
        ... # Variable automatically restored
    """
    original_value = os.environ.get(var_name)
    try:
        yield
    finally:
        if original_value is not None:
            os.environ[var_name] = original_value
        elif var_name in os.environ:
            del os.environ[var_name]


def cleanup_test_tables(spark: Any, schema: str) -> None:
    """
    Clean up all tables in a specific schema.

    This is a helper for tests to clean up their own tables at the end.

    Args:
        spark: SparkSession instance
        schema: Schema name to clean up

    Example:
        >>> cleanup_test_tables(spark, "bronze_0_12345_abc")
        >>> # Drops all tables in that schema
    """
    if not spark:
        return

    try:
        # List all tables in the schema
        tables = spark.catalog.listTables(schema)
        for table in tables:
            table_name = table.name if hasattr(table, "name") else str(table)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")
            except Exception:
                pass

        # Optionally drop the schema itself
        try:
            spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
        except Exception:
            pass
    except Exception:
        pass  # Ignore errors during cleanup


class ThreadLocalEnvVar:
    """
    Thread-local environment variable proxy for parallel test isolation.
    
    This class provides thread-local storage for environment variables,
    allowing each test thread to have its own isolated environment state
    without affecting other parallel tests.
    
    Example:
        >>> env_var = ThreadLocalEnvVar("SPARKFORGE_ENGINE")
        >>> env_var.set("pyspark")
        >>> value = env_var.get()  # Returns "pyspark" for this thread only
        >>> env_var.delete()  # Remove thread-local value
    """
    
    _local = threading.local()
    
    def __init__(self, var_name: str):
        """
        Initialize thread-local environment variable proxy.
        
        Args:
            var_name: Name of the environment variable to proxy
        """
        self.var_name = var_name
    
    def get(self) -> Optional[str]:
        """
        Get the value of the environment variable.
        
        Checks thread-local storage first, then falls back to os.environ.
        
        Returns:
            Environment variable value, or None if not set
        """
        if hasattr(self._local, 'env') and self.var_name in self._local.env:
            return self._local.env[self.var_name]
        return os.environ.get(self.var_name)
    
    def set(self, value: str) -> None:
        """
        Set the value of the environment variable in thread-local storage.
        
        Args:
            value: Value to set
        """
        if not hasattr(self._local, 'env'):
            self._local.env = {}
        self._local.env[self.var_name] = value
    
    def delete(self) -> None:
        """
        Delete the thread-local value (falls back to os.environ).
        """
        if hasattr(self._local, 'env') and self.var_name in self._local.env:
            del self._local.env[self.var_name]
    
    def __enter__(self):
        """Context manager entry - save original value."""
        self._original_value = os.environ.get(self.var_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - restore original value."""
        if self._original_value is not None:
            self.set(self._original_value)
        else:
            self.delete()


def reset_engine_state() -> None:
    """
    Reset thread-local engine state.
    
    This clears the thread-local engine configuration, causing the next
    get_engine() call to use global state or raise an error.
    
    Useful for test isolation to ensure clean engine state between tests.
    """
    try:
        from pipeline_builder.engine_config import reset_engine_state as _reset
        _reset()
    except Exception:
        pass  # Ignore errors if engine_config is not available
