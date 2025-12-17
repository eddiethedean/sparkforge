"""
Compatibility helpers for working with protocol-based Spark sessions.
"""

from __future__ import annotations

from typing import Any, Optional

from .compat import SparkSession


def create_dataframe_compat(
    spark: SparkSession,  # type: ignore[valid-type]
    data: Any,
    schema: Optional[Any] = None,
    original_method: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """
    Create DataFrame with compatibility for PySpark.

    Supports all schema formats: list of strings, StructType, None.
    Handles PySpark 3.5+ schema argument position differences.

    Args:
        spark: SparkSession instance
        data: Data to create DataFrame from (list of tuples, list of dicts, etc.)
        schema: Schema definition (list of strings, StructType, or None)
        original_method: Original createDataFrame method (to avoid recursion when monkey-patched)
        **kwargs: Additional arguments passed to createDataFrame

    Returns:
        DataFrame instance
    """
    # Use original method if provided (to avoid recursion), otherwise use spark.createDataFrame
    create_df = (
        original_method if original_method is not None else spark.createDataFrame
    )

    # Call createDataFrame method
    # Handle PySpark 3.5+ schema argument issues (PySpark bug, not mock-spark)
    if schema is None:
        return create_df(data, **kwargs)
    else:
        # Try different calling patterns to handle PySpark version differences
        # Pattern 1: Positional schema (PySpark 3.5+)
        try:
            if kwargs:
                return create_df(data, schema, **kwargs)
            else:
                return create_df(data, schema)
        except Exception as e:
            error_str = str(e)
            # Check if this is the PySpark StructType error (known PySpark 3.5+ bug)
            if "NOT_LIST_OR_NONE_OR_STRUCT" in error_str:
                # Try without kwargs
                try:
                    return create_df(data, schema)
                except Exception:
                    # Last resort: try with schema as keyword
                    return create_df(data, schema=schema, **kwargs)
            # For other errors, try keyword argument (older PySpark)
            try:
                return create_df(data, schema=schema, **kwargs)
            except Exception:
                # Final fallback: try without kwargs and keyword schema
                return create_df(data, schema=schema)


def is_dataframe_like(obj: Any) -> bool:
    """
    Check if object is DataFrame-like using structural typing.

    Checks for essential DataFrame methods: count, columns (property), filter.

    Args:
        obj: Object to check

    Returns:
        True if object has DataFrame-like interface, False otherwise
    """
    # columns is typically a property (not callable), count and filter are methods
    return (
        hasattr(obj, "count")
        and hasattr(obj, "columns")
        and hasattr(obj, "filter")
        and callable(getattr(obj, "count", None))
        and callable(getattr(obj, "filter", None))
    )


def detect_spark_type(spark: SparkSession) -> str:
    """
    Detect if spark session is PySpark.

    Args:
        spark: SparkSession instance to check

    Returns:
        'pyspark', 'mock', or 'unknown'
    """
    # Fast-path: PySpark sessions have a JVM bridge
    if hasattr(spark, "sparkContext") and hasattr(spark.sparkContext, "_jsc"):
        return "pyspark"

    try:
        spark_module = type(spark).__module__
        if "pyspark" in spark_module:
            return "pyspark"
        # Detect sparkless/mock sessions by module path
        if "sparkless" in spark_module or "mock" in spark_module:
            return "mock"
    except Exception:
        pass

    # Fallback to engine name if available
    try:
        from .compat import compat_name  # Local import to avoid cycles

        engine_name = compat_name()
        if engine_name in {"mock", "sparkless"}:
            return "mock"
        if engine_name == "pyspark":
            return "pyspark"
    except Exception:
        pass

    return "unknown"


def create_test_dataframe(
    spark: SparkSession,  # type: ignore[valid-type]
    data: Any,
    schema: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """
    High-level helper for creating test DataFrames.

    Provides a consistent API for creating DataFrames in tests.
    Handles PySpark 3.5+ schema argument position differences.

    Args:
        spark: SparkSession instance (PySpark or mock-spark)
        data: Data to create DataFrame from (list of tuples, list of dicts, etc.)
        schema: Schema definition (list of strings, StructType, or None)
        **kwargs: Additional arguments passed to createDataFrame

    Returns:
        DataFrame instance
    """
    return create_dataframe_compat(spark, data, schema, **kwargs)
