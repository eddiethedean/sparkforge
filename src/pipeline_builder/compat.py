# mypy: ignore-errors
"""
Protocol-based compatibility layer for engine abstraction.

This module provides a compatibility layer that abstracts over different
Spark/PySpark implementations (real PySpark, mock Spark, sparkless, etc.).
It exposes protocol aliases and injected engine components that are configured
at runtime through the engine configuration system.

**Key Features:**
    - **Engine Detection**: Automatically detects and uses the configured engine
    - **Protocol Aliases**: Provides type-safe aliases for DataFrame, SparkSession, Column
    - **Lazy Loading**: Components are loaded lazily to avoid import-time cycles
    - **Mock Support**: Supports both real PySpark and mock Spark for testing

**Usage:**
    Before using pipeline_builder, you must configure the engine:

    >>> from pipeline_builder.engine_config import configure_engine
    >>> from pyspark.sql import SparkSession
    >>>
    >>> spark = SparkSession.builder.appName("test").getOrCreate()
    >>> configure_engine(spark=spark)

    Then you can import and use the compatibility layer:

    >>> from pipeline_builder.compat import DataFrame, SparkSession, F
    >>> df: DataFrame = spark.createDataFrame([(1, "test")], ["id", "name"])

**Exported Components:**
    - **DataFrame**: Protocol alias for DataFrame type
    - **SparkSession**: Protocol alias for SparkSession type
    - **Column**: Protocol alias for Column type
    - **F**: Functions module (PySpark functions or mock equivalent)
    - **types**: Types module (StructType, StringType, etc.)
    - **AnalysisException**: Exception class for analysis errors
    - **Window**: Window functions
    - **desc**: Descending sort function

**Engine Configuration:**
    The engine must be configured before use. See `engine_config.configure_engine()`
    for details on how to configure the engine with your Spark/PySpark objects.

Example:
    >>> from pipeline_builder.engine_config import configure_engine
    >>> from pipeline_builder.compat import DataFrame, F
    >>> from pyspark.sql import SparkSession
    >>>
    >>> # Configure engine
    >>> spark = SparkSession.builder.appName("test").getOrCreate()
    >>> configure_engine(spark=spark)
    >>>
    >>> # Use compatibility layer
    >>> from pipeline_builder.compat import DataFrame
    >>> df: DataFrame = spark.createDataFrame([(1, "test")], ["id", "name"])
    >>> result = df.filter(F.col("id") > 0)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from .engine_config import get_engine
from .protocols import (
    ColumnProtocol,
    DataFrameProtocol,
    SparkSessionProtocol,
)

# Type aliases for typing
if TYPE_CHECKING:
    from .protocols import ColumnProtocol as Column
    from .protocols import DataFrameProtocol as DataFrame
    from .protocols import SparkSessionProtocol as SparkSession
else:
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    Column = Any  # type: ignore[assignment]

# Try to bind engine components immediately if configured
try:
    _eng = get_engine()
    DataFrame = cast(Any, _eng.dataframe_cls or DataFrameProtocol)
    SparkSession = cast(Any, _eng.spark_session_cls or SparkSessionProtocol)
    Column = cast(Any, _eng.column_cls or ColumnProtocol)
    F = _eng.functions
    types = _eng.types
    AnalysisException = _eng.analysis_exception  # type: ignore[assignment]
    Window = _eng.window  # type: ignore[assignment]
    desc = _eng.desc
except Exception:
    # Defer to __getattr__ if not configured yet
    pass


def __getattr__(name: str) -> Any:
    """Lazily resolve injected engine components to avoid import-time cycles.

    This function is called when an attribute is accessed that wasn't found
    during module initialization. It allows lazy loading of engine components
    to avoid circular import issues.

    Args:
        name: Name of the attribute to resolve. Supported names:
            - "F": Functions module
            - "types": Types module
            - "AnalysisException": Analysis exception class
            - "Window": Window functions
            - "desc": Descending sort function
            - "DataFrame": DataFrame protocol/class
            - "SparkSession": SparkSession protocol/class
            - "Column": Column protocol/class

    Returns:
        The requested engine component from the configured engine.

    Raises:
        AttributeError: If the requested attribute is not supported or
            the engine is not configured.

    Note:
        This function is automatically called by Python when accessing
        module attributes that don't exist at import time. It should not
        be called directly.
    """
    if name in {"F", "types", "AnalysisException", "Window", "desc"}:
        eng = get_engine()
        if name == "F":
            return eng.functions
        if name == "types":
            return eng.types
        if name == "AnalysisException":
            return eng.analysis_exception
        if name == "Window":
            return eng.window
        if name == "desc":
            return eng.desc
    if name == "DataFrame":
        eng = get_engine()
        return eng.dataframe_cls or DataFrameProtocol
    if name == "SparkSession":
        eng = get_engine()
        return eng.spark_session_cls or SparkSessionProtocol
    if name == "Column":
        eng = get_engine()
        return eng.column_cls or ColumnProtocol
    raise AttributeError(f"module {__name__} has no attribute {name}")


def is_mock_spark() -> bool:
    """Check if the configured engine is a mock Spark implementation.

    This function is useful for conditional logic that needs to behave
    differently in test environments vs production.

    Returns:
        True if the configured engine is "mock", False otherwise.
        Returns False if the engine is not configured or an error occurs.

    Example:
        >>> from pipeline_builder.compat import is_mock_spark
        >>> if is_mock_spark():
        ...     print("Running in test mode")
        ... else:
        ...     print("Running with real PySpark")
    """
    try:
        return get_engine().engine_name == "mock"
    except Exception:
        return False


def compat_name() -> str:
    """Get the name of the currently configured engine.

    Returns the engine name that was configured via `engine_config.configure_engine()`.
    Common values include "pyspark", "mock", "sparkless", etc.

    Returns:
        Engine name string. Returns "unknown" if the engine is not configured
        or an error occurs.

    Example:
        >>> from pipeline_builder.compat import compat_name
        >>> engine_name = compat_name()
        >>> print(f"Using engine: {engine_name}")
    """
    try:
        return get_engine().engine_name
    except Exception:
        return "unknown"


def get_functions_from_session(spark: SparkSession) -> Any:
    """Get functions module from a SparkSession.

    Compatibility helper function that returns the configured functions
    module (F). The spark parameter is accepted for API compatibility
    but is not used, as the functions module comes from the configured
    engine, not from the SparkSession directly.

    Args:
        spark: SparkSession instance. Accepted for API compatibility
            but not used internally.

    Returns:
        Functions module (F) from the configured engine. This is the
        same as accessing `F` directly from the compat module.

    Example:
        >>> from pipeline_builder.compat import get_functions_from_session
        >>> from pyspark.sql import SparkSession
        >>>
        >>> spark = SparkSession.builder.appName("test").getOrCreate()
        >>> F = get_functions_from_session(spark)
        >>> # Use F for DataFrame operations
        >>> df.select(F.col("id"), F.lit("test"))
    """
    return F


def get_current_timestamp() -> Any:
    """Get current timestamp using the configured engine's timestamp function.

    Returns the current timestamp using the engine's `current_timestamp()`
    function if available, otherwise falls back to a Python datetime ISO string.

    Returns:
        Current timestamp as a Column expression (if using PySpark) or
        ISO format string (if fallback is used).

    Example:
        >>> from pipeline_builder.compat import get_current_timestamp
        >>> timestamp = get_current_timestamp()
        >>> # Use in DataFrame operations
        >>> df.withColumn("created_at", timestamp)
    """
    ct = getattr(F, "current_timestamp", None)
    if callable(ct):
        return ct()
    # Fallback: literal current timestamp string
    from datetime import datetime

    return datetime.now().isoformat()


__all__ = [
    "DataFrame",
    "SparkSession",
    "Column",
    "F",
    "types",
    "AnalysisException",
    "Window",
    "desc",
    "get_functions_from_session",
    "get_current_timestamp",
    "is_mock_spark",
]
