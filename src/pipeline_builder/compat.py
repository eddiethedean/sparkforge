"""
Compatibility layer for PySpark and mock-spark.

This module provides a unified interface for both PySpark and mock-spark,
allowing the codebase to work with either implementation seamlessly.
"""

from __future__ import annotations

import os
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Tuple,
    Type,
)

# TypeAlias is available in Python 3.10+, use typing_extensions for 3.8/3.9
# Import from typing_extensions for Python 3.8 compatibility
if TYPE_CHECKING:
    from typing_extensions import TypeAlias
else:
    # Runtime: TypeAlias is not needed
    TypeAlias = Any  # type: ignore[misc,assignment]


def _select_engine() -> Tuple[str, Tuple[Any, Any, Any, Any, Any, Any], Any, Any]:
    """
    Select the appropriate engine (PySpark or mock-spark) based on environment.

    Returns:
        Tuple of (engine_name, (DataFrame, SparkSession, Column, F, types, AnalysisException))
    """
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "real":
        # Use real PySpark
        try:
            from pyspark.sql import (
                Column as PySparkColumn,
                DataFrame as PySparkDataFrame,
                SparkSession as PySparkSparkSession,
            )
            from pyspark.sql import functions as PySparkF
            from pyspark.sql import types as PySparkTypes
            from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
            from pyspark.sql.window import Window as PySparkWindow
            from pyspark.sql.functions import desc as PySparkDesc

            return (
                "pyspark",
                (PySparkDataFrame, PySparkSparkSession, PySparkColumn, PySparkF, PySparkTypes, PySparkAnalysisException),
                PySparkWindow,
                PySparkDesc,
            )
        except ImportError as e:
            raise ImportError(
                "SPARK_MODE=real but PySpark is not installed. "
                "Install with: pip install sparkforge[pyspark]"
            ) from e
    else:
        # Use mock-spark
        try:
            from mock_spark import (
                Column as MockColumn,
                DataFrame as MockDataFrame,
                SparkSession as MockSparkSession,
            )
            from mock_spark import functions as MockF
            from mock_spark import spark_types as MockTypes
            from mock_spark import AnalysisException as MockAnalysisException
            from mock_spark import Window as MockWindow
            from mock_spark.functions import desc as MockDesc

            return (
                "mock",
                (MockDataFrame, MockSparkSession, MockColumn, MockF, MockTypes, MockAnalysisException),
                MockWindow,
                MockDesc,
            )
        except ImportError as e:
            raise ImportError(
                "SPARK_MODE=mock but mock-spark is not installed. "
                "Install with: pip install sparkforge[mock]"
            ) from e


_ENGINE_NAME, (_DataFrame, _SparkSession, _Column, F, types, AnalysisException), Window, desc = _select_engine()

# Type aliases for mypy - defined in TYPE_CHECKING block
if TYPE_CHECKING:
    # Import actual PySpark types for type checking when available
    try:
        from pyspark.sql import Column as _PySparkColumnType
        from pyspark.sql import DataFrame as _PySparkDataFrameType
        from pyspark.sql import SparkSession as _PySparkSparkSessionType
    except ImportError:
        # Use Any as fallback if PySpark not available during type checking
        _PySparkColumnType = Any  # type: ignore[assignment,misc]
        _PySparkDataFrameType = Any  # type: ignore[assignment,misc]
        _PySparkSparkSessionType = Any  # type: ignore[assignment,misc]
    
    # Define type aliases for mypy
    # Using TypeAlias from typing_extensions (available in Python 3.8+)
    DataFrame: TypeAlias = _PySparkDataFrameType
    SparkSession: TypeAlias = _PySparkSparkSessionType
    Column: TypeAlias = _PySparkColumnType
else:
    # Runtime: these will be assigned from the selected engine below
    # Dummy assignments so variables exist (not used, just to satisfy mypy)
    pass  # Variables will be assigned below

# Runtime assignment - these are the actual classes
# The type aliases in TYPE_CHECKING block tell mypy what types these should be
# We use assignment and no-redef to allow reassignment from the type alias to the runtime class
# The misc error code is needed because we're assigning runtime classes to type aliases
DataFrame = _DataFrame  # type: ignore[misc]
SparkSession = _SparkSession  # type: ignore[misc]
Column = _Column  # type: ignore[misc]


def is_mock_spark() -> bool:
    """Check if currently using mock-spark."""
    return bool(_ENGINE_NAME == "mock")


def compat_name() -> str:
    """Get the name of the current compatibility engine."""
    return str(_ENGINE_NAME)


def get_functions_from_session(spark: SparkSession) -> Any:
    """
    Get functions module from a SparkSession.

    This is a compatibility helper for getting the functions module
    from either PySpark or mock-spark SparkSession.

    Args:
        spark: SparkSession instance

    Returns:
        Functions module (F) from the appropriate engine
    """
    return F


def get_current_timestamp() -> Any:
    """Get current timestamp."""
    ct = getattr(F, "current_timestamp", None)
    if callable(ct):
        return ct()
    # Fallback: literal current timestamp string
    from datetime import datetime

    return datetime.now().isoformat()


# Export commonly used items
__all__ = [
    "DataFrame",
    "SparkSession",
    "Column",
    "F",
    "types",
    "AnalysisException",
    "Window",
    "desc",
    "is_mock_spark",
    "compat_name",
    "get_functions_from_session",
    "get_current_timestamp",
]
