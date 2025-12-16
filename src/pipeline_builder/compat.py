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
    Tuple,
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
    spark_mode_explicit = "SPARK_MODE" in os.environ  # Check if user explicitly set it

    if spark_mode == "real":
        # Use real PySpark
        try:
            from pyspark.sql import (
                Column as PySparkColumn,
            )
            from pyspark.sql import (
                DataFrame as PySparkDataFrame,
            )
            from pyspark.sql import (
                SparkSession as PySparkSparkSession,
            )
            from pyspark.sql import functions as PySparkF
            from pyspark.sql import types as PySparkTypes
            from pyspark.sql.functions import desc as PySparkDesc
            from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
            from pyspark.sql.window import Window as PySparkWindow

            return (
                "pyspark",
                (
                    PySparkDataFrame,
                    PySparkSparkSession,
                    PySparkColumn,
                    PySparkF,
                    PySparkTypes,
                    PySparkAnalysisException,
                ),
                PySparkWindow,
                PySparkDesc,
            )
        except ImportError as e:
            raise ImportError(
                "SPARK_MODE=real but PySpark is not installed. "
                "Install with: pip install sparkforge[pyspark]"
            ) from e
    else:
        # Use sparkless as the mock engine backend
        try:
            from sparkless import (
                AnalysisException as MockAnalysisException,  # type: ignore[import]
            )
            from sparkless import (  # type: ignore[import]
                Column as MockColumn,
            )
            from sparkless import (
                DataFrame as MockDataFrame,
            )
            from sparkless import (
                SparkSession as MockSparkSession,
            )
            from sparkless import Window as MockWindow  # type: ignore[import]
            from sparkless import functions as MockF  # type: ignore[import]
            from sparkless import spark_types as MockTypes  # type: ignore[import]
            from sparkless.functions import desc as MockDesc  # type: ignore[import]
            from sparkless.functions.core.column import (
                ColumnOperation,  # type: ignore[import]
            )

            # Compatibility wrapper to make aggregate functions return Column-compatible objects
            # This fixes the issue where sparkless aggregate functions return ColumnOperation
            # instead of Column, making them incompatible with isinstance() checks.
            # Make ColumnCompatibleWrapper actually inherit from Column so isinstance() works
            class ColumnCompatibleWrapper(MockColumn):  # type: ignore[misc,valid-type]
                """
                Wrapper that makes ColumnOperation instances compatible with Column.

                This class inherits from Column to pass isinstance() checks,
                while delegating all operations to the wrapped ColumnOperation.
                """

                def __new__(cls, column_op: Any) -> Any:
                    """Create a new instance that is also a Column."""
                    # Create instance without calling Column.__init__
                    instance = object.__new__(cls)
                    object.__setattr__(instance, "_column_op", column_op)
                    return instance

                def __getattr__(self, name: str) -> Any:
                    """Delegate all attribute access to the wrapped ColumnOperation."""
                    if name == "_column_op":
                        raise AttributeError(name)
                    return getattr(self._column_op, name)

                def __setattr__(self, name: str, value: Any) -> None:
                    """Handle attribute setting."""
                    if name == "_column_op":
                        object.__setattr__(self, name, value)
                    elif hasattr(self, "_column_op"):
                        setattr(self._column_op, name, value)
                    else:
                        object.__setattr__(self, name, value)

                def __repr__(self) -> str:
                    """Return representation of wrapped object."""
                    return repr(self._column_op)

                def __str__(self) -> str:
                    """Return string representation of wrapped object."""
                    return str(self._column_op)

            # Create a wrapper for functions to ensure aggregate functions return Column
            class FunctionsWrapper:
                """
                Wrapper for sparkless functions to ensure PySpark compatibility.

                This wrapper ensures that aggregate functions like count(), sum(), etc.
                return Column-compatible objects like PySpark does.
                """

                def __init__(self, original_functions: Any) -> None:
                    """Initialize the wrapper with the original functions module."""
                    self._original = original_functions
                    # Aggregate functions that should return Column
                    self._aggregate_functions = {
                        "count",
                        "sum",
                        "avg",
                        "mean",
                        "max",
                        "min",
                        "countDistinct",
                        "stddev",
                        "stddev_pop",
                        "stddev_samp",
                        "variance",
                        "var_pop",
                        "var_samp",
                        "collect_list",
                        "collect_set",
                        "first",
                        "last",
                        "kurtosis",
                        "skewness",
                        "corr",
                        "covar_pop",
                        "covar_samp",
                        "grouping",
                        "grouping_id",
                    }

                def __getattr__(self, name: str) -> Any:
                    """Get attribute from original functions, wrapping aggregate functions."""
                    attr = getattr(self._original, name)

                    # If it's an aggregate function, wrap it to return Column-compatible object
                    if name in self._aggregate_functions and callable(attr):

                        def wrapped_agg(*args: Any, **kwargs: Any) -> Any:
                            result = attr(*args, **kwargs)
                            # If result is ColumnOperation, wrap it to be Column-compatible
                            if isinstance(result, ColumnOperation):
                                return ColumnCompatibleWrapper(result)  # type: ignore[return-value]
                            return result

                        return wrapped_agg

                    return attr

            # Wrap the functions module
            wrapped_functions = FunctionsWrapper(MockF)

            return (
                "mock",
                (
                    MockDataFrame,
                    MockSparkSession,
                    MockColumn,
                    wrapped_functions,  # Use wrapped functions
                    MockTypes,
                    MockAnalysisException,
                ),
                MockWindow,
                MockDesc,
            )
        except ImportError as e:
            # If sparkless is not available, fall back to PySpark if:
            # 1. User didn't explicitly set SPARK_MODE=mock (default is mock but not explicit)
            # 2. PySpark is available
            if not spark_mode_explicit:
                # Try to fall back to PySpark
                try:
                    from pyspark.sql import (
                        Column as PySparkColumn,
                    )
                    from pyspark.sql import (
                        DataFrame as PySparkDataFrame,
                    )
                    from pyspark.sql import (
                        SparkSession as PySparkSparkSession,
                    )
                    from pyspark.sql import functions as PySparkF
                    from pyspark.sql import types as PySparkTypes
                    from pyspark.sql.functions import desc as PySparkDesc
                    from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
                    from pyspark.sql.window import Window as PySparkWindow

                    # Log a warning that we're falling back to PySpark
                    import warnings
                    warnings.warn(
                        "sparkless is not installed. Falling back to PySpark. "
                        "To use mock mode, install with: pip install sparkforge[mock]",
                        UserWarning,
                        stacklevel=2
                    )

                    return (
                        "pyspark",
                        (
                            PySparkDataFrame,
                            PySparkSparkSession,
                            PySparkColumn,
                            PySparkF,
                            PySparkTypes,
                            PySparkAnalysisException,
                        ),
                        PySparkWindow,
                        PySparkDesc,
                    )
                except ImportError:
                    # Neither sparkless nor PySpark available
                    raise ImportError(
                        "Neither sparkless (for mock mode) nor PySpark (for real mode) is installed. "
                        "Install with: pip install sparkforge[mock] or pip install sparkforge[pyspark]"
                    ) from e
            else:
                # User explicitly set SPARK_MODE=mock, so raise error
                raise ImportError(
                    "SPARK_MODE=mock but sparkless is not installed. "
                    "Install with: pip install sparkforge[mock]"
                ) from e


(
    _ENGINE_NAME,
    (_DataFrame, _SparkSession, _Column, F, types, AnalysisException),
    Window,
    desc,
) = _select_engine()

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
