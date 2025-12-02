"""
Compatibility layer to abstract over PySpark and mock-spark.

This module provides a unified interface for working with Spark-like data processing
frameworks, allowing sparkforge to work with either PySpark or mock-spark (or any
other compatible implementation).

Resolution order:
- Respect SPARKFORGE_ENGINE env var if set (pyspark|mock)
- Otherwise prefer PySpark if importable, else mock-spark
"""

from __future__ import annotations

import os
from typing import Any, Optional, Tuple, Type

_ENGINE = os.getenv("SPARKFORGE_ENGINE", "auto").lower()


def _try_import_pyspark() -> Optional[
    Tuple[Type[Any], Type[Any], Type[Any], Any, Any, Type[Exception]]
]:
    """Try to import PySpark modules."""
    try:
        from pyspark.sql import Column as _Column
        from pyspark.sql import DataFrame as _DataFrame
        from pyspark.sql import SparkSession as _SparkSession
        from pyspark.sql import functions as _F
        from pyspark.sql import types as _types
        from pyspark.sql.utils import (
            AnalysisException as _AnalysisException,
        )

        return _DataFrame, _SparkSession, _Column, _F, _types, _AnalysisException
    except Exception:
        return None


def _try_import_mockspark() -> Optional[
    Tuple[Type[Any], Type[Any], Type[Any], Any, Any, Type[Exception]]
]:
    """Try to import mock-spark modules."""
    try:
        # Use PySpark-compatible import paths (available in mock-spark 3.8.0+)
        from mock_spark.sql import Column as _Column
        from mock_spark.sql import DataFrame as _DataFrame
        from mock_spark.sql import SparkSession as _SparkSession
        from mock_spark.sql import types as _types
        from mock_spark.sql.utils import (
            AnalysisException as _AnalysisException,
        )
        from mock_spark.sql import functions as _F

        return _DataFrame, _SparkSession, _Column, _F, _types, _AnalysisException
    except Exception:
        # Fallback to old import paths for backward compatibility with older mock-spark versions
        try:
            from mock_spark import Column as _Column
            from mock_spark import DataFrame as _DataFrame
            from mock_spark import SparkSession as _SparkSession
            from mock_spark import spark_types as _types
            from mock_spark.errors import (
                AnalysisException as _AnalysisException,
            )
            from mock_spark.functions import F as _F

            return _DataFrame, _SparkSession, _Column, _F, _types, _AnalysisException
        except Exception:
            # Log the error for debugging but don't fail
            # Note: mock-spark 3.1.0 has Python 3.8 compatibility issues
            # This is a known issue with the mock-spark package
            return None


def _select_engine() -> Tuple[
    str, Tuple[Type[Any], Type[Any], Type[Any], Any, Any, Type[Exception]]
]:
    """Select the appropriate engine based on environment and availability."""
    if _ENGINE in ("pyspark", "spark", "real"):
        ps = _try_import_pyspark()
        if ps is None:
            raise ImportError(
                "SPARKFORGE_ENGINE=pyspark but pyspark is not importable. "
                "Install with: pip install sparkforge[pyspark]"
            )
        return "pyspark", ps
    if _ENGINE in ("mock", "mockspark"):
        ms = _try_import_mockspark()
        if ms is None:
            raise ImportError(
                "SPARKFORGE_ENGINE=mock but mock-spark is not importable. "
                "Install with: pip install sparkforge[mock]"
            )
        return "mock", ms

    # auto mode: prefer PySpark if available, otherwise mock-spark
    ps = _try_import_pyspark()
    if ps is not None:
        return "pyspark", ps
    ms = _try_import_mockspark()
    if ms is not None:
        return "mock", ms

    raise ImportError(
        "Neither pyspark nor mock-spark could be imported. "
        "Install with: pip install sparkforge[pyspark] or pip install sparkforge[mock]"
    )


_ENGINE_NAME, (DataFrame, SparkSession, Column, F, types, AnalysisException) = (
    _select_engine()
)

# Type annotations to help mypy understand these are not Optional
if False:
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        # These are type aliases for mypy's benefit
        DataFrame = DataFrame  # noqa: F811
        SparkSession = SparkSession  # noqa: F811
        Column = Column  # noqa: F811


def is_mock_spark() -> bool:
    """Check if currently using mock-spark."""
    return bool(_ENGINE_NAME == "mock")


def compat_name() -> str:
    """Get the name of the current compatibility engine."""
    return str(_ENGINE_NAME)


def require_pyspark(message: str | None = None) -> None:
    """Raise an error if not using PySpark."""
    if is_mock_spark():
        raise RuntimeError(
            message
            or "This operation requires PySpark and is not supported in mock mode"
        )


# Function shims when running in mock mode (no-op fallbacks)
def desc(col_name: str) -> Any:
    """Get descending order expression for a column."""
    if _ENGINE_NAME == "pyspark":
        # Delegate to PySpark's desc via functions
        return F.desc(col_name)
    # mock-spark: return a tuple understood by orderBy implementation if present
    return (col_name, False)


def col(col_name: str) -> Any:
    """Get a column by name."""
    return F.col(col_name)


def lit(value: Any) -> Any:
    """Create a literal column."""
    return F.lit(value)


def current_timestamp() -> Any:
    """Get current timestamp."""
    ct = getattr(F, "current_timestamp", None)
    if callable(ct):
        return ct()
    # Fallback: literal current timestamp string
    import datetime as _dt

    return lit(_dt.datetime.now().isoformat())


def get_functions_from_session(spark: SparkSession) -> Any:
    """
    Get functions object from a spark session.

    This ensures we get the right functions object (PySpark or mock-spark)
    that matches the session.

    Args:
        spark: SparkSession instance

    Returns:
        Functions object matching the session type
    """
    # Detect from session type, not just from is_mock_spark()
    try:
        spark_module = type(spark).__module__
        if "mock_spark" in spark_module:
            try:
                import mock_spark.functions as mock_functions

                return mock_functions
            except ImportError:
                pass
    except Exception:
        pass

    # Fallback: check is_mock_spark() for cases where module detection fails
    if is_mock_spark():
        try:
            import mock_spark.functions as mock_functions

            return mock_functions
        except ImportError:
            pass
    # Fallback to default functions from compat layer
    return F


# Export Window if available
try:
    from pyspark.sql import Window
except ImportError:
    # Fallback Window for mock-spark
    class Window:  # type: ignore[no-redef]
        @staticmethod
        def orderBy(*cols: Any) -> Any:
            return None
