"""
Compatibility layer to abstract over PySpark and mock-spark.

Resolution order:
- Respect SPARKFORGE_ENGINE env var if set (pyspark|mock)
- Otherwise prefer PySpark if importable, else mock-spark
"""

from __future__ import annotations

import os
from typing import Any

_ENGINE = os.getenv("SPARKFORGE_ENGINE", "auto").lower()


def _try_import_pyspark():
    try:
        from pyspark.sql import Column as _Column  # type: ignore
        from pyspark.sql import DataFrame as _DataFrame  # type: ignore
        from pyspark.sql import SparkSession as _SparkSession  # type: ignore
        from pyspark.sql import functions as _F  # type: ignore
        from pyspark.sql import types as _types  # type: ignore

        return _DataFrame, _SparkSession, _Column, _F, _types
    except Exception:
        return None


def _try_import_mockspark():
    try:
        from mock_spark import (
            Column as _Column,  # type: ignore
        )
        from mock_spark import (
            MockDataFrame as _DataFrame,  # type: ignore
        )
        from mock_spark import (
            MockSparkSession as _SparkSession,  # type: ignore
        )
        from mock_spark import spark_types as _types  # type: ignore
        from mock_spark.functions import F as _F  # type: ignore

        return _DataFrame, _SparkSession, _Column, _F, _types
    except Exception:
        return None


def _select_engine():
    if _ENGINE in ("pyspark", "spark", "real"):
        ps = _try_import_pyspark()
        if ps is None:
            raise ImportError("SPARKFORGE_ENGINE=pyspark but pyspark is not importable")
        return "pyspark", ps
    if _ENGINE in ("mock", "mockspark"):
        ms = _try_import_mockspark()
        if ms is None:
            raise ImportError("SPARKFORGE_ENGINE=mock but mock-spark is not importable")
        return "mock", ms

    # auto mode: prefer mock-spark for test/development friendliness
    ms = _try_import_mockspark()
    if ms is not None:
        return "mock", ms
    ps = _try_import_pyspark()
    if ps is not None:
        return "pyspark", ps
    raise ImportError("Neither pyspark nor mock-spark could be imported")


_ENGINE_NAME, (_DataFrame, _SparkSession, _Column, _F, _types) = _select_engine()

# Public exports
DataFrame = _DataFrame
SparkSession = _SparkSession
Column = _Column
F = _F
types = _types


def is_mock_spark() -> bool:
    return _ENGINE_NAME == "mock"


def compat_name() -> str:
    return _ENGINE_NAME


def require_pyspark(message: str | None = None) -> None:
    if is_mock_spark():
        raise RuntimeError(
            message
            or "This operation requires PySpark and is not supported in mock mode"
        )


# Function shims when running in mock mode (no-op fallbacks)
def desc(col_name: str) -> Any:
    if _ENGINE_NAME == "pyspark":
        # Delegate to PySpark's desc via functions
        return F.desc(col_name)
    # mock-spark: return a tuple understood by orderBy implementation if present
    return (col_name, False)


def col(col_name: str) -> Any:
    return F.col(col_name)


def lit(value: Any) -> Any:
    return F.lit(value)


def current_timestamp() -> Any:
    ct = getattr(F, "current_timestamp", None)
    if callable(ct):
        return ct()
    # Fallback: literal current timestamp string
    import datetime as _dt

    return lit(_dt.datetime.now().isoformat())
