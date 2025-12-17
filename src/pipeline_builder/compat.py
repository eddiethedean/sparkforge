# mypy: ignore-errors
"""
Protocol-based compatibility layer.

Exposes protocol aliases and injected engine components (functions, types,
AnalysisException, Window/desc). Users must call `engine.configure_engine(...)`
with their engine objects (PySpark, sparkless, etc.) before using pipeline_builder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from .engine_config import get_engine
from .protocols import (
    ColumnProtocol,
    DataFrameProtocol,
    FunctionsProtocol,
    SparkSessionProtocol,
    TypesProtocol,
    WindowProtocol,
)

# Type aliases for typing
if TYPE_CHECKING:
    from .protocols import DataFrameProtocol as DataFrame
    from .protocols import SparkSessionProtocol as SparkSession
    from .protocols import ColumnProtocol as Column
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
    """Lazily resolve injected engine components to avoid import-time cycles."""
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
    """Compatibility stub for legacy checks."""
    try:
        return get_engine().engine_name == "mock"
    except Exception:
        return False


def compat_name() -> str:
    """Return configured engine name."""
    try:
        return get_engine().engine_name
    except Exception:
        return "unknown"


def get_functions_from_session(spark: SparkSession) -> Any:
    """
    Get functions module from a SparkSession.

    This is a compatibility helper for getting the functions module.

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
