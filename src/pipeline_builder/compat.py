"""
Protocol-based compatibility layer.

Exposes protocol aliases and injected engine components (functions, types,
AnalysisException, Window/desc). Users must call `engine.configure_engine(...)`
with their engine objects (PySpark, sparkless, etc.) before using pipeline_builder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Tuple, cast
from typing_extensions import TypeAlias

from .engine_config import get_engine
from .protocols import (
    AnalysisExceptionProtocol,
    ColumnProtocol,
    DataFrameProtocol,
    FunctionsProtocol,
    SparkSessionProtocol,
    TypesProtocol,
    WindowProtocol,
)

# Type aliases for typing
if TYPE_CHECKING:
    DataFrame: TypeAlias = DataFrameProtocol
    SparkSession: TypeAlias = SparkSessionProtocol
    Column: TypeAlias = ColumnProtocol
else:
    DataFrame = None  # resolved lazily
    SparkSession = None  # resolved lazily
    Column = None  # resolved lazily

# Try to bind engine components immediately if configured
try:
    _eng = get_engine()
    DataFrame = _eng.dataframe_cls or DataFrameProtocol  # type: ignore[assignment]
    SparkSession = _eng.spark_session_cls or SparkSessionProtocol  # type: ignore[assignment]
    Column = _eng.column_cls or ColumnProtocol  # type: ignore[assignment]
    F = cast(FunctionsProtocol, _eng.functions)
    types = cast(TypesProtocol, _eng.types)
    AnalysisException = _eng.analysis_exception  # type: ignore[assignment]
    Window = cast(WindowProtocol | None, _eng.window)
    desc = _eng.desc
except Exception:
    # Defer to __getattr__ if not configured yet
    pass


def __getattr__(name: str) -> Any:
    """Lazily resolve injected engine components to avoid import-time cycles."""
    if name in {"F", "types", "AnalysisException", "Window", "desc"}:
        eng = get_engine()
        if name == "F":
            return cast(FunctionsProtocol, eng.functions)
        if name == "types":
            return cast(TypesProtocol, eng.types)
        if name == "AnalysisException":
            return eng.analysis_exception
        if name == "Window":
            return cast(WindowProtocol | None, eng.window)
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
