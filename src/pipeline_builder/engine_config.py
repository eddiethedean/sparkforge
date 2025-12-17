"""
Engine injection for pipeline_builder.

Holds injected engine components (functions, types, window, desc,
AnalysisException) that satisfy the protocols in `protocols.py`. Users/tests
must call `configure_engine(...)` after creating their engine (PySpark,
sparkless, etc.). Defaults raise to ensure misconfiguration surfaces early.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .protocols import (
    AnalysisExceptionProtocol,
    FunctionsProtocol,
    TypesProtocol,
    WindowProtocol,
)


@dataclass
class EngineConfig:
    functions: FunctionsProtocol
    types: TypesProtocol
    analysis_exception: type[BaseException] | AnalysisExceptionProtocol
    window: Optional[WindowProtocol] = None
    desc: Optional[Any] = None
    engine_name: str = "unknown"
    dataframe_cls: Optional[Any] = None
    spark_session_cls: Optional[Any] = None
    column_cls: Optional[Any] = None


_engine: Optional[EngineConfig] = None


def configure_engine(
    *,
    functions: FunctionsProtocol,
    types: TypesProtocol,
    analysis_exception: type[BaseException] | AnalysisExceptionProtocol,
    window: Optional[WindowProtocol] = None,
    desc: Optional[Any] = None,
    engine_name: str = "unknown",
    dataframe_cls: Optional[Any] = None,
    spark_session_cls: Optional[Any] = None,
    column_cls: Optional[Any] = None,
) -> None:
    """Inject engine components."""

    global _engine
    _engine = EngineConfig(
        functions=functions,
        types=types,
        analysis_exception=analysis_exception,
        window=window,
        desc=desc,
        engine_name=engine_name,
        dataframe_cls=dataframe_cls,
        spark_session_cls=spark_session_cls,
        column_cls=column_cls,
    )


def get_engine() -> EngineConfig:
    """Get the current engine config, raising if not configured."""

    if _engine is None:
        raise RuntimeError(
            "Engine not configured. Call configure_engine(functions=..., types=..., analysis_exception=..., window=..., desc=...) before using pipeline_builder."
        )
    return _engine


__all__ = ["EngineConfig", "configure_engine", "get_engine"]
