"""
Engine injection for pipeline_builder.

This module holds injected engine components (functions, types, window, desc,
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


_engine: Optional[EngineConfig] = None


def configure_engine(
    *,
    functions: FunctionsProtocol,
    types: TypesProtocol,
    analysis_exception: type[BaseException] | AnalysisExceptionProtocol,
    window: Optional[WindowProtocol] = None,
    desc: Optional[Any] = None,
) -> None:
    """Inject engine components."""

    global _engine
    _engine = EngineConfig(
        functions=functions,
        types=types,
        analysis_exception=analysis_exception,
        window=window,
        desc=desc,
    )


def get_engine() -> EngineConfig:
    """Get the current engine config, raising if not configured."""

    if _engine is None:
        raise RuntimeError(
            "Engine not configured. Call configure_engine(functions=..., types=..., analysis_exception=..., window=..., desc=...) before using pipeline_builder."
        )
    return _engine


__all__ = ["EngineConfig", "configure_engine", "get_engine"]

