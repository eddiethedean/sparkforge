"""
Engine injection for pipeline_builder.

Holds injected engine components (functions, types, window, desc,
AnalysisException) that satisfy the protocols in `protocols.py`. Users/tests
must call `configure_engine(...)` after creating their engine (PySpark,
sparkless, etc.). Defaults raise to ensure misconfiguration surfaces early.
"""

from __future__ import annotations

import threading
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


# Global engine state (for backward compatibility)
_engine: Optional[EngineConfig] = None

# Thread-local engine state (for parallel test isolation)
_thread_local = threading.local()


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
    """
    Inject engine components.

    Sets both thread-local and global engine state for backward compatibility.
    Thread-local state takes precedence in get_engine() for parallel test isolation.
    """

    global _engine
    engine_config = EngineConfig(
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

    # Set global state (for backward compatibility)
    _engine = engine_config

    # Set thread-local state (for parallel test isolation)
    _thread_local.engine = engine_config


def get_engine() -> EngineConfig:
    """
    Get the current engine config, raising if not configured.

    Checks thread-local storage first (for parallel test isolation),
    then falls back to global state (for backward compatibility).
    """

    # Try thread-local first (for parallel test isolation)
    if hasattr(_thread_local, "engine") and _thread_local.engine is not None:
        engine: EngineConfig = _thread_local.engine
        return engine

    # Fallback to global state (for backward compatibility)
    if _engine is None:
        raise RuntimeError(
            "Engine not configured. Call configure_engine(functions=..., types=..., analysis_exception=..., window=..., desc=...) before using pipeline_builder."
        )
    return _engine


def reset_engine_state() -> None:
    """
    Reset thread-local engine state.

    This is useful for test isolation - clears the thread-local engine
    so the next get_engine() call will use global state or raise an error.
    """
    if hasattr(_thread_local, "engine"):
        delattr(_thread_local, "engine")


__all__ = ["EngineConfig", "configure_engine", "get_engine", "reset_engine_state"]
