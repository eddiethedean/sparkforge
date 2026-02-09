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


def _configure_engine_from_session(spark: Any) -> None:
    """Configure engine from a SparkSession (PySpark or sparkless). Used by configure_engine(spark=...)."""
    session_module = type(spark).__module__
    if "pyspark" in session_module:
        from pyspark.sql import functions as pyspark_functions
        from pyspark.sql import types as pyspark_types
        from pyspark.sql.functions import desc as pyspark_desc
        from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
        from pyspark.sql.window import Window as PySparkWindow
        from pyspark.sql import (
            DataFrame as PySparkDataFrame,
            SparkSession as PySparkSparkSession,
            Column as PySparkColumn,
        )
        configure_engine(
            functions=pyspark_functions,
            types=pyspark_types,
            analysis_exception=PySparkAnalysisException,
            window=PySparkWindow,
            desc=pyspark_desc,
            engine_name="pyspark",
            dataframe_cls=PySparkDataFrame,
            spark_session_cls=PySparkSparkSession,
            column_cls=PySparkColumn,
        )
    elif "sparkless" in session_module or "mock_spark" in session_module:
        from sparkless import functions as mock_functions  # type: ignore[import]
        from sparkless import spark_types as mock_types  # type: ignore[import]
        from sparkless import AnalysisException as MockAnalysisException  # type: ignore[import]
        from sparkless import Window as MockWindow  # type: ignore[import]
        from sparkless.functions import desc as mock_desc  # type: ignore[import]
        from sparkless import (  # type: ignore[import]
            DataFrame as MockDataFrame,
            SparkSession as MockSparkSession,
            Column as MockColumn,
        )
        configure_engine(
            functions=mock_functions,
            types=mock_types,
            analysis_exception=MockAnalysisException,
            window=MockWindow,
            desc=mock_desc,
            engine_name="mock",
            dataframe_cls=MockDataFrame,
            spark_session_cls=MockSparkSession,
            column_cls=MockColumn,
        )
    else:
        raise ValueError(
            f"Unknown Spark session type: {type(spark).__module__}. "
            "Use configure_engine(functions=..., types=..., analysis_exception=...) for custom engines."
        )


def configure_engine(
    *,
    spark: Optional[Any] = None,
    functions: Optional[FunctionsProtocol] = None,
    types: Optional[TypesProtocol] = None,
    analysis_exception: Optional[type[BaseException] | AnalysisExceptionProtocol] = None,
    window: Optional[WindowProtocol] = None,
    desc: Optional[Any] = None,
    engine_name: str = "unknown",
    dataframe_cls: Optional[Any] = None,
    spark_session_cls: Optional[Any] = None,
    column_cls: Optional[Any] = None,
) -> None:
    """
    Inject engine components.

    Convenience: pass spark=your_spark_session to auto-configure from PySpark or sparkless.
    Otherwise pass functions=..., types=..., analysis_exception=... (and optional window, desc, etc.).

    Sets both thread-local and global engine state for backward compatibility.
    Thread-local state takes precedence in get_engine() for parallel test isolation.
    """
    if spark is not None:
        _configure_engine_from_session(spark)
        return
    if functions is None or types is None or analysis_exception is None:
        raise TypeError(
            "configure_engine() requires either spark=<SparkSession> or "
            "functions=..., types=..., and analysis_exception=..."
        )
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
