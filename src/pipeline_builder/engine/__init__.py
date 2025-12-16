"""
Engine helpers for pipeline_builder.

Kept minimal to avoid import cycles with compat. Use configure_engine/get_engine
from this package to inject engine components.
"""

from ..engine_config import EngineConfig, configure_engine, get_engine
from .spark_engine import SparkEngine

__all__ = ["EngineConfig", "configure_engine", "get_engine", "SparkEngine"]
