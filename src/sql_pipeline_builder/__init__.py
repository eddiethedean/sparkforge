"""
SQL Pipeline Builder - SQLAlchemy-based pipeline builder.

This package provides a SQLAlchemy-based implementation of the Medallion Architecture
pattern, similar to pipeline_builder but using SQL databases instead of Spark.
"""

__version__ = "2.2.0"

# Export main components
from .engine import SqlEngine
from .pipeline import SqlPipelineBuilder, SqlPipelineRunner

__all__ = [
    "SqlEngine",
    "SqlPipelineBuilder",
    "SqlPipelineRunner",
]
