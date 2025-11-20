"""
Unified dependency analyzer for the framework pipelines.

This module re-exports the base DependencyAnalyzer which works with Spark steps
via protocol-based typing.
"""

from __future__ import annotations

# Re-export from base - the base analyzer uses protocols so it works with Spark steps
from pipeline_builder_base.dependencies import (
    AnalysisStrategy,
    DependencyAnalysisResult,
    DependencyAnalyzer,
    DependencyError,
    DependencyGraph,
    StepNode,
    StepType,
)

# Keep for backward compatibility - the base analyzer works with any step type via protocols
__all__ = [
    "DependencyAnalyzer",
    "DependencyAnalysisResult",
    "AnalysisStrategy",
    "DependencyGraph",
    "StepNode",
    "StepType",
    "DependencyError",
]
