"""
Dependency analysis system for SparkForge pipelines.

This package provides a unified dependency analysis system that replaces
both DependencyAnalyzer and UnifiedDependencyAnalyzer with a single,
more maintainable solution.

Key Features:
- Single analyzer for all step types
- Dependency graph construction
- Cycle detection and resolution
- Execution group optimization
- Performance analysis
"""

from ..execution.engine import StepComplexity
from ..models import ExecutionMode
from .analyzer import AnalysisStrategy, DependencyAnalysisResult, DependencyAnalyzer
from .exceptions import (
    CircularDependencyError,
    DependencyAnalysisError,
    DependencyConflictError,
    DependencyError,
    InvalidDependencyError,
)
from .graph import DependencyGraph, StepNode, StepType

__all__ = [
    "DependencyAnalyzer",
    "DependencyAnalysisResult",
    "AnalysisStrategy",
    "DependencyGraph",
    "StepNode",
    "StepType",
    "DependencyError",
    "CircularDependencyError",
    "InvalidDependencyError",
    "DependencyConflictError",
    "DependencyAnalysisError",
    "ExecutionMode",
    "StepComplexity",
]
