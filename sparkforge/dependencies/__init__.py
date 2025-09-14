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

from .analyzer import DependencyAnalyzer, DependencyAnalysisResult, AnalysisStrategy
from .graph import DependencyGraph, StepNode, StepType
from .exceptions import DependencyError, CircularDependencyError, InvalidDependencyError, DependencyConflictError, DependencyAnalysisError
from ..models import ExecutionMode
from ..execution.engine import StepComplexity

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
    "StepComplexity"
]
