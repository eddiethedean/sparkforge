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

from .analyzer import DependencyAnalyzer, DependencyAnalysisResult
from .graph import DependencyGraph, StepNode
from .exceptions import DependencyError, CircularDependencyError, InvalidDependencyError

__all__ = [
    "DependencyAnalyzer",
    "DependencyAnalysisResult", 
    "DependencyGraph",
    "StepNode",
    "DependencyError",
    "CircularDependencyError",
    "InvalidDependencyError"
]
