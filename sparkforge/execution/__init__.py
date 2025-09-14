"""
Unified execution system for SparkForge pipelines.

This package provides a consolidated execution engine that replaces the separate
ExecutionEngine and UnifiedExecutionEngine with a single, more maintainable solution.

Key Features:
- Single execution engine for all step types
- Pluggable execution strategies
- Comprehensive error handling
- Performance monitoring
- Resource management
"""

from .engine import ExecutionEngine, ExecutionConfig, ExecutionMode, RetryStrategy
from .strategies import ExecutionStrategy, SequentialStrategy, ParallelStrategy, AdaptiveStrategy
from .results import ExecutionResult, ExecutionStats, StepExecutionResult
from .exceptions import ExecutionError, StepExecutionError, DependencyError

__all__ = [
    "ExecutionEngine",
    "ExecutionConfig", 
    "ExecutionMode",
    "RetryStrategy",
    "ExecutionStrategy",
    "SequentialStrategy",
    "ParallelStrategy", 
    "AdaptiveStrategy",
    "ExecutionResult",
    "ExecutionStats",
    "StepExecutionResult",
    "ExecutionError",
    "StepExecutionError",
    "DependencyError"
]
