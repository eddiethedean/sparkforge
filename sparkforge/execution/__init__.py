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

from .engine import ExecutionConfig, ExecutionEngine, ExecutionMode, RetryStrategy
from .exceptions import DependencyError, ExecutionError, StepExecutionError
from .results import ExecutionResult, ExecutionStats, StepExecutionResult
from .strategies import (
    AdaptiveStrategy,
    ExecutionStrategy,
    ParallelStrategy,
    SequentialStrategy,
)

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
    "DependencyError",
]
