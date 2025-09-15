# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

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
