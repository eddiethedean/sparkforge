# # # # Copyright (c) 2024 Odos Matthews
# # # #
# # # # Permission is hereby granted, free of charge, to any person obtaining a copy
# # # # of this software and associated documentation files (the "Software"), to deal
# # # # in the Software without restriction, including without limitation the rights
# # # # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # # # copies of the Software, and to permit persons to whom the Software is
# # # # furnished to do so, subject to the following conditions:
# # # #
# # # # The above copyright notice and this permission notice shall be included in all
# # # # copies or substantial portions of the Software.
# # # #
# # # # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # # # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # # # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # # # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # # # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # # # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # # # SOFTWARE.
# #
#
#


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

# StepComplexity removed - was not used in dependencies module
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
]
