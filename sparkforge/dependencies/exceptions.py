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
Dependency analysis exceptions for SparkForge.

This module defines exceptions specific to dependency analysis operations.
"""

from typing import List, Optional


class DependencyError(Exception):
    """Base exception for dependency-related errors."""

    def __init__(self, message: str, step_name: Optional[str] = None):
        super().__init__(message)
        self.step_name = step_name


class DependencyAnalysisError(DependencyError):
    """Raised when dependency analysis fails."""

    def __init__(self, message: str, analysis_step: Optional[str] = None):
        super().__init__(message, analysis_step)
        self.analysis_step = analysis_step


class CircularDependencyError(DependencyError):
    """Raised when circular dependencies are detected."""

    def __init__(self, message: str, cycle: List[str]):
        super().__init__(message)
        self.cycle = cycle


class InvalidDependencyError(DependencyError):
    """Raised when invalid dependencies are detected."""

    def __init__(self, message: str, invalid_dependencies: List[str]):
        super().__init__(message)
        self.invalid_dependencies = invalid_dependencies


class DependencyConflictError(DependencyError):
    """Raised when dependency conflicts are detected."""

    def __init__(self, message: str, conflicting_steps: List[str]):
        super().__init__(message)
        self.conflicting_steps = conflicting_steps
