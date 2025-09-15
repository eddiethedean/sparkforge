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
