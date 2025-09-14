"""
Performance-specific exceptions for SparkForge.

This module defines exceptions specific to performance monitoring and optimization,
providing detailed error context for performance-related issues.
"""

from __future__ import annotations
from typing import Optional

from .base import SparkForgeError, ErrorCategory, ErrorSeverity


class PerformanceError(SparkForgeError):
    """Raised when performance monitoring or optimization fails."""
    
    def __init__(
        self,
        message: str,
        *,
        performance_metric: Optional[str] = None,
        threshold_value: Optional[float] = None,
        actual_value: Optional[float] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.PERFORMANCE,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.performance_metric = performance_metric
        self.threshold_value = threshold_value
        self.actual_value = actual_value


class PerformanceThresholdError(PerformanceError):
    """Raised when performance thresholds are exceeded."""
    
    def __init__(
        self,
        message: str,
        *,
        metric_name: str,
        threshold: float,
        actual_value: float,
        **kwargs
    ):
        super().__init__(
            message,
            performance_metric=metric_name,
            threshold_value=threshold,
            actual_value=actual_value,
            **kwargs
        )
        self.metric_name = metric_name
        self.threshold = threshold
        self.actual_value = actual_value
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        return f"{base_msg} (Metric: {self.metric_name}, Threshold: {self.threshold}, Actual: {self.actual_value})"


class PerformanceMonitoringError(PerformanceError):
    """Raised when performance monitoring fails."""
    
    def __init__(
        self,
        message: str,
        *,
        monitoring_step: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            **kwargs
        )
        self.monitoring_step = monitoring_step


__all__ = [
    "PerformanceError",
    "PerformanceThresholdError", 
    "PerformanceMonitoringError"
]
