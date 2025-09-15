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
Performance-specific exceptions for SparkForge.

This module defines exceptions specific to performance monitoring and optimization,
providing detailed error context for performance-related issues.
"""

from __future__ import annotations

from .base import ErrorCategory, ErrorSeverity, SparkForgeError


class PerformanceError(SparkForgeError):
    """Raised when performance monitoring or optimization fails."""

    def __init__(
        self,
        message: str,
        *,
        performance_metric: str | None = None,
        threshold_value: float | None = None,
        actual_value: float | None = None,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.PERFORMANCE,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
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
        **kwargs,
    ):
        super().__init__(
            message,
            performance_metric=metric_name,
            threshold_value=threshold,
            actual_value=actual_value,
            **kwargs,
        )
        self.metric_name = metric_name
        self.threshold = threshold
        self.actual_value = actual_value

    def __str__(self) -> str:
        base_msg = super().__str__()
        return f"{base_msg} (Metric: {self.metric_name}, Threshold: {self.threshold}, Actual: {self.actual_value})"


class PerformanceMonitoringError(PerformanceError):
    """Raised when performance monitoring fails."""

    def __init__(self, message: str, *, monitoring_step: str | None = None, **kwargs):
        super().__init__(message, **kwargs)
        self.monitoring_step = monitoring_step


__all__ = [
    "PerformanceError",
    "PerformanceThresholdError",
    "PerformanceMonitoringError",
]
