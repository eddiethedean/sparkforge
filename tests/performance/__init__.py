"""
Performance testing module for SparkForge.

This module provides performance testing infrastructure including:
- Performance baseline measurements
- Regression detection
- Memory usage monitoring
- Timing analysis
"""

from .performance_monitor import PerformanceMonitor, PerformanceResult
from .performance_tests import (
    run_model_creation_performance_tests,
    run_serialization_performance_tests,
    run_validation_performance_tests,
)

__all__ = [
    "PerformanceMonitor",
    "PerformanceResult",
    "run_validation_performance_tests",
    "run_model_creation_performance_tests",
    "run_serialization_performance_tests",
]
