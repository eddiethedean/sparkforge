"""
Pipeline system for SparkForge.

This package provides a refactored, modular pipeline system that replaces
the monolithic PipelineBuilder with focused, maintainable components.

Key Components:
- PipelineBuilder: Fluent API for pipeline construction
- PipelineRunner: Pipeline execution engine
- StepExecutor: Individual step execution
- PipelineValidator: Pipeline validation and error checking
- PipelineMonitor: Metrics, reporting, and monitoring
"""

from .builder import PipelineBuilder
from .runner import PipelineRunner
from .executor import StepExecutor
from .validator import PipelineValidator
from .monitor import PipelineMonitor, PipelineMetrics, PipelineReport
from .models import PipelineMode, PipelineStatus

__all__ = [
    "PipelineBuilder",
    "PipelineRunner", 
    "StepExecutor",
    "PipelineValidator",
    "PipelineMonitor",
    "PipelineMetrics",
    "PipelineReport",
    "PipelineMode",
    "PipelineStatus"
]
