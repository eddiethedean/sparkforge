"""
Pipeline Builder Base - Shared models, utilities, and interfaces.

This package contains code shared between pipeline_builder (Spark) and
sql_pipeline_builder (SQLAlchemy) implementations.
"""

__version__ = "2.2.0"

# Export main components
from .errors import (
    ErrorCategory,
    ErrorSeverity,
    PipelineValidationError,
    SparkForgeError,
    ValidationError,
)
from .logging import PipelineLogger
from .models import (
    BaseModel,
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    ParallelConfig,
    PipelineConfig,
    PipelineMetrics,
    PipelinePhase,
    StageStats,
    StepResult,
    ValidationThresholds,
)
from .reporting import (
    DataMetrics,
    ExecutionSummary,
    PerformanceMetrics,
    SummaryReport,
    TransformReport,
    ValidationReport,
    WriteReport,
)

__all__ = [
    # Errors
    "SparkForgeError",
    "ValidationError",
    "PipelineValidationError",
    "ErrorSeverity",
    "ErrorCategory",
    # Logging
    "PipelineLogger",
    # Models
    "BaseModel",
    "PipelineConfig",
    "ValidationThresholds",
    "ParallelConfig",
    "PipelineMetrics",
    "StageStats",
    "StepResult",
    "ExecutionContext",
    "ExecutionResult",
    "PipelinePhase",
    "ExecutionMode",
    # Reporting
    "ValidationReport",
    "TransformReport",
    "WriteReport",
    "ExecutionSummary",
    "PerformanceMetrics",
    "DataMetrics",
    "SummaryReport",
]

