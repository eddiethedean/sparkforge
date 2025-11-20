"""
Shared models for pipeline builders.
"""

from .base import BaseModel, ParallelConfig, ValidationThresholds
from .enums import ExecutionMode, PipelinePhase
from .execution import (
    ExecutionContext,
    ExecutionResult,
    PipelineMetrics,
    StageStats,
    StepResult,
)
from .exceptions import PipelineConfigurationError, PipelineExecutionError
from .pipeline import PipelineConfig
from .types import ModelValue, Serializable, Validatable

__all__ = [
    "BaseModel",
    "ValidationThresholds",
    "ParallelConfig",
    "PipelineConfig",
    "PipelineMetrics",
    "StageStats",
    "StepResult",
    "ExecutionContext",
    "ExecutionResult",
    "PipelinePhase",
    "ExecutionMode",
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "ModelValue",
    "Validatable",
    "Serializable",
]

