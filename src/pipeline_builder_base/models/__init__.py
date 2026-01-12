"""
Shared models for pipeline builders.
"""

from .base import BaseModel, ValidationThresholds
from .enums import ExecutionMode, PipelinePhase
from .exceptions import PipelineConfigurationError, PipelineExecutionError
from .execution import (
    ExecutionContext,
    ExecutionResult,
    PipelineMetrics,
    StageStats,
    StepResult,
)
from .pipeline import PipelineConfig
from .types import ModelValue, Serializable, Validatable

__all__ = [
    "BaseModel",
    "ValidationThresholds",
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
