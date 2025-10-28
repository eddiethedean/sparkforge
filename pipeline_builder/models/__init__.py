"""
Enhanced data models and type definitions for the Pipeline Builder.

This module contains all the dataclasses and type definitions used throughout
the pipeline system. All models include comprehensive validation, type safety,
and clear documentation.

Key Features:
- Type-safe dataclasses with comprehensive validation
- Enhanced error handling with custom exceptions
- Clear separation of concerns with proper abstractions
- Immutable data structures where appropriate
- Rich metadata and documentation
- Protocol definitions for better type checking
- Factory methods for common object creation
"""

# Import validation error from main errors module
from ..errors import PipelineValidationError
from .base import BaseModel, ParallelConfig, ValidationThresholds
from .dependencies import (
    CrossLayerDependency,
    SilverDependencyInfo,
    UnifiedExecutionPlan,
    UnifiedStepConfig,
)
from .enums import ExecutionMode, PipelinePhase, ValidationResult, WriteMode

# Import all models for easy access
from .exceptions import PipelineConfigurationError, PipelineExecutionError
from .execution import ExecutionContext, ExecutionResult, StageStats, StepResult
from .factory import (
    create_execution_context,
    create_pipeline_config,
    deserialize_pipeline_config,
    serialize_pipeline_config,
    validate_pipeline_config,
    validate_step_config,
)
from .pipeline import PipelineConfig, PipelineMetrics
from .steps import BronzeStep, GoldStep, SilverStep
from .types import (
    ColumnRule,
    ColumnRules,
    GoldTransformFunction,
    ModelValue,
    ResourceValue,
    Serializable,
    SilverTransformFunction,
    TransformFunction,
    Validatable,
)

# Make all models available at package level
__all__ = [
    # Exceptions
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "PipelineValidationError",
    # Enums
    "ExecutionMode",
    "PipelinePhase",
    "ValidationResult",
    "WriteMode",
    # Types
    "ColumnRule",
    "ColumnRules",
    "GoldTransformFunction",
    "ModelValue",
    "ResourceValue",
    "Serializable",
    "SilverTransformFunction",
    "TransformFunction",
    "Validatable",
    # Base classes
    "BaseModel",
    "ValidationThresholds",
    "ParallelConfig",
    # Step models
    "BronzeStep",
    "GoldStep",
    "SilverStep",
    # Execution models
    "ExecutionContext",
    "ExecutionResult",
    "StageStats",
    "StepResult",
    # Pipeline models
    "PipelineConfig",
    "PipelineMetrics",
    # Dependency models
    "CrossLayerDependency",
    "SilverDependencyInfo",
    "UnifiedExecutionPlan",
    "UnifiedStepConfig",
    # Factory functions
    "create_execution_context",
    "create_pipeline_config",
    "deserialize_pipeline_config",
    "serialize_pipeline_config",
    "validate_pipeline_config",
    "validate_step_config",
]
