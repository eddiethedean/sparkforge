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

# Import shared models from pipeline_builder_base
from pipeline_builder_base.errors import PipelineValidationError
from pipeline_builder_base.models import (
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
from pipeline_builder_base.models.exceptions import (
    PipelineConfigurationError,
    PipelineExecutionError,
)

# Import Spark-specific models
from .dependencies import (
    CrossLayerDependency,
    SilverDependencyInfo,
    UnifiedExecutionPlan,
    UnifiedStepConfig,
)
from .enums import ValidationResult, WriteMode
from .factory import (
    create_execution_context,
    create_pipeline_config,
    deserialize_pipeline_config,
    serialize_pipeline_config,
    validate_pipeline_config,
    validate_step_config,
)
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
