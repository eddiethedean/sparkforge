"""
Type definitions and utilities for SparkForge.

This package provides comprehensive type definitions, type aliases,
and type utilities for better type safety across all SparkForge modules.

Key Features:
- Comprehensive type aliases for common patterns
- Protocol definitions for better type checking
- Type utilities and helpers
- Generic type definitions
- Type-safe data structures
"""

from .aliases import (
    # Core types
    DataFrame,
    SparkSession,
    Column,
    
    # Pipeline types
    StepName,
    StepType,
    PipelineId,
    ExecutionId,
    TableName,
    SchemaName,
    
    # Function types
    TransformFunction,
    BronzeTransformFunction,
    SilverTransformFunction,
    GoldTransformFunction,
    ValidationFunction,
    FilterFunction,
    
    # Data types
    ColumnRules,
    ValidationRules,
    QualityThresholds,
    ExecutionContext,
    StepContext,
    
    # Result types
    StepResult,
    PipelineResult,
    ValidationResult,
    ExecutionResult,
    
    # Configuration types
    PipelineConfig,
    ExecutionConfig,
    ValidationConfig,
    MonitoringConfig,
    
    # Error types
    ErrorCode,
    ErrorContext,
    ErrorSuggestions,
    
    # Utility types
    OptionalDict,
    OptionalList,
    StringDict,
    AnyDict,
    NumericDict
)

from .protocols import (
    # Core protocols
    Validatable,
    Serializable,
    Executable,
    Monitorable,
    
    # Pipeline protocols
    PipelineStep,
    PipelineValidator,
    PipelineMonitor,
    PipelineExecutor,
    
    # Data protocols
    DataSource,
    DataSink,
    DataTransformer,
    DataValidator,
    
    # System protocols
    Configurable,
    Loggable,
    Cacheable,
    Retryable
)

from .generics import (
    # Generic types
    T,
    K,
    V,
    R,
    
    # Bounded generics
    Numeric,
    String,
    DictLike,
    ListLike,
    CallableLike
)

from .utils import (
    # Type checking utilities
    is_valid_type,
    get_type_hints,
    validate_type,
    
    # Type conversion utilities
    cast_safe,
    convert_type,
    normalize_type,
    
    # Type inference utilities
    infer_type,
    infer_return_type,
    infer_parameter_types
)

__all__ = [
    # Aliases
    "DataFrame",
    "SparkSession", 
    "Column",
    "StepName",
    "StepType",
    "PipelineId",
    "ExecutionId",
    "TableName",
    "SchemaName",
    "TransformFunction",
    "BronzeTransformFunction",
    "SilverTransformFunction", 
    "GoldTransformFunction",
    "ValidationFunction",
    "FilterFunction",
    "ColumnRules",
    "ValidationRules",
    "QualityThresholds",
    "ExecutionContext",
    "StepContext",
    "StepResult",
    "PipelineResult",
    "ValidationResult",
    "ExecutionResult",
    "PipelineConfig",
    "ExecutionConfig",
    "ValidationConfig",
    "MonitoringConfig",
    "ErrorCode",
    "ErrorContext",
    "ErrorSuggestions",
    "OptionalDict",
    "OptionalList",
    "StringDict",
    "AnyDict",
    "NumericDict",
    
    # Protocols
    "Validatable",
    "Serializable",
    "Executable",
    "Monitorable",
    "PipelineStep",
    "PipelineValidator",
    "PipelineMonitor",
    "PipelineExecutor",
    "DataSource",
    "DataSink",
    "DataTransformer",
    "DataValidator",
    "Configurable",
    "Loggable",
    "Cacheable",
    "Retryable",
    
    # Generics
    "T",
    "K",
    "V", 
    "R",
    "Numeric",
    "String",
    "DictLike",
    "ListLike",
    "CallableLike",
    
    # Utils
    "is_valid_type",
    "get_type_hints",
    "validate_type",
    "cast_safe",
    "convert_type",
    "normalize_type",
    "infer_type",
    "infer_return_type",
    "infer_parameter_types"
]
