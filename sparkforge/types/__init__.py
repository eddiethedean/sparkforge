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

from .aliases import (  # Core types; Pipeline types; Function types; Data types; Result types; Configuration types; Error types; Utility types
    AnyDict,
    BronzeTransformFunction,
    Column,
    ColumnRules,
    DataFrame,
    ErrorCode,
    ErrorContext,
    ErrorSuggestions,
    ExecutionConfig,
    ExecutionContext,
    ExecutionId,
    ExecutionResult,
    FilterFunction,
    GoldTransformFunction,
    MonitoringConfig,
    NumericDict,
    OptionalDict,
    OptionalList,
    PipelineConfig,
    PipelineId,
    PipelineResult,
    QualityThresholds,
    SchemaName,
    SilverTransformFunction,
    SparkSession,
    StepContext,
    StepName,
    StepResult,
    StepType,
    StringDict,
    TableName,
    TransformFunction,
    ValidationConfig,
    ValidationFunction,
    ValidationResult,
    ValidationRules,
)
from .generics import (  # Generic types; Bounded generics
    CallableLike,
    DictLike,
    K,
    ListLike,
    Numeric,
    R,
    String,
    T,
    V,
)
from .protocols import (  # Core protocols; Pipeline protocols; Data protocols; System protocols
    Cacheable,
    Configurable,
    DataSink,
    DataSource,
    DataTransformer,
    DataValidator,
    Executable,
    Loggable,
    Monitorable,
    PipelineExecutor,
    PipelineMonitor,
    PipelineStep,
    PipelineValidator,
    Retryable,
    Serializable,
    Validatable,
)
from .utils import (  # Type checking utilities; Type conversion utilities; Type inference utilities
    cast_safe,
    convert_type,
    get_type_hints,
    infer_parameter_types,
    infer_return_type,
    infer_type,
    is_valid_type,
    normalize_type,
    validate_type,
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
    "infer_parameter_types",
]
