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
Type aliases for SparkForge.

This module defines comprehensive type aliases for common patterns
used throughout SparkForge, improving type safety and readability.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

# Try to import PySpark types, fallback to Any if not available
try:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType, StructType
except ImportError:
    # Fallback types for when PySpark is not available
    DataFrame = Any
    SparkSession = Any
    Column = Any
    StructType = Any
    DataType = Any

# ============================================================================
# Core Types
# ============================================================================

# String types
StepName = str
PipelineId = str
ExecutionId = str
TableName = str
SchemaName = str
ErrorCode = str
ErrorMessage = str

# Numeric types
QualityRate = float
Duration = float
RowCount = int
WorkerCount = int
RetryCount = int

# ============================================================================
# Enum Types
# ============================================================================


class StepType(Enum):
    """Types of pipeline steps."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class PipelineMode(Enum):
    """Pipeline execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    VALIDATION_ONLY = "validation_only"


class PipelineStatus(Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Step execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ErrorSeverity(Enum):
    """Error severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories."""

    CONFIGURATION = "configuration"
    VALIDATION = "validation"
    EXECUTION = "execution"
    DATA_QUALITY = "data_quality"
    RESOURCE = "resource"
    SYSTEM = "system"
    NETWORK = "network"
    STORAGE = "storage"
    PERFORMANCE = "performance"


# ============================================================================
# Function Types
# ============================================================================

# Transform functions
TransformFunction = Callable[[DataFrame], DataFrame]
BronzeTransformFunction = Callable[[DataFrame], DataFrame]
SilverTransformFunction = Callable[
    [SparkSession, DataFrame, Dict[str, DataFrame]], DataFrame
]
GoldTransformFunction = Callable[[Dict[str, DataFrame]], DataFrame]

# Validation functions
ValidationFunction = Callable[[DataFrame], bool]
FilterFunction = Callable[[DataFrame], DataFrame]

# Step execution functions
StepExecutionFunction = Callable[[str, Dict[str, Any]], Dict[str, Any]]
PipelineExecutionFunction = Callable[[Dict[str, DataFrame]], Dict[str, Any]]

# ============================================================================
# Data Types
# ============================================================================

# Column and validation rules
ColumnRules = Dict[str, List[Any]]
ValidationRules = Dict[str, List[Any]]
QualityThresholds = Dict[str, float]

# Context types
ExecutionContext = Dict[str, Any]
StepContext = Dict[str, Any]
PipelineContext = Dict[str, Any]

# Result types
StepResult = Dict[str, Any]
PipelineResult = Dict[str, Any]
ValidationResult = Dict[str, Any]
ExecutionResult = Dict[str, Any]

# ============================================================================
# Configuration Types
# ============================================================================


class PipelineConfig(TypedDict, total=False):
    """Pipeline configuration."""

    schema: SchemaName
    min_bronze_rate: QualityRate
    min_silver_rate: QualityRate
    min_gold_rate: QualityRate
    verbose: bool
    enable_parallel_silver: bool
    max_parallel_workers: WorkerCount
    enable_caching: bool
    enable_monitoring: bool


class ExecutionConfig(TypedDict, total=False):
    """Execution configuration."""

    mode: PipelineMode
    max_workers: WorkerCount
    timeout_seconds: Duration
    retry_count: RetryCount
    enable_parallel: bool


class ValidationConfig(TypedDict, total=False):
    """Validation configuration."""

    enable_validation: bool
    quality_thresholds: QualityThresholds
    strict_mode: bool
    fail_fast: bool


class MonitoringConfig(TypedDict, total=False):
    """Monitoring configuration."""

    enable_monitoring: bool
    log_level: str
    metrics_interval: Duration
    performance_tracking: bool


# ============================================================================
# Error Types
# ============================================================================

ErrorContext = Dict[str, Any]
ErrorSuggestions = List[str]


class ErrorInfo(TypedDict, total=False):
    """Error information."""

    message: ErrorMessage
    code: ErrorCode
    category: ErrorCategory
    severity: ErrorSeverity
    context: ErrorContext
    suggestions: ErrorSuggestions
    timestamp: datetime
    cause: str | None


# ============================================================================
# Utility Types
# ============================================================================

# Optional types
OptionalDict = Optional[Dict[str, Any]]
OptionalList = Optional[List[Any]]
OptionalString = Optional[str]
OptionalInt = Optional[int]
OptionalFloat = Optional[float]
OptionalBool = Optional[bool]

# Specific dictionary types
StringDict = Dict[str, str]
AnyDict = Dict[str, Any]
NumericDict = Dict[str, Union[int, float]]
StringListDict = Dict[str, List[str]]
StringSetDict = Dict[str, Set[str]]

# List types
StringList = List[str]
IntList = List[int]
FloatList = List[float]
AnyList = List[Any]

# Tuple types
StringTuple = Tuple[str, ...]
IntTuple = Tuple[int, ...]
AnyTuple = Tuple[Any, ...]

# Set types
StringSet = Set[str]
IntSet = Set[int]
AnySet = Set[Any]

# ============================================================================
# Generic Types
# ============================================================================

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")

# Bounded type variables
Numeric = TypeVar("Numeric", bound=Union[int, float])
String = TypeVar("String", bound=str)
DictLike = TypeVar("DictLike", bound=Dict[str, Any])
ListLike = TypeVar("ListLike", bound=List[Any])
CallableLike = TypeVar("CallableLike", bound=Callable[..., Any])

# ============================================================================
# Complex Types
# ============================================================================


# Step definitions
class StepDefinition(TypedDict, total=False):
    """Step definition."""

    name: StepName
    step_type: StepType
    transform: TransformFunction
    rules: ColumnRules
    dependencies: list[StepName]
    metadata: dict[str, Any]


# Execution results
class StepExecutionResult(TypedDict, total=False):
    """Step execution result."""

    step_name: StepName
    step_type: StepType
    success: bool
    duration: Duration
    rows_processed: RowCount
    rows_written: RowCount
    quality_rate: QualityRate
    error_message: ErrorMessage | None


class PipelineExecutionResult(TypedDict, total=False):
    """Pipeline execution result."""

    pipeline_id: PipelineId
    execution_id: ExecutionId
    success: bool
    duration: Duration
    total_steps: int
    successful_steps: int
    failed_steps: int
    step_results: dict[StepName, StepExecutionResult]
    errors: list[ErrorInfo]


# Performance metrics
class PerformanceMetrics(TypedDict, total=False):
    """Performance metrics."""

    total_duration: Duration
    average_step_duration: Duration
    parallel_efficiency: float
    cache_hit_rate: float
    memory_usage: int
    cpu_usage: float


# ============================================================================
# Type Guards
# ============================================================================


def is_step_name(value: Any) -> bool:
    """Check if value is a valid step name."""
    return isinstance(value, str) and len(value) > 0


def is_pipeline_id(value: Any) -> bool:
    """Check if value is a valid pipeline ID."""
    return isinstance(value, str) and len(value) > 0


def is_quality_rate(value: Any) -> bool:
    """Check if value is a valid quality rate."""
    return isinstance(value, (int, float)) and 0 <= value <= 100


def is_duration(value: Any) -> bool:
    """Check if value is a valid duration."""
    return isinstance(value, (int, float)) and value >= 0


def is_row_count(value: Any) -> bool:
    """Check if value is a valid row count."""
    return isinstance(value, int) and value >= 0


def is_worker_count(value: Any) -> bool:
    """Check if value is a valid worker count."""
    return isinstance(value, int) and value > 0
