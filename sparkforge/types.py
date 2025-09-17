"""
Simplified type definitions for SparkForge.

This module provides essential type definitions and aliases
for better type safety without over-engineering.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

# Try to import PySpark types, fallback to object if not available
try:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType, StructType
except ImportError:
    # Fallback types for when PySpark is not available
    DataFrame = object  # type: ignore
    SparkSession = object  # type: ignore
    Column = object  # type: ignore
    StructType = object  # type: ignore
    DataType = object  # type: ignore

# ============================================================================
# Basic Type Aliases
# ============================================================================

# String types
StepName = str
PipelineId = str
ExecutionId = str
TableName = str
SchemaName = str
ErrorCode = str

# Numeric types
QualityRate = float
Duration = float
RowCount = int

# Dictionary types
StringDict = Dict[str, str]
NumericDict = Dict[str, Union[int, float]]
GenericDict = Dict[str, Any]
OptionalDict = Optional[Dict[str, Any]]
OptionalList = Optional[List[Any]]

# ============================================================================
# Enums
# ============================================================================


class StepType(Enum):
    """Types of pipeline steps."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class StepStatus(Enum):
    """Step execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineMode(Enum):
    """Pipeline execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"


# ============================================================================
# Function Types
# ============================================================================

# Transform function types
TransformFunction = Callable[[SparkSession, DataFrame], DataFrame]
BronzeTransformFunction = Callable[[SparkSession, DataFrame], DataFrame]
SilverTransformFunction = Callable[
    [SparkSession, DataFrame, Dict[str, DataFrame]], DataFrame
]
GoldTransformFunction = Callable[[SparkSession, Dict[str, DataFrame]], DataFrame]

# Filter function type
FilterFunction = Callable[[DataFrame], DataFrame]

# ============================================================================
# Data Types
# ============================================================================

# Column rules type
ColumnRules = Dict[str, List[Union[str, Column]]]

# Result types
StepResult = Dict[str, Any]
PipelineResult = Dict[str, Any]
ExecutionResult = Dict[str, Any]
ValidationResult = Dict[str, Any]

# Context types
StepContext = Dict[str, Any]
ExecutionContext = Dict[str, Any]

# Configuration types
PipelineConfig = Dict[str, Any]
ExecutionConfig = Dict[str, Any]
ValidationConfig = Dict[str, Any]
MonitoringConfig = Dict[str, Any]

# Quality types
QualityThresholds = Dict[str, float]

# Error types
ErrorContext = Dict[str, Any]
ErrorSuggestions = List[str]

# ============================================================================
# Protocols (Simplified)
# ============================================================================

from typing import Protocol


class Validatable(Protocol):
    """Protocol for objects that can be validated."""

    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid."""
        ...


class Serializable(Protocol):
    """Protocol for objects that can be serialized."""

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dictionary."""
        ...


# ============================================================================
# Backward Compatibility Aliases
# ============================================================================

# Keep some aliases for backward compatibility
PipelinePhase = StepType
WriteMode = PipelineMode
