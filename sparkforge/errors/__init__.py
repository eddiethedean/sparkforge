#


"""
Standardized error handling system for SparkForge.

This package provides a comprehensive, consistent error handling system
across all SparkForge modules, improving debugging and error reporting.

Key Features:
- Hierarchical exception structure
- Rich error context and metadata
- Consistent error codes and messages
- Better debugging information
- Error recovery suggestions
"""

from .base import (
    ConfigurationError,
    DataQualityError,
    ExecutionError,
    ResourceError,
    SparkForgeError,
    ValidationError,
)
from .data import DataError, SchemaError, TableOperationError
from .data import DataQualityError as DataQualityError
from .data import ValidationError as DataValidationError
from .execution import ExecutionEngineError, RetryError, StrategyError
from .execution import StepExecutionError as ExecutionStepError
from .execution import TimeoutError as ExecutionTimeoutError
from .performance import (
    PerformanceError,
    PerformanceMonitoringError,
    PerformanceThresholdError,
)
from .pipeline import (
    CircularDependencyError,
    DependencyError,
    InvalidDependencyError,
    PipelineConfigurationError,
    PipelineError,
    PipelineExecutionError,
    PipelineValidationError,
    StepError,
    StepExecutionError,
    StepValidationError,
)
from .system import ConfigurationError as SystemConfigurationError
from .system import NetworkError, StorageError, SystemError
from .system import ResourceError as SystemResourceError

__all__ = [
    # Base exceptions
    "SparkForgeError",
    "ConfigurationError",
    "ValidationError",
    "ExecutionError",
    "DataQualityError",
    "ResourceError",
    # Pipeline exceptions
    "PipelineError",
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "PipelineValidationError",
    "StepError",
    "StepExecutionError",
    "StepValidationError",
    "DependencyError",
    "CircularDependencyError",
    "InvalidDependencyError",
    # Execution exceptions
    "ExecutionEngineError",
    "ExecutionStepError",
    "StrategyError",
    "RetryError",
    "ExecutionTimeoutError",
    # Data exceptions
    "DataError",
    "DataQualityError",
    "SchemaError",
    "DataValidationError",
    "TableOperationError",
    # System exceptions
    "SystemError",
    "SystemResourceError",
    "SystemConfigurationError",
    "NetworkError",
    "StorageError",
    # Performance exceptions
    "PerformanceError",
    "PerformanceThresholdError",
    "PerformanceMonitoringError",
]
