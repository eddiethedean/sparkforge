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
    SparkForgeError,
    ConfigurationError,
    ValidationError,
    ExecutionError,
    DataQualityError,
    ResourceError
)

from .pipeline import (
    PipelineError,
    PipelineConfigurationError,
    PipelineExecutionError,
    PipelineValidationError,
    StepError,
    StepExecutionError,
    StepValidationError,
    DependencyError,
    CircularDependencyError,
    InvalidDependencyError
)

from .execution import (
    ExecutionEngineError,
    StepExecutionError as ExecutionStepError,
    StrategyError,
    RetryError,
    TimeoutError as ExecutionTimeoutError
)

from .data import (
    DataError,
    DataQualityError as DataQualityError,
    SchemaError,
    ValidationError as DataValidationError,
    TableOperationError
)

from .system import (
    SystemError,
    ResourceError as SystemResourceError,
    ConfigurationError as SystemConfigurationError,
    NetworkError,
    StorageError
)

from .performance import (
    PerformanceError,
    PerformanceThresholdError,
    PerformanceMonitoringError
)

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
    "PerformanceMonitoringError"
]
