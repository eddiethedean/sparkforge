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
