"""
Writer Module - Refactored with Modular Architecture

Enhanced log writer for PipelineBuilder reports with full framework integration.
This module provides a comprehensive logging and reporting system for pipeline
execution results, integrating seamlessly with the existing framework ecosystem.

**New Simplified API:**
    The LogWriter now supports a simplified initialization API using `schema`
    and `table_name` parameters directly:

    >>> from pipeline_builder.writer import LogWriter
    >>> writer = LogWriter(spark, schema="analytics", table_name="logs")

**Deprecated API:**
    The old API using `WriterConfig` is still supported but deprecated:

    >>> from pipeline_builder.writer import LogWriter, WriterConfig, WriteMode
    >>> config = WriterConfig(table_schema="analytics", table_name="logs")
    >>> writer = LogWriter(spark, config=config)  # Deprecated

Architecture:
    - **Core**: Main LogWriter class that orchestrates all components
    - **Operations**: Data processing and transformation operations
    - **Storage**: Delta Lake and table management operations
    - **Monitoring**: Performance tracking and metrics collection
    - **Analytics**: Data quality analysis and trend detection

Key Features:
    - Simplified API with direct schema/table_name initialization
    - Full integration with framework models (StepResult, ExecutionResult, PipelineMetrics)
    - Enhanced type safety with proper TypedDict definitions
    - Comprehensive error handling and validation
    - Performance monitoring and optimization
    - Flexible configuration system
    - Delta Lake integration for persistent logging
    - Modular architecture for better maintainability

Classes:
    LogWriter: Main writer class for pipeline log operations
    DataProcessor: Handles data processing and transformations
    StorageManager: Manages Delta Lake storage operations
    PerformanceMonitor: Tracks performance metrics
    AnalyticsEngine: Provides analytics and trend analysis
    DataQualityAnalyzer: Analyzes data quality metrics
    TrendAnalyzer: Analyzes execution trends

Functions:
    create_log_rows_from_execution_result: Convert ExecutionResult to log rows
    create_log_schema: Create Spark schema for log tables
    validate_log_data: Validate log data before writing

Example (New API):
    >>> from pipeline_builder.writer import LogWriter
    >>> from pipeline_builder.models.execution import ExecutionResult
    >>>
    >>> # Initialize with new simplified API
    >>> writer = LogWriter(spark, schema="analytics", table_name="pipeline_logs")
    >>>
    >>> # Write execution result
    >>> result = writer.write_execution_result(execution_result)
    >>> print(f"Wrote {result['rows_written']} rows")

Example (Deprecated API):
    >>> from pipeline_builder.writer import LogWriter, WriterConfig, WriteMode
    >>>
    >>> # Old API (deprecated, emits warning)
    >>> config = WriterConfig(
    ...     table_schema="analytics",
    ...     table_name="pipeline_logs",
    ...     write_mode=WriteMode.APPEND
    ... )
    >>> writer = LogWriter(spark, config=config)  # DeprecationWarning
"""

from .analytics import DataQualityAnalyzer, TrendAnalyzer

# Core writer class
from .core import LogWriter

# Models and exceptions
from .exceptions import (
    WriterConfigurationError,
    WriterDataQualityError,
    WriterError,
    WriterPerformanceError,
    WriterTableError,
    WriterValidationError,
)
from .models import (
    LogRow,
    WriteMode,
    WriterConfig,
    WriterMetrics,
    create_log_rows_from_execution_result,
    create_log_schema,
    validate_log_data,
)
from .monitoring import AnalyticsEngine, PerformanceMonitor

# Component classes
from .operations import DataProcessor
from .query_builder import QueryBuilder
from .storage import StorageManager

__all__ = [
    # Core writer
    "LogWriter",
    # Component classes
    "DataProcessor",
    "StorageManager",
    "PerformanceMonitor",
    "AnalyticsEngine",
    "DataQualityAnalyzer",
    "TrendAnalyzer",
    "QueryBuilder",
    # Models and configuration
    "WriterConfig",
    "LogRow",
    "WriteMode",
    "WriterMetrics",
    # Utility functions
    "create_log_rows_from_execution_result",
    "create_log_schema",
    "validate_log_data",
    # Exceptions
    "WriterError",
    "WriterConfigurationError",
    "WriterValidationError",
    "WriterTableError",
    "WriterDataQualityError",
    "WriterPerformanceError",
]

# Version information
__version__ = "2.7.0"
__author__ = "Framework Team"

# Depends on:
#   analytics
#   core
#   enums
#   exceptions
#   models
#   monitoring
#   operations
#   query_builder
#   storage

# Depends on:
#   analytics
#   core
#   enums
#   exceptions
#   models
#   monitoring
#   operations
#   query_builder
#   storage

# Depends on:
#   analytics
#   core
#   enums
#   exceptions
#   models
#   monitoring
#   operations
#   query_builder
#   storage

# Depends on:
#   analytics
#   core
#   exceptions
#   models
#   models.enums
#   monitoring
#   operations
#   query_builder
#   storage

# Depends on:
#   analytics
#   core
#   exceptions
#   models
#   models.enums
#   monitoring
#   operations
#   query_builder
#   storage
__description__ = "Modular log writer for the framework pipeline execution results"
