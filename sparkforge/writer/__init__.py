"""
SparkForge Writer Module - Refactored with Modular Architecture

Enhanced log writer for PipelineBuilder reports with full SparkForge integration.
This module provides a comprehensive logging and reporting system for pipeline
execution results, integrating seamlessly with the existing SparkForge ecosystem.

Architecture:
- Core: Main LogWriter class that orchestrates all components
- Operations: Data processing and transformation operations
- Storage: Delta Lake and table management operations
- Monitoring: Performance tracking and metrics collection
- Analytics: Data quality analysis and trend detection

Key Features:
- Full integration with SparkForge models (StepResult, ExecutionResult, PipelineMetrics)
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
    flatten_execution_result: Convert ExecutionResult to log rows
    create_log_schema: Create Spark schema for log tables
    validate_log_data: Validate log data before writing

Example:
    from sparkforge.writer import LogWriter, WriterConfig
    from sparkforge.models import ExecutionResult

    # Configure writer
    config = WriterConfig(
        table_schema="analytics",
        table_name="pipeline_logs",
        write_mode=WriteMode.APPEND
    )

    # Create writer
    writer = LogWriter(spark, config)

    # Write execution result
    result = writer.write_execution_result(execution_result)
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
__version__ = "0.8.0"
__author__ = "SparkForge Team"
__description__ = "Modular log writer for SparkForge pipeline execution results"
