"""
SparkForge Writer Module

Enhanced log writer for PipelineBuilder reports with full SparkForge integration.

This module provides a comprehensive logging and reporting system for pipeline
execution results, integrating seamlessly with the existing SparkForge ecosystem.

Key Features:
- Full integration with SparkForge models (StepResult, ExecutionResult, PipelineMetrics)
- Enhanced type safety with proper TypedDict definitions
- Comprehensive error handling and validation
- Performance monitoring and optimization
- Flexible configuration system
- Delta Lake integration for persistent logging

Classes:
    LogWriter: Main writer class for pipeline log operations
    WriterConfig: Configuration class for writer settings
    LogRow: Enhanced log row model with full type safety

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

from __future__ import annotations

from .core import LogWriter
from .exceptions import WriterConfigurationError, WriterError, WriterValidationError
from .models import LogRow, WriteMode, WriterConfig

__all__ = [
    "LogWriter",
    "WriterConfig",
    "LogRow",
    "WriteMode",
    "WriterError",
    "WriterValidationError",
    "WriterConfigurationError",
]
