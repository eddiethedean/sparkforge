"""
Writer-specific models and type definitions.

This module contains all the dataclasses, TypedDict definitions, and type aliases
used by the writer module. It integrates with existing SparkForge models while
providing writer-specific functionality.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Literal, TypedDict

from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..models import ExecutionContext, ExecutionResult, StepResult

# ============================================================================
# Enums
# ============================================================================


class WriteMode(Enum):
    """Write mode for log operations."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
    IGNORE = "ignore"


class LogLevel(Enum):
    """Log level for writer operations."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ============================================================================
# TypedDict Definitions
# ============================================================================


class LogRow(TypedDict):
    """
    Enhanced log row with full type safety and SparkForge integration.

    This replaces the previous MinimalLogRow with proper integration
    with SparkForge models and enhanced type safety.
    """

    # Run-level information
    run_id: str
    run_mode: Literal["initial", "incremental", "full_refresh", "validation_only"]
    run_started_at: datetime | None
    run_ended_at: datetime | None

    # Execution context
    execution_id: str
    pipeline_id: str
    schema: str

    # Step-level information
    phase: Literal["bronze", "silver", "gold"]
    step_name: str
    step_type: str

    # Timing information
    start_time: datetime | None
    end_time: datetime | None
    duration_secs: float

    # Table information
    table_fqn: str | None
    write_mode: Literal["overwrite", "append"] | None

    # Data metrics
    input_rows: int | None
    output_rows: int | None
    rows_written: int | None
    rows_processed: int

    # Validation metrics
    valid_rows: int
    invalid_rows: int
    validation_rate: float

    # Execution status
    success: bool
    error_message: str | None

    # Performance metrics
    memory_usage_mb: float | None
    cpu_usage_percent: float | None

    # Metadata
    metadata: Dict[str, Any]


class WriterMetrics(TypedDict):
    """Metrics for writer operations."""

    total_writes: int
    successful_writes: int
    failed_writes: int
    total_duration_secs: float
    avg_write_duration_secs: float
    total_rows_written: int
    memory_usage_peak_mb: float


# ============================================================================
# Configuration Models
# ============================================================================


@dataclass
class WriterConfig:
    """
    Configuration for the LogWriter.

    Provides comprehensive configuration options for the writer module
    including table settings, performance tuning, and feature flags.
    """

    # Table configuration
    table_schema: str
    table_name: str
    write_mode: WriteMode = WriteMode.APPEND

    # Custom table naming patterns
    table_name_pattern: str | None = None  # e.g., "{schema}.{pipeline_id}_{timestamp}"
    table_suffix_pattern: str | None = None  # e.g., "_{run_mode}_{date}"

    # Partitioning and optimization
    partition_columns: list[str] | None = None
    partition_count: int | None = None
    compression: str = "snappy"

    # Schema options
    enable_schema_evolution: bool = True
    schema_validation_mode: str = "strict"  # strict, lenient, ignore
    auto_optimize_schema: bool = True

    # Performance settings
    batch_size: int = 1000
    max_file_size_mb: int = 128
    enable_optimization: bool = True
    parallel_write_threads: int = 4
    memory_fraction: float = 0.6

    # Feature flags
    enable_performance_monitoring: bool = True
    enable_data_quality_checks: bool = True
    enable_validation: bool = True
    enable_metrics_collection: bool = True
    enable_audit_trail: bool = True
    enable_backup_before_write: bool = False

    # Logging configuration
    log_level: LogLevel = LogLevel.INFO
    enable_detailed_logging: bool = False
    log_performance_metrics: bool = True
    log_data_quality_results: bool = True

    # Error handling
    max_retries: int = 3
    retry_delay_secs: float = 1.0
    fail_fast: bool = False
    retry_exponential_backoff: bool = True

    # Data quality thresholds
    min_validation_rate: float = 95.0
    max_invalid_rows_percent: float = 5.0
    enable_anomaly_detection: bool = False

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.table_schema:
            raise ValueError("Table schema cannot be empty")
        if not self.table_name:
            raise ValueError("Table name cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.max_file_size_mb <= 0:
            raise ValueError("Max file size must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.retry_delay_secs < 0:
            raise ValueError("Retry delay cannot be negative")
        if self.parallel_write_threads <= 0:
            raise ValueError("Parallel write threads must be positive")
        if not 0 < self.memory_fraction <= 1:
            raise ValueError("Memory fraction must be between 0 and 1")
        if self.schema_validation_mode not in ["strict", "lenient", "ignore"]:
            raise ValueError(
                "Schema validation mode must be 'strict', 'lenient', or 'ignore'"
            )
        if not 0 <= self.min_validation_rate <= 100:
            raise ValueError("Min validation rate must be between 0 and 100")
        if not 0 <= self.max_invalid_rows_percent <= 100:
            raise ValueError("Max invalid rows percent must be between 0 and 100")

    def generate_table_name(
        self,
        pipeline_id: str | None = None,
        run_mode: str | None = None,
        timestamp: str | None = None,
    ) -> str:
        """
        Generate dynamic table name based on patterns.

        Args:
            pipeline_id: Pipeline identifier
            run_mode: Run mode (initial, incremental, etc.)
            timestamp: Timestamp for naming

        Returns:
            Generated table name
        """
        table_name = self.table_name

        # Apply suffix pattern if provided
        if self.table_suffix_pattern:
            # Use explicit None checking instead of 'or' to avoid masking None values
            if run_mode is None:
                raise ValueError("run_mode cannot be None when using table_suffix_pattern")
            if timestamp is None:
                raise ValueError("timestamp cannot be None when using table_suffix_pattern")
                
            suffix_vars = {
                "run_mode": run_mode,
                "date": timestamp,
                "timestamp": timestamp,
            }
            suffix = self.table_suffix_pattern.format(**suffix_vars)
            table_name = f"{table_name}{suffix}"

        # Apply full pattern if provided
        if self.table_name_pattern:
            # Use explicit None checking instead of 'or' to avoid masking None values
            if pipeline_id is None:
                raise ValueError("pipeline_id cannot be None when using table_name_pattern")
            if run_mode is None:
                raise ValueError("run_mode cannot be None when using table_name_pattern")
            if timestamp is None:
                raise ValueError("timestamp cannot be None when using table_name_pattern")
                
            pattern_vars = {
                "schema": self.table_schema,
                "table_name": table_name,
                "pipeline_id": pipeline_id,
                "run_mode": run_mode,
                "date": timestamp,
                "timestamp": timestamp,
            }
            return self.table_name_pattern.format(**pattern_vars)

        return table_name


# ============================================================================
# Spark Schema Definitions
# ============================================================================


def create_log_schema() -> StructType:
    """
    Create the Spark schema for log tables.

    Returns:
        StructType: Spark schema for log tables with proper types
    """
    return StructType(
        [
            # Run-level fields
            StructField("run_id", StringType(), False),
            StructField("run_mode", StringType(), False),
            StructField("run_started_at", TimestampType(), True),
            StructField("run_ended_at", TimestampType(), True),
            # Execution context
            StructField("execution_id", StringType(), False),
            StructField("pipeline_id", StringType(), False),
            StructField("schema", StringType(), False),
            # Step-level fields
            StructField("phase", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("step_type", StringType(), False),
            # Timing fields
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_secs", FloatType(), False),
            # Table fields
            StructField("table_fqn", StringType(), True),
            StructField("write_mode", StringType(), True),
            # Data metrics
            StructField("input_rows", IntegerType(), True),
            StructField("output_rows", IntegerType(), True),
            StructField("rows_written", IntegerType(), True),
            StructField("rows_processed", IntegerType(), False),
            # Validation metrics
            StructField("valid_rows", IntegerType(), False),
            StructField("invalid_rows", IntegerType(), False),
            StructField("validation_rate", FloatType(), False),
            # Execution status
            StructField("success", BooleanType(), False),
            StructField("error_message", StringType(), True),
            # Performance metrics
            StructField("memory_usage_mb", FloatType(), True),
            StructField("cpu_usage_percent", FloatType(), True),
            # Metadata (stored as JSON string)
            StructField("metadata", StringType(), True),
        ]
    )


# ============================================================================
# Factory Functions
# ============================================================================


def create_log_row_from_step_result(
    step_result: StepResult,
    execution_context: ExecutionContext,
    run_id: str,
    run_mode: str,
    metadata: Dict[str, Any] | None = None,
) -> LogRow:
    """
    Create a LogRow from a StepResult and ExecutionContext.

    Args:
        step_result: The step result to convert
        execution_context: The execution context
        run_id: Unique run identifier
        run_mode: Mode of the run (initial, incremental, etc.)
        metadata: Additional metadata

    Returns:
        LogRow: Log row with all fields populated
    """
    return LogRow(
        # Run-level information
        run_id=run_id,
        run_mode=run_mode,  # type: ignore[typeddict-item]
        run_started_at=execution_context.started_at,
        run_ended_at=execution_context.ended_at,
        # Execution context
        execution_id=execution_context.execution_id,
        pipeline_id=execution_context.pipeline_id,
        schema=execution_context.schema,
        # Step-level information
        phase=step_result.phase.value,
        step_name=step_result.step_name,
        step_type=step_result.step_type if step_result.step_type is not None else "unknown",
        # Timing information
        start_time=step_result.start_time,
        end_time=step_result.end_time,
        duration_secs=step_result.duration_secs,
        # Table information
        table_fqn=step_result.table_fqn,
        write_mode=step_result.write_mode,
        # Data metrics
        input_rows=step_result.input_rows,
        output_rows=step_result.rows_processed,
        rows_written=step_result.rows_written,
        rows_processed=step_result.rows_processed,
        # Validation metrics
        valid_rows=int(step_result.rows_processed * step_result.validation_rate / 100),
        invalid_rows=int(
            step_result.rows_processed * (100 - step_result.validation_rate) / 100
        ),
        validation_rate=step_result.validation_rate,
        # Execution status
        success=step_result.success,
        error_message=step_result.error_message,
        # Performance metrics
        memory_usage_mb=None,  # TODO: Add memory metrics to StepResult
        cpu_usage_percent=None,  # TODO: Add CPU metrics to StepResult
        # Metadata
        metadata=metadata or {},
    )


def create_log_rows_from_execution_result(
    execution_result: ExecutionResult,
    run_id: str,
    run_mode: str,
    metadata: Dict[str, Any] | None = None,
) -> list[LogRow]:
    """
    Create multiple LogRows from an ExecutionResult.

    Args:
        execution_result: The execution result to convert
        run_id: Unique run identifier
        run_mode: Mode of the run
        metadata: Additional metadata

    Returns:
        List[LogRow]: List of log rows for each step
    """
    rows = []
    for step_result in execution_result.step_results:
        row = create_log_row_from_step_result(
            step_result=step_result,
            execution_context=execution_result.context,
            run_id=run_id,
            run_mode=run_mode,
            metadata=metadata,
        )
        rows.append(row)
    return rows


# ============================================================================
# Validation Functions
# ============================================================================


def validate_log_row(row: LogRow) -> None:
    """
    Validate a log row for data quality.

    Args:
        row: The log row to validate

    Raises:
        ValueError: If the log row is invalid
    """
    # Validate required fields
    if not row["run_id"]:
        raise ValueError("Run ID cannot be empty")
    if not row["execution_id"]:
        raise ValueError("Execution ID cannot be empty")
    if not row["pipeline_id"]:
        raise ValueError("Pipeline ID cannot be empty")
    if not row["step_name"]:
        raise ValueError("Step name cannot be empty")

    # Validate numeric fields
    if row["duration_secs"] < 0:
        raise ValueError("Duration cannot be negative")
    if row["rows_processed"] < 0:
        raise ValueError("Rows processed cannot be negative")
    if row["valid_rows"] < 0:
        raise ValueError("Valid rows cannot be negative")
    if row["invalid_rows"] < 0:
        raise ValueError("Invalid rows cannot be negative")
    if not 0 <= row["validation_rate"] <= 100:
        raise ValueError("Validation rate must be between 0 and 100")

    # Validate logical consistency
    total_rows = row["valid_rows"] + row["invalid_rows"]
    if total_rows != row["rows_processed"]:
        raise ValueError("Valid + invalid rows must equal rows processed")


def validate_log_data(rows: list[LogRow]) -> Dict[str, Any]:
    """
    Validate a list of log rows.

    Args:
        rows: List of log rows to validate

    Returns:
        Dictionary with validation results
    """
    errors = []
    for i, row in enumerate(rows):
        try:
            validate_log_row(row)
        except ValueError as e:
            errors.append(f"Invalid log row at index {i}: {e}")

    return {"is_valid": len(errors) == 0, "errors": errors}
