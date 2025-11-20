"""
Writer-specific models and type definitions.

This module contains all the TypedDict definitions and type aliases
used by the writer module. It is engine-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Literal, TypedDict

from ..models import ExecutionResult, StepResult


# ============================================================================
# Enums
# ============================================================================


class WriteMode(Enum):
    """Write mode for log operations."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
    IGNORE = "ignore"


# ============================================================================
# TypedDict Definitions
# ============================================================================


class LogRow(TypedDict):
    """
    Enhanced log row with full type safety and framework integration.

    This is an engine-agnostic log row structure that can be used
    by both Spark and SQL implementations.
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
    phase: Literal["bronze", "silver", "gold", "pipeline"]
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
    table_total_rows: int | None  # Total rows in table after this write

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

    table_schema: str
    table_name: str
    write_mode: WriteMode = WriteMode.APPEND
    enable_analytics: bool = True
    enable_monitoring: bool = True
    enable_quality_checks: bool = True
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_secs: float = 1.0

    def validate(self) -> None:
        """Validate the writer configuration."""
        if not self.table_schema or not isinstance(self.table_schema, str):
            raise ValueError("table_schema must be a non-empty string")
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValueError("table_name must be a non-empty string")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay_secs < 0:
            raise ValueError("retry_delay_secs must be non-negative")


# ============================================================================
# Utility Functions
# ============================================================================


def create_log_rows_from_execution_result(
    execution_result: ExecutionResult,
    run_id: str,
    run_mode: str = "initial",
    metadata: Dict[str, Any] | None = None,
) -> list[LogRow]:
    """
    Create log rows from an execution result.

    This is an engine-agnostic function that creates log rows from
    execution results. Engine-specific implementations can use this
    as a base and extend it as needed.

    Args:
        execution_result: The execution result
        run_id: Run identifier
        run_mode: Mode of the run
        metadata: Additional metadata

    Returns:
        List of log rows
    """
    log_rows = []

    # Create a main log row for the execution
    context = execution_result.context
    main_row: LogRow = {
        "run_id": run_id,
        "run_mode": run_mode,  # type: ignore[typeddict-item]
        "run_started_at": context.start_time,
        "run_ended_at": context.end_time,
        "execution_id": context.execution_id,
        "pipeline_id": context.pipeline_id,
        "schema": context.schema,
        "phase": "pipeline",
        "step_name": "pipeline_execution",
        "step_type": "pipeline",
        "start_time": context.start_time,
        "end_time": context.end_time,
        "duration_secs": context.duration_secs or 0.0,
        "table_fqn": None,
        "write_mode": None,
        "input_rows": None,
        "output_rows": None,
        "rows_written": None,
        "rows_processed": 0,
        "table_total_rows": None,
        "valid_rows": 0,
        "invalid_rows": 0,
        "validation_rate": 100.0,
        "success": execution_result.success,
        "error_message": None,
        "memory_usage_mb": None,
        "cpu_usage_percent": None,
        "metadata": metadata or {},
    }

    log_rows.append(main_row)

    # Add step results
    for step_result in execution_result.step_results:
        step_row: LogRow = {
            "run_id": run_id,
            "run_mode": run_mode,  # type: ignore[typeddict-item]
            "run_started_at": context.start_time,
            "run_ended_at": context.end_time,
            "execution_id": context.execution_id,
            "pipeline_id": context.pipeline_id,
            "schema": context.schema,
            "phase": step_result.phase.value,  # type: ignore[typeddict-item]
            "step_name": step_result.step_name,
            "step_type": step_result.step_type or "unknown",
            "start_time": step_result.start_time,
            "end_time": step_result.end_time,
            "duration_secs": step_result.duration_secs,
            "table_fqn": step_result.table_fqn,
            "write_mode": step_result.write_mode,  # type: ignore[typeddict-item]
            "input_rows": step_result.input_rows,
            "output_rows": step_result.rows_written,
            "rows_written": step_result.rows_written,
            "rows_processed": step_result.rows_processed,
            "table_total_rows": None,
            "valid_rows": step_result.rows_processed,
            "invalid_rows": 0,
            "validation_rate": step_result.validation_rate,
            "success": step_result.success,
            "error_message": step_result.error_message,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "metadata": {},
        }
        log_rows.append(step_row)

    return log_rows


def validate_log_data(log_rows: list[LogRow]) -> None:
    """
    Validate log data for quality and consistency.

    Args:
        log_rows: List of log rows to validate

    Raises:
        ValueError: If validation fails
    """
    if not log_rows:
        return

    # Basic validation - check required fields
    required_fields = {"run_id", "phase", "step_name"}
    for i, row in enumerate(log_rows):
        missing_fields = required_fields - set(row.keys())
        if missing_fields:
            raise ValueError(
                f"Log row {i} missing required fields: {missing_fields}"
            )

