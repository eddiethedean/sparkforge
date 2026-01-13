"""
Reporting utilities for the pipeline framework.

This module re-exports reporting utilities from pipeline_builder_base
and adds Spark-specific reporting functions if needed.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

# TypedDict is available in typing for Python 3.8+
try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

# Re-export from base
from pipeline_builder_base.models import StageStats
from pipeline_builder_base.reporting import (
    format_duration,
    safe_divide,
)

from .execution_reporter import ExecutionReporter

# ============================================================================
# TypedDict Definitions
# ============================================================================


class ValidationReport(TypedDict):
    """Validation report structure."""

    stage: Optional[str]
    step: Optional[str]
    total_rows: int
    valid_rows: int
    invalid_rows: int
    validation_rate: float
    duration_secs: float
    start_at: datetime
    end_at: datetime


class TransformReport(TypedDict):
    """Transform operation report structure."""

    input_rows: int
    output_rows: int
    duration_secs: float
    skipped: bool
    start_at: datetime
    end_at: datetime


class WriteReport(TypedDict):
    """Write operation report structure."""

    mode: str
    rows_written: int
    duration_secs: float
    table_fqn: str
    skipped: bool
    start_at: datetime
    end_at: datetime


class ExecutionSummary(TypedDict):
    """Execution summary nested structure."""

    total_steps: int
    successful_steps: int
    failed_steps: int
    success_rate: float
    failure_rate: float


class PerformanceMetrics(TypedDict):
    """Performance metrics nested structure."""

    total_duration_secs: float
    formatted_duration: str
    avg_validation_rate: float


class DataMetrics(TypedDict):
    """Data metrics nested structure."""

    total_rows_processed: int
    total_rows_written: int
    processing_efficiency: float


class SummaryReport(TypedDict):
    """Complete summary report structure."""

    execution_summary: ExecutionSummary
    performance_metrics: PerformanceMetrics
    data_metrics: DataMetrics


# ============================================================================
# Reporting Functions
# ============================================================================


def create_validation_dict(
    stats: Optional[StageStats], *, start_at: datetime, end_at: datetime
) -> ValidationReport:
    """
    Create validation dictionary for reporting.

    Args:
        stats: Stage statistics
        start_at: Start time
        end_at: End time

    Returns:
        Validation dictionary
    """
    if stats is None:
        return {
            "stage": None,
            "step": None,
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "duration_secs": 0.0,
            "start_at": start_at,
            "end_at": end_at,
        }

    return {
        "stage": stats.stage,
        "step": stats.step,
        "total_rows": stats.total_rows,
        "valid_rows": stats.valid_rows,
        "invalid_rows": stats.invalid_rows,
        "validation_rate": round(stats.validation_rate, 2),
        "duration_secs": round(stats.duration_secs, 3),
        "start_at": start_at,
        "end_at": end_at,
    }


def create_transform_dict(
    input_rows: int,
    output_rows: int,
    duration_secs: float,
    skipped: bool,
    *,
    start_at: datetime,
    end_at: datetime,
) -> TransformReport:
    """
    Create transform dictionary for reporting.

    Args:
        input_rows: Number of input rows
        output_rows: Number of output rows
        duration_secs: Duration in seconds
        skipped: Whether operation was skipped
        start_at: Start time
        end_at: End time

    Returns:
        Transform dictionary
    """
    return {
        "input_rows": int(input_rows),
        "output_rows": int(output_rows),
        "duration_secs": round(duration_secs, 3),
        "skipped": bool(skipped),
        "start_at": start_at,
        "end_at": end_at,
    }


def create_write_dict(
    mode: str,
    rows: int,
    duration_secs: float,
    table_fqn: str,
    skipped: bool,
    *,
    start_at: datetime,
    end_at: datetime,
) -> WriteReport:
    """
    Create write dictionary for reporting.

    Args:
        mode: Write mode
        rows: Number of rows written
        duration_secs: Duration in seconds
        table_fqn: Fully qualified table name
        skipped: Whether operation was skipped
        start_at: Start time
        end_at: End time

    Returns:
        Write dictionary
    """
    return {
        "mode": mode,
        "rows_written": int(rows),
        "duration_secs": round(duration_secs, 3),
        "table_fqn": table_fqn,
        "skipped": bool(skipped),
        "start_at": start_at,
        "end_at": end_at,
    }


def create_summary_report(
    total_steps: int,
    successful_steps: int,
    failed_steps: int,
    total_duration: float,
    total_rows_processed: int,
    total_rows_written: int,
    avg_validation_rate: float,
) -> SummaryReport:
    """
    Create summary report from execution results.

    Args:
        total_steps: Total number of steps
        successful_steps: Number of successful steps
        failed_steps: Number of failed steps
        total_duration: Total duration in seconds
        total_rows_processed: Total rows processed
        total_rows_written: Total rows written
        avg_validation_rate: Average validation rate

    Returns:
        Summary report dictionary
    """
    return {
        "execution_summary": {
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "success_rate": round(
                safe_divide(successful_steps * 100.0, total_steps, 0.0), 2
            ),
            "failure_rate": round(
                safe_divide(failed_steps * 100.0, total_steps, 0.0), 2
            ),
        },
        "performance_metrics": {
            "total_duration_secs": round(total_duration, 3),
            "formatted_duration": format_duration(total_duration),
            "avg_validation_rate": round(avg_validation_rate, 2),
        },
        "data_metrics": {
            "total_rows_processed": total_rows_processed,
            "total_rows_written": total_rows_written,
            "processing_efficiency": round(
                safe_divide(total_rows_written * 100.0, total_rows_processed, 0.0), 2
            ),
        },
    }


__all__ = [
    "ExecutionReporter",
    "create_validation_dict",
    "create_transform_dict",
    "create_write_dict",
    "create_summary_report",
    "ValidationReport",
    "TransformReport",
    "WriteReport",
    "SummaryReport",
    "format_duration",
    "safe_divide",
]
