"""
Reporting utilities for the pipeline framework.

This module contains functions for creating reports, statistics, and summaries
for pipeline execution.

# Depends on:
#   models.execution
#   performance
#   validation.utils
"""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict

from .models import StageStats
from .validation import safe_divide

# ============================================================================
# TypedDict Definitions
# ============================================================================


class ValidationReport(TypedDict):
    """Validation report structure."""

    stage: str | None
    step: str | None
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


def create_validation_dict(
    stats: StageStats | None, *, start_at: datetime, end_at: datetime
) -> ValidationReport:
    """
    Create a validation report dictionary from stage stats.

    Args:
        stats: Stage statistics
        start_at: Start time
        end_at: End time

    Returns:
        Validation report dictionary
    """
    if stats is None:
        return {
            "stage": None,
            "step": None,
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 0.0,
            "duration_secs": (end_at - start_at).total_seconds(),
            "start_at": start_at,
            "end_at": end_at,
        }

    return {
        "stage": stats.stage,
        "step": stats.step,
        "total_rows": stats.total_rows,
        "valid_rows": stats.valid_rows,
        "invalid_rows": stats.invalid_rows,
        "validation_rate": stats.validation_rate,
        "duration_secs": stats.duration_secs,
        "start_at": start_at,
        "end_at": end_at,
    }


def create_transform_dict(
    *,
    input_rows: int,
    output_rows: int,
    start_at: datetime,
    end_at: datetime,
    skipped: bool = False,
) -> TransformReport:
    """
    Create a transform report dictionary.

    Args:
        input_rows: Number of input rows
        output_rows: Number of output rows
        start_at: Start time
        end_at: End time
        skipped: Whether the transform was skipped

    Returns:
        Transform report dictionary
    """
    return {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "duration_secs": (end_at - start_at).total_seconds(),
        "skipped": skipped,
        "start_at": start_at,
        "end_at": end_at,
    }


def create_write_dict(
    *,
    mode: str,
    rows_written: int,
    table_fqn: str,
    start_at: datetime,
    end_at: datetime,
    skipped: bool = False,
) -> WriteReport:
    """
    Create a write report dictionary.

    Args:
        mode: Write mode
        rows_written: Number of rows written
        table_fqn: Fully qualified table name
        start_at: Start time
        end_at: End time
        skipped: Whether the write was skipped

    Returns:
        Write report dictionary
    """
    return {
        "mode": mode,
        "rows_written": rows_written,
        "duration_secs": (end_at - start_at).total_seconds(),
        "table_fqn": table_fqn,
        "skipped": skipped,
        "start_at": start_at,
        "end_at": end_at,
    }


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.2f}s"


def create_summary_report(
    *,
    total_steps: int,
    successful_steps: int,
    failed_steps: int,
    total_duration_secs: float,
    total_rows_processed: int,
    total_rows_written: int,
    avg_validation_rate: float,
) -> SummaryReport:
    """
    Create a complete summary report.

    Args:
        total_steps: Total number of steps
        successful_steps: Number of successful steps
        failed_steps: Number of failed steps
        total_duration_secs: Total duration in seconds
        total_rows_processed: Total rows processed
        total_rows_written: Total rows written
        avg_validation_rate: Average validation rate

    Returns:
        Complete summary report
    """
    success_rate = safe_divide(successful_steps, total_steps, 0.0) * 100
    failure_rate = 100.0 - success_rate
    processing_efficiency = (
        safe_divide(total_rows_written, total_rows_processed, 0.0) * 100
    )

    return {
        "execution_summary": {
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "success_rate": success_rate,
            "failure_rate": failure_rate,
        },
        "performance_metrics": {
            "total_duration_secs": total_duration_secs,
            "formatted_duration": format_duration(total_duration_secs),
            "avg_validation_rate": avg_validation_rate,
        },
        "data_metrics": {
            "total_rows_processed": total_rows_processed,
            "total_rows_written": total_rows_written,
            "processing_efficiency": processing_efficiency,
        },
    }
