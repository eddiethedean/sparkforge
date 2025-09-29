"""
Reporting utilities for the SparkForge pipeline framework.

This module contains functions for creating reports, statistics, and summaries
for pipeline execution.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from .models import StageStats
from .performance import format_duration
from .validation import safe_divide


def create_validation_dict(
    stats: StageStats | None, *, start_at: datetime, end_at: datetime
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
    """
    Create a summary report for pipeline execution.

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
    if total_steps == 0:
        success_rate = 0.0
        failure_rate = 0.0
    else:
        success_rate = safe_divide(successful_steps * 100.0, total_steps, 0.0)
        failure_rate = 100.0 - success_rate

    return {
        "execution_summary": {
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "success_rate": round(success_rate, 2),
            "failure_rate": round(failure_rate, 2),
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
