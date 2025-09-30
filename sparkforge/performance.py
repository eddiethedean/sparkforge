"""
Performance monitoring utilities for the SparkForge pipeline framework.

This module contains functions for timing operations, monitoring performance,
and managing execution metrics.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Generator

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def now_dt() -> datetime:
    """Get current UTC datetime."""
    return datetime.utcnow()


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
        minutes = seconds / 60
        return f"{minutes:.2f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def time_operation(operation_name: str = "operation") -> Callable[[Callable], Callable]:
    """Decorator to time operations and log performance."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            logger.info(f"Starting {operation_name}...")

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"Completed {operation_name} in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Failed {operation_name} after {duration:.3f}s: {e}")
                raise

        return wrapper

    return decorator


@contextmanager
def performance_monitor(
    operation_name: str, max_duration: float | None = None
) -> Generator[None, None, None]:
    """Context manager to monitor operation performance."""
    start_time = time.time()
    logger.info(f"Starting {operation_name}...")

    try:
        yield
        duration = time.time() - start_time
        logger.info(f"Completed {operation_name} in {duration:.3f}s")

        if max_duration and duration > max_duration:
            logger.warning(
                f"{operation_name} took {duration:.3f}s, exceeding threshold of {max_duration}s"
            )

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Failed {operation_name} after {duration:.3f}s: {e}")
        raise


@time_operation("write operation")
def time_write_operation(
    mode: str, df: DataFrame, fqn: str, **options: Any
) -> tuple[int, float, datetime, datetime]:
    """
    Time a write operation and return results with timing info.

    Args:
        mode: Write mode (overwrite/append)
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options

    Returns:
        Tuple of (rows_written, duration_secs, start_time, end_time)

    Raises:
        ValueError: If mode is invalid
        TableOperationError: If write operation fails
    """
    from .table_operations import write_append_table, write_overwrite_table
    
    start = now_dt()
    t0 = time.time()

    try:
        if mode == "overwrite":
            rows = write_overwrite_table(df, fqn, **options)
        elif mode == "append":
            rows = write_append_table(df, fqn, **options)
        else:
            raise ValueError(
                f"Unknown write mode '{mode}'. Supported modes: overwrite, append"
            )

        t1 = time.time()
        end = now_dt()
        duration = round(t1 - t0, 3)

        logger.info(f"Write operation completed: {rows} rows in {duration}s to {fqn}")
        return rows, duration, start, end

    except Exception as e:
        t1 = time.time()
        end = now_dt()
        duration = round(t1 - t0, 3)
        logger.error(f"Write operation failed after {duration}s: {e}")
        raise


def monitor_performance(
    operation_name: str, max_duration: float | None = None
) -> Callable:
    """
    Decorator factory for performance monitoring.

    Args:
        operation_name: Name of the operation
        max_duration: Maximum allowed duration in seconds

    Returns:
        Decorator function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with performance_monitor(operation_name, max_duration):
                return func(*args, **kwargs)

        return wrapper

    return decorator
