"""
Utility functions for SparkForge validation.

This module provides utility functions for data analysis and validation operations.
"""

from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: The numerator
        denominator: The denominator
        default: Default value to return if denominator is zero

    Returns:
        The division result or default value
    """
    if denominator == 0:
        return default
    return numerator / denominator


def get_dataframe_info(df: DataFrame) -> Dict[str, Any]:
    """
    Get basic information about a DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary with DataFrame information
    """
    try:
        row_count = df.count()
        column_count = len(df.columns)
        schema = df.schema

        return {
            "row_count": row_count,
            "column_count": column_count,
            "columns": df.columns,
            "schema": str(schema),
            "is_empty": row_count == 0,
        }
    except Exception as e:
        return {
            "error": str(e),
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "schema": "unknown",
            "is_empty": True,
        }
