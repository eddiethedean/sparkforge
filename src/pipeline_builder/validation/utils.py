"""
Utility functions for the framework validation.

This module provides utility functions for data analysis and validation operations.

# Depends on:
#   compat
"""

from __future__ import annotations

from typing import Any, Dict

# Re-export safe_divide from base for backward compatibility
from pipeline_builder_base.validation import safe_divide  # noqa: F401

from ..compat import DataFrame


def get_dataframe_info(df: DataFrame) -> Dict[str, Any]:
    """
    Get basic information about a DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary with DataFrame information
    """
    try:
        row_count = df.count()  # type: ignore[attr-defined]
        column_count = len(df.columns)  # type: ignore[attr-defined]
        schema = df.schema  # type: ignore[attr-defined]

        return {
            "row_count": row_count,
            "column_count": column_count,
            "columns": df.columns,  # type: ignore[attr-defined]
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
