"""
Builder helper functions for creating step dictionaries.

This module provides helper functions for creating step configuration dictionaries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def create_bronze_step_dict(
    name: str,
    rules: Dict[str, Any],
    incremental_col: Optional[str] = None,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a dictionary representing a bronze step configuration.

    Args:
        name: Step name
        rules: Validation rules dictionary
        incremental_col: Optional incremental column name
        schema: Optional schema name
        **kwargs: Additional step attributes

    Returns:
        Dictionary with bronze step configuration

    Example:
        >>> step_dict = create_bronze_step_dict(
        ...     name="events",
        ...     rules={"user_id": ["not_null"]},
        ...     incremental_col="timestamp"
        ... )
    """
    step_dict: Dict[str, Any] = {
        "name": name,
        "rules": rules,
        "incremental_col": incremental_col,
        "schema": schema,
        **kwargs,
    }
    return step_dict


def create_silver_step_dict(
    name: str,
    source_bronze: str,
    transform: Any,
    rules: Dict[str, Any],
    table_name: str,
    watermark_col: Optional[str] = None,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a dictionary representing a silver step configuration.

    Args:
        name: Step name
        source_bronze: Source bronze step name
        transform: Transformation function
        rules: Validation rules dictionary
        table_name: Target table name
        watermark_col: Optional watermark column name
        schema: Optional schema name
        **kwargs: Additional step attributes

    Returns:
        Dictionary with silver step configuration

    Example:
        >>> step_dict = create_silver_step_dict(
        ...     name="clean_events",
        ...     source_bronze="events",
        ...     transform=clean_func,
        ...     rules={"user_id": ["not_null"]},
        ...     table_name="clean_events"
        ... )
    """
    step_dict: Dict[str, Any] = {
        "name": name,
        "source_bronze": source_bronze,
        "transform": transform,
        "rules": rules,
        "table_name": table_name,
        "watermark_col": watermark_col,
        "schema": schema,
        **kwargs,
    }
    return step_dict


def create_gold_step_dict(
    name: str,
    transform: Any,
    rules: Dict[str, Any],
    table_name: str,
    source_silvers: Optional[List[str]] = None,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a dictionary representing a gold step configuration.

    Args:
        name: Step name
        transform: Transformation function
        rules: Validation rules dictionary
        table_name: Target table name
        source_silvers: Optional list of source silver step names
        schema: Optional schema name
        **kwargs: Additional step attributes

    Returns:
        Dictionary with gold step configuration

    Example:
        >>> step_dict = create_gold_step_dict(
        ...     name="daily_metrics",
        ...     transform=metrics_func,
        ...     rules={"metric": ["not_null"]},
        ...     table_name="daily_metrics",
        ...     source_silvers=["clean_events"]
        ... )
    """
    step_dict: Dict[str, Any] = {
        "name": name,
        "transform": transform,
        "rules": rules,
        "table_name": table_name,
        "source_silvers": source_silvers or [],
        "schema": schema,
        **kwargs,
    }
    return step_dict
