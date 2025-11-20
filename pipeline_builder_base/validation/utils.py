"""
Utility functions for the framework validation.

This module provides utility functions for data analysis and validation operations.
"""

from __future__ import annotations


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero or None.

    Args:
        numerator: The numerator
        denominator: The denominator
        default: Default value to return if denominator is zero or None

    Returns:
        The division result or default value
    """
    if denominator is None or numerator is None or denominator == 0:
        return default
    return numerator / denominator

