#!/usr/bin/env python3
"""
Property-based tests for sparkforge/validation.py module using Hypothesis.

This module tests validation functions with generated data to ensure robustness
and catch edge cases that might be missed by traditional unit tests.
"""

from typing import List
from unittest.mock import Mock

from hypothesis import given, settings
from hypothesis import strategies as st

from sparkforge.validation import safe_divide, validate_dataframe_schema


class TestValidationPropertyBased:
    """Property-based tests for validation functions."""

    @given(
        numerator=st.floats(
            min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
        denominator=st.floats(
            min_value=0.001, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
        default_value=st.floats(
            min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_safe_divide_properties(
        self, numerator: float, denominator: float, default_value: float
    ) -> None:
        """Test safe_divide with generated float values."""
        result = safe_divide(numerator, denominator, default_value)

        # Property: When denominator is non-zero, should return actual division
        expected = numerator / denominator
        assert abs(result - expected) < 1e-10  # Allow for floating point precision

    @given(
        numerator=st.floats(
            min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
        default_value=st.floats(
            min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_safe_divide_zero_denominator_properties(
        self, numerator: float, default_value: float
    ) -> None:
        """Test safe_divide with zero denominator."""
        result = safe_divide(numerator, 0.0, default_value)

        # Property: When denominator is zero, should return default value
        assert result == default_value

    @given(
        actual_columns=st.lists(
            st.text(min_size=1, max_size=20), min_size=0, max_size=10
        ),
        expected_columns=st.lists(
            st.text(min_size=1, max_size=20), min_size=0, max_size=10
        ),
    )
    @settings(max_examples=100)
    def test_validate_dataframe_schema_properties(
        self, actual_columns: List[str], expected_columns: List[str]
    ) -> None:
        """Test validate_dataframe_schema with generated column lists."""
        # Create mock DataFrame
        mock_df = Mock()
        mock_df.columns = actual_columns

        result = validate_dataframe_schema(mock_df, expected_columns)

        # Property: Result should be boolean
        assert isinstance(result, bool)

        # Property: Should be True if and only if all expected columns are in actual columns
        expected_result = all(col in actual_columns for col in expected_columns)
        assert result == expected_result

        # Property: If actual columns is empty, result should be True only if expected is also empty
        if not actual_columns:
            assert result == (not expected_columns)

    @given(columns=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=20))
    @settings(max_examples=50)
    def test_dataframe_schema_edge_cases_properties(self, columns: List[str]) -> None:
        """Test dataframe schema validation with edge cases."""
        # Create mock DataFrame
        mock_df = Mock()
        mock_df.columns = columns

        # Test with empty expected columns
        result = validate_dataframe_schema(mock_df, [])
        assert result is True  # Empty expected columns should always pass

        # Test with subset of actual columns
        if columns:
            subset = columns[: len(columns) // 2] if len(columns) > 1 else columns
            result = validate_dataframe_schema(mock_df, subset)
            assert result is True  # Subset should always pass

        # Test with non-existent columns
        non_existent = ["non_existent_col"]
        result = validate_dataframe_schema(mock_df, non_existent)
        assert result is False  # Non-existent columns should fail
