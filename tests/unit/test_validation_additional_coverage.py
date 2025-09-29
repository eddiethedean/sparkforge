#!/usr/bin/env python3
"""
Additional tests for validation.py to improve coverage to 80%+.

This module focuses on covering missing lines and edge cases that are not
currently covered by the existing test suite.
"""

from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import Column, col

from sparkforge.validation import (
    _convert_rule_to_expression,
    _convert_rules_to_expressions,
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    get_dataframe_info,
    safe_divide,
    validate_dataframe_schema,
)


class TestValidationEdgeCases:
    """Test validation function edge cases and missing coverage."""

    def test_convert_rule_to_expression_string_handling(self) -> None:
        """Test _convert_rule_to_expression with string rules."""
        # Test with string rule
        result = _convert_rule_to_expression("col1 > 0", "col1")
        assert isinstance(result, Column)

    def test_and_all_rules_empty_expressions(self) -> None:
        """Test and_all_rules with empty expressions."""
        # Test with empty expressions dict
        rules = {"col1": []}
        result = and_all_rules(rules)
        assert result is True

    def test_and_all_rules_no_column_expressions(self) -> None:
        """Test and_all_rules when no valid column expressions exist."""
        # Test with rules that don't produce valid column expressions
        rules = {"col1": [None]}
        # This should return True when no valid expressions are found
        result = and_all_rules(rules)
        assert result is True

    def test_apply_column_rules_validation_predicate_true(self) -> None:
        """Test apply_column_rules with empty rules."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.limit.return_value = mock_df

        rules = {}  # Empty rules should return all rows as valid

        result = apply_column_rules(
            mock_df, rules, stage="test_stage", step="test_step"
        )

        assert result is not None
        mock_df.limit.assert_called_once_with(0)

    def test_apply_column_rules_with_rules(self) -> None:
        """Test apply_column_rules with actual rules."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.filter.return_value = mock_df
        mock_df.limit.return_value = mock_df

        rules = {"col1": ["not_null"]}

        result = apply_column_rules(
            mock_df,
            rules,
            stage="test_stage",
            step="test_step",
            filter_columns_by_rules=False,
        )

        assert result is not None

    def test_safe_divide_edge_cases(self) -> None:
        """Test safe_divide with edge cases."""
        # Test with zero denominator
        result = safe_divide(10, 0)
        assert result == 0.0

        # Test with zero denominator and custom default
        result = safe_divide(10, 0, default=1.0)
        assert result == 1.0

    def test_validate_dataframe_schema_edge_cases(self) -> None:
        """Test validate_dataframe_schema with edge cases."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["col1", "col2"]

        # Test with empty expected columns
        result = validate_dataframe_schema(mock_df, [])
        assert result is True

    def test_get_dataframe_info_edge_cases(self) -> None:
        """Test get_dataframe_info with edge cases."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        mock_df.columns = []

        # Test with empty DataFrame
        result = get_dataframe_info(mock_df)
        assert result["row_count"] == 0
        assert result["column_count"] == 0

    def test_get_dataframe_info_error_handling(self) -> None:
        """Test get_dataframe_info error handling."""
        # Create mock DataFrame that raises exception
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Test error")

        # This should handle the exception gracefully
        result = get_dataframe_info(mock_df)
        assert "error" in result

    def test_assess_data_quality_edge_cases(self) -> None:
        """Test assess_data_quality with edge cases."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100

        # Test with empty rules
        result = assess_data_quality(mock_df, {})
        assert result is not None

    def test_apply_column_rules_edge_cases(self) -> None:
        """Test apply_column_rules with edge cases."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100

        # Test with empty rules
        result = apply_column_rules(mock_df, {}, "test_stage", "test_step")
        assert result is not None

    def test_convert_rules_to_expressions_complex_cases(self) -> None:
        """Test _convert_rules_to_expressions with complex cases."""
        # Test with mixed rule types
        rules = {
            "col1": ["not_null"],
            "col2": [col("col2") > 0],
            "col3": [None, "invalid"],
        }

        result = _convert_rules_to_expressions(rules)
        assert isinstance(result, dict)

    def test_convert_rule_to_expression_edge_cases(self) -> None:
        """Test _convert_rule_to_expression with edge cases."""
        # Test with valid string expressions
        result = _convert_rule_to_expression("col1 > 0", "col1")
        assert isinstance(result, Column)

    def test_and_all_rules_single_expression(self) -> None:
        """Test and_all_rules with single expression."""
        # Test with single valid expression
        rules = {"col1": ["col1 > 0"]}
        result = and_all_rules(rules)
        assert isinstance(result, Column)

    def test_and_all_rules_multiple_expressions(self) -> None:
        """Test and_all_rules with multiple expressions."""
        # Test with multiple expressions
        rules = {"col1": ["col1 > 0"], "col2": ["col2 IS NOT NULL"]}
        result = and_all_rules(rules)
        assert isinstance(result, Column)

    def test_string_rule_conversion_edge_cases(self) -> None:
        """Test string rule conversion edge cases."""
        # Test various string rule formats
        test_cases = [
            "col1 > 0",
            "col1 IS NOT NULL",
            "col1 IN ('a', 'b', 'c')",
            "LENGTH(col1) > 5",
        ]

        for rule in test_cases:
            result = _convert_rule_to_expression(rule, "col1")
            assert isinstance(result, Column)

    def test_validation_error_handling(self) -> None:
        """Test validation error handling paths."""
        # Test with invalid DataFrame
        with pytest.raises((AttributeError, TypeError)):
            apply_column_rules(None, {"col1": ["not_null"]})  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            apply_column_rules("invalid", {"col1": ["not_null"]})  # type: ignore
