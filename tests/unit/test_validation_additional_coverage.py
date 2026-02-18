#!/usr/bin/env python3
"""
Additional tests for validation.py to improve coverage to 80%+.

This module focuses on covering missing lines and edge cases that are not
currently covered by the existing test suite.
"""

import os
from unittest.mock import Mock

import pytest

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import DataFrame as DataFrame  # type: ignore[import]
    from sparkless import functions as F  # type: ignore[import]
    from sparkless.functions import col  # type: ignore[import]
else:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col

from pipeline_builder.validation import (
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

    def test_convert_rule_to_expression_string_handling(self, spark_session) -> None:
        """Test _convert_rule_to_expression with string rules."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Test with string rule
        result = _convert_rule_to_expression("col1 > 0", "col1", F)
        # Check for Column-like object (works with both PySpark and mock-spark)
        assert hasattr(result, "__and__") and hasattr(result, "__invert__")

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
            mock_df, rules, stage="test_stage", step="test_step", functions=F
        )

        assert result is not None
        mock_df.limit.assert_called_once_with(0)

    def test_apply_column_rules_with_rules(self, spark_session) -> None:
        """Test apply_column_rules with actual rules."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.filter.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.columns = ["col1", "col2", "col3"]

        rules = {"col1": ["not_null"]}

        result = apply_column_rules(
            mock_df,
            rules,
            stage="test_stage",
            step="test_step",
            filter_columns_by_rules=False,
            functions=F,
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
        result = apply_column_rules(mock_df, {}, "test_stage", "test_step", functions=F)
        assert result is not None

    def test_convert_rules_to_expressions_complex_cases(self, spark_session) -> None:
        """Test _convert_rules_to_expressions with complex cases."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Test with mixed rule types
        rules = {
            "col1": ["not_null"],
            "col2": [col("col2") > 0],
            "col3": [None, "invalid"],
        }

        result = _convert_rules_to_expressions(rules, F)
        assert isinstance(result, dict)


class TestConvertRulesToExpressionsColumnBoolFix:
    """Tests for _convert_rules_to_expressions when rules contain PySpark Columns.

    Ensures we never compare a Column to the string \"in\" in an if-condition,
    which would trigger Column.__bool__ and raise ValueError in PySpark.
    """

    def test_rules_with_column_only_does_not_raise(self, spark_session) -> None:
        """Rule list with only Column expression must not raise ValueError."""
        # When rule is a Column, code must not do (rule == "in") in an if
        rules = {"x": [F.col("x") > 0]}
        result = _convert_rules_to_expressions(rules, F)
        assert "x" in result
        assert len(result["x"]) == 1
        assert hasattr(result["x"][0], "__and__")

    def test_rules_with_column_and_string_mixed_does_not_raise(self, spark_session) -> None:
        """Rule list with Column first then string rules must not raise."""
        rules = {"id": [F.col("id") > 0, "not_null"], "name": ["not_null"]}
        result = _convert_rules_to_expressions(rules, F)
        assert "id" in result
        assert len(result["id"]) == 2
        assert "name" in result
        assert len(result["name"]) == 1

    def test_rules_string_in_two_element_still_works(self, spark_session) -> None:
        """Doc-style \"in\" rule [\"in\", [\"a\", \"b\"]] must still be coalesced."""
        rules = {"status": ["in", ["active", "inactive"]]}
        result = _convert_rules_to_expressions(rules, F)
        assert "status" in result
        assert len(result["status"]) == 1
        assert hasattr(result["status"][0], "__and__")

    def test_rules_column_then_in_literal_not_coalesced(self, spark_session) -> None:
        """Column followed by \"in\" and list: Column is kept, \"in\" handled as two-element."""
        # List is [Column, "in", ["a","b"]] - first element is Column so we must not
        # compare it to "in"; second element is "in" so we coalesce "in" + ["a","b"]
        rules = {"x": [F.col("x") > 0, "in", ["a", "b"]]}
        result = _convert_rules_to_expressions(rules, F)
        assert "x" in result
        # Expect: one Column (passed through), one expression from ["in", ["a","b"]]
        assert len(result["x"]) == 2


class TestValidationEdgeCasesContinued:
    """Remaining validation edge case tests (split for clarity)."""

    def test_convert_rule_to_expression_edge_cases(self, spark_session) -> None:
        """Test _convert_rule_to_expression with edge cases."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Test with valid string expressions
        result = _convert_rule_to_expression("col1 > 0", "col1", F)
        # Check for Column-like object (works with both PySpark and mock-spark)
        assert hasattr(result, "__and__") and hasattr(result, "__invert__")

    def test_and_all_rules_single_expression(self, spark_session) -> None:
        """Test and_all_rules with single expression."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Test with single valid expression
        rules = {"col1": ["col1 > 0"]}
        result = and_all_rules(rules, F)
        # Check for Column-like object (works with both PySpark and mock-spark)
        assert hasattr(result, "__and__") and hasattr(result, "__invert__")

    def test_and_all_rules_multiple_expressions(self, spark_session) -> None:
        """Test and_all_rules with multiple expressions."""
        # mock-spark 3.11.0+ requires active SparkSession for function calls (like PySpark)
        # Test with multiple expressions
        rules = {"col1": ["col1 > 0"], "col2": ["col2 IS NOT NULL"]}
        result = and_all_rules(rules, F)
        # Check for Column-like object (works with both PySpark and mock-spark)
        assert hasattr(result, "__and__") and hasattr(result, "__invert__")

    def test_string_rule_conversion_edge_cases(self, spark_session) -> None:
        """Test string rule conversion edge cases."""
        # mock-spark 3.12.0+ requires active SparkSession for function calls (like PySpark)
        # Test various string rule formats
        import os

        spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

        test_cases = [
            "col1 > 0",
            "col1 IS NOT NULL",
            "LENGTH(col1) > 5",
        ]

        # IN clause syntax may not be supported by mock-spark parser
        # Only test it with real PySpark
        if spark_mode == "real":
            test_cases.append("col1 IN ('a', 'b', 'c')")

        for rule in test_cases:
            result = _convert_rule_to_expression(rule, "col1", F)
            # Check for Column-like object (works with both PySpark and mock-spark)
            assert hasattr(result, "__and__") and hasattr(result, "__invert__")

    def test_validation_error_handling(self) -> None:
        """Test validation error handling paths."""
        # Test with invalid DataFrame
        with pytest.raises((AttributeError, TypeError)):
            apply_column_rules(None, {"col1": ["not_null"]})  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            apply_column_rules("invalid", {"col1": ["not_null"]})  # type: ignore
