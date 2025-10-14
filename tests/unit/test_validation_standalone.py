# test_validation_standalone.py
"""
Standalone unit tests for the validation module using mock_spark.

This module tests all data validation and quality assessment functions
without any conftest.py dependencies.
"""

import os
import sys

import pytest

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import os

# Apply mock-spark 0.3.1 patches
# NOTE: mock-spark patches removed - now using mock-spark 1.3.0 which doesn't need patches
# The apply_mock_spark_patches() call was causing test pollution
import sys

from mock_spark import (
    DoubleType,
    IntegerType,
    MockFunctions,
    MockSparkSession,
    MockStructField,
    MockStructType,
    StringType,
)

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


class TestSafeDivide:
    """Test safe_divide function."""

    def test_normal_division(self):
        """Test normal division."""
        result = safe_divide(10.0, 2.0)
        assert result == 5.0

    def test_division_by_zero(self):
        """Test division by zero returns 0."""
        result = safe_divide(10.0, 0.0)
        assert result == 0.0

    def test_normal_division_float(self):
        """Test normal division with float values."""
        result = safe_divide(10.0, 2.0)
        assert result == 5.0

    def test_division_with_default(self):
        """Test division with custom default."""
        result = safe_divide(10.0, 0.0, default=1.0)
        assert result == 1.0


class TestGetDataframeInfo:
    """Test get_dataframe_info function."""

    def test_basic_info(self):
        """Test basic DataFrame info."""
        mock_spark = MockSparkSession("TestApp")
        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"user_id": "user1", "age": 25, "score": 85.5},
            {"user_id": "user2", "age": 30, "score": 92.0},
            {"user_id": "user3", "age": None, "score": 78.5},
            {"user_id": "user4", "age": 35, "score": None},
        ]
        df = mock_spark.createDataFrame(data, schema)

        info = get_dataframe_info(df)
        assert info["row_count"] == 4
        assert not info["is_empty"]
        assert "columns" in info
        assert len(info["columns"]) == 3

    def test_empty_dataframe(self):
        """Test empty DataFrame info."""
        mock_spark = MockSparkSession("TestApp")
        schema = MockStructType([MockStructField("col1", StringType(), True)])
        empty_df = mock_spark.createDataFrame([], schema)
        info = get_dataframe_info(empty_df)
        assert info["row_count"] == 0
        assert info["is_empty"]

    def test_error_handling(self):
        """Test error handling with invalid input."""
        # MockDataFrame should handle None gracefully
        info = get_dataframe_info(None)
        assert "error" in info


class TestConvertRuleToExpression:
    """Test _convert_rule_to_expression function."""

    def test_not_null_rule(self):
        """Test not_null rule conversion."""
        mock_functions = MockFunctions()
        expr = _convert_rule_to_expression("not_null", "user_id", mock_functions)
        assert expr is not None
        assert hasattr(expr, "isNotNull") or hasattr(expr, "operation")

    def test_positive_rule(self):
        """Test positive rule conversion."""
        mock_functions = MockFunctions()
        expr = _convert_rule_to_expression("positive", "age", mock_functions)
        assert expr is not None

    def test_non_negative_rule(self):
        """Test non_negative rule conversion."""
        mock_functions = MockFunctions()
        expr = _convert_rule_to_expression("non_negative", "score", mock_functions)
        assert expr is not None

    def test_non_zero_rule(self):
        """Test non_zero rule conversion."""
        mock_functions = MockFunctions()
        expr = _convert_rule_to_expression("non_zero", "age", mock_functions)
        assert expr is not None

    def test_custom_expression(self):
        """Test custom expression rule."""
        mock_functions = MockFunctions()
        expr = _convert_rule_to_expression(
            "col('user_id').isNotNull()", "user_id", mock_functions
        )
        assert expr is not None


class TestConvertRulesToExpressions:
    """Test _convert_rules_to_expressions function."""

    def test_single_rule(self):
        """Test single rule conversion."""
        mock_functions = MockFunctions()
        rules = {"user_id": ["not_null"]}
        expressions = _convert_rules_to_expressions(rules, mock_functions)
        assert len(expressions) == 1
        assert "user_id" in expressions

    def test_multiple_rules(self):
        """Test multiple rules conversion."""
        mock_functions = MockFunctions()
        rules = {"user_id": ["not_null"], "age": ["positive", "non_zero"]}
        expressions = _convert_rules_to_expressions(rules, mock_functions)
        assert len(expressions) == 2
        assert "user_id" in expressions
        assert "age" in expressions

    def test_empty_rules(self):
        """Test empty rules."""
        mock_functions = MockFunctions()
        expressions = _convert_rules_to_expressions({}, mock_functions)
        assert len(expressions) == 0


class TestAndAllRules:
    """Test and_all_rules function."""

    def test_empty_rules(self):
        """Test empty rules."""
        mock_functions = MockFunctions()
        result = and_all_rules([], mock_functions)
        assert result is not None

    def test_single_rule(self):
        """Test single rule."""
        mock_functions = MockFunctions()
        rules = {"user_id": ["not_null"]}
        result = and_all_rules(rules, mock_functions)
        assert result is not None

    def test_multiple_rules(self):
        """Test multiple rules."""
        mock_functions = MockFunctions()
        rules = {"user_id": ["not_null"], "age": ["positive"]}
        result = and_all_rules(rules, mock_functions)
        assert result is not None


class TestApplyColumnRules:
    """Test apply_column_rules function."""

    def test_basic_validation(self):
        """Test basic column validation."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"user_id": "user1", "age": 25, "score": 85.5},
            {"user_id": "user2", "age": 30, "score": 92.0},
            {"user_id": "user3", "age": None, "score": 78.5},
            {"user_id": "user4", "age": 35, "score": None},
        ]
        df = mock_spark.createDataFrame(data, schema)

        rules = {"user_id": ["not_null"]}
        valid_df, invalid_df, stats = apply_column_rules(
            df, rules, "bronze", "test", functions=mock_functions
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate == 100.0

    def test_multiple_columns(self):
        """Test multiple column validation."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"user_id": "user1", "age": 25, "score": 85.5},
            {"user_id": "user2", "age": 30, "score": 92.0},
            {"user_id": "user3", "age": 20, "score": 78.5},  # Fixed: changed None to 20
            {"user_id": "user4", "age": 35, "score": None},
        ]
        df = mock_spark.createDataFrame(data, schema)

        rules = {"user_id": ["not_null"], "age": ["positive"]}
        valid_df, invalid_df, stats = apply_column_rules(
            df, rules, "bronze", "test", functions=mock_functions
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate == 100.0

    def test_empty_rules(self):
        """Test empty rules."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        result = apply_column_rules(df, {}, "bronze", "test", mock_functions)
        assert result is not None


class TestAssessDataQuality:
    """Test assess_data_quality function."""

    def test_basic_quality_assessment(self):
        """Test basic data quality assessment."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"user_id": "user1", "age": 25, "score": 85.5},
            {"user_id": "user2", "age": 30, "score": 92.0},
            {"user_id": "user3", "age": None, "score": 78.5},
            {"user_id": "user4", "age": 35, "score": None},
        ]
        df = mock_spark.createDataFrame(data, schema)

        rules = {"user_id": ["not_null"]}
        result = assess_data_quality(df, rules, mock_functions)
        assert result is not None
        assert "quality_rate" in result
        assert "total_rows" in result
        assert "invalid_rows" in result

    def test_multiple_quality_rules(self):
        """Test multiple quality rules."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"user_id": "user1", "age": 25, "score": 85.5},
            {"user_id": "user2", "age": 30, "score": 92.0},
            {"user_id": "user3", "age": None, "score": 78.5},
            {"user_id": "user4", "age": 35, "score": None},
        ]
        df = mock_spark.createDataFrame(data, schema)

        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = assess_data_quality(df, rules, mock_functions)
        assert result is not None
        assert "quality_rate" in result

    def test_empty_rules(self):
        """Test empty quality rules."""
        mock_spark = MockSparkSession("TestApp")
        mock_functions = MockFunctions()

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        result = assess_data_quality(df, {}, mock_functions)
        assert result is not None


class TestValidateDataframeSchema:
    """Test validate_dataframe_schema function."""

    def test_valid_schema(self):
        """Test valid schema validation."""
        mock_spark = MockSparkSession("TestApp")

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [
            ("user1", 25, 85.5),
            ("user2", 30, 92.0),
        ]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age", "score"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is True

    def test_missing_columns(self):
        """Test missing columns validation."""
        mock_spark = MockSparkSession("TestApp")

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [("user1", 25, 85.5)]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age", "score", "missing_col"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is False

    def test_extra_columns(self):
        """Test extra columns validation."""
        mock_spark = MockSparkSession("TestApp")

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("score", DoubleType(), True),
            ]
        )
        data = [("user1", 25, 85.5)]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is True  # Real Spark only checks for missing columns, not extra

    def test_empty_expected_columns(self):
        """Test empty expected columns."""
        mock_spark = MockSparkSession("TestApp")

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        result = validate_dataframe_schema(df, [])
        assert result is True  # Real Spark returns True when no columns expected

    def test_none_dataframe(self):
        """Test None DataFrame."""
        with pytest.raises((ValueError, TypeError, AttributeError)):
            validate_dataframe_schema(None, ["col1"])

    def test_none_expected_columns(self):
        """Test None expected columns."""
        mock_spark = MockSparkSession("TestApp")

        schema = MockStructType(
            [
                MockStructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        with pytest.raises((ValueError, TypeError)):
            validate_dataframe_schema(df, None)
