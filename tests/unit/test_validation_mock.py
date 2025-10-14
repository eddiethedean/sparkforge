# test_validation_mock.py
"""
Unit tests for the validation module using mock_spark.

This module tests all data validation and quality assessment functions.
"""


# NOTE: mock-spark patches removed - now using mock-spark 1.3.0 which doesn't need patches
# The apply_mock_spark_patches() call was causing test pollution

import pytest
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


@pytest.fixture
def mock_spark_session():
    """Create mock Spark session for testing."""
    return MockSparkSession("TestApp")


@pytest.fixture
def mock_functions():
    """Create mock functions for testing."""
    return MockFunctions()


@pytest.fixture
def sample_dataframe(mock_spark_session):
    """Create sample DataFrame for testing."""
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
        {"user_id": "user3", "age": 20, "score": 78.5},
        {"user_id": "user4", "age": 35, "score": 50.0},
    ]
    return mock_spark_session.createDataFrame(data, schema)


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

    def test_division_by_none(self):
        """Test division by None returns 0."""
        result = safe_divide(10.0, None)
        assert result == 0.0

    def test_none_numerator(self):
        """Test None numerator returns 0."""
        result = safe_divide(None, 2.0)
        assert result == 0.0

    def test_both_none(self):
        """Test both None returns 0."""
        result = safe_divide(None, None)
        assert result == 0.0


class TestGetDataframeInfo:
    """Test get_dataframe_info function."""

    def test_basic_info(self, sample_dataframe):
        """Test basic DataFrame info."""
        info = get_dataframe_info(sample_dataframe)
        assert info["row_count"] == 4
        assert not info["is_empty"]
        assert "columns" in info
        assert len(info["columns"]) == 3

    def test_empty_dataframe(self, mock_spark_session):
        """Test empty DataFrame info."""
        schema = MockStructType([MockStructField("col1", StringType(), True)])
        empty_df = mock_spark_session.createDataFrame([], schema)
        info = get_dataframe_info(empty_df)
        assert info["row_count"] == 0
        assert info["is_empty"]

    def test_error_handling(self):
        """Test error handling with invalid input."""
        result = get_dataframe_info(None)
        assert result is not None
        assert "error" in result
        assert result["row_count"] == 0


class TestConvertRuleToExpression:
    """Test _convert_rule_to_expression function."""

    def test_not_null_rule(self, mock_functions):
        """Test not_null rule conversion."""
        expr = _convert_rule_to_expression("not_null", "user_id", mock_functions)
        assert expr is not None
        assert hasattr(expr, "isNotNull") or hasattr(expr, "operation")

    def test_positive_rule(self, mock_functions):
        """Test positive rule conversion."""
        expr = _convert_rule_to_expression("positive", "age", mock_functions)
        assert expr is not None

    def test_non_negative_rule(self, mock_functions):
        """Test non_negative rule conversion."""
        expr = _convert_rule_to_expression("non_negative", "score", mock_functions)
        assert expr is not None

    def test_non_zero_rule(self, mock_functions):
        """Test non_zero rule conversion."""
        expr = _convert_rule_to_expression("non_zero", "age", mock_functions)
        assert expr is not None

    def test_custom_expression(self, mock_functions):
        """Test custom expression rule."""
        expr = _convert_rule_to_expression(
            "col('user_id').isNotNull()", "user_id", mock_functions
        )
        assert expr is not None


class TestConvertRulesToExpressions:
    """Test _convert_rules_to_expressions function."""

    def test_single_rule(self, mock_functions):
        """Test single rule conversion."""
        rules = {"user_id": ["not_null"]}
        expressions = _convert_rules_to_expressions(rules, mock_functions)
        assert len(expressions) == 1
        assert "user_id" in expressions

    def test_multiple_rules(self, mock_functions):
        """Test multiple rules conversion."""
        rules = {"user_id": ["not_null"], "age": ["positive", "non_zero"]}
        expressions = _convert_rules_to_expressions(rules, mock_functions)
        assert len(expressions) == 2
        assert "user_id" in expressions
        assert "age" in expressions

    def test_empty_rules(self, mock_functions):
        """Test empty rules."""
        expressions = _convert_rules_to_expressions({}, mock_functions)
        assert len(expressions) == 0


class TestAndAllRules:
    """Test and_all_rules function."""

    def test_empty_rules(self, mock_functions):
        """Test empty rules."""
        result = and_all_rules({}, mock_functions)
        assert result is not None

    def test_single_rule(self, mock_functions):
        """Test single rule."""
        rules = {"user_id": ["not_null"]}
        result = and_all_rules(rules, mock_functions)
        assert result is not None

    def test_multiple_rules(self, mock_functions):
        """Test multiple rules."""
        rules = {"user_id": ["not_null"], "age": ["positive"]}
        result = and_all_rules(rules, mock_functions)
        assert result is not None


class TestApplyColumnRules:
    """Test apply_column_rules function."""

    def test_basic_validation(self, sample_dataframe, mock_functions):
        """Test basic column validation."""
        rules = {"user_id": ["not_null"]}
        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, rules, "bronze", "test", functions=mock_functions
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate >= 0

    def test_multiple_columns(self, sample_dataframe, mock_functions):
        """Test multiple column validation."""
        rules = {"user_id": ["not_null"], "age": ["positive"]}
        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, rules, "bronze", "test", functions=mock_functions
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate >= 0

    def test_empty_rules(self, sample_dataframe, mock_functions):
        """Test empty rules."""
        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, {}, "bronze", "test", mock_functions
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None


class TestAssessDataQuality:
    """Test assess_data_quality function."""

    def test_basic_quality_assessment(self, sample_dataframe, mock_functions):
        """Test basic data quality assessment."""
        rules = {"user_id": ["not_null"]}
        result = assess_data_quality(sample_dataframe, rules, mock_functions)
        assert result is not None
        assert "quality_rate" in result
        assert "total_rows" in result
        assert "invalid_rows" in result

    def test_multiple_quality_rules(self, sample_dataframe, mock_functions):
        """Test multiple quality rules."""
        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = assess_data_quality(sample_dataframe, rules, mock_functions)
        assert result is not None
        assert "quality_rate" in result

    def test_empty_rules(self, sample_dataframe, mock_functions):
        """Test empty quality rules."""
        result = assess_data_quality(sample_dataframe, {}, mock_functions)
        assert result is not None


class TestValidateDataframeSchema:
    """Test validate_dataframe_schema function."""

    def test_valid_schema(self, sample_dataframe):
        """Test valid schema validation."""
        expected_columns = ["user_id", "age", "score"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is True

    def test_missing_columns(self, sample_dataframe):
        """Test missing columns validation."""
        expected_columns = ["user_id", "age", "score", "missing_col"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is False

    def test_extra_columns(self, sample_dataframe):
        """Test extra columns validation."""
        expected_columns = ["user_id", "age"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is True  # Function returns True if expected columns are present

    def test_empty_expected_columns(self, sample_dataframe):
        """Test empty expected columns."""
        result = validate_dataframe_schema(sample_dataframe, [])
        assert result is True

    def test_none_dataframe(self):
        """Test None DataFrame."""
        with pytest.raises((ValueError, TypeError, AttributeError)):
            validate_dataframe_schema(None, ["col1"])

    def test_none_expected_columns(self, sample_dataframe):
        """Test None expected columns."""
        with pytest.raises((ValueError, TypeError)):
            validate_dataframe_schema(sample_dataframe, None)
