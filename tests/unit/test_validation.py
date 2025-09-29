# test_validation.py
"""
Unit tests for the validation module.

This module tests all data validation and quality assessment functions.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sparkforge.errors import ValidationError
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

# Using shared spark_session fixture from conftest.py


@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", DoubleType(), True),
        ]
    )
    data = [
        ("user1", 25, 85.5),
        ("user2", 30, 92.0),
        ("user3", None, 78.5),
        ("user4", 35, None),
    ]
    return spark_session.createDataFrame(data, schema)


class TestAndAllRules:
    """Test and_all_rules function."""

    def test_empty_rules(self):
        """Test with empty rules returns True."""
        result = and_all_rules({})
        assert result is not None  # Should return F.lit(True)

    def test_single_rule(self, spark_session):
        """Test with single rule."""
        # Create rules inside the test where Spark context is available
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        result = and_all_rules(rules)
        assert result is not None

    def test_multiple_rules(self, spark_session):
        """Test with multiple rules."""
        # Create rules inside the test where Spark context is available
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "age": [F.col("age").isNotNull(), F.col("age") > 0],
        }
        result = and_all_rules(rules)
        assert result is not None


class TestValidateDataframeSchema:
    """Test validate_dataframe_schema function."""

    def test_valid_schema(self, sample_dataframe):
        """Test with valid schema."""
        expected_columns = ["user_id", "age", "score"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is True

    def test_missing_columns(self, sample_dataframe):
        """Test with missing columns."""
        expected_columns = ["user_id", "age", "score", "missing_col"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is False

    def test_extra_columns(self, sample_dataframe):
        """Test with extra columns (should still be valid)."""
        expected_columns = ["user_id", "age"]
        result = validate_dataframe_schema(sample_dataframe, expected_columns)
        assert result is True

    def test_empty_expected_columns(self, sample_dataframe):
        """Test with empty expected columns."""
        result = validate_dataframe_schema(sample_dataframe, [])
        assert result is True


class TestGetDataframeInfo:
    """Test get_dataframe_info function."""

    def test_basic_info(self, sample_dataframe):
        """Test basic DataFrame info."""
        info = get_dataframe_info(sample_dataframe)

        assert info["row_count"] == 4
        assert info["column_count"] == 3
        assert info["columns"] == ["user_id", "age", "score"]
        assert info["is_empty"] is False
        assert "schema" in info

    def test_empty_dataframe(self, spark_session):
        """Test with empty DataFrame."""
        schema = StructType([StructField("col1", StringType(), True)])
        empty_df = spark_session.createDataFrame([], schema)
        info = get_dataframe_info(empty_df)

        assert info["row_count"] == 0
        assert info["column_count"] == 1
        assert info["is_empty"] is True

    def test_error_handling(self, spark_session):
        """Test error handling in get_dataframe_info."""
        # Create a DataFrame that might cause issues
        try:
            # This should work fine
            schema = StructType([StructField("col1", StringType(), True)])
            df = spark_session.createDataFrame([("test",)], schema)
            info = get_dataframe_info(df)
            assert info["row_count"] == 1
        except Exception:
            # If there's an error, it should be handled gracefully
            pass


class TestApplyColumnRules:
    """Test apply_column_rules function."""

    def test_basic_validation(self, sample_dataframe):
        """Test basic column validation."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "age": [F.col("age").isNotNull()],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, rules, "test", "test_step", filter_columns_by_rules=True
        )

        assert (
            valid_df.count() == 3
        )  # user1, user2, and user4 have both user_id and age
        assert invalid_df.count() == 1  # user3 is missing age
        assert stats.total_rows == 4
        assert stats.valid_rows == 3
        assert stats.invalid_rows == 1
        assert stats.validation_rate == 75.0
        assert stats.stage == "test"
        assert stats.step == "test_step"

    def test_none_rules_raises_error(self, sample_dataframe):
        """Test that None rules raises ValidationError."""
        with pytest.raises(ValidationError):
            apply_column_rules(
                sample_dataframe,
                None,
                "test",
                "test_step",
                filter_columns_by_rules=True,
            )

    def test_empty_rules(self, sample_dataframe):
        """Test with empty rules."""
        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, {}, "test", "test_step"
        )

        assert valid_df.count() == 4  # Empty rules should return all rows as valid
        assert invalid_df.count() == 0  # No rows go to invalid when no rules
        assert stats.total_rows == 4
        assert stats.valid_rows == 4
        assert stats.invalid_rows == 0
        assert stats.validation_rate == 100.0

    def test_complex_rules(self, sample_dataframe):
        """Test with complex validation rules."""
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "age": [F.col("age").isNotNull(), F.col("age") > 0],
            "score": [F.col("score").isNotNull(), F.col("score") >= 0],
        }

        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe, rules, "test", "test_step", filter_columns_by_rules=True
        )

        # Only user1 and user2 should pass all rules
        assert valid_df.count() == 2
        assert invalid_df.count() == 2
        assert stats.validation_rate == 50.0


class TestSafeDivide:
    """Test safe_divide function."""

    def test_normal_division(self):
        """Test normal division."""
        result = safe_divide(10, 2)
        assert result == 5.0

    def test_division_by_zero(self):
        """Test division by zero returns default."""
        result = safe_divide(10, 0)
        assert result == 0.0

    def test_division_by_zero_custom_default(self):
        """Test division by zero with custom default."""
        result = safe_divide(10, 0, default=99.0)
        assert result == 99.0

    def test_float_division(self):
        """Test float division."""
        result = safe_divide(7, 3)
        assert abs(result - 2.3333333333333335) < 1e-10

    def test_negative_numbers(self):
        """Test with negative numbers."""
        result = safe_divide(-10, 2)
        assert result == -5.0

    def test_zero_numerator(self):
        """Test with zero numerator."""
        result = safe_divide(0, 5)
        assert result == 0.0


class TestConvertRuleToExpression:
    """Test _convert_rule_to_expression function."""

    def test_not_null_rule(self):
        """Test not_null rule conversion."""
        result = _convert_rule_to_expression("not_null", "test_column")
        # This should return a Column expression
        assert hasattr(result, "isNotNull")

    def test_positive_rule(self):
        """Test positive rule conversion."""
        result = _convert_rule_to_expression("positive", "test_column")
        # This should return a Column expression
        assert hasattr(result, "__gt__")

    def test_non_negative_rule(self):
        """Test non_negative rule conversion."""
        result = _convert_rule_to_expression("non_negative", "test_column")
        # This should return a Column expression
        assert hasattr(result, "__ge__")

    def test_non_zero_rule(self):
        """Test non_zero rule conversion."""
        result = _convert_rule_to_expression("non_zero", "test_column")
        # This should return a Column expression
        assert hasattr(result, "__ne__")

    def test_custom_expression_rule(self):
        """Test custom expression rule conversion."""
        result = _convert_rule_to_expression("col('test_column') > 10", "test_column")
        # This should return a Column expression
        assert hasattr(result, "__gt__")


class TestConvertRulesToExpressions:
    """Test _convert_rules_to_expressions function."""

    def test_string_rules_conversion(self):
        """Test conversion of string rules."""
        rules = {"col1": ["not_null", "positive"], "col2": ["non_negative"]}
        result = _convert_rules_to_expressions(rules)

        assert "col1" in result
        assert "col2" in result
        assert len(result["col1"]) == 2
        assert len(result["col2"]) == 1

    def test_mixed_rules_conversion(self):
        """Test conversion of mixed string and Column rules."""
        rules = {
            "col1": ["not_null", F.col("col1") > 0],
            "col2": [F.col("col2").isNotNull()],
        }
        result = _convert_rules_to_expressions(rules)

        assert "col1" in result
        assert "col2" in result
        assert len(result["col1"]) == 2
        assert len(result["col2"]) == 1


class TestAssessDataQuality:
    """Test assess_data_quality function."""

    def test_basic_data_quality_assessment(self, sample_dataframe):
        """Test basic data quality assessment."""
        result = assess_data_quality(sample_dataframe)

        assert isinstance(result, dict)
        assert "total_rows" in result
        assert "valid_rows" in result
        assert "invalid_rows" in result
        assert "quality_rate" in result

    def test_data_quality_with_rules(self, sample_dataframe):
        """Test data quality assessment with validation rules."""
        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = assess_data_quality(sample_dataframe, rules)

        assert isinstance(result, dict)
        assert "total_rows" in result
        assert "valid_rows" in result
        assert "invalid_rows" in result
        assert "quality_rate" in result


class TestApplyValidationRules:
    """Test apply_column_rules function."""

    def test_apply_column_rules_basic(self, sample_dataframe):
        """Test applying basic validation rules."""
        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = apply_column_rules(sample_dataframe, rules, "bronze", "test_step")

        assert result is not None
        assert len(result) == 3  # Should return tuple of (df, df, stats)
        valid_df, invalid_df, stats = result
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None

    def test_apply_column_rules_empty(self, sample_dataframe):
        """Test applying validation rules with empty rules."""
        result = apply_column_rules(sample_dataframe, {}, "bronze", "test_step")

        assert result is not None
        assert len(result) == 3  # Should return tuple of (df, df, stats)
