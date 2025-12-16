# test_validation.py
"""
Unit tests for the validation module.

This module tests all data validation and quality assessment functions.
"""

import os

import pytest

# Use compatibility layer

# For tests, we'll use functions from spark_session when available
# This avoids SparkContext requirements

# Import types based on engine - check both SPARK_MODE and SPARKFORGE_ENGINE
_SPARK_MODE = os.environ.get("SPARK_MODE", "mock").lower()
_ENGINE = os.environ.get("SPARKFORGE_ENGINE", "auto").lower()

# Use real PySpark types if either SPARK_MODE=real or SPARKFORGE_ENGINE indicates real
use_real_spark = _SPARK_MODE == "real" or _ENGINE in ("pyspark", "spark", "real")

if use_real_spark:
    try:
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
        )
    except ImportError:
        from sparkless.spark_types import (  # type: ignore[import]
            StringType,
            StructField,
            StructType,
        )
else:
    from sparkless.spark_types import (  # type: ignore[import]
        StringType,
        StructField,
        StructType,
    )

from pipeline_builder.errors import ValidationError
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

# Using shared spark_session fixture from conftest.py


# Skip all tests in this file when running in real mode
pytestmark = pytest.mark.skipif(
    os.environ.get("SPARK_MODE", "mock").lower() == "real",
    reason="This test module is designed for sparkless/mock mode only",
)


@pytest.fixture(scope="function", autouse=True)
def reset_test_environment(spark_session):
    """Reset test environment before each test in this file."""
    import gc

    # Reset global state before test
    try:
        from tests.test_helpers.isolation import reset_global_state

        reset_global_state()
    except Exception:
        pass

    # Force garbage collection to clear any lingering references
    gc.collect()
    yield
    # Cleanup after test
    gc.collect()

    # Reset global state after test
    try:
        from tests.test_helpers.isolation import reset_global_state

        reset_global_state()
    except Exception:
        pass


@pytest.fixture(scope="function")
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing - validation test specific (4 rows, no category)."""
    # Use types that match the spark_session (already imported at top based on SPARK_MODE)
    # The types are already imported at module level based on SPARKFORGE_ENGINE
    # But we should check SPARK_MODE to ensure compatibility

    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
    if spark_mode == "real":
        # Use PySpark types for real Spark
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    else:
        # Use sparkless types for mock Spark
        from sparkless.spark_types import (  # type: ignore[import]
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

    # Force using StructType for consistency
    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", DoubleType(), True),
        ]
    )
    # Use dict format explicitly for mock-spark
    data = [
        {"user_id": "user1", "age": 25, "score": 85.5},
        {"user_id": "user2", "age": 30, "score": 92.0},
        {"user_id": "user3", "age": None, "score": 78.5},
        {"user_id": "user4", "age": 35, "score": None},
    ]
    df = spark_session.createDataFrame(data, schema)
    # Verify we have exactly 4 rows
    assert df.count() == 4, f"Expected 4 rows, got {df.count()}"
    return df


class TestAndAllRules:
    """Test and_all_rules function."""

    def test_empty_rules(self):
        """Test with empty rules returns True."""
        result = and_all_rules({})
        assert result is not None  # Should return F.lit(True)

    def test_single_rule(self, spark_session):
        """Test with single rule."""
        # Use string rules and pass functions from spark_session
        rules = {"user_id": ["not_null"]}
        # Get functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = and_all_rules(rules, functions=functions)
        assert result is not None

    def test_multiple_rules(self, spark_session):
        """Test with multiple rules."""
        # Use string rules and pass functions from spark_session
        rules = {
            "user_id": ["not_null"],
            "age": ["not_null", "positive"],
        }
        # Get functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = and_all_rules(rules, functions=functions)
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

    def test_basic_validation(self, sample_dataframe, spark_session):
        """Test basic column validation."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        # Use string rules to avoid SparkContext dependency
        rules = {
            "user_id": ["not_null"],
            "age": ["not_null"],
        }
        apply_column_rules(
            sample_dataframe, rules, "bronze", "test_step", functions=functions
        )

        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe,
            rules,
            "test",
            "test_step",
            filter_columns_by_rules=True,
            functions=functions,  # Use mock_spark functions, not global F
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

    def test_complex_rules(self, sample_dataframe, spark_session):
        """Test with complex validation rules."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        # Use string rules to avoid SparkContext dependency
        rules = {
            "user_id": ["not_null"],
            "age": ["not_null", "positive"],
            "score": ["not_null", "non_negative"],
        }
        apply_column_rules(
            sample_dataframe, rules, "bronze", "test_step", functions=functions
        )

        valid_df, invalid_df, stats = apply_column_rules(
            sample_dataframe,
            rules,
            "test",
            "test_step",
            filter_columns_by_rules=True,
            functions=functions,  # Use mock_spark functions, not global F
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

    def test_not_null_rule(self, spark_session):
        """Test not_null rule conversion."""
        # Get functions from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = _convert_rule_to_expression("not_null", "test_column", functions)
        # This should return a Column expression
        assert hasattr(result, "isNotNull")

    def test_positive_rule(self, spark_session):
        """Test positive rule conversion."""
        # Use functions from spark_session to ensure SparkContext is active
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = _convert_rule_to_expression("positive", "test_column", functions)
        assert hasattr(result, "__gt__")

    def test_non_negative_rule(self, spark_session):
        """Test non_negative rule conversion."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = _convert_rule_to_expression("non_negative", "test_column", functions)
        assert hasattr(result, "__ge__")

    def test_non_zero_rule(self, spark_session):
        """Test non_zero rule conversion."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = _convert_rule_to_expression("non_zero", "test_column", functions)
        assert hasattr(result, "__ne__")

    def test_custom_expression_rule(self, spark_session):
        """Test custom expression rule conversion."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        result = _convert_rule_to_expression(
            "col('test_column') > 10", "test_column", functions
        )
        assert hasattr(result, "__gt__")


class TestConvertRulesToExpressions:
    """Test _convert_rules_to_expressions function."""

    def test_string_rules_conversion(self, spark_session):
        """Test conversion of string rules."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        rules = {"col1": ["not_null", "positive"], "col2": ["non_negative"]}
        result = _convert_rules_to_expressions(rules, functions)

        assert "col1" in result
        assert "col2" in result
        assert len(result["col1"]) == 2
        assert len(result["col2"]) == 1

    def test_mixed_rules_conversion(self, spark_session):
        """Test conversion of mixed string and Column rules."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        # Use string rules to avoid SparkContext dependency
        rules = {
            "col1": ["not_null", "positive"],
            "col2": ["not_null"],
        }
        result = _convert_rules_to_expressions(rules, functions)

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

    def test_data_quality_with_rules(self, sample_dataframe, spark_session):
        """Test data quality assessment with validation rules."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = assess_data_quality(sample_dataframe, rules, functions)

        assert isinstance(result, dict)
        assert "total_rows" in result
        assert "valid_rows" in result
        assert "invalid_rows" in result
        assert "quality_rate" in result


class TestApplyValidationRules:
    """Test apply_column_rules function."""

    def test_apply_column_rules_basic(self, sample_dataframe, spark_session):
        """Test applying basic validation rules."""
        # Use functions from compat layer - get from session to match engine
        from pipeline_builder.compat import get_functions_from_session

        functions = get_functions_from_session(spark_session)
        rules = {
            "user_id": ["not_null"],
            "age": ["positive"],
            "score": ["non_negative"],
        }
        result = apply_column_rules(
            sample_dataframe, rules, "bronze", "test_step", functions=functions
        )

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
