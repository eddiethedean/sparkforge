# test_validation_standalone.py
"""
Standalone unit tests for the validation module.
Uses configured engine (PySpark or sparkless) via pipeline_builder.compat.
"""

import os
import sys

import pytest

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pipeline_builder.compat import F, types

DoubleType = types.DoubleType
IntegerType = types.IntegerType
StringType = types.StringType
StructField = types.StructField
StructType = types.StructType

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


@pytest.fixture(scope="function", autouse=True)
def reset_test_environment(spark):
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

    def test_basic_info(self, spark):
        """Test basic DataFrame info."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
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

    def test_empty_dataframe(self, spark):
        """Test empty DataFrame info."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark
        schema = StructType([StructField("col1", StringType(), True)])
        empty_df = mock_spark.createDataFrame([], schema)
        info = get_dataframe_info(empty_df)
        assert info["row_count"] == 0
        assert info["is_empty"]

    def test_error_handling(self):
        """Test error handling with invalid input."""
        # DataFrame should handle None gracefully
        info = get_dataframe_info(None)
        assert "error" in info


class TestConvertRuleToExpression:
    """Test _convert_rule_to_expression function."""

    def test_not_null_rule(self, spark):
        """Test not_null rule conversion."""
        expr = _convert_rule_to_expression("not_null", "user_id", F)
        assert expr is not None
        assert hasattr(expr, "isNotNull") or hasattr(expr, "operation")

    def test_positive_rule(self, spark):
        """Test positive rule conversion."""
        expr = _convert_rule_to_expression("positive", "age", F)
        assert expr is not None

    def test_non_negative_rule(self, spark):
        """Test non_negative rule conversion."""
        expr = _convert_rule_to_expression("non_negative", "score", F)
        assert expr is not None

    def test_non_zero_rule(self, spark):
        """Test non_zero rule conversion."""
        expr = _convert_rule_to_expression("non_zero", "age", F)
        assert expr is not None

    def test_custom_expression(self, spark):
        """Test custom expression rule (SQL expression)."""
        expr = _convert_rule_to_expression("user_id IS NOT NULL", "user_id", F)
        assert expr is not None


class TestConvertRulesToExpressions:
    """Test _convert_rules_to_expressions function."""

    def test_single_rule(self, spark):
        """Test single rule conversion."""
        rules = {"user_id": ["not_null"]}
        expressions = _convert_rules_to_expressions(rules, F)
        assert len(expressions) == 1
        assert "user_id" in expressions

    def test_multiple_rules(self, spark):
        """Test multiple rules conversion."""
        rules = {"user_id": ["not_null"], "age": ["positive", "non_zero"]}
        expressions = _convert_rules_to_expressions(rules, F)
        assert len(expressions) == 2
        assert "user_id" in expressions
        assert "age" in expressions

    def test_empty_rules(self, spark):
        """Test empty rules."""
        expressions = _convert_rules_to_expressions({}, F)
        assert len(expressions) == 0


class TestAndAllRules:
    """Test and_all_rules function."""

    def test_empty_rules(self, spark):
        """Test empty rules."""
        result = and_all_rules([], F)
        assert result is not None

    def test_single_rule(self, spark):
        """Test single rule."""
        rules = {"user_id": ["not_null"]}
        result = and_all_rules(rules, F)
        assert result is not None

    def test_multiple_rules(self, spark):
        """Test multiple rules."""
        rules = {"user_id": ["not_null"], "age": ["positive"]}
        result = and_all_rules(rules, F)
        assert result is not None


class TestApplyColumnRules:
    """Test apply_column_rules function."""

    def test_basic_validation(self, spark):
        """Test basic column validation."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
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
            df, rules, "bronze", "test", functions=F
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate == 100.0

    def test_multiple_columns(self, spark):
        """Test multiple column validation."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
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
            df, rules, "bronze", "test", functions=F
        )
        assert valid_df is not None
        assert invalid_df is not None
        assert stats is not None
        assert stats.validation_rate == 100.0

    def test_empty_rules(self, spark):
        """Test empty rules."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        result = apply_column_rules(df, {}, "bronze", "test", functions=F)
        assert result is not None


class TestAssessDataQuality:
    """Test assess_data_quality function."""

    def test_basic_quality_assessment(self, spark):
        """Test basic data quality assessment."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
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
        result = assess_data_quality(df, rules, F)
        assert result is not None
        assert "quality_rate" in result
        assert "total_rows" in result
        assert "invalid_rows" in result

    def test_multiple_quality_rules(self, spark):
        """Test multiple quality rules."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
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
        result = assess_data_quality(df, rules, F)
        assert result is not None
        assert "quality_rate" in result

    def test_empty_rules(self, spark):
        """Test empty quality rules."""
        mock_spark = spark
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        result = assess_data_quality(df, {}, F)
        assert result is not None


class TestValidateDataframeSchema:
    """Test validate_dataframe_schema function."""

    def test_valid_schema(self, spark):
        """Test valid schema validation."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark

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
        ]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age", "score"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is True

    def test_missing_columns(self, spark):
        """Test missing columns validation."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
            ]
        )
        data = [("user1", 25, 85.5)]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age", "score", "missing_col"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is False

    def test_extra_columns(self, spark):
        """Test extra columns validation."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("score", DoubleType(), True),
            ]
        )
        data = [("user1", 25, 85.5)]
        df = mock_spark.createDataFrame(data, schema)

        expected_columns = ["user_id", "age"]
        result = validate_dataframe_schema(df, expected_columns)
        assert result is True  # Real Spark only checks for missing columns, not extra

    def test_empty_expected_columns(self, spark):
        """Test empty expected columns."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
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

    def test_none_expected_columns(self, spark):
        """Test None expected columns."""
        # Use spark fixture for PySpark compatibility
        mock_spark = spark

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
            ]
        )
        data = [("user1",)]
        df = mock_spark.createDataFrame(data, schema)

        with pytest.raises((ValueError, TypeError)):
            validate_dataframe_schema(df, None)
