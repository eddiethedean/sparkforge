#!/usr/bin/env python3
"""
Comprehensive tests for validation module functionality.

This module tests all validation functions and classes with extensive coverage.
"""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from sparkforge.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from sparkforge.validation import (
    UnifiedValidator,
    ValidationResult,
    _convert_rule_to_expression,
    _convert_rules_to_expressions,
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    get_dataframe_info,
    safe_divide,
    validate_dataframe_schema,
)


class TestConvertRuleToExpression:
    """Test cases for _convert_rule_to_expression function."""

    def test_not_null_rule(self):
        """Test not_null rule conversion."""
        result = _convert_rule_to_expression("not_null", "test_column")
        # This should return a PySpark Column expression
        assert result is not None

    def test_positive_rule(self):
        """Test positive rule conversion."""
        result = _convert_rule_to_expression("positive", "test_column")
        assert result is not None

    def test_non_negative_rule(self):
        """Test non_negative rule conversion."""
        result = _convert_rule_to_expression("non_negative", "test_column")
        assert result is not None

    def test_non_zero_rule(self):
        """Test non_zero rule conversion."""
        result = _convert_rule_to_expression("non_zero", "test_column")
        assert result is not None

    def test_unknown_rule(self):
        """Test unknown rule converts to F.expr."""
        result = _convert_rule_to_expression("custom_rule", "test_column")
        # Should return a Column object created by F.expr
        assert hasattr(result, "isNull")  # Column objects have isNull method


class TestConvertRulesToExpressions:
    """Test cases for _convert_rules_to_expressions function."""

    def test_string_rules_conversion(self):
        """Test conversion of string rules to expressions."""
        rules = {
            "id": ["not_null", "positive"],
            "name": ["not_null"],
            "age": ["non_negative"],
        }

        result = _convert_rules_to_expressions(rules)

        assert "id" in result
        assert "name" in result
        assert "age" in result
        assert len(result["id"]) == 2
        assert len(result["name"]) == 1
        assert len(result["age"]) == 1

    def test_mixed_rules_conversion(self):
        """Test conversion of mixed string and expression rules."""
        rules = {"id": ["not_null", F.col("id") > 0], "name": ["not_null"]}

        result = _convert_rules_to_expressions(rules)

        assert "id" in result
        assert "name" in result
        assert len(result["id"]) == 2
        assert len(result["name"]) == 1

    def test_empty_rules(self):
        """Test conversion of empty rules."""
        result = _convert_rules_to_expressions({})
        assert result == {}


class TestAndAllRules:
    """Test cases for and_all_rules function."""

    def test_empty_rules(self):
        """Test and_all_rules with empty rules."""
        result = and_all_rules({})
        assert result is True

    def test_single_column_single_rule(self):
        """Test and_all_rules with single column and single rule."""
        rules = {"id": [F.col("id").isNotNull()]}
        result = and_all_rules(rules)
        assert result is not None

    def test_single_column_multiple_rules(self):
        """Test and_all_rules with single column and multiple rules."""
        rules = {"id": [F.col("id").isNotNull(), F.col("id") > 0]}
        result = and_all_rules(rules)
        assert result is not None

    def test_multiple_columns(self):
        """Test and_all_rules with multiple columns."""
        rules = {"id": [F.col("id").isNotNull()], "name": [F.col("name").isNotNull()]}
        result = and_all_rules(rules)
        assert result is not None

    def test_complex_rules(self):
        """Test and_all_rules with complex rule combinations."""
        rules = {
            "id": [F.col("id").isNotNull(), F.col("id") > 0],
            "name": [F.col("name").isNotNull()],
            "age": [F.col("age") >= 0, F.col("age") <= 120],
        }
        result = and_all_rules(rules)
        assert result is not None

    def test_empty_rules_returns_true(self):
        """Test that empty rules return True (all data valid)."""
        rules = {}
        result = and_all_rules(rules)
        assert result is True

    def test_no_valid_expressions_returns_true(self):
        """Test that when no valid expressions are generated, returns True."""
        # This tests the case where _convert_rules_to_expressions returns empty dict
        with patch(
            "sparkforge.validation.data_validation._convert_rules_to_expressions"
        ) as mock_convert:
            mock_convert.return_value = {}  # No expressions generated
            rules = {
                "test": ["invalid_rule"]
            }  # Invalid rule that generates no expressions
            result = and_all_rules(rules)
            assert result is True


class TestValidateDataframeSchema:
    """Test cases for validate_dataframe_schema function."""

    def test_valid_schema(self, spark_session):
        """Test validation with valid schema."""
        df = spark_session.createDataFrame(
            [("1", "Alice", 25), ("2", "Bob", 30)], ["id", "name", "age"]
        )

        result = validate_dataframe_schema(df, ["id", "name", "age"])
        assert result is True

    def test_missing_columns(self, spark_session):
        """Test validation with missing columns."""
        df = spark_session.createDataFrame(
            [("1", "Alice"), ("2", "Bob")], ["id", "name"]
        )

        result = validate_dataframe_schema(df, ["id", "name", "age"])
        assert result is False

    def test_extra_columns(self, spark_session):
        """Test validation with extra columns (should pass as function only checks missing columns)."""
        df = spark_session.createDataFrame(
            [("1", "Alice", 25, "extra"), ("2", "Bob", 30, "extra")],
            ["id", "name", "age", "extra_col"],
        )

        result = validate_dataframe_schema(df, ["id", "name", "age"])
        assert (
            result is True
        )  # Function only checks for missing columns, not extra ones

    def test_empty_expected_columns(self, spark_session):
        """Test validation with empty expected columns."""
        df = spark_session.createDataFrame(
            [("1", "Alice"), ("2", "Bob")], ["id", "name"]
        )

        result = validate_dataframe_schema(df, [])
        assert result is True

    def test_empty_dataframe(self, spark_session):
        """Test validation with empty DataFrame."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        result = validate_dataframe_schema(df, ["id", "name"])
        assert result is True


class TestSafeDivide:
    """Test cases for safe_divide function."""

    def test_normal_division(self):
        """Test normal division."""
        result = safe_divide(10, 2)
        assert result == 5.0

    def test_division_by_zero(self):
        """Test division by zero with default."""
        result = safe_divide(10, 0)
        assert result == 0.0

    def test_division_by_zero_custom_default(self):
        """Test division by zero with custom default."""
        result = safe_divide(10, 0, default=1.0)
        assert result == 1.0

    def test_float_division(self):
        """Test float division."""
        result = safe_divide(10.5, 2.5)
        assert result == 4.2

    def test_negative_numbers(self):
        """Test division with negative numbers."""
        result = safe_divide(-10, 2)
        assert result == -5.0

    def test_zero_numerator(self):
        """Test division with zero numerator."""
        result = safe_divide(0, 5)
        assert result == 0.0


class TestGetDataframeInfo:
    """Test cases for get_dataframe_info function."""

    def test_basic_info(self, spark_session):
        """Test basic DataFrame info extraction."""
        df = spark_session.createDataFrame(
            [("1", "Alice", 25), ("2", "Bob", 30)], ["id", "name", "age"]
        )

        info = get_dataframe_info(df)

        assert "row_count" in info
        assert "column_count" in info
        assert "columns" in info
        assert info["row_count"] == 2
        assert info["column_count"] == 3
        assert "id" in info["columns"]
        assert "name" in info["columns"]
        assert "age" in info["columns"]

    def test_empty_dataframe(self, spark_session):
        """Test info extraction from empty DataFrame."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        info = get_dataframe_info(df)

        assert info["row_count"] == 0
        assert info["column_count"] == 2

    def test_error_handling(self, spark_session):
        """Test error handling in get_dataframe_info."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark_session.createDataFrame([], schema)

        # Mock count to raise an exception
        with patch.object(df, "count", side_effect=Exception("Count failed")):
            info = get_dataframe_info(df)

            assert "error" in info
            assert info["error"] == "Count failed"


class TestAssessDataQuality:
    """Test cases for assess_data_quality function."""

    def test_empty_dataframe(self, spark_session):
        """Test quality assessment of empty DataFrame."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        result = assess_data_quality(df)

        assert result["total_rows"] == 0
        assert result["valid_rows"] == 0
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is True

    def test_dataframe_without_rules(self, spark_session):
        """Test quality assessment without validation rules."""
        df = spark_session.createDataFrame(
            [("1", "Alice"), ("2", "Bob")], ["id", "name"]
        )

        result = assess_data_quality(df)

        assert result["total_rows"] == 2
        assert result["valid_rows"] == 2
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False

    def test_dataframe_with_rules(self, spark_session):
        """Test quality assessment with validation rules."""
        df = spark_session.createDataFrame(
            [("1", "Alice"), ("2", "Bob")], ["id", "name"]
        )

        rules = {"id": [F.col("id").isNotNull()], "name": [F.col("name").isNotNull()]}

        result = assess_data_quality(df, rules)

        assert "total_rows" in result
        assert "valid_rows" in result
        assert "invalid_rows" in result
        assert "quality_rate" in result
        assert "is_empty" in result

    def test_error_handling(self, spark_session):
        """Test error handling in assess_data_quality."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark_session.createDataFrame([], schema)

        # Mock count to raise an exception
        with patch.object(df, "count", side_effect=Exception("Assessment failed")):
            result = assess_data_quality(df)

            assert "error" in result
            assert result["error"] == "Assessment failed"
            assert result["total_rows"] == 0
            assert result["valid_rows"] == 0
            assert result["invalid_rows"] == 0
            assert result["quality_rate"] == 0.0
            assert result["is_empty"] is True


class TestValidationResult:
    """Test cases for ValidationResult class."""

    def test_validation_result_creation(self):
        """Test ValidationResult creation."""
        result = ValidationResult(
            is_valid=True,
            errors=["error1"],
            warnings=["warning1"],
            recommendations=["recommendation1"],
        )

        assert result.is_valid is True
        assert result.errors == ["error1"]
        assert result.warnings == ["warning1"]
        assert result.recommendations == ["recommendation1"]

    def test_validation_result_defaults(self):
        """Test ValidationResult with default values."""
        result = ValidationResult(
            is_valid=False, errors=[], warnings=[], recommendations=[]
        )

        assert result.is_valid is False
        assert result.errors == []
        assert result.warnings == []
        assert result.recommendations == []


class TestUnifiedValidator:
    """Test cases for UnifiedValidator class."""

    def test_unified_validator_initialization(self):
        """Test UnifiedValidator initialization."""
        validator = UnifiedValidator()

        assert validator.custom_validators == []
        assert validator.logger is not None

    def test_unified_validator_with_custom_logger(self):
        """Test UnifiedValidator with custom logger."""
        mock_logger = Mock()
        validator = UnifiedValidator(mock_logger)

        assert validator.logger == mock_logger
        assert validator.custom_validators == []

    def test_add_validator(self):
        """Test adding custom validator."""
        validator = UnifiedValidator()
        mock_validator = Mock()

        validator.add_validator(mock_validator)

        assert len(validator.custom_validators) == 1
        assert validator.custom_validators[0] == mock_validator

    def test_validate_step_with_custom_validators(self):
        """Test step validation with custom validators."""
        validator = UnifiedValidator()
        mock_validator = Mock()
        mock_validator.validate.return_value = ["error1"]
        validator.add_validator(mock_validator)

        mock_step = Mock()
        mock_context = Mock()

        result = validator.validate_step(mock_step, "bronze", mock_context)

        assert result.is_valid is False
        assert "error1" in result.errors
        mock_validator.validate.assert_called_once_with(mock_step, mock_context)

    def test_validate_step_validator_exception(self):
        """Test step validation when validator raises exception."""
        validator = UnifiedValidator()
        mock_validator = Mock()
        mock_validator.validate.side_effect = Exception("Validator failed")
        validator.add_validator(mock_validator)

        mock_step = Mock()
        mock_context = Mock()

        result = validator.validate_step(mock_step, "bronze", mock_context)

        assert result.is_valid is False
        assert any(
            "Custom validator" in error and "failed" in error for error in result.errors
        )

    def test_validate_pipeline_config_validation(self):
        """Test pipeline validation with config errors."""
        validator = UnifiedValidator()

        # Create invalid config (missing schema)
        config = PipelineConfig(
            schema="",  # Empty schema should cause error
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
        )

        result = validator.validate_pipeline(config, {}, {}, {})

        assert result.is_valid is False
        assert any("Pipeline schema is required" in error for error in result.errors)

    def test_validate_pipeline_success(self):
        """Test successful pipeline validation."""
        validator = UnifiedValidator()

        # Create valid config
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
        )

        result = validator.validate_pipeline(config, {}, {}, {})

        assert result.is_valid is True
        assert result.errors == []

    def test_validate_bronze_steps(self):
        """Test bronze steps validation."""
        validator = UnifiedValidator()

        # Create valid bronze step for testing
        rules = {"id": [F.col("id").isNotNull()]}
        bronze_steps = {"test_bronze": BronzeStep(name="test_bronze", rules=rules)}

        errors, warnings = validator._validate_bronze_steps(bronze_steps)

        # Should not have any errors for valid bronze step
        assert len(errors) == 0
        assert len(warnings) == 0

    def test_validate_silver_steps(self):
        """Test silver steps validation."""
        validator = UnifiedValidator()

        # Create valid silver step for testing
        rules = {"id": [F.col("id").isNotNull()]}
        silver_steps = {
            "test_silver": SilverStep(
                name="test_silver",
                source_bronze="test_bronze",
                transform=lambda spark, df, silvers: df,
                rules=rules,
                table_name="test_table",
            )
        }

        errors, warnings = validator._validate_silver_steps(
            silver_steps, {"test_bronze": BronzeStep(name="test_bronze", rules=rules)}
        )

        # Should not have any errors for valid silver step
        assert len(errors) == 0
        assert len(warnings) == 0

    def test_validate_gold_steps(self):
        """Test gold steps validation."""
        validator = UnifiedValidator()

        # Create valid gold step for testing
        rules = {"id": [F.col("id").isNotNull()]}
        gold_steps = {
            "test_gold": GoldStep(
                name="test_gold",
                transform=lambda spark, silvers: silvers["test_silver"],
                rules=rules,
                table_name="test_gold",
                source_silvers=["test_silver"],
            )
        }

        errors, warnings = validator._validate_gold_steps(
            gold_steps,
            {
                "test_silver": SilverStep(
                    name="test_silver",
                    source_bronze="test_bronze",
                    transform=lambda spark, df, silvers: df,
                    rules=rules,
                    table_name="test_table",
                )
            },
        )

        # Should not have any errors for valid gold step
        assert len(errors) == 0
        assert len(warnings) == 0

    def test_validate_dependencies(self):
        """Test dependency validation."""
        validator = UnifiedValidator()

        # Create valid steps for testing
        rules = {"id": [F.col("id").isNotNull()]}
        bronze_steps = {"bronze1": BronzeStep(name="bronze1", rules=rules)}
        silver_steps = {}
        gold_steps = {}

        errors, warnings = validator._validate_dependencies(
            bronze_steps, silver_steps, gold_steps
        )

        # The current implementation only checks for circular dependencies
        # and the steps we created don't have dependencies attribute, so no errors expected
        assert len(errors) == 0


class TestApplyValidationRules:
    """Test cases for apply_column_rules function."""

    def test_apply_column_rules_deprecated(self):
        """Test that apply_column_rules is a backward compatibility alias."""
        # The function doesn't show deprecation warning, it's just an alias
        # Test that it calls apply_column_rules with the right arguments
        with pytest.raises(TypeError, match="missing 4 required positional arguments"):
            apply_column_rules()


# Fixtures
@pytest.fixture
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.appName("ValidationTests").getOrCreate()
    yield spark
    spark.stop()
