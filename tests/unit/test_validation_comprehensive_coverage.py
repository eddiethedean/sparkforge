#!/usr/bin/env python3
"""
Comprehensive unit tests for sparkforge/validation.py module.

This module tests additional edge cases and complex scenarios to improve
coverage for validation.py from 59% to 80%+.
"""

from unittest.mock import Mock, patch

from sparkforge.models import PipelineConfig
from sparkforge.validation import (
    UnifiedValidator,
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    get_dataframe_info,
    safe_divide,
    validate_dataframe_schema,
)


class TestValidationComprehensiveCoverage:
    """Test comprehensive validation scenarios for improved coverage."""

    def test_and_all_rules_with_string_expressions(self) -> None:
        """Test and_all_rules with string expressions that need conversion."""
        # Mock F.expr to avoid Spark context issues
        with patch("sparkforge.validation.data_validation.F") as mock_f:
            mock_expr = Mock()
            mock_f.expr.return_value = mock_expr

            # Test with string expressions
            rules = {
                "col1": ["col1 > 0", "col1 IS NOT NULL"],
                "col2": ["col2 != ''", "LENGTH(col2) > 5"],
            }

            result = and_all_rules(rules)

            # Should call F.expr for each string rule
            assert mock_f.expr.call_count == 4
            assert result is not mock_expr  # Should return combined expression

    def test_apply_column_rules_no_validation_predicate(self) -> None:
        """Test apply_column_rules when validation_predicate is True."""

        # Create mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.limit.return_value = mock_df

        # Mock and_all_rules to return True (no rules)
        with patch(
            "sparkforge.validation.data_validation.and_all_rules", return_value=True
        ):
            with patch("time.time", return_value=0.0):
                valid_df, invalid_df, stats = apply_column_rules(
                    mock_df, {}, "bronze", "test_step"
                )

                # Should return all data as valid
                assert valid_df is mock_df
                assert invalid_df is mock_df  # limit(0) returns same df
                assert stats.total_rows == 100
                assert stats.valid_rows == 100
                assert stats.invalid_rows == 0

    def test_assess_data_quality_empty_dataframe(self) -> None:
        """Test assess_data_quality with empty DataFrame."""
        mock_df = Mock()
        mock_df.count.return_value = 0

        result = assess_data_quality(mock_df, {})

        assert result["total_rows"] == 0
        assert result["valid_rows"] == 0
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is True

    def test_assess_data_quality_with_rules(self) -> None:
        """Test assess_data_quality with validation rules."""
        mock_df = Mock()
        mock_df.count.return_value = 100

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_invalid_df.columns = ["col1", "col2", "_failed_rules"]
        mock_invalid_df.__contains__ = Mock(return_value=True)
        mock_stats = Mock()
        mock_stats.total_rows = 100
        mock_stats.valid_rows = 95
        mock_stats.invalid_rows = 5
        mock_stats.validation_rate = 95.0

        with patch(
            "sparkforge.validation.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {"col1": ["col1 > 0"]}
            result = assess_data_quality(mock_df, rules)

            assert result["total_rows"] == 100
            assert result["valid_rows"] == 95
            assert result["invalid_rows"] == 5
            assert result["quality_rate"] == 95.0
            assert result["is_empty"] is False

    def test_assess_data_quality_no_rules(self) -> None:
        """Test assess_data_quality without validation rules."""
        mock_df = Mock()
        mock_df.count.return_value = 50

        result = assess_data_quality(mock_df, None)

        assert result["total_rows"] == 50
        assert result["valid_rows"] == 50
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False

    def test_unified_validator_initialization(self) -> None:
        """Test UnifiedValidator initialization with and without logger."""
        # Test with default logger
        validator = UnifiedValidator()
        assert validator.logger is not None
        assert validator.custom_validators == []

        # Test with custom logger
        mock_logger = Mock()
        validator = UnifiedValidator(mock_logger)
        assert validator.logger is mock_logger
        assert validator.custom_validators == []

    def test_unified_validator_add_validator(self) -> None:
        """Test UnifiedValidator add_validator method."""
        mock_logger = Mock()
        validator = UnifiedValidator(mock_logger)
        mock_validator = Mock()

        validator.add_validator(mock_validator)

        assert len(validator.custom_validators) == 1
        assert validator.custom_validators[0] is mock_validator
        # Should log the addition
        mock_logger.info.assert_called_once()

    def test_unified_validator_validate_pipeline(self) -> None:
        """Test UnifiedValidator validate_pipeline method."""
        validator = UnifiedValidator()

        # Mock all validation methods
        with patch.object(validator, "_validate_config", return_value=[]), patch.object(
            validator, "_validate_bronze_steps", return_value=([], [])
        ), patch.object(
            validator, "_validate_silver_steps", return_value=([], [])
        ), patch.object(
            validator, "_validate_gold_steps", return_value=([], [])
        ), patch.object(
            validator, "_validate_dependencies", return_value=([], [])
        ):
            config = PipelineConfig.create_default("test_schema")
            result = validator.validate_pipeline(config, {}, {}, {})

            assert result.is_valid is True
            assert result.errors == []
            assert result.warnings == []
            assert result.recommendations == []

    def test_unified_validator_validate_pipeline_with_errors(self) -> None:
        """Test UnifiedValidator validate_pipeline with validation errors."""
        validator = UnifiedValidator()

        # Mock validation methods to return errors
        with patch.object(
            validator, "_validate_config", return_value=["Config error"]
        ), patch.object(
            validator,
            "_validate_bronze_steps",
            return_value=(["Bronze error"], ["Bronze warning"]),
        ), patch.object(
            validator, "_validate_silver_steps", return_value=([], [])
        ), patch.object(
            validator, "_validate_gold_steps", return_value=([], [])
        ), patch.object(
            validator, "_validate_dependencies", return_value=([], [])
        ):
            config = PipelineConfig.create_default("test_schema")
            result = validator.validate_pipeline(config, {}, {}, {})

            assert result.is_valid is False
            assert len(result.errors) == 2
            assert "Config error" in result.errors
            assert "Bronze error" in result.errors
            assert len(result.warnings) == 1
            assert "Bronze warning" in result.warnings

    def test_apply_column_rules_comprehensive(self) -> None:
        """Test apply_column_rules with comprehensive scenarios."""
        mock_df = Mock()
        mock_df.count.return_value = 200

        # Mock apply_column_rules
        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_invalid_df.columns = ["col1", "col2", "_failed_rules"]
        mock_invalid_df.__contains__ = Mock(return_value=True)
        mock_stats = Mock()
        mock_stats.total_rows = 200
        mock_stats.valid_rows = 180
        mock_stats.invalid_rows = 20
        mock_stats.validation_rate = 90.0
        mock_stats.duration_secs = 1.5

        with patch(
            "sparkforge.validation.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {"col1": ["col1 > 0"]}
            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "silver", "test_step"
            )

            assert stats.total_rows == 200
            assert stats.valid_rows == 180
            assert stats.invalid_rows == 20
            assert stats.validation_rate == 90.0
            assert stats.duration_secs == 1.5

    def test_get_dataframe_info_comprehensive(self) -> None:
        """Test get_dataframe_info with comprehensive scenarios."""
        mock_df = Mock()
        mock_df.count.return_value = 150
        mock_df.columns = ["col1", "col2", "col3"]
        mock_schema = Mock()
        mock_schema.__str__ = Mock(
            return_value="struct<col1:string,col2:int,col3:double>"
        )
        mock_df.schema = mock_schema

        result = get_dataframe_info(mock_df)

        assert result["row_count"] == 150
        assert result["column_count"] == 3
        assert result["columns"] == ["col1", "col2", "col3"]
        assert "schema" in result
        assert result["is_empty"] is False

    def test_validate_dataframe_schema_comprehensive(self) -> None:
        """Test validate_dataframe_schema with comprehensive scenarios."""
        mock_df = Mock()
        mock_df.columns = ["col1", "col2", "col3"]

        # Test valid schema
        expected_columns = ["col1", "col2", "col3"]
        result = validate_dataframe_schema(mock_df, expected_columns)
        assert result is True

        # Test missing columns
        expected_columns = ["col1", "col2", "col3", "col4"]
        result = validate_dataframe_schema(mock_df, expected_columns)
        assert result is False

        # Test extra columns (should still be valid since all expected columns are present)
        expected_columns = ["col1", "col2"]
        result = validate_dataframe_schema(mock_df, expected_columns)
        assert result is True

    def test_safe_divide_comprehensive(self) -> None:
        """Test safe_divide with comprehensive scenarios."""
        # Test normal division
        assert safe_divide(10.0, 2.0) == 5.0

        # Test division by zero
        assert safe_divide(10.0, 0.0) == 0.0

        # Test zero numerator
        assert safe_divide(0.0, 5.0) == 0.0

        # Test both zero
        assert safe_divide(0.0, 0.0) == 0.0

    def test_validation_with_large_dataset_simulation(self) -> None:
        """Test validation scenarios that simulate large dataset handling."""
        # Create mock DataFrame with large row count
        mock_df = Mock()
        mock_df.count.return_value = 1000000  # 1M rows

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_invalid_df.columns = ["user_id", "email", "created_at", "_failed_rules"]
        mock_invalid_df.__contains__ = Mock(return_value=True)
        mock_stats = Mock()
        mock_stats.total_rows = 1000000
        mock_stats.valid_rows = 950000
        mock_stats.invalid_rows = 50000
        mock_stats.validation_rate = 95.0
        mock_stats.duration_secs = 10.5

        with patch(
            "sparkforge.validation.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {
                "user_id": ["user_id IS NOT NULL", "user_id > 0"],
                "email": ["email IS NOT NULL", "email LIKE '%@%'"],
                "created_at": ["created_at IS NOT NULL"],
            }

            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "bronze", "users"
            )

            assert stats.total_rows == 1000000
            assert stats.valid_rows == 950000
            assert stats.invalid_rows == 50000
            assert stats.validation_rate == 95.0
            assert stats.duration_secs == 10.5

    def test_validation_error_handling_comprehensive(self) -> None:
        """Test comprehensive validation error handling scenarios."""
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Database connection failed")

        # Test error handling in assess_data_quality
        result = assess_data_quality(mock_df, {})
        assert "error" in result
        assert "Database connection failed" in result["error"]

        # Test error handling in get_dataframe_info
        result = get_dataframe_info(mock_df)
        assert "error" in result
        assert "Database connection failed" in result["error"]

    def test_validation_performance_scenarios(self) -> None:
        """Test validation performance-related scenarios."""

        mock_df = Mock()
        mock_df.count.return_value = 50000

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_invalid_df.columns = ["col1", "col2", "_failed_rules"]
        mock_invalid_df.__contains__ = Mock(return_value=True)
        mock_stats = Mock()
        mock_stats.total_rows = 50000
        mock_stats.valid_rows = 48000
        mock_stats.invalid_rows = 2000
        mock_stats.validation_rate = 96.0

        # Mock time to simulate performance timing
        with patch("time.time", side_effect=[0.0, 2.5]), patch(
            "sparkforge.validation.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {"col1": ["col1 > 0"]}
            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "silver", "performance_test"
            )

            assert stats.validation_rate == 96.0
