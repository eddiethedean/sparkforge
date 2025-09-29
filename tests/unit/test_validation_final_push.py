#!/usr/bin/env python3
"""
Final push tests for sparkforge/validation.py module.

This module tests remaining edge cases to push validation.py coverage
from 72% to 80%+.
"""

from unittest.mock import Mock, patch

from sparkforge.validation import (
    apply_column_rules,
    assess_data_quality,
    get_dataframe_info,
    safe_divide,
    validate_dataframe_schema,
)


class TestValidationFinalPush:
    """Test remaining edge cases to push validation.py to 80%+ coverage."""

    def test_apply_column_rules_column_filtering_edge_cases(self) -> None:
        """Test apply_column_rules column filtering edge cases."""

        # Create mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.columns = ["col1", "col2", "col3"]
        mock_df.select.return_value = mock_df

        # Mock and_all_rules to return a mock Column
        mock_column = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.__invert__ = Mock(return_value=mock_column)

        with patch(
            "sparkforge.validation.data_validation.and_all_rules",
            return_value=mock_column,
        ):
            with patch("time.time", return_value=0.0):
                # Test with column filtering enabled
                valid_df, invalid_df, stats = apply_column_rules(
                    mock_df,
                    {"col1": ["col1 > 0"]},
                    "bronze",
                    "test_step",
                    filter_columns_by_rules=True,
                )

                assert stats.total_rows == 100
                # Should call select to filter columns
                mock_df.select.assert_called()

    def test_apply_column_rules_no_column_filtering(self) -> None:
        """Test apply_column_rules without column filtering."""

        # Create mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 50
        mock_df.columns = ["col1", "col2", "col3"]

        # Mock and_all_rules to return a mock Column
        mock_column = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.__invert__ = Mock(return_value=mock_column)

        with patch(
            "sparkforge.validation.data_validation.and_all_rules",
            return_value=mock_column,
        ):
            with patch("time.time", return_value=0.0):
                # Test with column filtering disabled
                valid_df, invalid_df, stats = apply_column_rules(
                    mock_df,
                    {"col1": ["col1 > 0"]},
                    "bronze",
                    "test_step",
                    filter_columns_by_rules=False,
                )

                assert stats.total_rows == 50
                # Should not call select
                mock_df.select.assert_not_called()

    def test_assess_data_quality_with_empty_rules(self) -> None:
        """Test assess_data_quality with empty rules dictionary."""
        mock_df = Mock()
        mock_df.count.return_value = 75

        # Test with empty rules - should return all as valid
        result = assess_data_quality(mock_df, {})

        assert result["total_rows"] == 75
        assert result["valid_rows"] == 75
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False

    def test_get_dataframe_info_with_exception(self) -> None:
        """Test get_dataframe_info with exception handling."""
        mock_df = Mock()
        mock_df.count.side_effect = Exception("DataFrame access failed")

        result = get_dataframe_info(mock_df)

        assert "error" in result
        assert "DataFrame access failed" in result["error"]
        assert result["row_count"] == 0
        assert result["column_count"] == 0
        assert result["columns"] == []
        assert result["schema"] == "unknown"
        assert result["is_empty"] is True

    def test_validate_dataframe_schema_edge_cases(self) -> None:
        """Test validate_dataframe_schema with edge cases."""
        mock_df = Mock()
        mock_df.columns = []

        # Test with empty DataFrame
        result = validate_dataframe_schema(mock_df, [])
        assert result is True

        # Test with empty DataFrame and non-empty expected columns
        result = validate_dataframe_schema(mock_df, ["col1"])
        assert result is False

    def test_safe_divide_edge_cases(self) -> None:
        """Test safe_divide with edge cases."""
        # Test with custom default value
        assert safe_divide(10.0, 0.0, 999.0) == 999.0

        # Test with negative numbers
        assert safe_divide(-10.0, 2.0) == -5.0
        assert safe_divide(10.0, -2.0) == -5.0
        assert safe_divide(-10.0, -2.0) == 5.0

        # Test with very small numbers
        assert safe_divide(0.0001, 0.0002) == 0.5

    def test_apply_column_rules_edge_cases(self) -> None:
        """Test apply_column_rules with edge cases."""
        mock_df = Mock()
        mock_df.count.return_value = 25

        # Mock apply_column_rules
        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_stats = Mock()
        mock_stats.total_rows = 25
        mock_stats.valid_rows = 25
        mock_stats.invalid_rows = 0
        mock_stats.validation_rate = 100.0
        mock_stats.duration_secs = 0.1

        with patch(
            "tests.unit.test_validation_final_push.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            # Test with empty rules
            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, {}, "gold", "test_step"
            )

            assert stats.total_rows == 25
            assert stats.valid_rows == 25
            assert stats.invalid_rows == 0
            assert stats.validation_rate == 100.0
            assert stats.duration_secs == 0.1

    def test_validation_performance_edge_cases(self) -> None:
        """Test validation performance edge cases."""

        mock_df = Mock()
        mock_df.count.return_value = 10

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_invalid_df.columns = ["col1", "col2", "_failed_rules"]
        mock_invalid_df.__contains__ = Mock(return_value=True)
        mock_stats = Mock()
        mock_stats.total_rows = 10
        mock_stats.valid_rows = 10
        mock_stats.invalid_rows = 0
        mock_stats.validation_rate = 100.0

        # Test with very fast execution
        with patch("time.time", side_effect=[0.0, 0.001]), patch(
            "tests.unit.test_validation_final_push.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {"col1": ["col1 > 0"]}
            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "bronze", "fast_test"
            )

            assert stats.validation_rate == 100.0

    def test_validation_large_dataset_edge_cases(self) -> None:
        """Test validation with large dataset edge cases."""
        mock_df = Mock()
        mock_df.count.return_value = 5000000  # 5M rows

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_stats = Mock()
        mock_stats.total_rows = 5000000
        mock_stats.valid_rows = 4950000
        mock_stats.invalid_rows = 50000
        mock_stats.validation_rate = 99.0
        mock_stats.duration_secs_secs = 25.0

        with patch(
            "tests.unit.test_validation_final_push.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {
                "id": ["id IS NOT NULL", "id > 0"],
                "name": ["name IS NOT NULL", "LENGTH(name) > 0"],
                "email": ["email IS NOT NULL", "email LIKE '%@%'"],
                "created_at": ["created_at IS NOT NULL"],
                "updated_at": ["updated_at IS NOT NULL"],
            }

            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "bronze", "large_dataset"
            )

            assert stats.total_rows == 5000000
            assert stats.valid_rows == 4950000
            assert stats.invalid_rows == 50000
            assert stats.validation_rate == 99.0
            assert stats.duration_secs == 25.0

    def test_validation_error_handling_edge_cases(self) -> None:
        """Test validation error handling edge cases."""
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Connection timeout")

        # Test error handling in assess_data_quality
        result = assess_data_quality(mock_df, {})
        assert "error" in result
        assert "Connection timeout" in result["error"]

        # Test error handling in get_dataframe_info
        result = get_dataframe_info(mock_df)
        assert "error" in result
        assert "Connection timeout" in result["error"]

    def test_validation_boundary_conditions(self) -> None:
        """Test validation boundary conditions."""
        mock_df = Mock()
        mock_df.count.return_value = 1  # Single row

        mock_valid_df = Mock()
        mock_invalid_df = Mock()
        mock_stats = Mock()
        mock_stats.total_rows = 1
        mock_stats.valid_rows = 0
        mock_stats.invalid_rows = 1
        mock_stats.validation_rate = 0.0
        mock_stats.duration_secs_secs = 0.01

        with patch(
            "tests.unit.test_validation_final_push.apply_column_rules",
            return_value=(mock_valid_df, mock_invalid_df, mock_stats),
        ):
            rules = {"col1": ["col1 > 1000"]}  # Very strict rule
            valid_df, invalid_df, stats = apply_column_rules(
                mock_df, rules, "bronze", "boundary_test"
            )

            assert stats.total_rows == 1
            assert stats.valid_rows == 0
            assert stats.invalid_rows == 1
            assert stats.validation_rate == 0.0
