#!/usr/bin/env python3
"""
Test for Trap 1: Silent Exception Handling fix.

This test verifies that the assess_data_quality function properly
raises exceptions instead of silently returning fallback responses.
"""

from unittest.mock import patch

import pytest

from pipeline_builder.compat import F
from pipeline_builder.errors import ValidationError
from pipeline_builder.validation.data_validation import assess_data_quality


class TestTrap1SilentExceptionHandling:
    """Test that exceptions are properly raised instead of silently handled."""

    def test_validation_error_is_re_raised(self, spark):
        """Test that ValidationError is re-raised when ALL columns don't exist."""
        # Create a DataFrame that will cause a ValidationError
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark.createDataFrame(sample_data, ["user_id", "action"])

        # Create rules that reference ONLY non-existent columns (all missing)
        rules = {
            "value": [F.col("value") > 0],  # This column doesn't exist
            "timestamp": [F.col("timestamp").isNotNull()],  # This column doesn't exist
        }

        # The function should raise ValidationError when ALL columns are missing
        with pytest.raises(ValidationError) as excinfo:
            assess_data_quality(df, rules)

        # Verify the error message is helpful
        error_msg = str(excinfo.value)
        assert "All columns referenced in validation rules do not exist" in error_msg
        assert "value" in error_msg

    def test_unexpected_error_is_logged_and_re_raised(self, spark):
        """Test that unexpected errors are logged and re-raised with context."""
        sample_data = [("user1", "click")]
        df = spark.createDataFrame(sample_data, ["user_id", "action"])

        # Patch abstraction layer so we don't patch read-only DataFrame.count
        with patch(
            "pipeline_builder.validation.data_validation._get_dataframe_count",
            side_effect=RuntimeError("Mock database connection failed"),
        ):
            with patch("logging.getLogger") as mock_get_logger:
                # The function should raise ValidationError with context
                with pytest.raises(ValidationError) as excinfo:
                    assess_data_quality(df, None)

                # Verify error logging occurred
                mock_get_logger.return_value.error.assert_called_once()
                log_call = mock_get_logger.return_value.error.call_args[0][0]
                assert "Unexpected error in assess_data_quality" in log_call
                assert "Mock database connection failed" in log_call

                # Verify the re-raised error has context
                error_msg = str(excinfo.value)
                assert "Data quality assessment failed" in error_msg
                assert "Mock database connection failed" in error_msg

    def test_successful_assessment_returns_correct_metrics(self, spark):
        """Test that successful assessment returns correct metrics without fallback."""
        # Create a DataFrame
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark.createDataFrame(sample_data, ["user_id", "action"])

        # Test without rules
        result = assess_data_quality(df, None)

        assert result["total_rows"] == 2
        assert result["valid_rows"] == 2
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False
        assert "error" not in result  # No error field in successful case

    def test_successful_assessment_with_rules_returns_correct_metrics(
        self, spark
    ):
        """Test that successful assessment with rules returns correct metrics."""
        # Create a DataFrame
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark.createDataFrame(sample_data, ["user_id", "action"])

        # Create valid rules
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
        }

        result = assess_data_quality(df, rules)

        assert result["total_rows"] == 2
        assert result["valid_rows"] == 2
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False
        assert "error" not in result  # No error field in successful case

    def test_empty_dataframe_returns_correct_metrics(self, spark, spark_imports):
        """Test that empty DataFrame returns correct metrics without fallback."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        StringType = spark_imports.StringType

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)

        result = assess_data_quality(df, None)

        assert result["total_rows"] == 0
        assert result["valid_rows"] == 0
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is True
        assert "error" not in result  # No error field in successful case

    def test_no_fallback_response_for_errors(self, spark):
        """Test that errors are not masked with fallback responses."""
        # Create a DataFrame
        sample_data = [("user1", "click")]
        df = spark.createDataFrame(sample_data, ["user_id", "action"])

        # Create rules that will cause validation error (ALL columns missing)
        rules = {
            "value": [F.col("value") > 0],  # Missing column
            "timestamp": [F.col("timestamp").isNotNull()],  # Missing column
        }

        # Should raise exception when ALL columns are missing, not return fallback
        with pytest.raises(ValidationError):
            assess_data_quality(df, rules)

        # Verify no fallback response was returned
        # (This test passes if the exception is raised as expected)
