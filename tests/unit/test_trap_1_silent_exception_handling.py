#!/usr/bin/env python3
"""
Test for Trap 1: Silent Exception Handling fix.

This test verifies that the assess_data_quality function properly
raises exceptions instead of silently returning fallback responses.
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkforge.validation.data_validation import assess_data_quality
from sparkforge.errors import ValidationError


class TestTrap1SilentExceptionHandling:
    """Test that exceptions are properly raised instead of silently handled."""

    def test_validation_error_is_re_raised(self, spark_session):
        """Test that ValidationError is re-raised instead of masked."""
        # Create a DataFrame that will cause a ValidationError
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
        # Create rules that reference non-existent columns
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "value": [F.col("value") > 0],  # This column doesn't exist
        }
        
        # The function should raise ValidationError, not return fallback
        with pytest.raises(ValidationError) as excinfo:
            assess_data_quality(df, rules)
        
        # Verify the error message is helpful
        error_msg = str(excinfo.value)
        assert "Columns referenced in validation rules do not exist" in error_msg
        assert "value" in error_msg

    def test_unexpected_error_is_logged_and_re_raised(self, spark_session):
        """Test that unexpected errors are logged and re-raised with context."""
        # Create a DataFrame
        sample_data = [("user1", "click")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
        # Mock df.count() to raise an unexpected exception
        with patch.object(df, 'count', side_effect=RuntimeError("Mock database connection failed")):
            with patch('logging.getLogger') as mock_get_logger:
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

    def test_successful_assessment_returns_correct_metrics(self, spark_session):
        """Test that successful assessment returns correct metrics without fallback."""
        # Create a DataFrame
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
        # Test without rules
        result = assess_data_quality(df, None)
        
        assert result["total_rows"] == 2
        assert result["valid_rows"] == 2
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is False
        assert "error" not in result  # No error field in successful case

    def test_successful_assessment_with_rules_returns_correct_metrics(self, spark_session):
        """Test that successful assessment with rules returns correct metrics."""
        # Create a DataFrame
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
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

    def test_empty_dataframe_returns_correct_metrics(self, spark_session):
        """Test that empty DataFrame returns correct metrics without fallback."""
        # Create empty DataFrame
        df = spark_session.createDataFrame([], "user_id STRING, action STRING")
        
        result = assess_data_quality(df, None)
        
        assert result["total_rows"] == 0
        assert result["valid_rows"] == 0
        assert result["invalid_rows"] == 0
        assert result["quality_rate"] == 100.0
        assert result["is_empty"] is True
        assert "error" not in result  # No error field in successful case

    def test_no_fallback_response_for_errors(self, spark_session):
        """Test that errors are not masked with fallback responses."""
        # Create a DataFrame
        sample_data = [("user1", "click")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
        # Create rules that will cause validation error
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "value": [F.col("value") > 0],  # Missing column
        }
        
        # Should raise exception, not return fallback
        with pytest.raises(ValidationError):
            assess_data_quality(df, rules)
        
        # Verify no fallback response was returned
        # (This test passes if the exception is raised as expected)
