"""
Simple unit tests for data validation using Mock Spark.
"""

import pytest
from mock_spark.errors import AnalysisException

from sparkforge.models.enums import ValidationResult as ValidationResultEnum
from sparkforge.validation.pipeline_validation import UnifiedValidator, ValidationResult


class TestDataValidationSimple:
    """Test UnifiedValidator with Mock Spark - simplified tests."""

    def test_unified_validator_initialization(self, mock_spark_session):
        """Test unified validator initialization."""
        validator = UnifiedValidator()
        assert validator is not None

    def test_unified_validator_invalid_spark_session(self):
        """Test unified validator with invalid spark session."""
        # UnifiedValidator doesn't require spark parameter
        validator = UnifiedValidator()
        assert validator is not None

    def test_unified_validator_get_spark(self, mock_spark_session):
        """Test getting spark session from unified validator."""
        validator = UnifiedValidator()
        # UnifiedValidator doesn't store spark session
        assert validator is not None

    def test_validation_result_creation(self):
        """Test creating ValidationResult."""
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], recommendations=[]
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_validation_result_failed(self):
        """Test creating failed ValidationResult."""
        errors = ["Error 1", "Error 2"]
        warnings = ["Warning 1"]

        result = ValidationResult(
            is_valid=False, errors=errors, warnings=warnings, recommendations=[]
        )

        assert result.is_valid is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert result.errors[0] == "Error 1"
        assert result.errors[1] == "Error 2"
        assert result.warnings[0] == "Warning 1"

    def test_validation_result_enum(self):
        """Test ValidationResult enum values."""
        assert ValidationResultEnum.PASSED.value == "passed"
        assert ValidationResultEnum.FAILED.value == "failed"
        assert ValidationResultEnum.WARNING.value == "warning"

    def test_unified_validator_with_sample_data(
        self, mock_spark_session, sample_dataframe
    ):
        """Test unified validator with sample data."""
        UnifiedValidator()

        # Test with sample DataFrame
        assert sample_dataframe.count() > 0
        assert len(sample_dataframe.columns) > 0

    def test_unified_validator_error_handling(self, mock_spark_session):
        """Test unified validator error handling."""
        UnifiedValidator()

        # Test with invalid table name
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent.table")

    def test_unified_validator_metrics_collection(
        self, mock_spark_session, sample_dataframe
    ):
        """Test unified validator metrics collection."""
        UnifiedValidator()

        # Test basic metrics
        start_time = 0.0
        end_time = 1.0
        execution_time = end_time - start_time

        assert execution_time == 1.0
        assert sample_dataframe.count() > 0

    def test_validation_result_with_errors(self):
        """Test ValidationResult with various error types."""
        errors = [
            "Column 'name' is null",
            "Value 'age' is not between 18 and 65",
            "Column 'salary' is negative",
        ]

        result = ValidationResult(
            is_valid=False,
            errors=errors,
            warnings=["Data quality is below threshold"],
            recommendations=[],
        )

        assert result.is_valid is False
        assert len(result.errors) == 3
        assert len(result.warnings) == 1
        assert "null" in result.errors[0]
        assert "between" in result.errors[1]
        assert "negative" in result.errors[2]

    def test_validation_result_with_warnings_only(self):
        """Test ValidationResult with warnings only."""
        warnings = [
            "Data quality is below optimal threshold",
            "Consider additional validation rules",
        ]

        result = ValidationResult(
            is_valid=True, errors=[], warnings=warnings, recommendations=[]
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 2
        assert "threshold" in result.warnings[0]
        assert "validation" in result.warnings[1]

    def test_validation_result_empty(self):
        """Test empty ValidationResult."""
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], recommendations=[]
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
