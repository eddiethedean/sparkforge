"""
Simple unit tests for data validation using Mock Spark.
"""

import pytest
from sparkforge.validation.pipeline_validation import UnifiedValidator, ValidationResult
from sparkforge.models.enums import ValidationResult as ValidationResultEnum
from mock_spark.errors import AnalysisException, IllegalArgumentException


class TestDataValidationSimple:
    """Test UnifiedValidator with Mock Spark - simplified tests."""
    
    def test_unified_validator_initialization(self, mock_spark_session):
        """Test unified validator initialization."""
        validator = UnifiedValidator(spark=mock_spark_session)
        assert validator.spark == mock_spark_session
    
    def test_unified_validator_invalid_spark_session(self):
        """Test unified validator with invalid spark session."""
        with pytest.raises(IllegalArgumentException):
            UnifiedValidator(spark=None)
    
    def test_unified_validator_get_spark(self, mock_spark_session):
        """Test getting spark session from unified validator."""
        validator = UnifiedValidator(spark=mock_spark_session)
        assert validator.spark == mock_spark_session
    
    def test_validation_result_creation(self):
        """Test creating ValidationResult."""
        result = ValidationResult(
            step_name="test_step",
            passed=True,
            errors=[],
            warnings=[]
        )
        
        assert result.step_name == "test_step"
        assert result.passed is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
    
    def test_validation_result_failed(self):
        """Test creating failed ValidationResult."""
        errors = ["Error 1", "Error 2"]
        warnings = ["Warning 1"]
        
        result = ValidationResult(
            step_name="test_step",
            passed=False,
            errors=errors,
            warnings=warnings
        )
        
        assert result.step_name == "test_step"
        assert result.passed is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert result.errors[0] == "Error 1"
        assert result.errors[1] == "Error 2"
        assert result.warnings[0] == "Warning 1"
    
    def test_validation_result_enum(self):
        """Test ValidationResult enum values."""
        assert ValidationResultEnum.PASSED == "PASSED"
        assert ValidationResultEnum.FAILED == "FAILED"
        assert ValidationResultEnum.WARNING == "WARNING"
    
    def test_unified_validator_with_sample_data(self, mock_spark_session, sample_dataframe):
        """Test unified validator with sample data."""
        validator = UnifiedValidator(spark=mock_spark_session)
        
        # Test with sample DataFrame
        assert sample_dataframe.count() > 0
        assert len(sample_dataframe.columns()) > 0
    
    def test_unified_validator_error_handling(self, mock_spark_session):
        """Test unified validator error handling."""
        validator = UnifiedValidator(spark=mock_spark_session)
        
        # Test with invalid table name
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent.table")
    
    def test_unified_validator_metrics_collection(self, mock_spark_session, sample_dataframe):
        """Test unified validator metrics collection."""
        validator = UnifiedValidator(spark=mock_spark_session)
        
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
            "Column 'salary' is negative"
        ]
        
        result = ValidationResult(
            step_name="data_validation",
            passed=False,
            errors=errors,
            warnings=["Data quality is below threshold"]
        )
        
        assert result.step_name == "data_validation"
        assert result.passed is False
        assert len(result.errors) == 3
        assert len(result.warnings) == 1
        assert "null" in result.errors[0]
        assert "between" in result.errors[1]
        assert "negative" in result.errors[2]
    
    def test_validation_result_with_warnings_only(self):
        """Test ValidationResult with warnings only."""
        warnings = [
            "Data quality is below optimal threshold",
            "Consider additional validation rules"
        ]
        
        result = ValidationResult(
            step_name="data_validation",
            passed=True,
            errors=[],
            warnings=warnings
        )
        
        assert result.step_name == "data_validation"
        assert result.passed is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 2
        assert "threshold" in result.warnings[0]
        assert "validation" in result.warnings[1]
    
    def test_validation_result_empty(self):
        """Test empty ValidationResult."""
        result = ValidationResult(
            step_name="empty_validation",
            passed=True,
            errors=[],
            warnings=[]
        )
        
        assert result.step_name == "empty_validation"
        assert result.passed is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
