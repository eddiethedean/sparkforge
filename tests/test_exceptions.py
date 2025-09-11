# test_exceptions.py
"""
Unit tests for the exceptions module.

This module tests all custom exceptions used throughout the framework.
"""

import pytest
from sparkforge.exceptions import (
    ValidationError,
    TableOperationError,
    PerformanceError,
    PipelineValidationError,
    ExecutionError,
    ConfigurationError
)


class TestValidationError:
    """Test ValidationError exception."""
    
    def test_validation_error_creation(self):
        """Test creating ValidationError with message."""
        error = ValidationError("Data validation failed")
        assert str(error) == "Data validation failed"
        assert isinstance(error, Exception)
    
    def test_validation_error_inheritance(self):
        """Test ValidationError inheritance."""
        error = ValidationError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestTableOperationError:
    """Test TableOperationError exception."""
    
    def test_table_operation_error_creation(self):
        """Test creating TableOperationError with message."""
        error = TableOperationError("Table write failed")
        assert str(error) == "Table write failed"
        assert isinstance(error, Exception)
    
    def test_table_operation_error_inheritance(self):
        """Test TableOperationError inheritance."""
        error = TableOperationError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestPerformanceError:
    """Test PerformanceError exception."""
    
    def test_performance_error_creation(self):
        """Test creating PerformanceError with message."""
        error = PerformanceError("Performance threshold exceeded")
        assert str(error) == "Performance threshold exceeded"
        assert isinstance(error, Exception)
    
    def test_performance_error_inheritance(self):
        """Test PerformanceError inheritance."""
        error = PerformanceError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestPipelineValidationError:
    """Test PipelineValidationError exception."""
    
    def test_pipeline_validation_error_creation(self):
        """Test creating PipelineValidationError with message."""
        error = PipelineValidationError("Pipeline validation failed")
        assert str(error) == "Pipeline validation failed"
        assert isinstance(error, Exception)
    
    def test_pipeline_validation_error_inheritance(self):
        """Test PipelineValidationError inheritance."""
        error = PipelineValidationError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestExecutionError:
    """Test ExecutionError exception."""
    
    def test_execution_error_creation(self):
        """Test creating ExecutionError with message."""
        error = ExecutionError("Pipeline execution failed")
        assert str(error) == "Pipeline execution failed"
        assert isinstance(error, Exception)
    
    def test_execution_error_inheritance(self):
        """Test ExecutionError inheritance."""
        error = ExecutionError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestConfigurationError:
    """Test ConfigurationError exception."""
    
    def test_configuration_error_creation(self):
        """Test creating ConfigurationError with message."""
        error = ConfigurationError("Invalid configuration")
        assert str(error) == "Invalid configuration"
        assert isinstance(error, Exception)
    
    def test_configuration_error_inheritance(self):
        """Test ConfigurationError inheritance."""
        error = ConfigurationError("Test error")
        assert isinstance(error, Exception)
        assert not isinstance(error, ValueError)


class TestExceptionChaining:
    """Test exception chaining and context."""
    
    def test_exception_with_cause(self):
        """Test exception with underlying cause."""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            error = ValidationError("Validation failed")
            error.__cause__ = e
            assert str(error) == "Validation failed"
            assert error.__cause__ is e
    
    def test_exception_context(self):
        """Test exception context preservation."""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            error = TableOperationError("Table operation failed")
            error.__context__ = e
            assert str(error) == "Table operation failed"
            assert error.__context__ is not None
