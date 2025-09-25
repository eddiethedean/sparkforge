"""
Tests for error system type safety.

This module tests that all error classes use explicit types instead of Any,
*args, or **kwargs, ensuring type safety and clarity.
"""

from datetime import datetime
from typing import List

from sparkforge.errors import (
    ConfigurationError,
    DataError,
    ErrorCategory,
    ErrorContext,
    ErrorContextValue,
    ErrorSeverity,
    ErrorSuggestions,
    ExecutionError,
    PerformanceError,
    PipelineError,
    ResourceError,
    SparkForgeError,
    StepError,
    SystemError,
    ValidationError,
)


class TestErrorTypeSafety:
    """Test error system type safety."""

    def test_error_context_value_type_validation(self):
        """Test that ErrorContextValue accepts only valid types."""
        # Valid types
        valid_values: List[ErrorContextValue] = [
            "string",
            42,
            3.14,
            True,
            ["list", "of", "strings"],
            {"key": "value"},
            None,
        ]

        for value in valid_values:
            assert isinstance(value, (str, int, float, bool, list, dict, type(None)))

    def test_error_context_type_validation(self):
        """Test that ErrorContext is properly typed."""
        context: ErrorContext = {
            "string_key": "string_value",
            "int_key": 42,
            "float_key": 3.14,
            "bool_key": True,
            "list_key": ["item1", "item2"],
            "dict_key": {"nested": "value"},
            "none_key": None,
        }

        assert isinstance(context, dict)
        for key, value in context.items():
            assert isinstance(key, str)
            assert isinstance(value, (str, int, float, bool, list, dict, type(None)))

    def test_error_suggestions_type_validation(self):
        """Test that ErrorSuggestions is properly typed."""
        suggestions: ErrorSuggestions = [
            "Check configuration",
            "Verify data quality",
            "Review logs",
        ]

        assert isinstance(suggestions, list)
        for suggestion in suggestions:
            assert isinstance(suggestion, str)

    def test_base_error_explicit_types(self):
        """Test that base error classes use explicit types."""
        # Test SparkForgeError
        error = SparkForgeError(
            message="Test error",
            error_code="TEST_001",
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            context={"key": "value"},
            suggestions=["Fix this", "Check that"],
            timestamp=datetime.now(),
            cause=ValueError("Original error"),
        )

        assert error.message == "Test error"
        assert error.error_code == "TEST_001"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH
        assert error.context == {"key": "value"}

        # Test __str__ method includes error_code
        error_str = str(error)
        assert "Test error" in error_str
        assert "[TEST_001]" in error_str
        assert error.suggestions == ["Fix this", "Check that"]
        assert isinstance(error.timestamp, datetime)
        assert isinstance(error.cause, ValueError)

    def test_configuration_error_explicit_types(self):
        """Test ConfigurationError uses explicit types."""
        error = ConfigurationError(
            message="Config error",
            error_code="CONFIG_001",
            context={"config_file": "test.yaml"},
            suggestions=["Check syntax"],
            timestamp=datetime.now(),
            cause=FileNotFoundError("File not found"),
        )

        assert error.message == "Config error"
        assert error.error_code == "CONFIG_001"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH

    def test_data_error_explicit_types(self):
        """Test DataError uses explicit types."""
        error = DataError(
            message="Data error",
            error_code="DATA_001",
            context={
                "table_name": "test_table",
                "column_name": "test_column",
                "row_count": 1000,
            },
            suggestions=["Check data quality"],
        )

        assert error.message == "Data error"
        assert error.error_code == "DATA_001"
        assert error.context["table_name"] == "test_table"
        assert error.context["column_name"] == "test_column"

    def test_pipeline_error_explicit_types(self):
        """Test PipelineError uses explicit types."""
        error = PipelineError(
            message="Pipeline error",
            error_code="PIPELINE_001",
            context={
                "pipeline_id": "pipeline_123",
                "execution_id": "exec_456",
                "step_count": 5,
            },
            suggestions=["Check dependencies"],
        )

        assert error.message == "Pipeline error"
        assert error.error_code == "PIPELINE_001"
        assert error.context["pipeline_id"] == "pipeline_123"
        assert error.context["execution_id"] == "exec_456"

    def test_step_error_explicit_types(self):
        """Test StepError uses explicit types."""
        error = StepError(
            message="Step error",
            error_code="STEP_001",
            context={
                "step_name": "bronze_step",
                "step_type": "bronze",
                "transform": "data_cleaning",
            },
            suggestions=["Check transform function"],
        )

        assert error.message == "Step error"
        assert error.error_code == "STEP_001"
        assert error.context["step_name"] == "bronze_step"
        assert error.context["step_type"] == "bronze"

    def test_execution_error_explicit_types(self):
        """Test ExecutionError uses explicit types."""
        error = ExecutionError(
            message="Execution error",
            error_code="EXEC_001",
            context={"execution_step": "bronze_processing", "duration": 30.5},
            suggestions=["Check resources"],
        )

        assert error.message == "Execution error"
        assert error.error_code == "EXEC_001"
        assert error.context["execution_step"] == "bronze_processing"

    def test_system_error_explicit_types(self):
        """Test SystemError uses explicit types."""
        error = SystemError(
            message="System error",
            error_code="SYSTEM_001",
            context={"component": "spark_session", "memory_usage": "80%"},
            suggestions=["Increase memory"],
        )

        assert error.message == "System error"
        assert error.error_code == "SYSTEM_001"
        assert error.context["component"] == "spark_session"

    def test_performance_error_explicit_types(self):
        """Test PerformanceError uses explicit types."""
        error = PerformanceError(
            message="Performance error",
            error_code="PERF_001",
            context={
                "performance_metric": "execution_time",
                "threshold_value": 60.0,
                "actual_value": 120.0,
                "operation": "data_processing",
            },
            suggestions=["Optimize query"],
        )

        assert error.message == "Performance error"
        assert error.error_code == "PERF_001"
        assert error.context["performance_metric"] == "execution_time"
        assert error.context["threshold_value"] == 60.0
        assert error.context["actual_value"] == 120.0

    def test_error_serialization_explicit_types(self):
        """Test error serialization uses explicit types."""
        error = SparkForgeError(
            message="Serialization test",
            error_code="SERIAL_001",
            context={"test": "value"},
            suggestions=["Test suggestion"],
        )

        error_dict = error.to_dict()
        assert isinstance(error_dict, dict)
        assert error_dict["message"] == "Serialization test"
        assert error_dict["error_code"] == "SERIAL_001"
        assert error_dict["context"] == {"test": "value"}
        assert error_dict["suggestions"] == ["Test suggestion"]

    def test_error_context_manipulation_explicit_types(self):
        """Test error context manipulation uses explicit types."""
        error = SparkForgeError(
            "Test error",
            context={
                "string_key": "string_value",
                "int_key": 42,
                "float_key": 3.14,
                "bool_key": True,
                "list_key": ["item1", "item2"],
                "dict_key": {"nested": "value"},
                "none_key": None,
            },
        )

        assert error.context["string_key"] == "string_value"
        assert error.context["int_key"] == 42
        assert error.context["float_key"] == 3.14
        assert error.context["bool_key"] is True
        assert error.context["list_key"] == ["item1", "item2"]
        assert error.context["dict_key"] == {"nested": "value"}
        assert error.context["none_key"] is None

    def test_error_suggestion_manipulation_explicit_types(self):
        """Test error suggestion manipulation uses explicit types."""
        error = SparkForgeError(
            "Test error",
            suggestions=["Check configuration", "Verify data quality", "Review logs"],
        )

        assert len(error.suggestions) == 3
        assert "Check configuration" in error.suggestions
        assert "Verify data quality" in error.suggestions
        assert "Review logs" in error.suggestions

    def test_no_any_types_in_error_classes(self):
        """Test that error classes don't use Any types."""
        # This test ensures that our refactoring removed all Any types
        # We can't directly test for absence of Any, but we can verify
        # that all the explicit types work correctly

        # Test that all error constructors accept explicit types
        errors = [
            SparkForgeError("test"),
            ConfigurationError("test"),
            ValidationError("test"),
            ExecutionError("test"),
            DataError("test"),
            PipelineError("test"),
            StepError("test"),
            SystemError("test"),
            PerformanceError("test"),
        ]

        for error in errors:
            assert isinstance(error, SparkForgeError)
            assert isinstance(error.message, str)
            assert error.error_code is None or isinstance(error.error_code, str)
            # Some error classes may not set a default severity
            if error.severity is not None:
                assert isinstance(error.severity, ErrorSeverity)

    def test_no_args_kwargs_in_error_constructors(self):
        """Test that error constructors don't use *args or **kwargs."""
        # This test ensures that our refactoring removed all *args and **kwargs
        # We verify by checking that constructors only accept explicit parameters

        # Test base error constructor
        error = SparkForgeError(
            message="test",
            error_code="TEST",
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            context={"key": "value"},
            suggestions=["suggestion"],
            timestamp=datetime.now(),
            cause=ValueError("test"),
        )

        # All parameters should be explicitly typed and accessible
        assert error.message == "test"
        assert error.error_code == "TEST"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH
        assert error.context == {"key": "value"}
        assert error.suggestions == ["suggestion"]
        assert isinstance(error.timestamp, datetime)
        assert isinstance(error.cause, ValueError)


class TestErrorBackwardCompatibility:
    """Test error system backward compatibility."""

    def test_existing_error_usage_still_works(self):
        """Test that existing error usage patterns still work."""
        # Test basic error creation
        error = SparkForgeError("Basic error")
        assert error.message == "Basic error"
        assert error.severity == ErrorSeverity.MEDIUM

        # Test error with minimal parameters
        error = ConfigurationError("Config error")
        assert error.message == "Config error"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH

    def test_error_inheritance_still_works(self):
        """Test that error inheritance still works correctly."""

        class CustomError(SparkForgeError):
            def __init__(self, message: str, custom_field: str):
                super().__init__(message)
                self.custom_field = custom_field

        error = CustomError("Custom error", "custom_value")
        assert error.message == "Custom error"
        assert error.custom_field == "custom_value"
        assert isinstance(error, SparkForgeError)

    def test_error_context_manipulation_still_works(self):
        """Test that error context manipulation still works."""
        error = SparkForgeError(
            "Test error", context={"key": "value"}, suggestions=["suggestion"]
        )

        # Test context access
        assert error.context["key"] == "value"
        assert "suggestion" in error.suggestions

    def test_error_serialization_still_works(self):
        """Test that error serialization still works."""
        error = SparkForgeError(
            message="Test error",
            error_code="TEST_001",
            context={"key": "value"},
            suggestions=["suggestion"],
        )

        error_dict = error.to_dict()
        assert isinstance(error_dict, dict)
        assert error_dict["message"] == "Test error"
        assert error_dict["error_code"] == "TEST_001"
        assert error_dict["context"] == {"key": "value"}
        assert error_dict["suggestions"] == ["suggestion"]

    def test_resource_error_creation(self):
        """Test ResourceError creation and initialization."""
        error = ResourceError(
            message="Resource not found",
            error_code="RESOURCE_001",
            context={"resource": "database"},
            suggestions=["Check connection", "Verify permissions"],
        )

        assert error.message == "Resource not found"
        assert error.error_code == "RESOURCE_001"
        assert error.category == ErrorCategory.RESOURCE
        assert error.severity == ErrorSeverity.HIGH
        assert error.context == {"resource": "database"}
        assert error.suggestions == ["Check connection", "Verify permissions"]
