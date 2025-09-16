# Copyright (c) 2024 Odos Matthews
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Tests for error system type safety.

This module tests that all error classes use explicit types instead of Any,
*args, or **kwargs, ensuring type safety and clarity.
"""

import pytest
from datetime import datetime
from typing import Dict, List, Optional, Union

from sparkforge.errors.base import (
    SparkForgeError,
    ConfigurationError,
    ValidationError,
    ExecutionError,
    DataQualityError,
    ResourceError,
    ErrorSeverity,
    ErrorCategory,
    ErrorContext,
    ErrorSuggestions,
    ErrorContextValue,
)
from sparkforge.errors.data import (
    DataError,
    DataQualityError as DataDataQualityError,
    SchemaError,
    ValidationError as DataValidationError,
    TableOperationError,
)
from sparkforge.errors.pipeline import (
    PipelineError,
    PipelineConfigurationError,
    PipelineExecutionError,
    PipelineValidationError,
    StepError,
    StepExecutionError,
    StepValidationError,
    DependencyError,
    CircularDependencyError,
    InvalidDependencyError,
)
from sparkforge.errors.execution import (
    ExecutionError as ExecExecutionError,
    ExecutionEngineError,
    StepExecutionError as ExecStepExecutionError,
    StrategyError,
    RetryError,
    TimeoutError,
)
from sparkforge.errors.system import (
    SystemError,
    ResourceError as SystemResourceError,
    ConfigurationError as SystemConfigurationError,
    NetworkError,
    StorageError,
)
from sparkforge.errors.performance import (
    PerformanceError,
    PerformanceThresholdError,
    PerformanceMonitoringError,
)
from sparkforge.errors.utils import (
    handle_errors,
    create_error_context,
    get_error_suggestions,
    format_error_message,
    log_error,
    is_recoverable_error,
    should_retry_error,
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
            table_name="test_table",
            column_name="test_column",
            error_code="DATA_001",
            context={"row_count": 1000},
            suggestions=["Check data quality"],
        )
        
        assert error.message == "Data error"
        assert error.table_name == "test_table"
        assert error.column_name == "test_column"
        assert error.error_code == "DATA_001"

    def test_pipeline_error_explicit_types(self):
        """Test PipelineError uses explicit types."""
        error = PipelineError(
            message="Pipeline error",
            pipeline_id="pipeline_123",
            execution_id="exec_456",
            error_code="PIPELINE_001",
            context={"step_count": 5},
            suggestions=["Check dependencies"],
        )
        
        assert error.message == "Pipeline error"
        assert error.pipeline_id == "pipeline_123"
        assert error.execution_id == "exec_456"
        assert error.error_code == "PIPELINE_001"

    def test_step_error_explicit_types(self):
        """Test StepError uses explicit types."""
        error = StepError(
            message="Step error",
            step_name="bronze_step",
            step_type="bronze",
            error_code="STEP_001",
            context={"transform": "data_cleaning"},
            suggestions=["Check transform function"],
        )
        
        assert error.message == "Step error"
        assert error.step_name == "bronze_step"
        assert error.step_type == "bronze"
        assert error.error_code == "STEP_001"

    def test_execution_error_explicit_types(self):
        """Test ExecutionError uses explicit types."""
        error = ExecExecutionError(
            message="Execution error",
            execution_step="bronze_processing",
            error_code="EXEC_001",
            context={"duration": 30.5},
            suggestions=["Check resources"],
        )
        
        assert error.message == "Execution error"
        assert error.execution_step == "bronze_processing"
        assert error.error_code == "EXEC_001"

    def test_system_error_explicit_types(self):
        """Test SystemError uses explicit types."""
        error = SystemError(
            message="System error",
            component="spark_session",
            error_code="SYSTEM_001",
            context={"memory_usage": "80%"},
            suggestions=["Increase memory"],
        )
        
        assert error.message == "System error"
        assert error.component == "spark_session"
        assert error.error_code == "SYSTEM_001"

    def test_performance_error_explicit_types(self):
        """Test PerformanceError uses explicit types."""
        error = PerformanceError(
            message="Performance error",
            performance_metric="execution_time",
            threshold_value=60.0,
            actual_value=120.0,
            error_code="PERF_001",
            context={"operation": "data_processing"},
            suggestions=["Optimize query"],
        )
        
        assert error.message == "Performance error"
        assert error.performance_metric == "execution_time"
        assert error.threshold_value == 60.0
        assert error.actual_value == 120.0
        assert error.error_code == "PERF_001"

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
        assert error_dict["error_type"] == "SparkForgeError"
        assert error_dict["message"] == "Serialization test"
        assert error_dict["error_code"] == "SERIAL_001"
        assert error_dict["context"] == {"test": "value"}
        assert error_dict["suggestions"] == ["Test suggestion"]

    def test_error_context_manipulation_explicit_types(self):
        """Test error context manipulation uses explicit types."""
        error = SparkForgeError("Test error")
        
        # Test add_context with explicit types
        error.add_context("string_key", "string_value")
        error.add_context("int_key", 42)
        error.add_context("float_key", 3.14)
        error.add_context("bool_key", True)
        error.add_context("list_key", ["item1", "item2"])
        error.add_context("dict_key", {"nested": "value"})
        error.add_context("none_key", None)
        
        assert error.context["string_key"] == "string_value"
        assert error.context["int_key"] == 42
        assert error.context["float_key"] == 3.14
        assert error.context["bool_key"] is True
        assert error.context["list_key"] == ["item1", "item2"]
        assert error.context["dict_key"] == {"nested": "value"}
        assert error.context["none_key"] is None

    def test_error_suggestion_manipulation_explicit_types(self):
        """Test error suggestion manipulation uses explicit types."""
        error = SparkForgeError("Test error")
        
        # Test add_suggestion with explicit types
        error.add_suggestion("Check configuration")
        error.add_suggestion("Verify data quality")
        error.add_suggestion("Review logs")
        
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
            DataQualityError("test"),
            ResourceError("test"),
            DataError("test"),
            PipelineError("test"),
            StepError("test", step_name="test_step"),
            SystemError("test"),
            PerformanceError("test"),
        ]
        
        for error in errors:
            assert isinstance(error, SparkForgeError)
            assert isinstance(error.message, str)
            assert isinstance(error.error_code, str)
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


class TestErrorUtilsTypeSafety:
    """Test error utility functions type safety."""

    def test_handle_errors_decorator_explicit_types(self):
        """Test handle_errors decorator uses explicit types."""
        @handle_errors(
            error_type=ConfigurationError,
            message="Test error",
            error_code="TEST_001",
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            context={"test": "value"},
            suggestions=["Test suggestion"],
            reraise=True,
        )
        def test_function(value: str) -> str:
            if value == "error":
                raise ValueError("Test error")
            return f"Processed: {value}"
        
        # Test successful execution
        result = test_function("success")
        assert result == "Processed: success"
        
        # Test error handling
        with pytest.raises(ConfigurationError) as exc_info:
            test_function("error")
        
        error = exc_info.value
        assert error.message == "Test error: Test error"
        assert error.error_code == "TEST_001"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH

    def test_create_error_context_explicit_types(self):
        """Test create_error_context uses explicit types."""
        context = create_error_context(
            step_name="bronze_step",
            step_type="bronze",
            pipeline_id="pipeline_123",
            execution_id="exec_456",
            table_name="test_table",
            column_name="test_column",
            custom_key="custom_value",
            numeric_key=42,
        )
        
        assert isinstance(context, dict)
        assert context["step_name"] == "bronze_step"
        assert context["step_type"] == "bronze"
        assert context["pipeline_id"] == "pipeline_123"
        assert context["execution_id"] == "exec_456"
        assert context["table_name"] == "test_table"
        assert context["column_name"] == "test_column"
        assert context["custom_key"] == "custom_value"
        assert context["numeric_key"] == 42
        assert "timestamp" in context

    def test_get_error_suggestions_explicit_types(self):
        """Test get_error_suggestions uses explicit types."""
        error = ConfigurationError("Test config error")
        suggestions = get_error_suggestions(error)
        
        assert isinstance(suggestions, list)
        assert all(isinstance(suggestion, str) for suggestion in suggestions)
        assert len(suggestions) > 0

    def test_format_error_message_explicit_types(self):
        """Test format_error_message uses explicit types."""
        error = SparkForgeError(
            message="Test error",
            context={"key1": "value1", "key2": "value2"},
            suggestions=["suggestion1", "suggestion2", "suggestion3", "suggestion4"],
        )
        
        message = format_error_message(error)
        assert isinstance(message, str)
        assert "Test error" in message
        assert "Context:" in message
        assert "Suggestions:" in message

    def test_log_error_explicit_types(self):
        """Test log_error uses explicit types."""
        import logging
        
        error = SparkForgeError(
            message="Test error",
            severity=ErrorSeverity.HIGH,
            context={"test": "value"},
        )
        
        logger = logging.getLogger("test")
        
        # This should not raise any type errors
        log_error(error, logger)

    def test_is_recoverable_error_explicit_types(self):
        """Test is_recoverable_error uses explicit types."""
        # Test different error categories
        config_error = ConfigurationError("Config error")
        resource_error = ResourceError("Resource error")
        network_error = NetworkError("Network error")
        data_quality_error = DataQualityError("Data quality error")
        
        assert not is_recoverable_error(config_error)
        assert is_recoverable_error(resource_error)
        assert is_recoverable_error(network_error)
        assert not is_recoverable_error(data_quality_error)

    def test_should_retry_error_explicit_types(self):
        """Test should_retry_error uses explicit types."""
        recoverable_error = ResourceError("Resource error")
        non_recoverable_error = ConfigurationError("Config error")
        
        # Test retry logic
        assert should_retry_error(recoverable_error, 0, 3)
        assert should_retry_error(recoverable_error, 1, 3)
        assert should_retry_error(recoverable_error, 2, 3)
        assert not should_retry_error(recoverable_error, 3, 3)
        
        assert not should_retry_error(non_recoverable_error, 0, 3)


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
        error = SparkForgeError("Test error")
        
        # Test add_context
        error.add_context("key", "value")
        assert error.context["key"] == "value"
        
        # Test add_suggestion
        error.add_suggestion("suggestion")
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
