"""
Test type safety for the refactored logger system.

This module tests that all logger methods use explicit types instead of Any,
*args, or **kwargs, and that the new context-based API works correctly.
"""

import pytest
from datetime import datetime
from sparkforge.logger import (
    PipelineLogger,
    LogContext,
    PerformanceContext,
    ValidationContext,
    LogValue,
    ContextData,
)


class TestLoggerTypeSafety:
    """Test that logger methods use explicit types."""

    def test_logger_initialization(self):
        """Test that logger initializes with correct types."""
        logger = PipelineLogger()
        assert isinstance(logger, PipelineLogger)
        assert hasattr(logger, '_context_stack')
        assert isinstance(logger._context_stack, list)

    def test_basic_logging_methods_explicit_types(self):
        """Test that basic logging methods use explicit LogContext."""
        logger = PipelineLogger()
        
        # Test with explicit LogContext
        context = LogContext(
            pipeline_id="test_pipeline",
            step_id="test_step",
            execution_id="test_execution",
            user_id="test_user",
            timestamp=datetime.utcnow(),
            metadata={"test_key": "test_value"}
        )
        
        # These should not raise type errors
        logger.debug("Debug message", context)
        logger.info("Info message", context)
        logger.warning("Warning message", context)
        logger.error("Error message", context)
        logger.critical("Critical message", context)

    def test_basic_logging_methods_optional_context(self):
        """Test that basic logging methods work with optional context."""
        logger = PipelineLogger()
        
        # These should work without context
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")

    def test_pipeline_logging_methods_explicit_types(self):
        """Test that pipeline logging methods use explicit LogContext."""
        logger = PipelineLogger()
        
        context = LogContext(
            pipeline_id="test_pipeline",
            execution_id="test_execution"
        )
        
        # Test pipeline methods
        logger.pipeline_start("test_pipeline", "development", context)
        logger.pipeline_end("test_pipeline", 10.5, True, context)
        
        # Test step methods
        logger.step_start("bronze", "load_data", context)
        logger.step_complete("bronze", "load_data", 5.2, 1000, context)
        logger.step_skipped("silver", "transform_data", "No data", context)
        logger.step_failed("gold", "aggregate_data", "Connection error", 2.1, context)

    def test_validation_logging_methods_explicit_types(self):
        """Test that validation logging methods use explicit ValidationContext."""
        logger = PipelineLogger()
        
        context = ValidationContext(
            stage="bronze",
            step="load_data",
            validation_rate=95.5,
            threshold=90.0,
            passed=True,
            metadata={"source": "test"}
        )
        
        # Test validation methods
        logger.validation_passed("bronze", "load_data", 95.5, 90.0, context)
        logger.validation_failed("silver", "transform_data", 85.0, 90.0, context)

    def test_performance_logging_methods_explicit_types(self):
        """Test that performance logging methods use explicit PerformanceContext."""
        logger = PipelineLogger()
        
        context = PerformanceContext(
            operation_name="data_processing",
            duration=10.5,
            memory_usage=512.0,
            cpu_usage=75.0,
            rows_processed=10000,
            metadata={"source": "test"}
        )
        
        # Test performance method
        logger.performance_metric("processing_time", 10.5, "s", context)

    def test_parallel_logging_methods_explicit_types(self):
        """Test that parallel logging methods use explicit LogContext."""
        logger = PipelineLogger()
        
        context = LogContext(
            pipeline_id="test_pipeline",
            execution_id="test_execution"
        )
        
        # Test parallel methods
        logger.parallel_start(["step1", "step2"], 1, context)
        logger.parallel_complete("step1", context)

    def test_data_quality_logging_explicit_types(self):
        """Test that data quality logging uses explicit LogContext."""
        logger = PipelineLogger()
        
        context = LogContext(
            pipeline_id="test_pipeline",
            step_id="quality_check"
        )
        
        # Test data quality method
        logger.data_quality_report(
            "bronze", "load_data", 85.5, ["missing_values", "duplicates"], context
        )

    def test_dependency_analysis_explicit_types(self):
        """Test that dependency analysis uses explicit LogContext."""
        logger = PipelineLogger()
        
        context = LogContext(
            pipeline_id="test_pipeline",
            execution_id="test_execution"
        )
        
        # Test dependency analysis
        groups = {1: ["step1"], 2: ["step2", "step3"]}
        logger.dependency_analysis(groups, context)

    def test_execution_summary_explicit_types(self):
        """Test that execution summary uses explicit LogContext."""
        logger = PipelineLogger()
        
        context = LogContext(
            pipeline_id="test_pipeline",
            execution_id="test_execution"
        )
        
        # Test execution summary
        logger.execution_summary("development", 30.5, 50000, 98.5, context)

    def test_context_management_explicit_types(self):
        """Test that context management uses explicit LogValue types."""
        logger = PipelineLogger()
        
        # Test context manager with explicit LogValue types
        with logger.context(
            pipeline_id="test_pipeline",
            step_id="test_step",
            execution_id="test_execution",
            user_id="test_user",
            timestamp=datetime.utcnow().isoformat(),
            metadata={"test_key": "test_value"}
        ):
            logger.info("Message with context")
        
        # Test set_context with explicit LogContext
        context = LogContext(
            pipeline_id="test_pipeline",
            step_id="test_step",
            execution_id="test_execution",
            user_id="test_user",
            timestamp=datetime.utcnow(),
            metadata={"test_key": "test_value"}
        )
        logger.set_context(context)

    def test_log_value_type_validation(self):
        """Test that LogValue type accepts only valid types."""
        # Valid LogValue types
        valid_values: LogValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = ["item1", "item2"]
        valid_values = {"key": "value"}
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, list, dict))

    def test_context_data_type_validation(self):
        """Test that ContextData type accepts only valid LogValue types."""
        # Valid ContextData
        context_data: ContextData = {
            "string_key": "string_value",
            "int_key": 42,
            "float_key": 3.14,
            "bool_key": True,
            "list_key": ["item1", "item2"],
            "dict_key": {"nested": "value"}
        }
        
        # This should not raise type errors
        assert isinstance(context_data, dict)
        for key, value in context_data.items():
            assert isinstance(value, (str, int, float, bool, list, dict))

    def test_log_context_dataclass(self):
        """Test that LogContext dataclass works correctly."""
        context = LogContext(
            pipeline_id="test_pipeline",
            step_id="test_step",
            execution_id="test_execution",
            user_id="test_user",
            timestamp=datetime.utcnow(),
            metadata={"test_key": "test_value"}
        )
        
        assert context.pipeline_id == "test_pipeline"
        assert context.step_id == "test_step"
        assert context.execution_id == "test_execution"
        assert context.user_id == "test_user"
        assert isinstance(context.timestamp, datetime)
        assert context.metadata == {"test_key": "test_value"}

    def test_performance_context_dataclass(self):
        """Test that PerformanceContext dataclass works correctly."""
        context = PerformanceContext(
            operation_name="test_operation",
            duration=10.5,
            memory_usage=512.0,
            cpu_usage=75.0,
            rows_processed=10000,
            metadata={"source": "test"}
        )
        
        assert context.operation_name == "test_operation"
        assert context.duration == 10.5
        assert context.memory_usage == 512.0
        assert context.cpu_usage == 75.0
        assert context.rows_processed == 10000
        assert context.metadata == {"source": "test"}

    def test_validation_context_dataclass(self):
        """Test that ValidationContext dataclass works correctly."""
        context = ValidationContext(
            stage="bronze",
            step="load_data",
            validation_rate=95.5,
            threshold=90.0,
            passed=True,
            metadata={"source": "test"}
        )
        
        assert context.stage == "bronze"
        assert context.step == "load_data"
        assert context.validation_rate == 95.5
        assert context.threshold == 90.0
        assert context.passed is True
        assert context.metadata == {"source": "test"}

    def test_context_merging_behavior(self):
        """Test that context merging works correctly."""
        logger = PipelineLogger()
        
        # Create base context
        base_context = LogContext(
            pipeline_id="base_pipeline",
            execution_id="base_execution"
        )
        
        # Test pipeline_start with context merging
        logger.pipeline_start("test_pipeline", "development", base_context)
        
        # Test step_start with context merging
        logger.step_start("bronze", "load_data", base_context)
        
        # Test validation with ValidationContext
        validation_context = ValidationContext(
            stage="bronze",
            step="load_data",
            validation_rate=95.5,
            threshold=90.0,
            passed=True
        )
        
        logger.validation_passed("bronze", "load_data", 95.5, 90.0, validation_context)

    def test_execution_timer_explicit_types(self):
        """Test that ExecutionTimer uses explicit LogValue types."""
        from sparkforge.logger import ExecutionTimer
        logger = PipelineLogger()
        
        # Test ExecutionTimer with explicit LogValue types
        with ExecutionTimer(logger, "test_operation", 
                           pipeline_id="test_pipeline",
                           step_id="test_step",
                           execution_id="test_execution",
                           user_id="test_user",
                           timestamp=datetime.utcnow().isoformat(),
                           metadata={"test_key": "test_value"}):
            logger.info("Operation in progress")

    def test_log_performance_decorator_explicit_types(self):
        """Test that log_performance decorator uses explicit Callable types."""
        from sparkforge.logger import log_performance
        
        @log_performance("test_function", log_args=True)
        def test_function(param1: str, param2: int) -> str:
            return f"{param1}_{param2}"
        
        # This should work with explicit types
        result = test_function("test", 42)
        assert result == "test_42"

    def test_no_any_types_in_logger(self):
        """Test that no Any types are used in logger methods."""
        import inspect
        from sparkforge.logger import PipelineLogger
        
        logger = PipelineLogger()
        
        # Get all methods from PipelineLogger
        methods = [method for method in dir(logger) 
                  if not method.startswith('_') and callable(getattr(logger, method))]
        
        for method_name in methods:
            method = getattr(logger, method_name)
            if hasattr(method, '__annotations__'):
                annotations = method.__annotations__
                # Check that no parameter uses Any type
                for param_name, param_type in annotations.items():
                    if param_name != 'return':
                        assert param_type != 'Any', f"Method {method_name} uses Any type for parameter {param_name}"

    def test_no_args_kwargs_in_logger_methods(self):
        """Test that no logger methods use *args or **kwargs (except context manager)."""
        import inspect
        from sparkforge.logger import PipelineLogger
        
        logger = PipelineLogger()
        
        # Get all methods from PipelineLogger
        methods = [method for method in dir(logger) 
                  if not method.startswith('_') and callable(getattr(logger, method))]
        
        # Exclude context manager methods that legitimately use *args
        excluded_methods = {'context'}
        
        for method_name in methods:
            if method_name in excluded_methods:
                continue
                
            method = getattr(logger, method_name)
            if hasattr(method, '__code__'):
                code = method.__code__
                # Check that no method uses *args or **kwargs
                assert not code.co_flags & 0x04, f"Method {method_name} uses *args"
                assert not code.co_flags & 0x08, f"Method {method_name} uses **kwargs"


class TestLoggerBackwardCompatibility:
    """Test backward compatibility for logger changes."""

    def test_old_api_still_works(self):
        """Test that old API calls still work (should be deprecated but functional)."""
        logger = PipelineLogger()
        
        # These should still work but may show deprecation warnings
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")

    def test_context_parameter_optional(self):
        """Test that context parameter is optional for all methods."""
        logger = PipelineLogger()
        
        # All methods should work without context parameter
        logger.pipeline_start("test_pipeline", "development")
        logger.pipeline_end("test_pipeline", 10.5, True)
        logger.step_start("bronze", "load_data")
        logger.step_complete("bronze", "load_data", 5.2, 1000)
        logger.step_skipped("silver", "transform_data", "No data")
        logger.step_failed("gold", "aggregate_data", "Connection error", 2.1)
        logger.parallel_start(["step1", "step2"], 1)
        logger.parallel_complete("step1")
        logger.validation_passed("bronze", "load_data", 95.5, 90.0)
        logger.validation_failed("silver", "transform_data", 85.0, 90.0)
        logger.performance_metric("processing_time", 10.5, "s")
        logger.execution_summary("development", 30.5, 50000, 98.5)
        logger.data_quality_report("bronze", "load_data", 85.5, ["missing_values"])
        logger.dependency_analysis({1: ["step1"], 2: ["step2", "step3"]})


if __name__ == "__main__":
    pytest.main([__file__])
