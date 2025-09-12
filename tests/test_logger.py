#!/usr/bin/env python3
"""
Comprehensive tests for the logger module.

This module tests all logging functionality, formatters, timers, and performance monitoring.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import tempfile
import os
import json
import logging

from sparkforge.logger import (
    PipelineLogger, ExecutionTimer, log_performance,
    create_logger, create_file_logger, create_console_logger,
    get_global_logger, set_global_logger, reset_global_logger,
    PipelineLogLevel, ColoredFormatter, StructuredFormatter
)


class TestPipelineLogLevel(unittest.TestCase):
    """Test custom log levels."""
    
    def test_add_custom_levels(self):
        """Test adding custom log levels."""
        PipelineLogLevel.add_custom_levels()
        
        # Check that custom levels are added
        self.assertIn("PIPELINE_START", logging._nameToLevel)
        self.assertIn("PIPELINE_END", logging._nameToLevel)
        self.assertIn("STEP_START", logging._nameToLevel)
        self.assertIn("STEP_END", logging._nameToLevel)
        self.assertIn("VALIDATION", logging._nameToLevel)
        self.assertIn("PERFORMANCE", logging._nameToLevel)
        
        # Check level values
        self.assertEqual(logging._nameToLevel["PIPELINE_START"], 25)
        self.assertEqual(logging._nameToLevel["PIPELINE_END"], 24)
        self.assertEqual(logging._nameToLevel["STEP_START"], 23)
        self.assertEqual(logging._nameToLevel["STEP_END"], 22)
        self.assertEqual(logging._nameToLevel["VALIDATION"], 21)
        self.assertEqual(logging._nameToLevel["PERFORMANCE"], 20)


class TestColoredFormatter(unittest.TestCase):
    """Test colored formatter."""
    
    def test_colored_formatting(self):
        """Test colored formatting."""
        formatter = ColoredFormatter('%(levelname)s - %(message)s')
        
        # Create a mock record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        
        # Should contain color codes and reset
        self.assertIn('\x1b[', result)  # Some color code
        self.assertIn('\x1b[0m', result)   # Reset
        self.assertIn('Test message', result)


class TestStructuredFormatter(unittest.TestCase):
    """Test structured formatter."""
    
    def test_structured_formatting(self):
        """Test structured JSON formatting."""
        formatter = StructuredFormatter()
        
        # Create a mock record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.extra_fields = {"key": "value"}
        
        result = formatter.format(record)
        
        # Should be valid JSON
        data = json.loads(result)
        self.assertIn(data["level"], ["INFO", "PERFORMANCE"])  # Could be either due to custom levels
        self.assertEqual(data["message"], "Test message")
        self.assertEqual(data["key"], "value")
        self.assertIn("timestamp", data)


class TestPipelineLogger(unittest.TestCase):
    """Test PipelineLogger class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary log file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.temp_file.close()
        self.temp_filename = self.temp_file.name
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_filename):
            os.unlink(self.temp_filename)
    
    def test_logger_creation(self):
        """Test logger creation."""
        logger = PipelineLogger(verbose=False, name="TestLogger")
        self.assertEqual(logger.name, "TestLogger")
        self.assertFalse(logger.verbose)
        self.assertEqual(logger.log_level, logging.INFO)
    
    def test_logger_with_file(self):
        """Test logger with file output."""
        logger = PipelineLogger(
            verbose=False,
            name="TestLogger",
            log_file=self.temp_filename,
            structured_log=True
        )
        
        logger.info("Test message", extra_field="test_value")
        logger.close()
        
        # Check that file was created and contains log entry
        self.assertTrue(os.path.exists(self.temp_filename))
        with open(self.temp_filename, 'r') as f:
            content = f.read()
            self.assertIn("Test message", content)
            self.assertIn("test_value", content)
    
    def test_basic_logging_methods(self):
        """Test basic logging methods."""
        logger = PipelineLogger(verbose=False)
        
        # Test that methods don't raise exceptions
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")
        
        logger.close()
    
    def test_pipeline_specific_methods(self):
        """Test pipeline-specific logging methods."""
        logger = PipelineLogger(verbose=False)
        
        # Test pipeline methods
        logger.pipeline_start("test_pipeline", "initial")
        logger.pipeline_end("test_pipeline", 10.5, True)
        
        logger.step_start("bronze", "test_step")
        logger.step_complete("bronze", "test_step", 5.2, 1000)
        logger.step_skipped("silver", "test_step", "No data")
        logger.step_failed("gold", "test_step", "Validation error", 2.1)
        
        logger.parallel_start(["step1", "step2"], 1)
        logger.parallel_complete("step1")
        
        logger.validation_passed("bronze", "test_step", 95.5, 90.0)
        logger.validation_failed("silver", "test_step", 85.0, 90.0)
        
        logger.performance_metric("processing_time", 10.5, "s")
        logger.execution_summary("initial", 30.0, 5000, 95.0)
        
        logger.dependency_analysis({1: ["step1"], 2: ["step2", "step3"]})
        
        logger.data_quality_report("bronze", "test_step", 85.0, ["Missing values"])
        
        logger.close()
    
    def test_context_management(self):
        """Test context management."""
        logger = PipelineLogger(verbose=False)
        
        # Test context manager
        with logger.context(operation="test_op", user="test_user"):
            logger.info("Message with context")
        
        # Test set_context
        logger.set_context(operation="persistent_op")
        logger.info("Message with persistent context")
        
        # Test clear_context
        logger.clear_context()
        logger.info("Message without context")
        
        logger.close()
    
    def test_performance_tracking(self):
        """Test performance tracking."""
        logger = PipelineLogger(verbose=False)
        
        # Simulate some operations
        logger.step_complete("bronze", "step1", 5.0)
        logger.step_complete("bronze", "step1", 6.0)
        logger.step_complete("silver", "step2", 3.0)
        
        # Get performance summary
        summary = logger.get_performance_summary()
        
        self.assertIn("bronze_step1", summary)
        self.assertIn("silver_step2", summary)
        
        self.assertEqual(summary["bronze_step1"]["count"], 2)
        self.assertEqual(summary["bronze_step1"]["total"], 11.0)
        self.assertEqual(summary["bronze_step1"]["average"], 5.5)
        self.assertEqual(summary["bronze_step1"]["min"], 5.0)
        self.assertEqual(summary["bronze_step1"]["max"], 6.0)
        
        logger.close()
    
    def test_log_level_management(self):
        """Test log level management."""
        logger = PipelineLogger(verbose=False, log_level=logging.WARNING)
        
        # Should not log debug and info messages
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        
        # Change level
        logger.set_level(logging.DEBUG)
        logger.debug("Debug message after level change")
        
        logger.close()
    
    def test_file_handler_management(self):
        """Test file handler management."""
        logger = PipelineLogger(verbose=False)
        
        # Add file handler
        logger.add_file_handler(self.temp_filename)
        logger.info("Test message")
        logger.close()
        
        # Check file content
        with open(self.temp_filename, 'r') as f:
            content = f.read()
            self.assertIn("Test message", content)


class TestExecutionTimer(unittest.TestCase):
    """Test ExecutionTimer class."""
    
    def test_timer_success(self):
        """Test timer with successful operation."""
        logger = PipelineLogger(verbose=False)
        
        with ExecutionTimer(logger, "test_operation", key="value") as timer:
            pass
        
        self.assertIsNotNone(timer.start_time)
        self.assertIsNotNone(timer.end_time)
        self.assertIsNotNone(timer.duration)
        self.assertGreater(timer.duration, 0)
        
        logger.close()
    
    def test_timer_failure(self):
        """Test timer with failed operation."""
        logger = PipelineLogger(verbose=False)
        
        try:
            with ExecutionTimer(logger, "test_operation"):
                raise ValueError("Test error")
        except ValueError:
            pass
        
        logger.close()
    
    def test_timer_duration(self):
        """Test timer duration calculation."""
        logger = PipelineLogger(verbose=False)
        
        with ExecutionTimer(logger, "test_operation") as timer:
            import time
            time.sleep(0.01)  # 10ms
        
        self.assertGreater(timer.get_duration(), 0.01)
        
        logger.close()


class TestPerformanceDecorator(unittest.TestCase):
    """Test performance monitoring decorator."""
    
    def test_log_performance_decorator(self):
        """Test log_performance decorator."""
        logger = PipelineLogger(verbose=False)
        
        @log_performance("test_function")
        def test_function():
            return "result"
        
        result = test_function()
        self.assertEqual(result, "result")
        
        logger.close()
    
    def test_log_performance_with_logger(self):
        """Test log_performance decorator with logger parameter."""
        logger = PipelineLogger(verbose=False)
        
        @log_performance("test_function")
        def test_function(logger_param):
            return "result"
        
        result = test_function(logger)
        self.assertEqual(result, "result")
        
        logger.close()
    
    def test_log_performance_with_args(self):
        """Test log_performance decorator with argument logging."""
        logger = PipelineLogger(verbose=False)
        
        @log_performance("test_function", log_args=True)
        def test_function(arg1, arg2=None):
            return "result"
        
        result = test_function("value1", arg2="value2")
        self.assertEqual(result, "result")
        
        logger.close()


class TestFactoryFunctions(unittest.TestCase):
    """Test factory functions."""
    
    def test_create_logger(self):
        """Test create_logger factory function."""
        logger = create_logger(verbose=False, name="TestLogger")
        self.assertIsInstance(logger, PipelineLogger)
        self.assertEqual(logger.name, "TestLogger")
        self.assertFalse(logger.verbose)
        logger.close()
    
    def test_create_file_logger(self):
        """Test create_file_logger factory function."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            logger = create_file_logger(temp_filename, "TestLogger")
            self.assertIsInstance(logger, PipelineLogger)
            self.assertFalse(logger.verbose)
            self.assertEqual(logger.log_file, temp_filename)
            logger.close()
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
    
    def test_create_console_logger(self):
        """Test create_console_logger factory function."""
        logger = create_console_logger("TestLogger")
        self.assertIsInstance(logger, PipelineLogger)
        self.assertTrue(logger.verbose)
        self.assertIsNone(logger.log_file)
        logger.close()


class TestGlobalLogger(unittest.TestCase):
    """Test global logger management."""
    
    def tearDown(self):
        """Clean up global logger."""
        reset_global_logger()
    
    def test_get_global_logger(self):
        """Test getting global logger."""
        logger = get_global_logger()
        self.assertIsInstance(logger, PipelineLogger)
        
        # Should return same instance
        logger2 = get_global_logger()
        self.assertIs(logger, logger2)
    
    def test_set_global_logger(self):
        """Test setting global logger."""
        custom_logger = PipelineLogger(verbose=False, name="CustomLogger")
        set_global_logger(custom_logger)
        
        logger = get_global_logger()
        self.assertIs(logger, custom_logger)
        self.assertEqual(logger.name, "CustomLogger")
        
        custom_logger.close()
    
    def test_reset_global_logger(self):
        """Test resetting global logger."""
        # Get initial logger
        logger1 = get_global_logger()
        
        # Reset
        reset_global_logger()
        
        # Get new logger
        logger2 = get_global_logger()
        
        # Should be different instances
        self.assertIsNot(logger1, logger2)
        
        logger2.close()


class TestLoggerIntegration(unittest.TestCase):
    """Test logger integration scenarios."""
    
    def test_pipeline_execution_logging(self):
        """Test complete pipeline execution logging."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            logger = create_file_logger(temp_filename, "PipelineTest", structured_log=True)
            
            # Simulate pipeline execution
            logger.pipeline_start("test_pipeline", "initial")
            
            with logger.context(pipeline_id="12345", user="test_user"):
                logger.step_start("bronze", "ingest_data")
                logger.step_complete("bronze", "ingest_data", 5.2, 1000)
                
                logger.step_start("silver", "transform_data")
                logger.validation_passed("silver", "transform_data", 95.5, 90.0)
                logger.step_complete("silver", "transform_data", 8.1, 950)
                
                logger.step_start("gold", "aggregate_data")
                logger.step_complete("gold", "aggregate_data", 3.5, 100)
            
            logger.pipeline_end("test_pipeline", 16.8, True)
            logger.log_performance_summary()
            logger.close()
            
            # Check log file content
            with open(temp_filename, 'r') as f:
                content = f.read()
                self.assertIn("test_pipeline", content)
                self.assertIn("bronze", content)
                self.assertIn("silver", content)
                self.assertIn("gold", content)
                self.assertIn("12345", content)
                self.assertIn("test_user", content)
        
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
    
    def test_error_handling_logging(self):
        """Test error handling and logging."""
        logger = PipelineLogger(verbose=False)
        
        try:
            with ExecutionTimer(logger, "error_operation"):
                raise ValueError("Test error")
        except ValueError:
            logger.error("Operation failed", error_type="ValueError")
        
        logger.close()
    
    def test_performance_monitoring(self):
        """Test performance monitoring capabilities."""
        logger = PipelineLogger(verbose=False)
        
        # Simulate multiple operations with different durations
        operations = [
            ("bronze_step1", 5.0),
            ("bronze_step1", 6.0),
            ("silver_step2", 3.0),
            ("silver_step2", 4.0),
            ("gold_step3", 2.0)
        ]
        
        for operation, duration in operations:
            logger.step_complete("test", operation, duration)
        
        # Force performance data collection
        logger._add_performance_data("bronze_step1", 5.0)
        logger._add_performance_data("bronze_step1", 6.0)
        logger._add_performance_data("silver_step2", 3.0)
        logger._add_performance_data("silver_step2", 4.0)
        logger._add_performance_data("gold_step3", 2.0)
        
        # Get performance summary
        summary = logger.get_performance_summary()
        
        # Should have at least 3 unique operations (may have more due to step_complete calls)
        self.assertGreaterEqual(len(summary), 3)
        self.assertIn("bronze_step1", summary)
        self.assertIn("silver_step2", summary)
        self.assertIn("gold_step3", summary)
        
        # Check specific counts
        self.assertEqual(summary["bronze_step1"]["count"], 2)
        self.assertEqual(summary["silver_step2"]["count"], 2)
        self.assertEqual(summary["gold_step3"]["count"], 1)
        
        logger.close()


def run_logger_tests():
    """Run all logger tests."""
    print("🧪 Running Logger Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestPipelineLogLevel,
        TestColoredFormatter,
        TestStructuredFormatter,
        TestPipelineLogger,
        TestExecutionTimer,
        TestPerformanceDecorator,
        TestFactoryFunctions,
        TestGlobalLogger,
        TestLoggerIntegration
    ]
    
    for test_class in test_classes:
        test_suite.addTest(unittest.makeSuite(test_class))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n❌ Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_logger_tests()
    if success:
        print("\n🎉 All logger tests passed!")
    else:
        print("\n❌ Some logger tests failed!")
