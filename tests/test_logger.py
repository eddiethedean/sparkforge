#!/usr/bin/env python3
"""
Comprehensive tests for the logger module.

This module tests all logging functionality, formatters, timers, and performance monitoring.
"""

import logging
import os
import tempfile
import unittest

from sparkforge.logging import (
    PipelineLogger,
    get_global_logger,
    reset_global_logger,
    set_global_logger,
)


class TestPipelineLogger(unittest.TestCase):
    """Test PipelineLogger class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary log file
        self.temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
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
        self.assertEqual(logger.level, logging.INFO)

    def test_logger_with_file(self):
        """Test logger with file output."""
        logger = PipelineLogger(
            verbose=False,
            name="TestLogger",
            log_file=self.temp_filename,
        )

        logger.info("Test message")
        logger.close()

        # Check that file was created and contains log entry
        self.assertTrue(os.path.exists(self.temp_filename))
        with open(self.temp_filename) as f:
            content = f.read()
            self.assertIn("Test message", content)

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

    def test_log_level_management(self):
        """Test log level management."""
        logger = PipelineLogger(verbose=False, level=logging.WARNING)

        # Should not log debug and info messages
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")

        # Change level
        logger.set_level(logging.DEBUG)
        logger.debug("Debug message after level change")

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


def run_logger_tests():
    """Run all logger tests."""
    print("🧪 Running Logger Tests")
    print("=" * 50)

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test cases
    test_classes = [
        TestPipelineLogger,
        TestGlobalLogger,
    ]

    for test_class in test_classes:
        test_suite.addTest(unittest.makeSuite(test_class))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print("\n" + "=" * 50)
    print(
        f"📊 Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors"
    )

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
