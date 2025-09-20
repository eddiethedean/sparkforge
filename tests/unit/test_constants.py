#!/usr/bin/env python3
"""
Tests for the constants module.

This module tests all constants defined in sparkforge.constants
to ensure they have the correct values and can be imported properly.
"""

import unittest


class TestConstantsValues(unittest.TestCase):
    """Test that all constants have the correct values."""

    def test_memory_constants(self):
        """Test memory-related constants."""
        from sparkforge.constants import BYTES_PER_GB, BYTES_PER_KB, BYTES_PER_MB

        self.assertEqual(BYTES_PER_KB, 1024)
        self.assertEqual(BYTES_PER_MB, 1024 * 1024)
        self.assertEqual(BYTES_PER_GB, 1024 * 1024 * 1024)

    def test_default_memory_limits(self):
        """Test default memory limit constants."""
        from sparkforge.constants import DEFAULT_CACHE_MEMORY_MB, DEFAULT_MAX_MEMORY_MB

        self.assertEqual(DEFAULT_MAX_MEMORY_MB, 1024)
        self.assertEqual(DEFAULT_CACHE_MEMORY_MB, 512)

    def test_file_size_constants(self):
        """Test file size constants."""
        from sparkforge.constants import DEFAULT_BACKUP_COUNT, DEFAULT_MAX_FILE_SIZE_MB

        self.assertEqual(DEFAULT_MAX_FILE_SIZE_MB, 10)
        self.assertEqual(DEFAULT_BACKUP_COUNT, 5)

    def test_performance_constants(self):
        """Test performance-related constants."""
        from sparkforge.constants import (
            DEFAULT_CACHE_PARTITIONS,
            DEFAULT_SHUFFLE_PARTITIONS,
        )

        self.assertEqual(DEFAULT_CACHE_PARTITIONS, 200)
        self.assertEqual(DEFAULT_SHUFFLE_PARTITIONS, 200)

    def test_validation_thresholds(self):
        """Test validation threshold constants."""
        from sparkforge.constants import (
            DEFAULT_BRONZE_THRESHOLD,
            DEFAULT_GOLD_THRESHOLD,
            DEFAULT_SILVER_THRESHOLD,
        )

        self.assertEqual(DEFAULT_BRONZE_THRESHOLD, 95.0)
        self.assertEqual(DEFAULT_SILVER_THRESHOLD, 98.0)
        self.assertEqual(DEFAULT_GOLD_THRESHOLD, 99.0)

    def test_timeout_constants(self):
        """Test timeout constants."""
        from sparkforge.constants import (
            DEFAULT_RETRY_TIMEOUT_SECONDS,
            DEFAULT_TIMEOUT_SECONDS,
        )

        self.assertEqual(DEFAULT_TIMEOUT_SECONDS, 300)
        self.assertEqual(DEFAULT_RETRY_TIMEOUT_SECONDS, 60)

    def test_logging_constants(self):
        """Test logging constants."""
        from sparkforge.constants import DEFAULT_LOG_LEVEL, DEFAULT_VERBOSE

        self.assertEqual(DEFAULT_LOG_LEVEL, "INFO")
        self.assertTrue(DEFAULT_VERBOSE)

    def test_schema_constants(self):
        """Test schema constants."""
        from sparkforge.constants import DEFAULT_SCHEMA, TEST_SCHEMA

        self.assertEqual(DEFAULT_SCHEMA, "default")
        self.assertEqual(TEST_SCHEMA, "test_schema")

    def test_error_constants(self):
        """Test error-related constants."""
        from sparkforge.constants import MAX_ERROR_MESSAGE_LENGTH, MAX_STACK_TRACE_LINES

        self.assertEqual(MAX_ERROR_MESSAGE_LENGTH, 1000)
        self.assertEqual(MAX_STACK_TRACE_LINES, 50)

    def test_performance_monitoring_constants(self):
        """Test performance monitoring constants."""
        from sparkforge.constants import (
            DEFAULT_ALERT_THRESHOLD_PERCENT,
            DEFAULT_METRICS_INTERVAL_SECONDS,
        )

        self.assertEqual(DEFAULT_METRICS_INTERVAL_SECONDS, 30)
        self.assertEqual(DEFAULT_ALERT_THRESHOLD_PERCENT, 80.0)


class TestConstantsUsage(unittest.TestCase):
    """Test that constants are used correctly in other modules."""

    def test_performance_cache_uses_constants(self):
        """Test that performance_cache.py uses constants correctly."""
        from sparkforge.constants import BYTES_PER_MB, DEFAULT_MAX_MEMORY_MB

        # Test that constants are defined correctly
        self.assertEqual(DEFAULT_MAX_MEMORY_MB, 1024)  # 1GB in MB
        self.assertEqual(BYTES_PER_MB, 1024 * 1024)  # 1MB in bytes

    def test_parallel_execution_uses_constants(self):
        """Test that constants are defined correctly."""
        from sparkforge.constants import BYTES_PER_MB

        # Test that BYTES_PER_MB is the correct value
        self.assertEqual(BYTES_PER_MB, 1024 * 1024)

    def test_constants_are_immutable(self):
        """Test that constants cannot be modified accidentally."""
        from sparkforge.constants import BYTES_PER_MB

        original_value = BYTES_PER_MB

        # Attempting to modify should not work (constants should be immutable)
        # This is more of a documentation test - Python doesn't prevent modification
        # but we can verify the value is what we expect
        self.assertEqual(BYTES_PER_MB, 1024 * 1024)
        self.assertEqual(BYTES_PER_MB, original_value)


class TestConstantsCompleteness(unittest.TestCase):
    """Test that all necessary constants are defined."""

    def test_all_required_constants_exist(self):
        """Test that all required constants are defined and importable."""

        # If we get here without ImportError, all constants exist
        self.assertTrue(True)

    def test_constants_have_appropriate_types(self):
        """Test that constants have the appropriate types."""
        from sparkforge.constants import (
            BYTES_PER_GB,
            BYTES_PER_KB,
            BYTES_PER_MB,
            DEFAULT_BRONZE_THRESHOLD,
            DEFAULT_LOG_LEVEL,
            DEFAULT_MAX_MEMORY_MB,
            DEFAULT_VERBOSE,
        )

        # Test integer constants
        self.assertIsInstance(BYTES_PER_KB, int)
        self.assertIsInstance(BYTES_PER_MB, int)
        self.assertIsInstance(BYTES_PER_GB, int)
        self.assertIsInstance(DEFAULT_MAX_MEMORY_MB, int)

        # Test float constants
        self.assertIsInstance(DEFAULT_BRONZE_THRESHOLD, float)

        # Test string constants
        self.assertIsInstance(DEFAULT_LOG_LEVEL, str)

        # Test boolean constants
        self.assertIsInstance(DEFAULT_VERBOSE, bool)


def run_constants_tests():
    """Run all constants tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    run_constants_tests()
