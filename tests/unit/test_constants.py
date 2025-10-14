"""
Tests for sparkforge.constants module.

This module tests all constants and configuration values.
"""

from sparkforge.constants import (
    BYTES_PER_GB,
    # Memory and Size Constants
    BYTES_PER_KB,
    BYTES_PER_MB,
    DEFAULT_ALERT_THRESHOLD_PERCENT,
    DEFAULT_BACKUP_COUNT,
    # Validation Constants
    DEFAULT_BRONZE_THRESHOLD,
    DEFAULT_CACHE_MEMORY_MB,
    # Performance Constants
    DEFAULT_CACHE_PARTITIONS,
    DEFAULT_GOLD_THRESHOLD,
    # Logging Constants
    DEFAULT_LOG_LEVEL,
    # File Size Constants
    DEFAULT_MAX_FILE_SIZE_MB,
    # Default Memory Limits
    DEFAULT_MAX_MEMORY_MB,
    # Performance Monitoring Constants
    DEFAULT_METRICS_INTERVAL_SECONDS,
    DEFAULT_RETRY_TIMEOUT_SECONDS,
    # Schema Constants
    DEFAULT_SCHEMA,
    DEFAULT_SHUFFLE_PARTITIONS,
    DEFAULT_SILVER_THRESHOLD,
    # Timeout Constants
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_VERBOSE,
    # Error Constants
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_STACK_TRACE_LINES,
    TEST_SCHEMA,
)


class TestMemoryConstants:
    """Test memory and size constants."""

    def test_bytes_per_kb(self):
        """Test BYTES_PER_KB constant."""
        assert BYTES_PER_KB == 1024

    def test_bytes_per_mb(self):
        """Test BYTES_PER_MB constant."""
        assert BYTES_PER_MB == 1024 * 1024
        assert BYTES_PER_MB == BYTES_PER_KB * 1024

    def test_bytes_per_gb(self):
        """Test BYTES_PER_GB constant."""
        assert BYTES_PER_GB == 1024 * 1024 * 1024
        assert BYTES_PER_GB == BYTES_PER_MB * 1024


class TestDefaultMemoryLimits:
    """Test default memory limit constants."""

    def test_default_max_memory_mb(self):
        """Test DEFAULT_MAX_MEMORY_MB constant."""
        assert DEFAULT_MAX_MEMORY_MB == 1024
        assert isinstance(DEFAULT_MAX_MEMORY_MB, int)

    def test_default_cache_memory_mb(self):
        """Test DEFAULT_CACHE_MEMORY_MB constant."""
        assert DEFAULT_CACHE_MEMORY_MB == 512
        assert isinstance(DEFAULT_CACHE_MEMORY_MB, int)


class TestFileSizeConstants:
    """Test file size constants."""

    def test_default_max_file_size_mb(self):
        """Test DEFAULT_MAX_FILE_SIZE_MB constant."""
        assert DEFAULT_MAX_FILE_SIZE_MB == 10
        assert isinstance(DEFAULT_MAX_FILE_SIZE_MB, int)

    def test_default_backup_count(self):
        """Test DEFAULT_BACKUP_COUNT constant."""
        assert DEFAULT_BACKUP_COUNT == 5
        assert isinstance(DEFAULT_BACKUP_COUNT, int)


class TestPerformanceConstants:
    """Test performance constants."""

    def test_default_cache_partitions(self):
        """Test DEFAULT_CACHE_PARTITIONS constant."""
        assert DEFAULT_CACHE_PARTITIONS == 200
        assert isinstance(DEFAULT_CACHE_PARTITIONS, int)

    def test_default_shuffle_partitions(self):
        """Test DEFAULT_SHUFFLE_PARTITIONS constant."""
        assert DEFAULT_SHUFFLE_PARTITIONS == 200
        assert isinstance(DEFAULT_SHUFFLE_PARTITIONS, int)


class TestValidationConstants:
    """Test validation threshold constants."""

    def test_default_bronze_threshold(self):
        """Test DEFAULT_BRONZE_THRESHOLD constant."""
        assert DEFAULT_BRONZE_THRESHOLD == 95.0
        assert isinstance(DEFAULT_BRONZE_THRESHOLD, float)

    def test_default_silver_threshold(self):
        """Test DEFAULT_SILVER_THRESHOLD constant."""
        assert DEFAULT_SILVER_THRESHOLD == 98.0
        assert isinstance(DEFAULT_SILVER_THRESHOLD, float)

    def test_default_gold_threshold(self):
        """Test DEFAULT_GOLD_THRESHOLD constant."""
        assert DEFAULT_GOLD_THRESHOLD == 99.0
        assert isinstance(DEFAULT_GOLD_THRESHOLD, float)

    def test_threshold_ordering(self):
        """Test that thresholds are in ascending order."""
        assert DEFAULT_BRONZE_THRESHOLD < DEFAULT_SILVER_THRESHOLD
        assert DEFAULT_SILVER_THRESHOLD < DEFAULT_GOLD_THRESHOLD


class TestTimeoutConstants:
    """Test timeout constants."""

    def test_default_timeout_seconds(self):
        """Test DEFAULT_TIMEOUT_SECONDS constant."""
        assert DEFAULT_TIMEOUT_SECONDS == 300
        assert isinstance(DEFAULT_TIMEOUT_SECONDS, int)

    def test_default_retry_timeout_seconds(self):
        """Test DEFAULT_RETRY_TIMEOUT_SECONDS constant."""
        assert DEFAULT_RETRY_TIMEOUT_SECONDS == 60
        assert isinstance(DEFAULT_RETRY_TIMEOUT_SECONDS, int)

    def test_timeout_ordering(self):
        """Test that retry timeout is less than main timeout."""
        assert DEFAULT_RETRY_TIMEOUT_SECONDS < DEFAULT_TIMEOUT_SECONDS


class TestLoggingConstants:
    """Test logging constants."""

    def test_default_log_level(self):
        """Test DEFAULT_LOG_LEVEL constant."""
        assert DEFAULT_LOG_LEVEL == "INFO"
        assert isinstance(DEFAULT_LOG_LEVEL, str)

    def test_default_verbose(self):
        """Test DEFAULT_VERBOSE constant."""
        assert DEFAULT_VERBOSE is True
        assert isinstance(DEFAULT_VERBOSE, bool)


class TestSchemaConstants:
    """Test schema constants."""

    def test_default_schema(self):
        """Test DEFAULT_SCHEMA constant."""
        assert DEFAULT_SCHEMA == "default"
        assert isinstance(DEFAULT_SCHEMA, str)

    def test_test_schema(self):
        """Test TEST_SCHEMA constant."""
        assert TEST_SCHEMA == "test_schema"
        assert isinstance(TEST_SCHEMA, str)


class TestErrorConstants:
    """Test error constants."""

    def test_max_error_message_length(self):
        """Test MAX_ERROR_MESSAGE_LENGTH constant."""
        assert MAX_ERROR_MESSAGE_LENGTH == 1000
        assert isinstance(MAX_ERROR_MESSAGE_LENGTH, int)

    def test_max_stack_trace_lines(self):
        """Test MAX_STACK_TRACE_LINES constant."""
        assert MAX_STACK_TRACE_LINES == 50
        assert isinstance(MAX_STACK_TRACE_LINES, int)


class TestPerformanceMonitoringConstants:
    """Test performance monitoring constants."""

    def test_default_metrics_interval_seconds(self):
        """Test DEFAULT_METRICS_INTERVAL_SECONDS constant."""
        assert DEFAULT_METRICS_INTERVAL_SECONDS == 30
        assert isinstance(DEFAULT_METRICS_INTERVAL_SECONDS, int)

    def test_default_alert_threshold_percent(self):
        """Test DEFAULT_ALERT_THRESHOLD_PERCENT constant."""
        assert DEFAULT_ALERT_THRESHOLD_PERCENT == 80.0
        assert isinstance(DEFAULT_ALERT_THRESHOLD_PERCENT, float)


class TestConstantsIntegration:
    """Test constants integration and relationships."""

    def test_memory_calculations(self):
        """Test that memory calculations are correct."""
        # Test KB to MB conversion
        assert BYTES_PER_MB / BYTES_PER_KB == 1024

        # Test MB to GB conversion
        assert BYTES_PER_GB / BYTES_PER_MB == 1024

        # Test KB to GB conversion
        assert BYTES_PER_GB / BYTES_PER_KB == 1024 * 1024

    def test_threshold_percentages(self):
        """Test that thresholds are valid percentages."""
        assert 0 <= DEFAULT_BRONZE_THRESHOLD <= 100
        assert 0 <= DEFAULT_SILVER_THRESHOLD <= 100
        assert 0 <= DEFAULT_GOLD_THRESHOLD <= 100
        assert 0 <= DEFAULT_ALERT_THRESHOLD_PERCENT <= 100

    def test_positive_values(self):
        """Test that all numeric constants are positive."""
        positive_constants = [
            BYTES_PER_KB,
            BYTES_PER_MB,
            BYTES_PER_GB,
            DEFAULT_MAX_MEMORY_MB,
            DEFAULT_CACHE_MEMORY_MB,
            DEFAULT_MAX_FILE_SIZE_MB,
            DEFAULT_BACKUP_COUNT,
            DEFAULT_CACHE_PARTITIONS,
            DEFAULT_SHUFFLE_PARTITIONS,
            DEFAULT_BRONZE_THRESHOLD,
            DEFAULT_SILVER_THRESHOLD,
            DEFAULT_GOLD_THRESHOLD,
            DEFAULT_TIMEOUT_SECONDS,
            DEFAULT_RETRY_TIMEOUT_SECONDS,
            MAX_ERROR_MESSAGE_LENGTH,
            MAX_STACK_TRACE_LINES,
            DEFAULT_METRICS_INTERVAL_SECONDS,
            DEFAULT_ALERT_THRESHOLD_PERCENT,
        ]

        for constant in positive_constants:
            assert constant > 0, f"Constant {constant} should be positive"

    def test_string_constants_not_empty(self):
        """Test that string constants are not empty."""
        string_constants = [DEFAULT_LOG_LEVEL, DEFAULT_SCHEMA, TEST_SCHEMA]

        for constant in string_constants:
            assert (
                len(constant) > 0
            ), f"String constant '{constant}' should not be empty"
            assert isinstance(constant, str), f"Constant {constant} should be a string"
