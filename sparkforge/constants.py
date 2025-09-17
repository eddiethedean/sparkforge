"""
Constants and configuration values for SparkForge.

This module contains all magic numbers, default values, and configuration
constants used throughout the SparkForge codebase.
"""

# Memory and Size Constants
BYTES_PER_KB = 1024
BYTES_PER_MB = BYTES_PER_KB * 1024
BYTES_PER_GB = BYTES_PER_MB * 1024

# Default Memory Limits
DEFAULT_MAX_MEMORY_MB = 1024
DEFAULT_CACHE_MEMORY_MB = 512

# File Size Constants
DEFAULT_MAX_FILE_SIZE_MB = 10
DEFAULT_BACKUP_COUNT = 5

# Performance Constants
DEFAULT_CACHE_PARTITIONS = 200
DEFAULT_SHUFFLE_PARTITIONS = 200

# Validation Constants
DEFAULT_BRONZE_THRESHOLD = 95.0
DEFAULT_SILVER_THRESHOLD = 98.0
DEFAULT_GOLD_THRESHOLD = 99.0

# Timeout Constants (in seconds)
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_RETRY_TIMEOUT_SECONDS = 60

# Logging Constants
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_VERBOSE = True

# Schema Constants
DEFAULT_SCHEMA = "default"
TEST_SCHEMA = "test_schema"

# Error Constants
MAX_ERROR_MESSAGE_LENGTH = 1000
MAX_STACK_TRACE_LINES = 50

# Performance Monitoring Constants
DEFAULT_METRICS_INTERVAL_SECONDS = 30
DEFAULT_ALERT_THRESHOLD_PERCENT = 80.0
