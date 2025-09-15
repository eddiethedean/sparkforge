# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

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
