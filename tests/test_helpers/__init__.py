"""
Test helper utilities for pipeline tests.

This package provides comprehensive test utilities including:
- Isolation utilities for clean test state
- Data generators for test data creation
- Assertion helpers for common checks
- Mock factories for unit tests
- Spark session management
- Test patterns and utilities
"""

from .assertions import TestAssertions
from .data_generators import TestDataGenerator
from .isolation import (
    clear_all_tables,
    clear_all_test_state,
    clear_delta_tables,
    clear_spark_state,
    clear_spark_views,
    get_unique_schema,
    get_test_identifier,
    reset_engine_state,
    reset_execution_state,
    reset_global_state,
    ThreadLocalEnvVar,
)
from .mocks import (
    create_mock_dataframe,
    create_mock_logger,
    create_mock_pipeline_config,
    create_mock_spark_session,
    create_mock_step,
)
from .patterns import (
    isolated_spark_test,
    mock_mode,
    parametrize_spark_mode,
    real_mode,
    skip_if_mock,
    skip_if_real,
)
from .spark_helpers import (
    cleanup_spark_session,
    create_test_database,
    create_test_spark_session,
    drop_test_database,
)

__all__ = [
    # Isolation
    "clear_spark_state",
    "clear_spark_views",
    "clear_all_tables",
    "clear_delta_tables",
    "reset_global_state",
    "reset_execution_state",
    "reset_engine_state",
    "clear_all_test_state",
    "get_unique_schema",
    "get_test_identifier",
    "ThreadLocalEnvVar",
    # Data generators
    "TestDataGenerator",
    # Assertions
    "TestAssertions",
    # Mocks
    "create_mock_spark_session",
    "create_mock_dataframe",
    "create_mock_logger",
    "create_mock_pipeline_config",
    "create_mock_step",
    # Spark helpers
    "create_test_spark_session",
    "cleanup_spark_session",
    "create_test_database",
    "drop_test_database",
    # Patterns
    "isolated_spark_test",
    "mock_mode",
    "real_mode",
    "skip_if_mock",
    "skip_if_real",
    "parametrize_spark_mode",
]
