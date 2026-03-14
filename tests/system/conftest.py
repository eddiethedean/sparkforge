"""
System test configuration and fixtures.

This module provides fixtures and configuration for system tests,
using the sparkless.testing framework from the main conftest.
"""

import os
import time

import pytest


@pytest.fixture(scope="function")
def system_spark_session(spark):
    """
    Spark session for system tests.
    
    Delegates to the main `spark` fixture from sparkless.testing.
    Works in both sparkless and pyspark modes.
    """
    yield spark


@pytest.fixture(autouse=True, scope="function")
def cleanup_system_tables(spark):
    """Reset global state before and after each system test."""
    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state,
        )
        reset_global_state()
        reset_execution_state()
    except Exception:
        pass

    yield

    try:
        from tests.test_helpers.isolation import (
            reset_execution_state,
            reset_global_state,
        )
        reset_global_state()
        reset_execution_state()
    except Exception:
        pass


@pytest.fixture(scope="function")
def unique_schema(table_prefix):
    """Provide a unique schema name for each system test."""
    return f"test_schema_{table_prefix}"


@pytest.fixture(scope="function")
def unique_table_name(table_prefix):
    """Provide a function to generate unique table names for each system test."""
    def _get_unique_table(base_name: str) -> str:
        return f"{base_name}_{table_prefix}"
    return _get_unique_table


@pytest.fixture
def large_test_data(spark):
    """Create large test dataset for system tests."""
    data = []
    for i in range(10000):
        data.append(
            (
                f"user{i}",
                "click" if i % 2 == 0 else "view",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 100,
            )
        )

    return spark.createDataFrame(
        data, ["user_id", "action", "timestamp", "value"]
    )


@pytest.fixture
def performance_test_data(spark):
    """Create performance test dataset for system tests."""
    data = []
    for i in range(100000):
        data.append(
            (
                f"user{i % 1000}",
                "click" if i % 3 == 0 else "view" if i % 3 == 1 else "purchase",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 1000,
                f"category{i % 10}",
            )
        )

    return spark.createDataFrame(
        data, ["user_id", "action", "timestamp", "value", "category"]
    )


@pytest.fixture
def delta_lake_test_data(spark):
    """Create test data specifically for Delta Lake system tests."""
    data = []
    for i in range(1000):
        data.append(
            (
                i,
                f"user{i}",
                f"2024-01-01 {10 + i % 14:02d}:00:00",
                i % 100,
                f"status{i % 3}",
            )
        )

    return spark.createDataFrame(
        data, ["id", "user_id", "timestamp", "value", "status"]
    )


# Mark all tests in this directory as system tests
pytestmark = pytest.mark.system
