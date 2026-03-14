"""
Integration test configuration and fixtures.

This module provides fixtures and configuration for integration tests,
using the sparkless.testing framework from the main conftest.
"""

import pytest


@pytest.fixture(scope="function")
def integration_spark_session(spark):
    """
    Spark session for integration tests.
    
    Delegates to the main `spark` fixture from sparkless.testing.
    Works in both sparkless and pyspark modes.
    """
    yield spark


@pytest.fixture(autouse=True, scope="function")
def cleanup_integration_tables(spark):
    """Reset global state before and after each integration test."""
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
    """Provide a unique schema name for each integration test."""
    return f"test_schema_{table_prefix}"


@pytest.fixture(scope="function")
def unique_table_name(table_prefix):
    """Provide a function to generate unique table names for each integration test."""
    def _get_unique_table(base_name: str) -> str:
        return f"{base_name}_{table_prefix}"
    return _get_unique_table


@pytest.fixture
def sample_integration_data(spark):
    """Create sample data for integration tests."""
    data = [
        ("user1", "click", "2024-01-01 10:00:00"),
        ("user2", "view", "2024-01-01 11:00:00"),
        ("user3", "purchase", "2024-01-01 12:00:00"),
    ]
    return spark.createDataFrame(
        data, ["user_id", "action", "timestamp"]
    )


@pytest.fixture
def sample_integration_rules(spark_imports):
    """Create sample validation rules for integration tests."""
    F = spark_imports.F
    return {
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
    }


# Mark all tests in this directory as integration tests
pytestmark = pytest.mark.integration
