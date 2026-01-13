"""
Common test patterns and utilities.

This module provides reusable patterns and utilities for common test scenarios.
"""

import os
from contextlib import contextmanager
from typing import Callable, Optional

from pipeline_builder.compat import SparkSession


@contextmanager
def isolated_spark_test(spark: SparkSession, schema_name: Optional[str] = None):
    """
    Context manager for isolated Spark tests.

    Creates a unique schema, runs test code, and cleans up afterward.

    Args:
        spark: SparkSession instance
        schema_name: Optional schema name (default: auto-generated)

    Example:
        >>> with isolated_spark_test(spark) as schema:
        ...     # Test code here
        ...     spark.sql(f"CREATE TABLE {schema}.test_table ...")
    """
    from tests.test_helpers.isolation import get_unique_schema

    if schema_name is None:
        schema_name = get_unique_schema("test")

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        yield schema_name
    finally:
        try:
            spark.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except Exception:
            pass


@contextmanager
def mock_mode():
    """
    Context manager to temporarily set SPARK_MODE to mock.

    Example:
        >>> with mock_mode():
        ...     # Code that requires mock mode
        ...     pass
    """
    original_mode = os.environ.get("SPARK_MODE")
    os.environ["SPARK_MODE"] = "mock"
    try:
        yield
    finally:
        if original_mode:
            os.environ["SPARK_MODE"] = original_mode
        elif "SPARK_MODE" in os.environ:
            del os.environ["SPARK_MODE"]


@contextmanager
def real_mode():
    """
    Context manager to temporarily set SPARK_MODE to real.

    Example:
        >>> with real_mode():
        ...     # Code that requires real Spark
        ...     pass
    """
    original_mode = os.environ.get("SPARK_MODE")
    os.environ["SPARK_MODE"] = "real"
    try:
        yield
    finally:
        if original_mode:
            os.environ["SPARK_MODE"] = original_mode
        elif "SPARK_MODE" in os.environ:
            del os.environ["SPARK_MODE"]


def skip_if_mock(reason: str = "Test requires real Spark"):
    """
    Decorator to skip test if running in mock mode.

    Args:
        reason: Skip reason message

    Example:
        >>> @skip_if_mock("This test requires real Spark")
        ... def test_real_spark_feature():
        ...     pass
    """

    def decorator(func: Callable) -> Callable:
        import pytest

        def wrapper(*args, **kwargs):
            if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def skip_if_real(reason: str = "Test requires mock Spark"):
    """
    Decorator to skip test if running in real mode.

    Args:
        reason: Skip reason message

    Example:
        >>> @skip_if_real("This test requires mock Spark")
        ... def test_mock_spark_feature():
        ...     pass
    """

    def decorator(func: Callable) -> Callable:
        import pytest

        def wrapper(*args, **kwargs):
            if os.environ.get("SPARK_MODE", "mock").lower() == "real":
                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def parametrize_spark_mode():
    """
    Pytest parametrize decorator for running tests in both mock and real modes.

    Example:
        >>> @parametrize_spark_mode()
        ... def test_something(spark_mode):
        ...     assert spark_mode in ["mock", "real"]
    """
    import pytest

    return pytest.mark.parametrize("spark_mode", ["mock", "real"])
