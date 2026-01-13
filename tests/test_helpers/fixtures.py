"""
Reusable fixture factories for tests.

This module provides factory functions for creating common test fixtures.
"""

from typing import Callable

import pytest

from pipeline_builder.compat import DataFrame, SparkSession


def create_spark_session_fixture(
    scope: str = "function",
    enable_delta: bool = True,
) -> Callable:
    """
    Create a pytest fixture factory for Spark sessions.

    Args:
        scope: Fixture scope (function, class, module, session)
        enable_delta: Whether to enable Delta Lake

    Returns:
        Pytest fixture function
    """

    @pytest.fixture(scope=scope)
    def _spark_session():
        from tests.test_helpers.spark_helpers import create_test_spark_session

        spark = create_test_spark_session(enable_delta=enable_delta)
        yield spark

        # Cleanup
        try:
            spark.stop()
        except Exception:
            pass

    return _spark_session


def create_dataframe_fixture(
    generator: Callable[[SparkSession], DataFrame],
    scope: str = "function",
) -> Callable:
    """
    Create a pytest fixture factory for DataFrames.

    Args:
        generator: Function that takes SparkSession and returns DataFrame
        scope: Fixture scope

    Returns:
        Pytest fixture function
    """

    @pytest.fixture(scope=scope)
    def _dataframe(spark_session):
        return generator(spark_session)

    return _dataframe


def create_pipeline_config_fixture(
    schema: str = "test_schema",
    bronze_threshold: float = 95.0,
    silver_threshold: float = 98.0,
    gold_threshold: float = 99.0,
) -> Callable:
    """
    Create a pytest fixture factory for PipelineConfig.

    Args:
        schema: Schema name
        bronze_threshold: Bronze validation threshold
        silver_threshold: Silver validation threshold
        gold_threshold: Gold validation threshold

    Returns:
        Pytest fixture function
    """

    @pytest.fixture
    def _pipeline_config():
        from pipeline_builder.models import (
            PipelineConfig,
            ValidationThresholds,
        )

        thresholds = ValidationThresholds(
            bronze=bronze_threshold,
            silver=silver_threshold,
            gold=gold_threshold,
        )
        return PipelineConfig(
            schema=schema,
            thresholds=thresholds,
            verbose=False,
        )

    return _pipeline_config
