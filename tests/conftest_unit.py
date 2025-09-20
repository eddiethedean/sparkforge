"""
Unit test configuration and fixtures.

This module provides fixtures and configuration specifically for unit tests,
which should use mocked dependencies and run quickly.
"""

from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession for unit tests."""
    spark = Mock(spec=SparkSession)
    spark.createDataFrame.return_value = Mock(spec=DataFrame)
    spark.read.format.return_value.load.return_value = Mock(spec=DataFrame)
    spark.sql.return_value = Mock(spec=DataFrame)
    spark.table.return_value = Mock(spec=DataFrame)
    return spark


@pytest.fixture
def mock_dataframe():
    """Create a mock DataFrame for unit tests."""
    df = Mock(spec=DataFrame)
    df.count.return_value = 100
    df.columns = ["id", "name", "value"]
    df.collect.return_value = [{"id": 1, "name": "test", "value": 42}]
    df.filter.return_value = df
    df.withColumn.return_value = df
    df.select.return_value = df
    df.groupBy.return_value.agg.return_value = df
    return df


@pytest.fixture
def mock_logger():
    """Create a mock logger for unit tests."""
    logger = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.debug = Mock()
    logger.critical = Mock()
    return logger


@pytest.fixture
def mock_pipeline_config():
    """Create a mock PipelineConfig for unit tests."""
    from sparkforge.models import ParallelConfig, PipelineConfig, ValidationThresholds

    thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
    parallel = ParallelConfig(enabled=True, max_workers=4)
    config = PipelineConfig(
        schema="test_schema", thresholds=thresholds, parallel=parallel, verbose=False
    )
    return config


@pytest.fixture
def sample_validation_rules():
    """Create sample validation rules for unit tests."""
    return {
        "id": [F.col("id").isNotNull()],
        "name": [F.col("name").isNotNull()],
        "value": [F.col("value") > 0],
    }


# Mark all tests in this conftest as unit tests
pytestmark = pytest.mark.unit
