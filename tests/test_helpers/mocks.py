"""
Mock object factories for tests.

This module provides factories for creating mock objects commonly used in tests.
"""

from unittest.mock import Mock
from typing import Any, Dict, List, Optional

from pipeline_builder.compat import DataFrame, SparkSession


def create_mock_spark_session() -> Mock:
    """
    Create a mock SparkSession for unit tests.

    Returns:
        Mock SparkSession with common methods configured
    """
    spark = Mock(spec=SparkSession)
    spark.createDataFrame.return_value = Mock(spec=DataFrame)
    spark.read.format.return_value.load.return_value = Mock(spec=DataFrame)
    spark.sql.return_value = Mock(spec=DataFrame)
    spark.table.return_value = Mock(spec=DataFrame)
    spark.catalog = Mock()
    spark.catalog.listTables.return_value = []
    spark.catalog.listDatabases.return_value = []
    return spark


def create_mock_dataframe(
    count: int = 100,
    columns: Optional[List[str]] = None,
    collect_data: Optional[List[Dict[str, Any]]] = None,
) -> Mock:
    """
    Create a mock DataFrame for unit tests.

    Args:
        count: Return value for count() method
        columns: Column names (default: ["id", "name", "value"])
        collect_data: Data to return from collect() (default: single row)

    Returns:
        Mock DataFrame with common methods configured
    """
    if columns is None:
        columns = ["id", "name", "value"]

    if collect_data is None:
        collect_data = [{"id": 1, "name": "test", "value": 42}]

    df = Mock(spec=DataFrame)
    df.count.return_value = count
    df.columns = columns
    df.collect.return_value = collect_data
    df.filter.return_value = df
    df.withColumn.return_value = df
    df.select.return_value = df
    df.groupBy.return_value.agg.return_value = df
    df.orderBy.return_value = df
    df.limit.return_value = df
    return df


def create_mock_logger() -> Mock:
    """
    Create a mock logger for unit tests.

    Returns:
        Mock logger with common logging methods
    """
    logger = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.debug = Mock()
    logger.critical = Mock()
    logger.exception = Mock()
    return logger


def create_mock_pipeline_config(
    schema: str = "test_schema",
    bronze_threshold: float = 95.0,
    silver_threshold: float = 98.0,
    gold_threshold: float = 99.0,
) -> Mock:
    """
    Create a mock PipelineConfig for unit tests.

    Args:
        schema: Schema name
        bronze_threshold: Bronze validation threshold
        silver_threshold: Silver validation threshold
        gold_threshold: Gold validation threshold

    Returns:
        Mock PipelineConfig
    """
    from pipeline_builder.models import (
        PipelineConfig,
        ValidationThresholds,
    )

    thresholds = ValidationThresholds(
        bronze=bronze_threshold,
        silver=silver_threshold,
        gold=gold_threshold,
    )
    config = PipelineConfig(
        schema=schema,
        thresholds=thresholds,
        verbose=False,
    )
    return config


def create_mock_step(
    name: str,
    step_type: str = "bronze",
    source_bronze: Optional[str] = None,
    source_silvers: Optional[List[str]] = None,
    rules: Optional[Dict] = None,
) -> Mock:
    """
    Create a mock step object for unit tests.

    Args:
        name: Step name
        step_type: Step type (bronze, silver, gold)
        source_bronze: Source bronze step name
        source_silvers: List of source silver step names
        rules: Validation rules dictionary

    Returns:
        Mock step object
    """
    step = Mock()
    step.name = name
    step.type = step_type
    step.source_bronze = source_bronze
    step.source_silvers = source_silvers or []
    step.rules = rules or {}
    step.table_name = f"{name}_table"
    return step
