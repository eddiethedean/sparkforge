"""
Enhanced test utilities and helpers for the SparkForge test suite.

This module provides common test utilities, data generators, and helper functions
to reduce duplication and improve test maintainability.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkforge import PipelineBuilder


class TestDataGenerator:
    """Utility class for generating test data."""

    @staticmethod
    def create_events_data(
        spark: SparkSession,
        num_records: int = 100,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> DataFrame:
        """Create realistic events data for testing."""
        if start_date is None:
            start_date = datetime(2024, 1, 1, 10, 0, 0)
        if end_date is None:
            end_date = datetime(2024, 1, 1, 18, 0, 0)

        data = []
        for i in range(num_records):
            # Generate realistic user behavior
            user_id = f"user_{i % 20:02d}"  # 20 unique users
            action = ["click", "view", "purchase", "add_to_cart"][i % 4]

            # Generate timestamp within range
            time_diff = (end_date - start_date).total_seconds()
            random_seconds = (i * 17) % int(
                time_diff
            )  # Pseudo-random but deterministic
            timestamp = start_date + timedelta(seconds=random_seconds)

            data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S")))

        return spark.createDataFrame(data, ["user_id", "action", "timestamp"])

    @staticmethod
    def create_user_data(spark: SparkSession, num_users: int = 20) -> DataFrame:
        """Create user profile data for testing."""
        data = []
        for i in range(num_users):
            user_id = f"user_{i:02d}"
            age = 18 + (i * 3) % 50  # Ages 18-67
            country = ["US", "CA", "UK", "DE", "FR"][i % 5]
            created_at = datetime(2024, 1, 1) + timedelta(days=i)

            data.append(
                (user_id, age, country, created_at.strftime("%Y-%m-%d %H:%M:%S"))
            )

        return spark.createDataFrame(data, ["user_id", "age", "country", "created_at"])

    @staticmethod
    def create_validation_rules() -> Dict[str, List]:
        """Create standard validation rules for testing."""
        return {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
            "age": [F.col("age") > 0, F.col("age") < 120],
            "country": [F.col("country").isNotNull()],
        }


class TestPipelineBuilder:
    """Utility class for creating test pipelines."""

    @staticmethod
    def create_simple_pipeline(
        spark: SparkSession, schema: str = "test_schema", **kwargs
    ) -> PipelineBuilder:
        """Create a simple test pipeline with default configuration."""
        return PipelineBuilder(
            spark=spark,
            schema=schema,
            verbose=False,
            enable_parallel_silver=True,
            max_parallel_workers=2,
            **kwargs,
        )

    @staticmethod
    def create_sequential_pipeline(
        spark: SparkSession, schema: str = "test_schema", **kwargs
    ) -> PipelineBuilder:
        """Create a test pipeline with sequential execution."""
        return PipelineBuilder(
            spark=spark,
            schema=schema,
            verbose=False,
            enable_parallel_silver=False,
            max_parallel_workers=1,
            **kwargs,
        )

    @staticmethod
    def create_bronze_silver_gold_pipeline(
        spark: SparkSession,
        schema: str = "test_schema",
        bronze_data: DataFrame = None,
        **kwargs,
    ) -> Tuple[PipelineBuilder, DataFrame]:
        """Create a complete Bronze → Silver → Gold test pipeline."""
        if bronze_data is None:
            bronze_data = TestDataGenerator.create_events_data(spark, num_records=50)

        rules = TestDataGenerator.create_validation_rules()

        builder = TestPipelineBuilder.create_simple_pipeline(spark, schema, **kwargs)

        # Bronze layer
        builder.add_bronze_source(
            "events",
            bronze_data,
            {
                "user_id": rules["user_id"],
                "action": rules["action"],
                "timestamp": rules["timestamp"],
            },
        )

        # Silver layer
        def silver_transform(spark, bronze_df):
            return (
                bronze_df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("hour", F.hour("timestamp"))
                .select("user_id", "action", "event_date", "hour")
            )

        builder.add_silver_transform(
            "clean_events",
            silver_transform,
            {
                "user_id": rules["user_id"],
                "action": rules["action"],
                "event_date": rules["event_date"],
            },
            table_name="clean_events",
        )

        # Gold layer
        def gold_transform(spark, silvers):
            events_df = silvers["clean_events"]
            return (
                events_df.groupBy("action", "event_date")
                .agg(F.count("*").alias("event_count"))
                .orderBy("event_date", "action")
            )

        builder.add_gold_transform(
            "daily_summary",
            gold_transform,
            {
                "action": rules["action"],
                "event_date": rules["event_date"],
                "event_count": [F.col("event_count") > 0],
            },
            table_name="daily_summary",
            source_silvers=["clean_events"],
        )

        return builder, bronze_data


class TestAssertions:
    """Utility class for common test assertions."""

    @staticmethod
    def assert_pipeline_success(result) -> None:
        """Assert that a pipeline execution was successful."""
        assert result.status.value == "completed", f"Pipeline failed: {result.status}"
        assert (
            result.metrics.failed_steps == 0
        ), f"Failed steps: {result.metrics.failed_steps}"
        assert result.metrics.successful_steps > 0, "No successful steps"

    @staticmethod
    def assert_pipeline_failure(result, expected_failed_steps: int = None) -> None:
        """Assert that a pipeline execution failed as expected."""
        assert (
            result.status.value == "failed"
        ), f"Pipeline should have failed: {result.status}"
        if expected_failed_steps is not None:
            assert (
                result.metrics.failed_steps == expected_failed_steps
            ), f"Expected {expected_failed_steps} failed steps, got {result.metrics.failed_steps}"

    @staticmethod
    def assert_dataframe_has_columns(
        df: DataFrame, expected_columns: List[str]
    ) -> None:
        """Assert that a DataFrame has the expected columns."""
        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        missing_columns = expected_columns_set - actual_columns
        extra_columns = actual_columns - expected_columns_set

        assert not missing_columns, f"Missing columns: {missing_columns}"
        if extra_columns:
            print(f"Warning: Extra columns found: {extra_columns}")

    @staticmethod
    def assert_dataframe_not_empty(df: DataFrame) -> None:
        """Assert that a DataFrame is not empty."""
        assert df.count() > 0, "DataFrame is empty"

    @staticmethod
    def assert_dataframe_empty(df: DataFrame) -> None:
        """Assert that a DataFrame is empty."""
        assert df.count() == 0, f"DataFrame is not empty, has {df.count()} rows"


class TestPerformance:
    """Utility class for performance testing."""

    @staticmethod
    def measure_execution_time(func, *args, **kwargs) -> Tuple[Any, float]:
        """Measure the execution time of a function."""
        import time

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    @staticmethod
    def assert_execution_time_under(execution_time: float, max_seconds: float) -> None:
        """Assert that execution time is under a threshold."""
        assert (
            execution_time < max_seconds
        ), f"Execution time {execution_time:.2f}s exceeded threshold {max_seconds}s"


# Pytest markers for better test organization
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "spark: mark test as requiring Spark session")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "unit: mark test as unit test")


# Common test fixtures that can be reused
@pytest.fixture
def test_data_generator():
    """Provide TestDataGenerator instance."""
    return TestDataGenerator()


@pytest.fixture
def test_pipeline_builder():
    """Provide TestPipelineBuilder instance."""
    return TestPipelineBuilder()


@pytest.fixture
def test_assertions():
    """Provide TestAssertions instance."""
    return TestAssertions()


@pytest.fixture
def test_performance():
    """Provide TestPerformance instance."""
    return TestPerformance()


# Enhanced data fixtures
@pytest.fixture
def small_events_data(spark_session):
    """Create small events dataset for fast tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=10)


@pytest.fixture
def medium_events_data(spark_session):
    """Create medium events dataset for integration tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=100)


@pytest.fixture
def large_events_data(spark_session):
    """Create large events dataset for performance tests."""
    return TestDataGenerator.create_events_data(spark_session, num_records=1000)


@pytest.fixture
def user_data(spark_session):
    """Create user profile data."""
    return TestDataGenerator.create_user_data(spark_session)


@pytest.fixture
def validation_rules():
    """Create standard validation rules."""
    return TestDataGenerator.create_validation_rules()


# Pipeline fixtures
@pytest.fixture
def simple_pipeline(spark_session):
    """Create a simple test pipeline."""
    return TestPipelineBuilder.create_simple_pipeline(spark_session)


@pytest.fixture
def complete_pipeline(spark_session, medium_events_data):
    """Create a complete Bronze → Silver → Gold pipeline."""
    return TestPipelineBuilder.create_bronze_silver_gold_pipeline(
        spark_session, bronze_data=medium_events_data
    )
