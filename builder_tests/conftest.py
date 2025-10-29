"""
Pytest configuration and shared fixtures for builder tests.

This module provides comprehensive test fixtures for system-level testing
of the PipelineBuilder, PipelineRunner, and LogWriter integration.
"""

import os
import sys
from datetime import datetime, timedelta

import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Set environment variable to use mock Spark
os.environ["SPARK_MODE"] = "mock"

from mock_spark import (
    IntegerType,
    MockSparkSession,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline_builder.models import (
    PipelineConfig,
    ValidationThresholds,
)
from pipeline_builder.pipeline.builder import PipelineBuilder
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder.writer import LogWriter


@pytest.fixture(scope="session")
def spark_session():
    """Create a mock Spark session for testing."""
    spark = MockSparkSession("BuilderTests")
    return spark


@pytest.fixture
def test_schema():
    """Test schema name."""
    return "test_builder_schema"


@pytest.fixture
def pipeline_config(test_schema):
    """Create a test pipeline configuration."""
    return PipelineConfig(
        schema=test_schema,
        thresholds=ValidationThresholds(
            bronze=95.0,
            silver=98.0,
            gold=99.0,
        ),
        parallel=True,
    )


@pytest.fixture
def log_writer(spark_session, test_schema):
    """Create a LogWriter instance for testing."""
    return LogWriter(
        spark=spark_session,
        schema=test_schema,
        table_name="pipeline_logs",
    )


@pytest.fixture
def pipeline_runner(spark_session, pipeline_config):
    """Create a PipelineRunner instance for testing."""
    return SimplePipelineRunner(spark_session, pipeline_config)


@pytest.fixture
def pipeline_builder(spark_session, test_schema):
    """Create a PipelineBuilder instance for testing."""
    return PipelineBuilder(spark=spark_session, schema=test_schema)


# ============================================================================
# Simple Test Data Fixtures
# ============================================================================


@pytest.fixture
def simple_events_schema():
    """Schema for simple events data."""
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("value", IntegerType(), True),
        ]
    )


@pytest.fixture
def simple_events_data(spark_session, simple_events_schema):
    """Create simple events data for testing."""
    data = [
        {"id": 1, "name": "event1", "timestamp": datetime(2024, 1, 1, 10, 0, 0), "value": 100},
        {"id": 2, "name": "event2", "timestamp": datetime(2024, 1, 1, 11, 0, 0), "value": 200},
        {"id": 3, "name": "event3", "timestamp": datetime(2024, 1, 1, 12, 0, 0), "value": 300},
        {"id": 4, "name": "event4", "timestamp": datetime(2024, 1, 1, 13, 0, 0), "value": 400},
        {"id": 5, "name": "event5", "timestamp": datetime(2024, 1, 1, 14, 0, 0), "value": 500},
    ]

    return spark_session.createDataFrame(data, simple_events_schema)


@pytest.fixture
def simple_users_schema():
    """Schema for simple users data."""
    return StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("username", StringType(), False),
            StructField("email", StringType(), False),
            StructField("created_at", TimestampType(), False),
        ]
    )


@pytest.fixture
def simple_users_data(spark_session, simple_users_schema):
    """Create simple users data for testing."""
    data = [
        {"user_id": 1, "username": "user1", "email": "user1@example.com", "registration_date": datetime(2024, 1, 1, 0, 0, 0)},
        {"user_id": 2, "username": "user2", "email": "user2@example.com", "registration_date": datetime(2024, 1, 1, 0, 0, 0)},
        {"user_id": 3, "username": "user3", "email": "user3@example.com", "registration_date": datetime(2024, 1, 1, 0, 0, 0)},
    ]

    return spark_session.createDataFrame(data, simple_users_schema)


# ============================================================================
# Test Data Helpers
# ============================================================================


def create_incremental_events_data(
    spark_session, simple_events_schema, base_date: datetime, num_events: int = 3
):
    """Create incremental events data for testing incremental loads."""
    data = []
    for i in range(num_events):
        data.append(
            (
                10 + i,
                f"inc_event_{i}",
                base_date + timedelta(minutes=i * 5),
                100 + i * 10,
            )
        )

    return spark_session.createDataFrame(data, simple_events_schema)


def create_bronze_validation_rules():
    """Create validation rules for bronze layer."""
    from mock_spark import functions as F

    return {
        "id": [F.col("id").isNotNull()],
        "name": [F.col("name").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "value": [F.col("value").isNull() | (F.col("value") >= 0)],
    }


def create_silver_validation_rules():
    """Create validation rules for silver layer."""
    from mock_spark import functions as F

    return {
        "id": [F.col("id").isNotNull()],
        "name": [F.col("name").isNotNull()],
        "processed": [F.col("processed").isNotNull()],
    }


def create_gold_validation_rules():
    """Create validation rules for gold layer."""
    from mock_spark import functions as F

    return {
        "id": [F.col("id").isNotNull()],
        "count": [F.col("count") > 0],
    }
