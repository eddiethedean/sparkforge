"""
Tests for step executors.

This module tests the step executor classes (Bronze, Silver, Gold).
"""

import pytest
from unittest.mock import Mock

from pipeline_builder.models import BronzeStep, SilverStep, GoldStep
from pipeline_builder.step_executors import (
    BronzeStepExecutor,
    SilverStepExecutor,
    GoldStepExecutor,
)
from pipeline_builder_base.errors import ExecutionError


class TestBronzeStepExecutor:
    """Tests for BronzeStepExecutor."""

    def test_execute_with_valid_data(self, spark_session):
        """Test executing a bronze step with valid data."""
        executor = BronzeStepExecutor(spark_session)

        # Create test data
        data = [("user1", "click", "2024-01-01 10:00:00")]
        df = spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])

        # Create bronze step with real validation rules
        import os

        if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
            from sparkless import functions as F  # type: ignore[import]
        else:
            from pyspark.sql import functions as F

        step = BronzeStep(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
        )

        # Execute
        result = executor.execute(step, {"events": df})

        # Assert
        assert result is not None
        assert result.count() == 1

    def test_execute_with_missing_data(self, spark_session):
        """Test executing a bronze step with missing data raises error."""
        executor = BronzeStepExecutor(spark_session)

        step = BronzeStep(
            name="events",
            rules={"user_id": [Mock()]},
        )

        # Execute without data in context
        with pytest.raises(ExecutionError, match="requires data to be provided"):
            executor.execute(step, {})

    def test_execute_with_empty_dataframe(self, spark_session):
        """Test executing a bronze step with empty DataFrame logs warning."""
        executor = BronzeStepExecutor(spark_session)

        # Create empty DataFrame with schema
        import os

        if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
            from sparkless.spark_types import StringType, StructField, StructType  # type: ignore[import]
        else:
            from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        step = BronzeStep(
            name="events",
            rules={"user_id": [Mock()]},
        )

        # Execute - should not raise, but log warning
        result = executor.execute(step, {"events": df})

        # Assert
        assert result is not None
        assert result.count() == 0


class TestSilverStepExecutor:
    """Tests for SilverStepExecutor."""

    def test_execute_with_valid_transform(self, spark_session):
        """Test executing a silver step with valid transform."""
        executor = SilverStepExecutor(spark_session)

        # Create test data
        data = [("user1", "click", "2024-01-01 10:00:00")]
        bronze_df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp"]
        )

        # Create silver step with transform
        # Silver transform takes (spark, bronze_df, silvers_dict)
        import os

        if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
            from sparkless import functions as F  # type: ignore[import]
        else:
            from pyspark.sql import functions as F

        def transform(spark, bronze_df, silvers=None):
            if silvers is None:
                silvers = {}
            return bronze_df.withColumn("processed", F.lit("processed"))

        step = SilverStep(
            name="clean_events",
            source_bronze="events",
            transform=transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
        )

        # Execute
        from pipeline_builder_base.models import ExecutionMode

        result = executor.execute(step, {"events": bronze_df}, ExecutionMode.INITIAL)

        # Assert
        assert result is not None
        assert "processed" in result.columns


class TestGoldStepExecutor:
    """Tests for GoldStepExecutor."""

    def test_execute_with_valid_transform(self, spark_session):
        """Test executing a gold step with valid transform."""
        executor = GoldStepExecutor(spark_session)

        # Create test data
        data = [("user1", "click", "2024-01-01")]
        silver_df = spark_session.createDataFrame(
            data, ["user_id", "action", "event_date"]
        )

        # Create gold step with transform
        def transform(spark, silvers):
            return silvers["clean_events"]

        step = GoldStep(
            name="summary",
            source_silvers=["clean_events"],
            transform=transform,
            rules={"action": [Mock()]},
            table_name="summary",
        )

        # Execute
        result = executor.execute(step, {"clean_events": silver_df})

        # Assert
        assert result is not None
