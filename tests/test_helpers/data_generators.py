"""
Test data generation utilities.

This module provides utilities for generating test data for various scenarios.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipeline_builder.compat import DataFrame, SparkSession

# Use engine-specific functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F


class TestDataGenerator:
    """Utility class for generating test data."""

    @staticmethod
    def create_events_data(
        spark: SparkSession,
        num_records: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Create realistic events data for testing.

        Args:
            spark: SparkSession instance
            num_records: Number of records to generate
            start_date: Start date for timestamps (default: 2024-01-01 10:00:00)
            end_date: End date for timestamps (default: 2024-01-01 18:00:00)

        Returns:
            DataFrame with columns: user_id, action, timestamp
        """
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
        """
        Create user profile data for testing.

        Args:
            spark: SparkSession instance
            num_users: Number of users to generate

        Returns:
            DataFrame with columns: user_id, age, country, created_at
        """
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
        """
        Create standard validation rules for testing.

        Returns:
            Dictionary mapping column names to lists of validation rules
        """
        return {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
            "age": [F.col("age") > 0, F.col("age") < 120],
            "country": [F.col("country").isNotNull()],
        }

    @staticmethod
    def create_empty_dataframe(spark: SparkSession, schema: List[str]) -> DataFrame:
        """
        Create an empty DataFrame with specified schema.

        Args:
            spark: SparkSession instance
            schema: List of column names

        Returns:
            Empty DataFrame with specified columns
        """
        # Create empty data with schema
        empty_data = []
        return spark.createDataFrame(empty_data, schema)

    @staticmethod
    def create_simple_dataframe(
        spark: SparkSession,
        num_rows: int = 10,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Create a simple DataFrame with numeric data.

        Args:
            spark: SparkSession instance
            num_rows: Number of rows to generate
            columns: Column names (default: ["id", "value"])

        Returns:
            DataFrame with generated data
        """
        if columns is None:
            columns = ["id", "value"]

        data = [(i, i * 10) for i in range(num_rows)]
        return spark.createDataFrame(data, columns)
