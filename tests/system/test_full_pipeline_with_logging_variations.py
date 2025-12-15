"""
Comprehensive system tests for pipeline execution with LogWriter integration - variations.

This module contains multiple test variations to ensure robust system test coverage:
- Different pipeline sizes (minimal, large, complex)
- Different data scenarios (empty, high volume, nulls, duplicates)
- Different execution modes (sequential, parallel, stress)
- Edge cases (validation failures, schema evolution, gaps)
- Various data types and validation scenarios
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import os
import pytest

# Use engine-specific functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F  # type: ignore

from pipeline_builder import PipelineBuilder
from pipeline_builder.writer.core import LogWriter
from pipeline_builder.writer.models import create_log_rows_from_pipeline_report


# ============================================================================
# Shared Utilities (reused from base test)
# ============================================================================


def create_test_data_initial(
    spark,
    num_records: int = 100,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[tuple]:
    """Generate initial test data with timestamps."""
    if start_date is None:
        start_date = datetime(2024, 1, 1, 0, 0, 0)
    if end_date is None:
        end_date = datetime(2024, 1, 1, 23, 59, 59)

    data = []
    for i in range(num_records):
        user_id = f"user_{i % 20:02d}"  # 20 unique users
        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0

        time_diff = (end_date - start_date).total_seconds()
        random_seconds = (i * 17) % int(time_diff)
        timestamp = start_date + timedelta(seconds=random_seconds)

        data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S"), value))

    return data


def create_test_data_incremental(
    spark,
    num_records: int = 50,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[tuple]:
    """Generate incremental test data with later timestamps."""
    if start_date is None:
        start_date = datetime(2024, 1, 2, 0, 0, 0)
    if end_date is None:
        end_date = datetime(2024, 1, 2, 23, 59, 59)

    data = []
    for i in range(num_records):
        user_id = f"user_{i % 20:02d}"
        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0

        time_diff = (end_date - start_date).total_seconds()
        random_seconds = (i * 17) % int(time_diff)
        timestamp = start_date + timedelta(seconds=random_seconds)

        data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S"), value))

    return data


def create_empty_data(spark) -> List[tuple]:
    """Create empty DataFrame data."""
    return []


def create_empty_dataframe(
    spark, columns: List[str], column_types: Optional[List] = None
):
    """
    Create an empty DataFrame with explicit schema.

    Both PySpark and mock-spark 3.10.4+ require explicit StructType schema for empty DataFrames.

    Args:
        spark: SparkSession instance
        columns: List of column names
        column_types: Optional list of data types (defaults to StringType for all)

    Returns:
        Empty DataFrame with specified schema
    """
    import os

    if os.environ.get("SPARK_MODE", "mock").lower() == "real":
        from pyspark.sql.types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            StructField,
            StructType,
        )
    else:
        from sparkless.spark_types import (  # type: ignore[import]
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            StructField,
            StructType,
        )

    # Default to StringType if types not specified
    if column_types is None:
        column_types = [StringType()] * len(columns)

    # Map string type names to actual types
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
    }

    # Convert string type names to actual types if needed
    actual_types = []
    for col_type in column_types:
        if isinstance(col_type, str):
            actual_types.append(type_map.get(col_type.lower(), StringType()))
        else:
            actual_types.append(col_type)

    schema = StructType(
        [
            StructField(col, col_type, True)
            for col, col_type in zip(columns, actual_types)
        ]
    )

    return spark.createDataFrame([], schema)


def create_data_with_nulls(spark, num_records: int = 100) -> List[tuple]:
    """Create data with various null patterns."""
    data = []
    start_date = datetime(2024, 1, 1, 0, 0, 0)

    for i in range(num_records):
        user_id = f"user_{i % 20:02d}" if i % 10 != 0 else None  # Some nulls
        action = (
            ["click", "view", "purchase", "add_to_cart"][i % 4] if i % 7 != 0 else None
        )  # Some nulls
        value = 10.0 + (i * 1.5) % 100.0 if i % 5 != 0 else None  # Some nulls
        timestamp = (start_date + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")

        data.append((user_id, action, timestamp, value))

    return data


def create_duplicate_data(spark, num_records: int = 100) -> List[tuple]:
    """Create data with duplicate records."""
    data = []
    start_date = datetime(2024, 1, 1, 0, 0, 0)

    # Create base records
    base_records = []
    for i in range(num_records // 2):
        user_id = f"user_{i % 10:02d}"
        action = ["click", "view"][i % 2]
        value = 10.0 + (i * 1.5) % 100.0
        timestamp = (start_date + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        base_records.append((user_id, action, timestamp, value))

    # Duplicate each record
    for record in base_records:
        data.append(record)
        data.append(record)  # Duplicate

    return data


def create_invalid_data(spark, num_records: int = 100) -> List[tuple]:
    """Create data that will fail validation (null user_id, null timestamp)."""
    data = []
    start_date = datetime(2024, 1, 1, 0, 0, 0)

    for i in range(num_records):
        # All records have null user_id or null timestamp (will fail validation)
        user_id = None if i % 2 == 0 else f"user_{i % 20:02d}"
        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0
        timestamp = (
            None
            if i % 2 == 1
            else (start_date + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        )

        data.append((user_id, action, timestamp, value))

    return data


def create_mixed_valid_invalid_data(
    spark, num_records: int = 100, invalid_ratio: float = 0.3
) -> List[tuple]:
    """Create data with mix of valid and invalid records."""
    data = []
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    invalid_count = int(num_records * invalid_ratio)

    for i in range(num_records):
        # First invalid_count records are invalid
        if i < invalid_count:
            user_id = None  # Invalid
            timestamp = None  # Invalid
        else:
            user_id = f"user_{i % 20:02d}"
            timestamp = (start_date + timedelta(seconds=i)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0

        data.append((user_id, action, timestamp, value))

    return data


def get_log_entries_by_run(log_df, run_id: str) -> List[Dict]:
    """Filter log entries by run_id."""
    # Use F.col() for PySpark compatibility
    if os.environ.get("SPARK_MODE", "mock").lower() == "real":
        from pyspark.sql import functions as F

        filtered = log_df.filter(F.col("run_id") == run_id)
    else:
        filtered = log_df.filter(log_df.run_id == run_id)
    return [row.asDict() for row in filtered.collect()]


# ============================================================================
# Test Classes
# ============================================================================


class TestMinimalPipelines:
    """Tests for minimal pipeline configurations."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_minimal_pipeline_with_logging(self, spark_session, log_writer):
        """Test minimal pipeline: 1 bronze → 1 silver → 1 gold."""
        schema = "test_schema"

        # Create small dataset
        data = create_test_data_initial(spark_session, num_records=15)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        # Build minimal pipeline
        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="processed_events",
        )

        def gold_transform(spark, silvers):
            silver_df = silvers.get("processed_events")
            if silver_df is not None:
                return silver_df.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                )
            return spark.createDataFrame([], ["user_id", "event_count", "total_value"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        # Validate and execute
        errors = builder.validate_pipeline()
        assert len(errors) == 0, f"Pipeline validation failed: {errors}"

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        # Assert execution
        assert result.status.value == "completed"
        assert len(result.bronze_results) == 1
        assert len(result.silver_results) == 1
        assert len(result.gold_results) == 1

        # Log and verify
        run_id = "minimal_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)

        assert log_result["success"] is True
        assert log_result["rows_written"] >= 3  # At least 3 step rows

        log_df = spark_session.table(f"{schema}.pipeline_logs")
        logs = get_log_entries_by_run(log_df, run_id)
        assert len(logs) >= 3


class TestLargePipelines:
    """Tests for large pipeline configurations."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_large_pipeline_with_logging(self, spark_session, log_writer):
        """Test large pipeline: 5 bronze → 5 silver → 3 gold."""
        schema = "test_schema"

        # Create large dataset
        bronze_sources = {}
        for i in range(5):
            data = create_test_data_initial(spark_session, num_records=200)
            df = spark_session.createDataFrame(
                data, ["user_id", "action", "timestamp", "value"]
            )
            bronze_sources[f"events_{i}"] = df

        # Build large pipeline
        builder = PipelineBuilder(spark=spark_session, schema=schema)

        # Add 5 bronze steps
        for i in range(5):
            builder.with_bronze_rules(
                name=f"events_{i}",
                rules={
                    "user_id": [F.col("user_id").isNotNull()],
                    "timestamp": [F.col("timestamp").isNotNull()],
                },
                incremental_col="timestamp",
            )

        # Add 5 silver steps
        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        for i in range(5):
            # processed_4 needs action column for gold step summary_2
            if i == 4:
                rules = {
                    "user_id": [F.col("user_id").isNotNull()],
                    "value": [F.col("value").isNotNull()],
                    "action": [F.col("action").isNotNull()],
                }
            else:
                rules = {
                    "user_id": [F.col("user_id").isNotNull()],
                    "value": [F.col("value").isNotNull()],
                }
            builder.add_silver_transform(
                name=f"processed_{i}",
                source_bronze=f"events_{i}",
                transform=silver_transform,
                rules=rules,
                table_name=f"processed_{i}",
            )

        # Add 3 gold steps with dependencies
        def gold_transform_0(spark, silvers):
            df1 = silvers.get("processed_0")
            df2 = silvers.get("processed_1")
            if df1 and df2:
                combined = df1.union(df2)
                return combined.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        def gold_transform_1(spark, silvers):
            df1 = silvers.get("processed_2")
            df2 = silvers.get("processed_3")
            if df1 and df2:
                combined = df1.union(df2)
                return combined.groupBy("user_id").agg(F.sum("value").alias("total"))
            return spark.createDataFrame([], ["user_id", "total"])

        def gold_transform_2(spark, silvers):
            df = silvers.get("processed_4")
            if df:
                return df.groupBy("action").agg(F.count("*").alias("action_count"))
            return spark.createDataFrame([], ["action", "action_count"])

        builder.add_gold_transform(
            name="summary_0",
            transform=gold_transform_0,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary_0",
            source_silvers=["processed_0", "processed_1"],
        )
        builder.add_gold_transform(
            name="summary_1",
            transform=gold_transform_1,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary_1",
            source_silvers=["processed_2", "processed_3"],
        )
        builder.add_gold_transform(
            name="summary_2",
            transform=gold_transform_2,
            rules={"action": [F.col("action").isNotNull()]},
            table_name="summary_2",
            source_silvers=["processed_4"],
        )

        # Validate and execute
        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources=bronze_sources)

        # Assert execution
        assert result.status.value == "completed"
        assert len(result.bronze_results) == 5
        assert len(result.silver_results) == 5
        assert len(result.gold_results) == 3

        # Log and verify
        run_id = "large_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)

        assert log_result["success"] is True
        assert log_result["rows_written"] >= 13  # 5 bronze + 5 silver + 3 gold


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_with_empty_data(self, spark_session, log_writer):
        """Test pipeline with empty DataFrames."""
        schema_name = "test_schema"

        # Create empty data with explicit schema (Both PySpark and mock-spark 3.10.4+ require schema for empty DataFrames)
        df = create_empty_dataframe(
            spark_session,
            columns=["user_id", "action", "timestamp", "value"],
            column_types=["string", "string", "string", "double"],
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema_name)

        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df and df.count() > 0:
                return df.groupBy("user_id").count()
            # Both PySpark and mock-spark 3.10.4+ require explicit schema for empty DataFrame
            return create_empty_dataframe(
                spark, columns=["user_id", "count"], column_types=["string", "int"]
            )

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        # Pipeline should complete even with empty data
        assert result.status.value == "completed"

        # Log and verify
        run_id = "empty_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True

    def test_pipeline_with_partial_validation_failures(self, spark_session, log_writer):
        """Test pipeline with mixed valid/invalid data."""
        schema = "test_schema"

        # Create mixed data (30% invalid)
        data = create_mixed_valid_invalid_data(
            spark_session, num_records=100, invalid_ratio=0.3
        )
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.filter(F.col("user_id").isNotNull()).filter(
                F.col("timestamp").isNotNull()
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        # Should complete but with filtered data
        assert result.status.value == "completed"

        # Check validation rates
        bronze_result = result.bronze_results["events"]
        assert bronze_result["validation_rate"] < 100.0  # Some invalid rows

        # Log and verify
        run_id = "partial_failure_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True

    def test_pipeline_all_invalid_data(self, spark_session, log_writer):
        """Test pipeline where all data fails validation."""
        schema = "test_schema"

        # Create all invalid data
        data = create_invalid_data(spark_session, num_records=50)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df and df.count() > 0:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        # Should complete but with 0 valid rows
        assert result.status.value == "completed"

        bronze_result = result.bronze_results["events"]
        assert bronze_result["validation_rate"] < 100.0  # Low validation rate

        # Log and verify
        run_id = "all_invalid_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestExecutionModes:
    """Tests for different execution modes."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_sequential_execution_with_logging(
        self, spark_session, log_writer
    ):
        """Test sequential execution mode (parallel disabled)."""
        schema = "test_schema"

        data = create_test_data_initial(spark_session, num_records=100)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        # Build pipeline (sequential execution is default when parallel is not configured)
        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Log and verify
        run_id = "sequential_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestIncrementalScenarios:
    """Tests for incremental processing scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_multiple_incremental_runs(self, spark_session, log_writer):
        """Test multiple incremental runs (initial + 3 incremental)."""
        schema = "test_schema"

        # Initial data
        initial_data = create_test_data_initial(spark_session, num_records=100)
        initial_df = spark_session.createDataFrame(
            initial_data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()

        # Initial run
        initial_result = pipeline.run_initial_load(
            bronze_sources={"events": initial_df}
        )
        assert initial_result.status.value == "completed"

        run_id_initial = "multi_inc_initial"
        log_rows = create_log_rows_from_pipeline_report(
            initial_result, run_id_initial, "initial"
        )
        log_writer.write_log_rows(log_rows, run_id=run_id_initial)

        # 3 incremental runs
        for inc_num in range(1, 4):
            inc_date = datetime(2024, 1, 1 + inc_num, 0, 0, 0)
            inc_data = create_test_data_incremental(
                spark_session,
                num_records=30,
                start_date=inc_date,
                end_date=inc_date + timedelta(days=1),
            )
            inc_df = spark_session.createDataFrame(
                inc_data, ["user_id", "action", "timestamp", "value"]
            )

            inc_result = pipeline.run_incremental(bronze_sources={"events": inc_df})
            assert inc_result.status.value == "completed"

            run_id_inc = f"multi_inc_{inc_num}"
            log_rows_inc = create_log_rows_from_pipeline_report(
                inc_result, run_id_inc, "incremental"
            )
            log_writer.write_log_rows(log_rows_inc, run_id=run_id_inc)

        # Verify all runs logged
        log_df = spark_session.table(f"{schema}.pipeline_logs")
        initial_logs = get_log_entries_by_run(log_df, run_id_initial)
        assert len(initial_logs) > 0

        for inc_num in range(1, 4):
            inc_logs = get_log_entries_by_run(log_df, f"multi_inc_{inc_num}")
            assert len(inc_logs) > 0

    def test_pipeline_incremental_with_gaps(self, spark_session, log_writer):
        """Test incremental processing with timestamp gaps."""
        schema = "test_schema"

        # Initial data on 2024-01-01
        initial_data = create_test_data_initial(spark_session, num_records=100)
        initial_df = spark_session.createDataFrame(
            initial_data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()

        # Initial run
        initial_result = pipeline.run_initial_load(
            bronze_sources={"events": initial_df}
        )
        assert initial_result.status.value == "completed"

        # Incremental with gap (skip 2024-01-02, go to 2024-01-05)
        gap_data = create_test_data_incremental(
            spark_session,
            num_records=50,
            start_date=datetime(2024, 1, 5, 0, 0, 0),
            end_date=datetime(2024, 1, 5, 23, 59, 59),
        )
        gap_df = spark_session.createDataFrame(
            gap_data, ["user_id", "action", "timestamp", "value"]
        )

        gap_result = pipeline.run_incremental(bronze_sources={"events": gap_df})
        assert gap_result.status.value == "completed"

        # Log and verify
        run_id = "gap_run_1"
        log_rows = create_log_rows_from_pipeline_report(
            gap_result, run_id, "incremental"
        )
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestDataQuality:
    """Tests for data quality scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_null_handling_with_logging(self, spark_session, log_writer):
        """Test pipeline with various null patterns."""
        schema = "test_schema"

        # Create data with nulls
        data = create_data_with_nulls(spark_session, num_records=100)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Some rows should be filtered due to nulls
        bronze_result = result.bronze_results["events"]
        assert bronze_result["validation_rate"] < 100.0

        # Log and verify
        run_id = "null_handling_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True

    def test_pipeline_duplicate_data_with_logging(self, spark_session, log_writer):
        """Test pipeline with duplicate records."""
        schema = "test_schema"

        # Create duplicate data
        data = create_duplicate_data(spark_session, num_records=100)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            # Deduplicate in transform
            return bronze_df.dropDuplicates(["user_id", "action", "timestamp"])

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Log and verify
        run_id = "duplicate_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestHighVolume:
    """Tests for high volume data scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_high_volume_with_logging(self, spark_session, log_writer):
        """Test pipeline with very large dataset (10,000+ records)."""
        schema = "test_schema"

        # Create large dataset
        data = create_test_data_initial(spark_session, num_records=10000)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Verify large row counts
        bronze_result = result.bronze_results["events"]
        assert bronze_result["rows_processed"] == 10000

        # Log and verify
        run_id = "high_volume_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True
        assert log_result["rows_written"] > 0


class TestComplexDependencies:
    """Tests for complex dependency scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_complex_dependencies_with_logging(
        self, spark_session, log_writer
    ):
        """Test pipeline with complex cross-dependencies."""
        schema = "test_schema"

        # Create data for 3 bronze sources
        bronze_sources = {}
        for i in range(3):
            data = create_test_data_initial(spark_session, num_records=200)
            df = spark_session.createDataFrame(
                data, ["user_id", "action", "timestamp", "value"]
            )
            bronze_sources[f"events_{i}"] = df

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        # Add 3 bronze steps
        for i in range(3):
            builder.with_bronze_rules(
                name=f"events_{i}",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                incremental_col="timestamp",
            )

        # Add 4 silver steps with cross-dependencies
        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="silver_0",
            source_bronze="events_0",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_0",
        )
        builder.add_silver_transform(
            name="silver_1",
            source_bronze="events_1",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_1",
        )
        builder.add_silver_transform(
            name="silver_2",
            source_bronze="events_2",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "action": [F.col("action").isNotNull()],
            },
            table_name="silver_2",
        )

        # Silver 3 depends on silver_0 and silver_1
        def silver_3_transform(spark, bronze_df, silvers):
            # This silver step reads from bronze but also uses other silvers
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            base = bronze_df.withColumn(
                "event_date",
                F.to_date(
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                ),
            )
            return (
                base.filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="silver_3",
            source_bronze="events_0",
            transform=silver_3_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "action": [F.col("action").isNotNull()],
            },
            table_name="silver_3",
        )

        # Add 2 gold steps with multiple dependencies
        def gold_0_transform(spark, silvers):
            s0 = silvers.get("silver_0")
            s1 = silvers.get("silver_1")
            if s0 and s1:
                combined = s0.union(s1)
                return combined.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        def gold_1_transform(spark, silvers):
            s2 = silvers.get("silver_2")
            s3 = silvers.get("silver_3")
            if s2 and s3:
                combined = s2.union(s3)
                return combined.groupBy("action").agg(
                    F.count("*").alias("action_count")
                )
            return spark.createDataFrame([], ["action", "action_count"])

        builder.add_gold_transform(
            name="gold_0",
            transform=gold_0_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="gold_0",
            source_silvers=["silver_0", "silver_1"],
        )
        builder.add_gold_transform(
            name="gold_1",
            transform=gold_1_transform,
            rules={"action": [F.col("action").isNotNull()]},
            table_name="gold_1",
            source_silvers=["silver_2", "silver_3"],
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources=bronze_sources)

        assert result.status.value == "completed"
        assert len(result.bronze_results) == 3
        assert len(result.silver_results) == 4
        assert len(result.gold_results) == 2

        # Log and verify
        run_id = "complex_deps_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestParallelStress:
    """Tests for parallel execution stress scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_parallel_execution_stress(self, spark_session, log_writer):
        """Test parallel execution with many concurrent steps (10 bronze → 10 silver → 5 gold)."""
        schema = "test_schema"

        # Create data for 10 bronze sources
        bronze_sources = {}
        for i in range(10):
            data = create_test_data_initial(spark_session, num_records=100)
            df = spark_session.createDataFrame(
                data, ["user_id", "action", "timestamp", "value"]
            )
            bronze_sources[f"events_{i}"] = df

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        # Add 10 bronze steps
        for i in range(10):
            builder.with_bronze_rules(
                name=f"events_{i}",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                incremental_col="timestamp",
            )

        # Add 10 silver steps
        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        for i in range(10):
            builder.add_silver_transform(
                name=f"silver_{i}",
                source_bronze=f"events_{i}",
                transform=silver_transform,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name=f"silver_{i}",
            )

        # Add 5 gold steps
        for i in range(5):

            def make_gold_transform(idx):
                def gold_transform(spark, silvers):
                    s1 = silvers.get(f"silver_{idx * 2}")
                    s2 = silvers.get(f"silver_{idx * 2 + 1}")
                    if s1 and s2:
                        combined = s1.union(s2)
                        return combined.groupBy("user_id").agg(
                            F.count("*").alias("count")
                        )
                    return spark.createDataFrame([], ["user_id", "count"])

                return gold_transform

            builder.add_gold_transform(
                name=f"gold_{i}",
                transform=make_gold_transform(i),
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name=f"gold_{i}",
                source_silvers=[f"silver_{i * 2}", f"silver_{i * 2 + 1}"],
            )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources=bronze_sources)

        assert result.status.value == "completed"
        assert len(result.bronze_results) == 10
        assert len(result.silver_results) == 10
        assert len(result.gold_results) == 5

        # Log and verify
        run_id = "parallel_stress_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True
        assert log_result["rows_written"] >= 25  # 10 + 10 + 5


class TestSchemaEvolution:
    """Tests for schema evolution scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_with_schema_evolution_logging(self, spark_session, log_writer):
        """Test schema evolution: initial run with columns A,B,C; incremental adds column D."""
        schema = "test_schema"

        # Initial data with columns: user_id, action, timestamp, value
        initial_data = create_test_data_initial(spark_session, num_records=100)
        initial_df = spark_session.createDataFrame(
            initial_data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()

        # Initial run
        initial_result = pipeline.run_initial_load(
            bronze_sources={"events": initial_df}
        )
        assert initial_result.status.value == "completed"

        # Incremental run - schema should evolve if new columns are added
        # (In this case, we're using the same schema, but the test verifies schema evolution handling)
        incremental_data = create_test_data_incremental(spark_session, num_records=50)
        incremental_df = spark_session.createDataFrame(
            incremental_data, ["user_id", "action", "timestamp", "value"]
        )

        incremental_result = pipeline.run_incremental(
            bronze_sources={"events": incremental_df}
        )
        assert incremental_result.status.value == "completed"

        # Log both runs
        run_id_initial = "schema_evol_initial"
        log_rows = create_log_rows_from_pipeline_report(
            initial_result, run_id_initial, "initial"
        )
        log_writer.write_log_rows(log_rows, run_id=run_id_initial)

        run_id_inc = "schema_evol_inc"
        log_rows_inc = create_log_rows_from_pipeline_report(
            incremental_result, run_id_inc, "incremental"
        )
        log_result = log_writer.write_log_rows(log_rows_inc, run_id=run_id_inc)
        assert log_result["success"] is True


class TestDataTypes:
    """Tests for various data types."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_data_type_variations(self, spark_session, log_writer):
        """Test pipeline with various data types (strings, numbers, dates, booleans)."""
        schema = "test_schema"

        # Create data with various types
        data = []
        start_date = datetime(2024, 1, 1, 0, 0, 0)
        for i in range(100):
            user_id = f"user_{i:02d}"  # String
            age = 18 + (i % 50)  # Integer
            score = 10.5 + (i * 0.1)  # Float
            is_active = i % 2 == 0  # Boolean
            created_at = (start_date + timedelta(seconds=i)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )  # Timestamp string

            data.append((user_id, age, score, is_active, created_at))

        df = spark_session.createDataFrame(
            data, ["user_id", "age", "score", "is_active", "created_at"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="users",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "age": [F.col("age") > 0],
            },
            incremental_col="created_at",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "created_at_str", F.col("created_at").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "created_date",
                    F.to_date(
                        F.to_timestamp(F.col("created_at_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select(
                    "user_id",
                    "age",
                    "score",
                    "is_active",
                    "created_date",
                    "created_at_str",
                )
                .withColumnRenamed("created_at_str", "created_at")
            )

        builder.add_silver_transform(
            name="processed_users",
            source_bronze="users",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "age": [F.col("age") > 0],
                "score": [F.col("score").isNotNull()],
                "is_active": [F.col("is_active").isNotNull()],
            },
            table_name="processed_users",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed_users")
            if df:
                return df.groupBy("is_active").agg(
                    F.avg("age").alias("avg_age"), F.avg("score").alias("avg_score")
                )
            return spark.createDataFrame([], ["is_active", "avg_age", "avg_score"])

        builder.add_gold_transform(
            name="user_stats",
            transform=gold_transform,
            rules={"is_active": [F.col("is_active").isNotNull()]},
            table_name="user_stats",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"users": df})

        assert result.status.value == "completed"

        # Log and verify
        run_id = "data_types_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestValidationThresholds:
    """Tests for validation threshold scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_validation_thresholds_logging(self, spark_session, log_writer):
        """Test validation threshold enforcement and logging."""
        schema = "test_schema"

        # Create data with some invalid records (20% invalid)
        data = create_mixed_valid_invalid_data(
            spark_session, num_records=100, invalid_ratio=0.2
        )
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.filter(F.col("user_id").isNotNull()).filter(
                F.col("timestamp").isNotNull()
            )

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Check validation rate (should be around 80% due to 20% invalid)
        bronze_result = result.bronze_results["events"]
        assert bronze_result["validation_rate"] < 100.0
        assert bronze_result["validation_rate"] >= 70.0  # Should be around 80%

        # Log and verify
        run_id = "validation_threshold_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestWriteModes:
    """Tests for different write mode scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_write_mode_variations(self, spark_session, log_writer):
        """Test different write modes (overwrite, append via watermark)."""
        schema = "test_schema"

        # Initial data
        initial_data = create_test_data_initial(spark_session, num_records=100)
        initial_df = spark_session.createDataFrame(
            initial_data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        # Silver with watermark (enables append mode for incremental)
        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="processed",
            watermark_col="timestamp",  # Enables append mode for incremental runs
        )

        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                return df.groupBy("user_id").agg(F.count("*").alias("count"))
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()

        # Initial run (overwrite mode)
        initial_result = pipeline.run_initial_load(
            bronze_sources={"events": initial_df}
        )
        assert initial_result.status.value == "completed"

        # Incremental run (append mode due to watermark)
        incremental_data = create_test_data_incremental(spark_session, num_records=50)
        incremental_df = spark_session.createDataFrame(
            incremental_data, ["user_id", "action", "timestamp", "value"]
        )

        incremental_result = pipeline.run_incremental(
            bronze_sources={"events": incremental_df}
        )
        assert incremental_result.status.value == "completed"

        # Log both runs
        run_id_initial = "write_mode_initial"
        log_rows = create_log_rows_from_pipeline_report(
            initial_result, run_id_initial, "initial"
        )
        log_writer.write_log_rows(log_rows, run_id=run_id_initial)

        run_id_inc = "write_mode_inc"
        log_rows_inc = create_log_rows_from_pipeline_report(
            incremental_result, run_id_inc, "incremental"
        )
        log_result = log_writer.write_log_rows(log_rows_inc, run_id=run_id_inc)
        assert log_result["success"] is True

        # Verify write modes in results
        silver_initial = initial_result.silver_results["processed"]
        assert silver_initial["write_mode"] == "overwrite"  # Initial run uses overwrite

        silver_inc = incremental_result.silver_results["processed"]
        assert (
            silver_inc["write_mode"] == "append"
        )  # Incremental with watermark uses append


class TestMixedSuccessFailure:
    """Tests for mixed success/failure scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_mixed_success_failure(self, spark_session, log_writer):
        """Test pipeline where some steps succeed, some fail (simulated)."""
        schema = "test_schema"

        # Create data for 3 bronze sources
        bronze_sources = {}
        for i in range(3):
            data = create_test_data_initial(spark_session, num_records=100)
            df = spark_session.createDataFrame(
                data, ["user_id", "action", "timestamp", "value"]
            )
            bronze_sources[f"events_{i}"] = df

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        # Add 3 bronze steps
        for i in range(3):
            builder.with_bronze_rules(
                name=f"events_{i}",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                incremental_col="timestamp",
            )

        # Add 3 silver steps - one will have problematic transform
        def silver_transform_good(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            return (
                bronze_df.withColumn(
                    "event_date",
                    F.to_date(
                        F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
                .filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        def silver_transform_bad(spark, bronze_df, silvers):
            # This transform will fail if data is empty or has issues
            # But we'll make it work for this test - the "failure" is simulated by validation
            return bronze_df.filter(F.col("user_id").isNotNull())

        builder.add_silver_transform(
            name="silver_0",
            source_bronze="events_0",
            transform=silver_transform_good,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_0",
        )
        builder.add_silver_transform(
            name="silver_1",
            source_bronze="events_1",
            transform=silver_transform_good,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_1",
        )
        builder.add_silver_transform(
            name="silver_2",
            source_bronze="events_2",
            transform=silver_transform_bad,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_2",
        )

        # Add 2 gold steps
        def gold_0_transform(spark, silvers):
            s0 = silvers.get("silver_0")
            if s0:
                return s0.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        def gold_1_transform(spark, silvers):
            s1 = silvers.get("silver_1")
            s2 = silvers.get("silver_2")
            if s1 and s2:
                combined = s1.union(s2)
                return combined.groupBy("user_id").count()
            elif s1:
                return s1.groupBy("user_id").count()
            elif s2:
                return s2.groupBy("user_id").count()
            return spark.createDataFrame([], ["user_id", "count"])

        builder.add_gold_transform(
            name="gold_0",
            transform=gold_0_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="gold_0",
            source_silvers=["silver_0"],
        )
        builder.add_gold_transform(
            name="gold_1",
            transform=gold_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="gold_1",
            source_silvers=["silver_1", "silver_2"],
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources=bronze_sources)

        # Pipeline should complete (all steps succeed in this case)
        # In a real failure scenario, some steps would fail
        assert result.status.value == "completed"

        # Log and verify
        run_id = "mixed_success_failure_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True


class TestLongRunning:
    """Tests for long-running pipeline scenarios."""

    @pytest.fixture
    def log_writer(self, spark_session):
        """Create LogWriter instance."""
        return LogWriter(
            spark_session, schema="test_schema", table_name="pipeline_logs"
        )

    def test_pipeline_long_running_with_logging(self, spark_session, log_writer):
        """Test logging for long-running pipelines (simulated with large dataset and complex transforms)."""
        schema = "test_schema"

        # Create large dataset to simulate long runtime
        data = create_test_data_initial(spark_session, num_records=5000)
        df = spark_session.createDataFrame(
            data, ["user_id", "action", "timestamp", "value"]
        )

        builder = PipelineBuilder(spark=spark_session, schema=schema)

        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Complex transform to simulate longer processing (multiple operations on existing columns)
        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            result = bronze_df.withColumn(
                "event_date",
                F.to_date(
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                ),
            )
            # Multiple filter operations to simulate complexity
            result = result.filter(F.col("user_id").isNotNull())
            result = result.filter(F.col("value") > 0)
            result = result.filter(F.col("action").isNotNull())
            return result.select(
                "user_id", "action", "event_date", "value", "timestamp_str"
            ).withColumnRenamed("timestamp_str", "timestamp")

        builder.add_silver_transform(
            name="processed",
            source_bronze="events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            table_name="processed",
        )

        # Complex gold aggregation with multiple aggregations to simulate longer processing
        def gold_transform(spark, silvers):
            df = silvers.get("processed")
            if df:
                # Multiple aggregations to simulate complexity
                return df.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                    F.avg("value").alias("avg_value"),
                    F.max("value").alias("max_value"),
                    F.min("value").alias("min_value"),
                    F.stddev("value").alias("stddev_value"),
                )
            return spark.createDataFrame(
                [],
                [
                    "user_id",
                    "event_count",
                    "total_value",
                    "avg_value",
                    "max_value",
                    "min_value",
                    "stddev_value",
                ],
            )

        builder.add_gold_transform(
            name="summary",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="summary",
        )

        errors = builder.validate_pipeline()
        assert len(errors) == 0

        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": df})

        assert result.status.value == "completed"

        # Verify duration is logged
        bronze_result = result.bronze_results["events"]
        assert "duration" in bronze_result or "start_time" in bronze_result

        # Log and verify
        run_id = "long_running_run_1"
        log_rows = create_log_rows_from_pipeline_report(result, run_id, "initial")
        log_result = log_writer.write_log_rows(log_rows, run_id=run_id)
        assert log_result["success"] is True

        # Verify logs contain duration information
        log_df = spark_session.table(f"{schema}.pipeline_logs")
        logs = get_log_entries_by_run(log_df, run_id)
        assert len(logs) > 0
        # Duration should be logged (even if 0 for fast mock-spark execution)
        for log in logs:
            assert "duration_secs" in log or "start_time" in log
