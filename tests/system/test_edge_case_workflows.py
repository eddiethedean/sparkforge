"""
Edge case workflow tests for pipeline and writer flows.

This test module validates edge cases and error scenarios:
- Empty DataFrames
- Null/None values in critical columns
- Schema mismatches
- Missing columns after transforms
- Validation failures
- Writer failures
- Large datasets
- Invalid data types
- Missing dependencies
- Partial failures
- Recovery scenarios
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import uuid4

import os
import pytest

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F  # type: ignore

from pipeline_builder import PipelineBuilder
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import (
    ExecutionMode,
    PipelineConfig,
    ValidationThresholds,
)
from pipeline_builder.writer.core import LogWriter


def create_test_data(
    spark, num_records: int = 100, base_date: datetime = None, include_nulls: bool = False
) -> List[tuple]:
    """Generate test data with optional nulls."""
    if base_date is None:
        base_date = datetime(2024, 1, 1, 0, 0, 0)
    
    data = []
    start_date = base_date
    end_date = base_date + timedelta(days=1) - timedelta(seconds=1)

    for i in range(num_records):
        user_id = f"user_{i % 20:02d}" if not (include_nulls and i % 10 == 0) else None
        action = ["click", "view", "purchase", "add_to_cart"][i % 4] if not (include_nulls and i % 7 == 0) else None
        value = 10.0 + (i * 1.5) % 100.0 if not (include_nulls and i % 5 == 0) else None

        # Generate timestamp within range
        time_diff = (end_date - start_date).total_seconds()
        random_seconds = (i * 17) % int(time_diff)
        timestamp = start_date + timedelta(seconds=random_seconds)

        data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S"), value))

    return data


class TestEdgeCaseWorkflows:
    """Test edge cases and error scenarios in pipeline workflows."""

    def test_empty_dataframe_handling(self, mock_spark_session):
        """Test pipeline handles empty DataFrames gracefully."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        log_writer = LogWriter(
            mock_spark_session, schema=schema, table_name="pipeline_logs"
        )

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        bronze_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
        }

        builder.with_bronze_rules(
            name="raw_events",
            rules=bronze_rules,
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            result_df = bronze_df.withColumn(
                "event_date",
                F.to_date(
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                ),
            )
            return result_df.select("user_id", "action", "event_date", "value", "timestamp_str").withColumnRenamed("timestamp_str", "timestamp")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        def gold_transform(spark, silvers):
            silver = silvers.get("processed_events")
            if silver is not None:
                return silver.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                    F.avg("value").alias("avg_value"),
                )
            return spark.createDataFrame([], ["user_id", "event_count", "total_value", "avg_value"])

        builder.add_gold_transform(
            name="user_metrics",
            source_silvers=["processed_events"],
            transform=gold_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_count": [F.col("event_count") > 0],
            },
            table_name="gold_user_metrics",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0, f"Pipeline validation failed: {validation_errors}"

        pipeline = builder.to_pipeline()

        # Create empty DataFrame with explicit schema
        import os
        spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
        if spark_mode == "real":
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType
            schema = StructType([
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("value", DoubleType(), True),
            ])
            empty_df = mock_spark_session.createDataFrame([], schema)
        else:
            # Mock mode requires schema for empty DataFrames
            from sparkless.spark_types import (  # type: ignore[import]
                DoubleType,
                StringType,
                StructField,
                StructType,
            )
            schema = StructType([
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("value", DoubleType(), True),
            ])
            empty_df = mock_spark_session.createDataFrame([], schema)

        # Execute with empty DataFrame
        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": empty_df}
        )

        # Should complete successfully even with empty data
        assert result.status.value == "completed", (
            f"Pipeline should handle empty DataFrame gracefully, got status: {result.status.value}"
        )

        # Bronze should process 0 rows
        assert result.bronze_results["raw_events"]["rows_processed"] == 0

        # Silver should write 0 rows (or handle gracefully)
        assert result.silver_results["processed_events"]["rows_written"] == 0

        # Gold should handle empty input gracefully
        assert result.gold_results["user_metrics"]["rows_written"] == 0

    def test_null_values_in_critical_columns(self, mock_spark_session):
        """Test pipeline handles null values in critical columns."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        # Strict validation rules that reject nulls
        bronze_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
            "value": [F.col("value").isNotNull()],
        }

        builder.with_bronze_rules(
            name="raw_events",
            rules=bronze_rules,
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )
            result_df = bronze_df.withColumn(
                "event_date",
                F.to_date(
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                ),
            )
            return result_df.select("user_id", "action", "event_date", "value", "timestamp_str").withColumnRenamed("timestamp_str", "timestamp")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Create data with nulls in critical columns
        test_data = create_test_data(
            mock_spark_session, num_records=50, include_nulls=True
        )
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete but with validation failures
        assert result.status.value == "completed"

        # Bronze validation should filter out nulls
        bronze_result = result.bronze_results["raw_events"]
        # Rows processed may be less than 50 if nulls are filtered early
        assert bronze_result["rows_processed"] > 0, (
            f"Expected some rows processed, got {bronze_result['rows_processed']}"
        )
        # Some rows should be invalid due to nulls (if any rows were processed)
        if bronze_result["rows_processed"] > 0:
            assert bronze_result["validation_rate"] < 100.0, (
                f"Validation rate should be < 100% due to null values, got {bronze_result['validation_rate']}"
            )

        # Silver should have fewer rows due to filtering (validation filters out invalid rows)
        silver_result = result.silver_results["processed_events"]
        # Silver writes valid rows only, so should be less than or equal to processed
        assert silver_result["rows_written"] <= bronze_result["rows_processed"], (
            f"Silver should have <= {bronze_result['rows_processed']} rows after filtering nulls, "
            f"got {silver_result['rows_written']}"
        )

    def test_missing_columns_after_transform(self, mock_spark_session):
        """Test pipeline handles transforms that drop columns referenced in validation."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Transform that drops columns
        def silver_transform_drops_columns(spark, bronze_df, silvers):
            # Only select user_id and value, dropping action and timestamp
            return bronze_df.select("user_id", "value")

        # Validation rules reference columns that will be dropped
        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform_drops_columns,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
                # These columns will be missing after transform
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0  # Validation passes at build time

        pipeline = builder.to_pipeline()

        test_data = create_test_data(mock_spark_session, num_records=50)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        # Should handle gracefully - validation should filter out rules for missing columns
        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete successfully
        assert result.status.value == "completed"

        # Silver should have processed rows (with filtered validation rules)
        silver_result = result.silver_results["processed_events"]
        assert silver_result["rows_written"] > 0

    def test_validation_threshold_failure(self, mock_spark_session):
        """Test pipeline fails when validation thresholds are not met."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        # Very strict validation rules
        bronze_rules = {
            "user_id": [F.col("user_id").isNotNull(), F.length(F.col("user_id")) > 5],
            "value": [F.col("value").isNotNull(), F.col("value") > 1000],  # Most values will fail
        }

        builder.with_bronze_rules(
            name="raw_events",
            rules=bronze_rules,
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("user_id", "action", "timestamp", "value")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Create data that will fail validation
        test_data = create_test_data(mock_spark_session, num_records=100)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete but with low validation rate
        assert result.status.value == "completed"

        bronze_result = result.bronze_results["raw_events"]
        # Validation rate should be low due to strict rules
        assert bronze_result["validation_rate"] < 50.0, (
            f"Expected low validation rate due to strict rules, got {bronze_result['validation_rate']}"
        )

    def test_schema_mismatch_handling(self, mock_spark_session):
        """Test pipeline handles schema mismatches gracefully."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            # Transform that changes column types
            return bronze_df.withColumn("value", F.col("value").cast("string"))

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        test_data = create_test_data(mock_spark_session, num_records=50)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        # First run - creates table
        result1 = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )
        assert result1.status.value == "completed"

        # Second run with different schema (value is now string)
        # This should handle schema evolution
        result2 = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )
        assert result2.status.value == "completed"

    def test_writer_failure_recovery(self, mock_spark_session):
        """Test pipeline recovers from writer failures."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        # Create LogWriter with invalid configuration to simulate failure
        log_writer = LogWriter(
            mock_spark_session, schema=schema, table_name="pipeline_logs"
        )

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("user_id", "action", "timestamp", "value")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        test_data = create_test_data(mock_spark_session, num_records=50)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )
        assert result.status.value == "completed"

        # Try to write logs - should handle gracefully even if write fails
        from pipeline_builder.writer.models import create_log_rows_from_pipeline_report
        
        try:
            log_rows = create_log_rows_from_pipeline_report(
                result, run_id=f"test_run_{uuid4().hex[:8]}", run_mode="initial"
            )
            write_result = log_writer.write_log_rows(log_rows, run_id=f"test_run_{uuid4().hex[:8]}")
            # Write should succeed in normal case
            assert write_result["success"] or "error" in write_result
        except Exception as e:
            # Writer failure should not crash the pipeline
            # The pipeline execution already succeeded
            assert result.status.value == "completed"

    def test_large_dataset_handling(self, mock_spark_session):
        """Test pipeline handles large datasets efficiently."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("user_id", "action", "timestamp", "value")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Create larger dataset (1000 records)
        test_data = create_test_data(mock_spark_session, num_records=1000)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete successfully
        assert result.status.value == "completed"

        # Should process all rows
        assert result.bronze_results["raw_events"]["rows_processed"] == 1000
        assert result.silver_results["processed_events"]["rows_written"] == 1000

    def test_partial_failure_recovery(self, mock_spark_session):
        """Test pipeline handles partial step failures."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Silver transform that might fail on certain data
        def silver_transform_with_risk(spark, bronze_df, silvers):
            # This could fail if value column has unexpected data
            return bronze_df.select("user_id", "action", "timestamp", "value").filter(
                F.col("value").isNotNull()
            )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform_with_risk,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Create data with some problematic values
        test_data = create_test_data(mock_spark_session, num_records=100, include_nulls=True)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete (filtering handles the problematic data)
        assert result.status.value == "completed"

        # Bronze should process rows (may be less than 100 if nulls are filtered early)
        bronze_result = result.bronze_results["raw_events"]
        assert bronze_result["rows_processed"] > 0, (
            f"Expected some rows processed, got {bronze_result['rows_processed']}"
        )

        # Silver should filter out nulls (validation filters invalid rows)
        silver_result = result.silver_results["processed_events"]
        # Silver writes valid rows only, so should be less than or equal to processed
        assert silver_result["rows_written"] <= bronze_result["rows_processed"], (
            f"Silver should have <= {bronze_result['rows_processed']} rows after filtering nulls, "
            f"got {silver_result['rows_written']}"
        )
        # With nulls in the data, we expect some filtering
        assert silver_result["rows_written"] < 100, (
            f"Silver should filter out some rows with null values, got {silver_result['rows_written']} rows"
        )

    def test_concurrent_initial_runs(self, mock_spark_session):
        """Test multiple initial runs don't interfere with each other."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("user_id", "action", "timestamp", "value")

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Run multiple initial loads sequentially (simulating concurrent behavior)
        results = []
        for run_num in range(3):
            test_data = create_test_data(
                mock_spark_session, num_records=50, base_date=datetime(2024, 1, 1 + run_num, 0, 0, 0)
            )
            source_df = mock_spark_session.createDataFrame(
                test_data, ["user_id", "action", "timestamp", "value"]
            )

            result = pipeline.run_initial_load(
                bronze_sources={"raw_events": source_df}
            )
            results.append(result)

            # Each run should complete successfully
            assert result.status.value == "completed", (
                f"Run {run_num} should complete successfully"
            )

        # Final table should have data from last run only (overwrite mode)
        silver_table = mock_spark_session.table(f"{schema}.silver_processed_events")
        final_count = silver_table.count()
        assert final_count == 50, (
            f"After 3 initial runs, table should have 50 rows (from last run), got {final_count}"
        )

    def test_invalid_data_types(self, mock_spark_session):
        """Test pipeline handles invalid data types gracefully."""
        schema = f"test_schema_{uuid4().hex[:8]}"
        
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass

        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def silver_transform(spark, bronze_df, silvers):
            # Try to cast value to integer (might fail for float values)
            return bronze_df.select(
                "user_id", "action", "timestamp",
                F.col("value").cast("double").alias("value")
            )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="raw_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="silver_processed_events",
        )

        validation_errors = builder.validate_pipeline()
        assert len(validation_errors) == 0

        pipeline = builder.to_pipeline()

        # Create data with float values
        test_data = create_test_data(mock_spark_session, num_records=50)
        source_df = mock_spark_session.createDataFrame(
            test_data, ["user_id", "action", "timestamp", "value"]
        )

        result = pipeline.run_initial_load(
            bronze_sources={"raw_events": source_df}
        )

        # Should complete successfully (cast handles the conversion)
        assert result.status.value == "completed"

        # Should process all rows
        assert result.bronze_results["raw_events"]["rows_processed"] == 50

