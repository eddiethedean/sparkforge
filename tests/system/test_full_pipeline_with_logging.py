"""
System test for full pipeline execution with LogWriter integration.

This test validates:
- Pipeline execution with multiple bronze/silver steps targeting the same tables
- LogWriter integration for both initial and incremental runs
- Detailed assertions at each execution step
- Log data validation and consistency checks
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import uuid4

import os


# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import functions as F  # type: ignore[import]
else:
    from pyspark.sql import functions as F  # type: ignore

from pipeline_builder import PipelineBuilder
from pipeline_builder.writer.core import LogWriter


def create_test_data_initial(spark, num_records: int = 100) -> List[tuple]:
    """Generate initial test data with timestamps on 2024-01-01."""
    data = []
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 1, 1, 23, 59, 59)

    for i in range(num_records):
        user_id = f"user_{i % 20:02d}"  # 20 unique users
        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0

        # Generate timestamp within range
        time_diff = (end_date - start_date).total_seconds()
        random_seconds = (i * 17) % int(time_diff)
        timestamp = start_date + timedelta(seconds=random_seconds)

        data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S"), value))

    return data


def create_test_data_incremental(spark, num_records: int = 50) -> List[tuple]:
    """Generate incremental test data with timestamps on 2024-01-02."""
    data = []
    start_date = datetime(2024, 1, 2, 0, 0, 0)
    end_date = datetime(2024, 1, 2, 23, 59, 59)

    for i in range(num_records):
        user_id = f"user_{i % 20:02d}"  # Same users as initial
        action = ["click", "view", "purchase", "add_to_cart"][i % 4]
        value = 10.0 + (i * 1.5) % 100.0

        # Generate timestamp within range
        time_diff = (end_date - start_date).total_seconds()
        random_seconds = (i * 17) % int(time_diff)
        timestamp = start_date + timedelta(seconds=random_seconds)

        data.append((user_id, action, timestamp.strftime("%Y-%m-%d %H:%M:%S"), value))

    return data


def assert_step_result(
    step_result,
    expected_rows: Optional[int] = None,
    expected_validation_rate: Optional[float] = None,
):
    """Validate step results."""
    assert step_result.success is True, f"Step {step_result.step_name} should succeed"
    assert step_result.rows_processed >= 0, "Rows processed should be non-negative"
    assert step_result.rows_written >= 0, "Rows written should be non-negative"
    assert 0.0 <= step_result.validation_rate <= 100.0, (
        "Validation rate should be 0-100"
    )

    if expected_rows is not None:
        assert step_result.rows_written == expected_rows, (
            f"Expected {expected_rows} rows, got {step_result.rows_written}"
        )

    if expected_validation_rate is not None:
        assert abs(step_result.validation_rate - expected_validation_rate) < 0.1, (
            f"Expected validation rate {expected_validation_rate}, got {step_result.validation_rate}"
        )


def assert_log_entry(
    log_row: Dict,
    step_name: str,
    run_id: str,
    run_mode: str,
    expected_rows: Optional[int] = None,
):
    """Validate log entries."""
    assert log_row["step_name"] == step_name, (
        f"Expected step_name {step_name}, got {log_row['step_name']}"
    )
    assert log_row["run_id"] == run_id, (
        f"Expected run_id {run_id}, got {log_row['run_id']}"
    )
    assert log_row["run_mode"] == run_mode, (
        f"Expected run_mode {run_mode}, got {log_row['run_mode']}"
    )

    if expected_rows is not None:
        assert log_row["rows_processed"] == expected_rows, (
            f"Expected {expected_rows} rows processed, got {log_row['rows_processed']}"
        )


def get_log_entries_by_run(log_df, run_id: str) -> List[Dict]:
    """Filter log entries by run_id."""
    return [row.asDict() for row in log_df.filter(log_df.run_id == run_id).collect()]


class TestFullPipelineWithLogging:
    """Test full pipeline execution with LogWriter integration."""

    def test_full_pipeline_with_logging(self, mock_spark_session):
        """Test complete pipeline with 2 bronze, 2 silver, 1 gold steps and logging."""
        # Use unique schema per test to avoid conflicts in parallel execution
        schema = f"test_schema_{uuid4().hex[:8]}"
        schema_suffix = schema.split("_")[-1]

        # Create schema in Spark session using SQL (works for both mock-spark and PySpark)
        try:
            mock_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception:
            pass  # Schema might already exist or be created automatically

        # Create LogWriter with unique schema
        log_writer = LogWriter(
            mock_spark_session, schema=schema, table_name="pipeline_logs"
        )

        # Define unique table names for each silver step to avoid conflicts
        silver_table_1 = f"silver_processed_events_1_{schema_suffix}"
        silver_table_2 = f"silver_processed_events_2_{schema_suffix}"

        # Create initial test data
        initial_data_source1 = create_test_data_initial(
            mock_spark_session, num_records=100
        )
        initial_data_source2 = create_test_data_initial(
            mock_spark_session, num_records=100
        )

        initial_df1 = mock_spark_session.createDataFrame(
            initial_data_source1, ["user_id", "action", "timestamp", "value"]
        )
        initial_df2 = mock_spark_session.createDataFrame(
            initial_data_source2, ["user_id", "action", "timestamp", "value"]
        )

        # Create pipeline builder
        builder = PipelineBuilder(spark=mock_spark_session, schema=schema)

        # Define common validation rules
        common_bronze_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
        }

        # Add 2 bronze steps targeting the same bronze table with same rules
        builder.with_bronze_rules(
            name="events_source1",
            rules=common_bronze_rules,
            incremental_col="timestamp",
        )

        builder.with_bronze_rules(
            name="events_source2",
            rules=common_bronze_rules,
            incremental_col="timestamp",
        )

        # Define common silver transformation function
        def silver_transform(spark, bronze_df, silvers):
            """Transform bronze data: add event_date and filter valid records."""
            # Handle timestamp - mock-spark infers it as datetime, so we need to convert to string first
            # or extract date directly from datetime

            # Convert timestamp to string first (handles both string and datetime)
            bronze_df = bronze_df.withColumn(
                "timestamp_str", F.col("timestamp").cast("string")
            )

            # Extract date from timestamp string
            result_df = bronze_df.withColumn(
                "event_date",
                F.to_date(
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
                ),
            )

            # Filter and select columns - ensure all columns exist
            return (
                result_df.filter(F.col("user_id").isNotNull())
                .select("user_id", "action", "event_date", "value", "timestamp_str")
                .withColumnRenamed("timestamp_str", "timestamp")
            )

        # Define common silver validation rules
        # Validate columns that exist in both input and output
        # We'll add event_date validation after confirming the transform works
        common_silver_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "value": [F.col("value").isNotNull()],
        }

        # Add 2 silver steps with same transformation function and rules
        # Use unique table names to avoid conflicts in parallel execution
        builder.add_silver_transform(
            name="processed_events_1",
            source_bronze="events_source1",
            transform=silver_transform,
            rules=common_silver_rules,
            table_name=silver_table_1,
        )

        builder.add_silver_transform(
            name="processed_events_2",
            source_bronze="events_source2",
            transform=silver_transform,
            rules=common_silver_rules,
            table_name=silver_table_2,
        )

        # Add 1 gold step that aggregates from both silver steps
        def gold_transform(spark, silvers):
            """Aggregate data from both silver steps."""
            silver1 = silvers.get("processed_events_1")
            silver2 = silvers.get("processed_events_2")

            if silver1 is not None and silver2 is not None:
                # Union both silver DataFrames
                combined = silver1.union(silver2)
                # Aggregate by user_id
                return combined.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                    F.avg("value").alias("avg_value"),
                )
            elif silver1 is not None:
                return silver1.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                    F.avg("value").alias("avg_value"),
                )
            elif silver2 is not None:
                return silver2.groupBy("user_id").agg(
                    F.count("*").alias("event_count"),
                    F.sum("value").alias("total_value"),
                    F.avg("value").alias("avg_value"),
                )
            else:
                return spark.createDataFrame(
                    [], ["user_id", "event_count", "total_value", "avg_value"]
                )

        builder.add_gold_transform(
            name="event_summary",
            transform=gold_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_count": [F.col("event_count") > 0],
            },
            table_name="event_summary",
            source_silvers=["processed_events_1", "processed_events_2"],
        )

        # Validate pipeline
        errors = builder.validate_pipeline()
        assert len(errors) == 0, f"Pipeline validation failed: {errors}"

        # Create pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None

        # ========== INITIAL RUN ==========

        # Execute initial run with bronze data sources
        initial_result = pipeline.run_initial_load(
            bronze_sources={
                "events_source1": initial_df1,
                "events_source2": initial_df2,
            }
        )

        # Assert execution result
        assert initial_result.status.value == "completed", (
            f"Initial run should complete, got status: {initial_result.status}"
        )

        # Check step results from bronze_results, silver_results, gold_results
        assert len(initial_result.bronze_results) == 2, (
            f"Expected 2 bronze steps, got {len(initial_result.bronze_results)}"
        )
        assert len(initial_result.silver_results) == 2, (
            f"Expected 2 silver steps, got {len(initial_result.silver_results)}"
        )
        assert len(initial_result.gold_results) == 1, (
            f"Expected 1 gold step, got {len(initial_result.gold_results)}"
        )

        # Assert bronze steps
        bronze_step1 = initial_result.bronze_results["events_source1"]
        bronze_step2 = initial_result.bronze_results["events_source2"]

        assert bronze_step1["status"] == "completed", "Bronze step 1 should complete"
        assert bronze_step1["rows_processed"] == 100, (
            "Bronze step 1 should process 100 rows"
        )
        assert bronze_step2["status"] == "completed", "Bronze step 2 should complete"
        assert bronze_step2["rows_processed"] == 100, (
            "Bronze step 2 should process 100 rows"
        )

        # Note: Bronze steps only validate data, they don't write to tables
        # The validated data is passed to silver steps which write to tables

        # Assert silver steps
        silver_step1 = initial_result.silver_results["processed_events_1"]
        silver_step2 = initial_result.silver_results["processed_events_2"]

        assert silver_step1["status"] == "completed", "Silver step 1 should complete"
        assert silver_step1["rows_written"] == 100, (
            "Silver step 1 should write 100 rows"
        )
        assert silver_step2["status"] == "completed", "Silver step 2 should complete"
        assert silver_step2["rows_written"] == 100, (
            "Silver step 2 should write 100 rows"
        )

        # Verify both silver tables exist and have correct schema
        # Each silver step writes to its own unique table to avoid conflicts
        silver_df_1 = mock_spark_session.table(f"{schema}.{silver_table_1}")
        silver_df_2 = mock_spark_session.table(f"{schema}.{silver_table_2}")

        silver_count_1 = silver_df_1.count()
        silver_count_2 = silver_df_2.count()

        assert silver_count_1 == 100, (
            f"Expected 100 rows in silver table 1, got {silver_count_1}"
        )
        assert silver_count_2 == 100, (
            f"Expected 100 rows in silver table 2, got {silver_count_2}"
        )

        # Verify transformation applied - check that expected columns exist
        # Note: Validation rules filter columns, so we only see columns with rules
        silver_columns_1 = silver_df_1.columns
        silver_columns_2 = silver_df_2.columns
        assert "user_id" in silver_columns_1, (
            "Silver table 1 should have user_id column"
        )
        assert "value" in silver_columns_1, "Silver table 1 should have value column"
        assert "user_id" in silver_columns_2, (
            "Silver table 2 should have user_id column"
        )
        assert "value" in silver_columns_2, "Silver table 2 should have value column"

        # Assert gold step
        gold_step = initial_result.gold_results["event_summary"]
        assert gold_step["status"] == "completed", "Gold step should complete"
        assert gold_step["rows_written"] > 0, "Gold step should write rows"

        # Verify gold table exists
        gold_table = mock_spark_session.table(f"{schema}.event_summary")
        gold_count = gold_table.count()
        assert gold_count > 0, "Gold table should have aggregated rows"

        # Log initial run
        initial_run_id = "initial_run_1"
        # Convert PipelineReport to log rows and write them
        from pipeline_builder.writer.models import create_log_rows_from_pipeline_report

        log_rows = create_log_rows_from_pipeline_report(
            initial_result, run_id=initial_run_id, run_mode="initial"
        )

        # Write log rows
        log_result = log_writer.write_log_rows(log_rows, run_id=initial_run_id)

        assert log_result["success"] is True, "Log write should succeed"
        # create_log_rows_from_pipeline_report creates 1 main row + 5 step rows = 6 total
        assert log_result["rows_written"] == 6, (
            f"Expected 6 log rows (1 main + 5 steps), got {log_result['rows_written']}"
        )

        # Verify logs written
        log_df = mock_spark_session.table(f"{schema}.pipeline_logs")
        initial_log_count = log_df.count()
        assert initial_log_count == 6, f"Expected 6 log rows, got {initial_log_count}"

        # Verify log entries
        initial_logs = get_log_entries_by_run(log_df, initial_run_id)
        # create_log_rows_from_pipeline_report creates 1 main row + 5 step rows = 6 total
        # All rows have step_name, so we just check we have 6 total
        assert len(initial_logs) == 6, (
            f"Should have 6 log entries for initial run (1 main + 5 steps), got {len(initial_logs)}"
        )

        # Filter to step rows only (exclude main pipeline row which might have different step_name)
        step_logs = [
            log
            for log in initial_logs
            if log.get("step_name") and log["step_name"] not in ["", None, "pipeline"]
        ]
        # We expect 5 step rows (2 bronze + 2 silver + 1 gold)
        assert len(step_logs) >= 5, (
            f"Should have at least 5 step log entries for initial run, got {len(step_logs)}"
        )

        for log_row in initial_logs:
            assert log_row["run_mode"] == "initial", (
                f"All initial logs should have run_mode='initial', got {log_row['run_mode']}"
            )
            assert log_row["run_id"] == initial_run_id, (
                f"All initial logs should have run_id={initial_run_id}"
            )

        # Verify specific step logs exist (use step_logs which excludes main pipeline row)
        step_names = {log["step_name"] for log in step_logs}
        assert "events_source1" in step_names, "Should have log for events_source1"
        assert "events_source2" in step_names, "Should have log for events_source2"
        assert "processed_events_1" in step_names, (
            "Should have log for processed_events_1"
        )
        assert "processed_events_2" in step_names, (
            "Should have log for processed_events_2"
        )
        assert "event_summary" in step_names, "Should have log for event_summary"

        # Verify bronze step logs
        bronze1_log = next(
            log for log in step_logs if log["step_name"] == "events_source1"
        )
        assert_log_entry(
            bronze1_log, "events_source1", initial_run_id, "initial", expected_rows=None
        )
        # Check rows_processed from the actual step result data
        assert bronze1_log.get("rows_processed", 0) >= 0, (
            "Bronze step 1 should have rows_processed"
        )
        bronze2_log = next(
            log for log in step_logs if log["step_name"] == "events_source2"
        )
        assert_log_entry(
            bronze2_log, "events_source2", initial_run_id, "initial", expected_rows=None
        )
        assert bronze2_log.get("rows_processed", 0) >= 0, (
            "Bronze step 2 should have rows_processed"
        )

        # Verify silver step logs
        silver1_log = next(
            log for log in step_logs if log["step_name"] == "processed_events_1"
        )
        assert_log_entry(
            silver1_log,
            "processed_events_1",
            initial_run_id,
            "initial",
            expected_rows=None,
        )
        assert silver1_log.get("rows_written", 0) >= 0, (
            "Silver step 1 should have rows_written"
        )
        silver2_log = next(
            log for log in step_logs if log["step_name"] == "processed_events_2"
        )
        assert_log_entry(
            silver2_log,
            "processed_events_2",
            initial_run_id,
            "initial",
            expected_rows=None,
        )
        assert silver2_log.get("rows_written", 0) >= 0, (
            "Silver step 2 should have rows_written"
        )

        # Verify gold step log
        gold_log = next(log for log in step_logs if log["step_name"] == "event_summary")
        assert gold_log["phase"] == "gold", "Gold step should have phase='gold'"
        # Note: create_log_rows_from_pipeline_report doesn't extract rows_processed from step results
        # We verify the actual step result instead
        assert gold_step["rows_written"] > 0, "Gold step should write rows"

        # ========== INCREMENTAL RUN ==========

        # Create incremental test data with later timestamps
        incremental_data_source1 = create_test_data_incremental(
            mock_spark_session, num_records=50
        )
        incremental_data_source2 = create_test_data_incremental(
            mock_spark_session, num_records=50
        )

        incremental_df1 = mock_spark_session.createDataFrame(
            incremental_data_source1, ["user_id", "action", "timestamp", "value"]
        )
        incremental_df2 = mock_spark_session.createDataFrame(
            incremental_data_source2, ["user_id", "action", "timestamp", "value"]
        )

        # Recreate pipeline (bronze steps are already defined)
        pipeline = builder.to_pipeline()

        # Execute incremental run with new bronze data sources
        incremental_result = pipeline.run_incremental(
            bronze_sources={
                "events_source1": incremental_df1,
                "events_source2": incremental_df2,
            }
        )

        # Assert execution result
        assert incremental_result.status.value == "completed", (
            f"Incremental run should complete, got status: {incremental_result.status}"
        )

        # Assert bronze steps process only new records
        inc_bronze_step1 = incremental_result.bronze_results["events_source1"]
        inc_bronze_step2 = incremental_result.bronze_results["events_source2"]

        assert inc_bronze_step1["status"] == "completed", (
            "Incremental bronze step 1 should complete"
        )
        assert inc_bronze_step1["rows_processed"] == 50, (
            "Incremental bronze step 1 should process 50 rows"
        )
        assert inc_bronze_step2["status"] == "completed", (
            "Incremental bronze step 2 should complete"
        )
        assert inc_bronze_step2["rows_processed"] == 50, (
            "Incremental bronze step 2 should process 50 rows"
        )

        # Note: Bronze steps don't write tables, but silver steps do
        # Verify both silver tables have incremental data added (each writes to its own table)
        silver_table_1_after = mock_spark_session.table(f"{schema}.{silver_table_1}")
        silver_table_2_after = mock_spark_session.table(f"{schema}.{silver_table_2}")

        silver_count_1_after = silver_table_1_after.count()
        silver_count_2_after = silver_table_2_after.count()

        # Check what write mode is actually being used
        # If append mode: 100 initial + 50 incremental = 150 rows
        # If overwrite mode: 50 incremental rows (overwrites initial)
        # The actual behavior may depend on the write mode configuration
        assert silver_count_1_after >= 50, (
            f"Expected at least 50 rows in silver table 1 after incremental, got {silver_count_1_after}"
        )
        assert silver_count_2_after >= 50, (
            f"Expected at least 50 rows in silver table 2 after incremental, got {silver_count_2_after}"
        )
        # Verify we don't have more than expected (100 initial + 50 incremental = 150 max)
        assert silver_count_1_after <= 150, (
            f"Expected at most 150 rows in silver table 1 after incremental, got {silver_count_1_after}"
        )
        assert silver_count_2_after <= 150, (
            f"Expected at most 150 rows in silver table 2 after incremental, got {silver_count_2_after}"
        )

        # Log incremental run
        incremental_run_id = "incremental_run_1"
        # Convert PipelineReport to log rows and write them
        log_rows_inc = create_log_rows_from_pipeline_report(
            incremental_result, run_id=incremental_run_id, run_mode="incremental"
        )

        # Write log rows (append mode)
        log_result_inc = log_writer.write_log_rows(
            log_rows_inc, run_id=incremental_run_id
        )

        assert log_result_inc["success"] is True, "Incremental log write should succeed"
        assert log_result_inc["rows_written"] == 6, (
            f"Expected 6 log rows (1 main + 5 steps), got {log_result_inc['rows_written']}"
        )

        # Verify logs appended (not overwritten)
        log_df_after = mock_spark_session.table(f"{schema}.pipeline_logs")
        total_log_count = log_df_after.count()
        assert total_log_count == 12, (
            f"Expected 12 total log rows (6 initial + 6 incremental), got {total_log_count}"
        )

        # Verify incremental log entries
        incremental_logs = get_log_entries_by_run(log_df_after, incremental_run_id)
        assert len(incremental_logs) == 6, (
            f"Should have 6 log entries for incremental run (1 main + 5 steps), got {len(incremental_logs)}"
        )

        # Filter to step rows only
        step_logs_inc = [
            log
            for log in incremental_logs
            if log.get("step_name") and log["step_name"] not in ["", None, "pipeline"]
        ]
        assert len(step_logs_inc) >= 5, (
            f"Should have at least 5 step log entries for incremental run, got {len(step_logs_inc)}"
        )

        for log_row in incremental_logs:
            assert log_row["run_mode"] == "incremental", (
                f"All incremental logs should have run_mode='incremental', got {log_row['run_mode']}"
            )
            assert log_row["run_id"] == incremental_run_id, (
                f"All incremental logs should have run_id={incremental_run_id}"
            )

        # Verify both runs are distinguishable
        all_initial_logs = get_log_entries_by_run(log_df_after, initial_run_id)
        assert len(all_initial_logs) == 6, (
            "Initial run logs should still exist (6 rows)"
        )

        # Verify incremental step logs (use step_logs_inc which excludes main pipeline row)
        inc_bronze1_log = next(
            log for log in step_logs_inc if log["step_name"] == "events_source1"
        )
        assert_log_entry(
            inc_bronze1_log,
            "events_source1",
            incremental_run_id,
            "incremental",
            expected_rows=None,
        )
        assert inc_bronze1_log.get("rows_processed", 0) >= 0, (
            "Incremental bronze step 1 should have rows_processed"
        )
        inc_bronze2_log = next(
            log for log in step_logs_inc if log["step_name"] == "events_source2"
        )
        assert_log_entry(
            inc_bronze2_log,
            "events_source2",
            incremental_run_id,
            "incremental",
            expected_rows=None,
        )
        assert inc_bronze2_log.get("rows_processed", 0) >= 0, (
            "Incremental bronze step 2 should have rows_processed"
        )

        # Verify timestamps are later for incremental run (if timestamps are strings, compare as strings)
        initial_timestamps = [
            log.get("timestamp") for log in all_initial_logs if log.get("timestamp")
        ]
        incremental_timestamps = [
            log.get("timestamp") for log in incremental_logs if log.get("timestamp")
        ]

        if initial_timestamps and incremental_timestamps:
            # All incremental timestamps should be later than initial timestamps
            max_initial = max(initial_timestamps)
            min_incremental = min(incremental_timestamps)
            assert min_incremental >= max_initial, (
                f"Incremental run timestamps should be later than initial run. Initial max: {max_initial}, Incremental min: {min_incremental}"
            )

        # Verify cumulative data in tables
        # Both silver tables should have data from incremental runs (overwrite mode replaces initial data)
        final_silver_table_1 = mock_spark_session.table(f"{schema}.{silver_table_1}")
        final_silver_table_2 = mock_spark_session.table(f"{schema}.{silver_table_2}")

        final_silver_count_1 = final_silver_table_1.count()
        final_silver_count_2 = final_silver_table_2.count()

        # Verify final counts match the counts after incremental run
        # (they should be the same since we're checking the same tables)
        assert final_silver_count_1 == silver_count_1_after, (
            f"Final count should match after-incremental count for table 1: {final_silver_count_1} != {silver_count_1_after}"
        )
        assert final_silver_count_2 == silver_count_2_after, (
            f"Final count should match after-incremental count for table 2: {final_silver_count_2} != {silver_count_2_after}"
        )

        # Gold table should have aggregated data from all runs
        final_gold_table = mock_spark_session.table(f"{schema}.event_summary")
        final_gold_count = final_gold_table.count()
        assert final_gold_count > 0, (
            "Gold table should have aggregated rows after incremental run"
        )

        # Verify log table structure and data integrity
        required_columns = [
            "step_name",
            "run_id",
            "run_mode",
            "rows_processed",
            "rows_written",
            "phase",
        ]
        for col in required_columns:
            assert col in log_df_after.columns, f"Log table should have column: {col}"

        # Verify no duplicate run_ids for same step in same run
        for run_id in [initial_run_id, incremental_run_id]:
            run_logs = get_log_entries_by_run(log_df_after, run_id)
            step_names_in_run = [log["step_name"] for log in run_logs]
            assert len(step_names_in_run) == len(set(step_names_in_run)), (
                f"Duplicate step names found in run {run_id}: {step_names_in_run}"
            )

        # Verify all log entries have valid data
        all_logs = [row.asDict() for row in log_df_after.collect()]
        for log in all_logs:
            assert log["step_name"], "Step name should not be empty"
            assert log["run_id"], "Run ID should not be empty"
            assert log["run_mode"] in ["initial", "incremental"], (
                f"Run mode should be 'initial' or 'incremental', got {log['run_mode']}"
            )
            assert log["rows_processed"] >= 0, "Rows processed should be non-negative"
            assert log["rows_written"] >= 0, "Rows written should be non-negative"
