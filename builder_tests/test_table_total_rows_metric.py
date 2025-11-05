"""
Test the new table_total_rows metric functionality.

This module tests the new table_total_rows metric across various scenarios
to ensure it's correctly tracked and reported in the LogWriter.
"""

from mock_spark import functions as F


class TestTableTotalRowsMetric:
    """Test the new table_total_rows metric functionality."""

    def test_table_total_rows_in_initial_load(
        self,
        spark_session,
        pipeline_builder,
        log_writer,
        test_schema,
        simple_events_data,
    ):
        """Test table_total_rows metric after initial load."""
        builder = pipeline_builder

        # Build pipeline with silver step that writes to table
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 2  # bronze + silver

        # Verify table was created with correct row count
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        actual_row_count = clean_events_df.count()
        assert actual_row_count == 5

        # Create log entry with table_total_rows metric
        from pipeline_builder.writer.models import LogRow

        log_row: LogRow = {
            "run_id": "test_initial_load",
            "run_mode": "initial",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": actual_row_count,  # This is the key metric
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log entry
        result = log_writer.write_log_rows([log_row], run_id="test_initial_load")

        # Verify log entry was written successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify table_total_rows metric is correctly stored
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        log_row_data = log_df.filter(log_df.run_id == "test_initial_load").collect()[0]

        assert log_row_data["table_total_rows"] == 5
        assert log_row_data["rows_written"] == 5
        assert log_row_data["write_mode"] == "overwrite"

    def test_table_total_rows_in_incremental_load(
        self,
        spark_session,
        pipeline_builder,
        log_writer,
        test_schema,
        simple_events_data,
    ):
        """Test table_total_rows metric after incremental load."""
        builder = pipeline_builder

        # Build pipeline with silver step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()

        # Run initial load
        initial_report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )
        assert initial_report.success is True

        # Create incremental data
        incremental_data = spark_session.createDataFrame(
            [(6, "event6", "2024-01-02 10:00:00", 600)],
            ["id", "name", "timestamp", "value"],
        )

        # Run incremental load
        incremental_report = pipeline.run_incremental(
            bronze_sources={"events": incremental_data}
        )
        assert incremental_report.success is True

        # Verify table has accumulated rows
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        total_row_count = clean_events_df.count()
        assert total_row_count == 6  # 5 initial + 1 incremental

        # Create log entries for both runs
        from pipeline_builder.writer.models import LogRow

        log_rows = []

        # Initial load log entry
        initial_log_row: LogRow = {
            "run_id": "test_initial_load",
            "run_mode": "initial",
            "run_started_at": initial_report.start_time,
            "run_ended_at": initial_report.end_time,
            "execution_id": initial_report.run_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": initial_report.start_time,
            "end_time": initial_report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": 5,  # Initial load total
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }
        log_rows.append(initial_log_row)

        # Incremental load log entry
        incremental_log_row: LogRow = {
            "run_id": "test_incremental_load",
            "run_mode": "incremental",
            "run_started_at": incremental_report.start_time,
            "run_ended_at": incremental_report.end_time,
            "execution_id": incremental_report.run_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": incremental_report.start_time,
            "end_time": incremental_report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "append",
            "input_rows": 1,
            "output_rows": 1,
            "rows_written": 1,
            "rows_processed": 1,
            "table_total_rows": 6,  # Accumulated total after incremental load
            "valid_rows": 1,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }
        log_rows.append(incremental_log_row)

        # Write log entries
        result = log_writer.write_log_rows(log_rows)

        # Verify log entries were written successfully
        assert result["success"] is True
        assert result["rows_written"] == 2

        # Verify table_total_rows shows accumulation
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        initial_log = log_df.filter(log_df.run_id == "test_initial_load").collect()[0]
        incremental_log = log_df.filter(
            log_df.run_id == "test_incremental_load"
        ).collect()[0]

        assert initial_log["table_total_rows"] == 5
        assert incremental_log["table_total_rows"] == 6
        assert initial_log["write_mode"] == "overwrite"
        assert incremental_log["write_mode"] == "append"

    def test_table_total_rows_in_full_refresh(
        self,
        spark_session,
        pipeline_builder,
        log_writer,
        test_schema,
        simple_events_data,
    ):
        """Test table_total_rows metric after full refresh."""
        builder = pipeline_builder

        # Build pipeline with silver step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()

        # Run initial load
        initial_report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )
        assert initial_report.success is True

        # Create new data for full refresh
        new_data = spark_session.createDataFrame(
            [
                (7, "event7", "2024-01-03 10:00:00", 700),
                (8, "event8", "2024-01-03 11:00:00", 800),
            ],
            ["id", "name", "timestamp", "value"],
        )

        # Run full refresh (overwrite mode)
        full_refresh_report = pipeline.run_initial_load(
            bronze_sources={"events": new_data}
        )
        assert full_refresh_report.success is True

        # Verify table was refreshed with new data
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        total_row_count = clean_events_df.count()
        assert total_row_count == 2  # Only new data, old data was overwritten

        # Create log entry for full refresh
        from pipeline_builder.writer.models import LogRow

        log_row: LogRow = {
            "run_id": "test_full_refresh",
            "run_mode": "initial",  # Full refresh uses initial mode
            "run_started_at": full_refresh_report.start_time,
            "run_ended_at": full_refresh_report.end_time,
            "execution_id": full_refresh_report.run_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": full_refresh_report.start_time,
            "end_time": full_refresh_report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 2,
            "output_rows": 2,
            "rows_written": 2,
            "rows_processed": 2,
            "table_total_rows": total_row_count,  # New total after refresh
            "valid_rows": 2,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log entry
        result = log_writer.write_log_rows([log_row], run_id="test_full_refresh")

        # Verify log entry was written successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify table_total_rows reflects the refresh
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        refresh_log = log_df.filter(log_df.run_id == "test_full_refresh").collect()[0]

        assert refresh_log["table_total_rows"] == 2
        assert refresh_log["write_mode"] == "overwrite"
        assert refresh_log["rows_written"] == 2

    def test_table_total_rows_multiple_tables(
        self,
        spark_session,
        pipeline_builder,
        log_writer,
        test_schema,
        simple_events_data,
    ):
        """Test table_total_rows metric with multiple tables."""
        builder = pipeline_builder

        # Build pipeline with multiple silver steps
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="processed_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # bronze + 2 silver

        # Verify both tables were created
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        processed_events_df = spark_session.table(f"{test_schema}.processed_events")

        clean_count = clean_events_df.count()
        processed_count = processed_events_df.count()

        assert clean_count == 5
        assert processed_count == 5

        # Create log entries for both tables
        from pipeline_builder.writer.models import LogRow

        log_rows = []

        # Clean events log entry
        clean_log_row: LogRow = {
            "run_id": "test_multiple_tables",
            "run_mode": "initial",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": clean_count,
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }
        log_rows.append(clean_log_row)

        # Processed events log entry
        processed_log_row: LogRow = {
            "run_id": "test_multiple_tables",
            "run_mode": "initial",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "processed_events",
            "step_type": "silver",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.processed_events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": processed_count,
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }
        log_rows.append(processed_log_row)

        # Write log entries
        result = log_writer.write_log_rows(log_rows, run_id="test_multiple_tables")

        # Verify log entries were written successfully
        assert result["success"] is True
        assert result["rows_written"] == 2

        # Verify table_total_rows for both tables
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        log_data = log_df.filter(log_df.run_id == "test_multiple_tables").collect()

        assert len(log_data) == 2

        # Find log entries for each table
        clean_log = next(row for row in log_data if row["step_name"] == "clean_events")
        processed_log = next(
            row for row in log_data if row["step_name"] == "processed_events"
        )

        assert clean_log["table_total_rows"] == 5
        assert processed_log["table_total_rows"] == 5
        assert clean_log["table_fqn"] == f"{test_schema}.clean_events"
        assert processed_log["table_fqn"] == f"{test_schema}.processed_events"

    def test_table_total_rows_error_handling(
        self, spark_session, log_writer, test_schema
    ):
        """Test table_total_rows metric when table operations fail."""
        from pipeline_builder.writer.models import LogRow

        # Create log row for failed table operation
        log_row: LogRow = {
            "run_id": "test_error_handling",
            "run_mode": "initial",
            "run_started_at": "2024-01-01 10:00:00",
            "run_ended_at": "2024-01-01 10:01:00",
            "execution_id": "error_exec_001",
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": "2024-01-01 10:00:00",
            "end_time": "2024-01-01 10:01:00",
            "duration_secs": 60.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 0,
            "output_rows": 0,
            "rows_written": 0,
            "rows_processed": 0,
            "table_total_rows": None,  # None when table operation fails
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 0.0,
            "success": False,
            "error_message": "Table write operation failed",
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log entry
        result = log_writer.write_log_rows([log_row], run_id="test_error_handling")

        # Verify log entry was written successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify table_total_rows is None for failed operations
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        error_log = log_df.filter(F.col("run_id") == "test_error_handling").collect()[0]

        assert error_log["table_total_rows"] is None
        assert error_log["success"] is False
        assert error_log["error_message"] == "Table write operation failed"

    def test_table_total_rows_validation_only_mode(
        self,
        spark_session,
        pipeline_builder,
        log_writer,
        test_schema,
        simple_events_data,
    ):
        """Test table_total_rows metric in validation-only mode."""
        builder = pipeline_builder

        # Build pipeline with silver step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline in validation-only mode
        pipeline = builder.to_pipeline()
        # Note: For validation-only mode, we use run_initial_load but with validation-only mode
        # This will be supported in the future. For now, let's use run_initial_load
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )

        # Verify pipeline execution
        assert report.success is True

        # Create log entry for validation-only run
        from pipeline_builder.writer.models import LogRow

        log_row: LogRow = {
            "run_id": "test_validation_only",
            "run_mode": "validation_only",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": None,  # No write mode in validation-only
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 0,  # No rows written in validation-only
            "rows_processed": 5,
            "table_total_rows": None,  # No table total in validation-only
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log entry
        result = log_writer.write_log_rows([log_row], run_id="test_validation_only")

        # Verify log entry was written successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify table_total_rows is None for validation-only mode
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        validation_log = log_df.filter(
            log_df.run_id == "test_validation_only"
        ).collect()[0]

        assert validation_log["table_total_rows"] is None
        assert validation_log["write_mode"] is None
        assert validation_log["rows_written"] == 0
        assert validation_log["run_mode"] == "validation_only"

    def test_table_total_rows_large_dataset(
        self, spark_session, pipeline_builder, log_writer, test_schema
    ):
        """Test table_total_rows metric with larger dataset."""
        # Create larger dataset
        large_data = []
        for i in range(100):
            large_data.append(
                (i, f"event_{i}", f"2024-01-01 {10 + i % 24:02d}:00:00", i * 10)
            )

        large_df = spark_session.createDataFrame(
            large_data, ["id", "name", "timestamp", "value"]
        )

        builder = pipeline_builder

        # Build pipeline with silver step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": large_df})

        # Verify pipeline execution
        assert report.success is True

        # Verify table was created with correct row count
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        actual_row_count = clean_events_df.count()
        assert actual_row_count == 100

        # Create log entry with table_total_rows metric
        from pipeline_builder.writer.models import LogRow

        log_row: LogRow = {
            "run_id": "test_large_dataset",
            "run_mode": "initial",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "silver",
            "step_name": "clean_events",
            "step_type": "silver",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 5.0,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 100,
            "output_rows": 100,
            "rows_written": 100,
            "rows_processed": 100,
            "table_total_rows": actual_row_count,  # Large dataset total
            "valid_rows": 100,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 256.0,
            "cpu_usage_percent": 80.0,
            "metadata": {"dataset_size": "large"},
        }

        # Write log entry
        result = log_writer.write_log_rows([log_row], run_id="test_large_dataset")

        # Verify log entry was written successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify table_total_rows metric is correctly stored for large dataset
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        log_row_data = log_df.filter(log_df.run_id == "test_large_dataset").collect()[0]

        assert log_row_data["table_total_rows"] == 100
        assert log_row_data["rows_written"] == 100
        assert log_row_data["memory_usage_mb"] == 256.0
        assert log_row_data["cpu_usage_percent"] == 80.0
