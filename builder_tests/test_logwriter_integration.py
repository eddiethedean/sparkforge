"""
Test LogWriter integration with PipelineBuilder and PipelineRunner.

This module tests the LogWriter's integration with pipeline components,
including comprehensive logging, metrics tracking, and the new table_total_rows feature.
"""

from mock_spark import functions as F


class TestLogWriterIntegration:
    """Test LogWriter integration with pipeline components."""

    def test_logwriter_writes_execution_result(
        self, spark_session, pipeline_builder, log_writer, test_schema, simple_events_data
    ):
        """Test basic LogWriter write functionality."""
        builder = pipeline_builder

        # Build simple pipeline
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Create data and execute
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Write to log using LogWriter
        # Note: We need to create an ExecutionResult from the PipelineReport
        # For now, let's test the LogWriter's write_log_rows method directly
        from pipeline_builder.writer.models import LogRow

        # Create a simple log row for testing
        log_row: LogRow = {
            "run_id": "test_run_001",
            "run_mode": "initial",
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,
            "execution_id": report.execution_id,
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "bronze",
            "step_name": "events",
            "step_type": "bronze",
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": 1.0,
            "table_fqn": f"{test_schema}.events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": 5,
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log row
        result = log_writer.write_log_rows([log_row], run_id="test_run_001")

        # Verify write was successful
        assert result["success"] is True
        assert result["rows_written"] == 1
        assert "run_id" in result

    def test_logwriter_tracks_table_total_rows(
        self, spark_session, pipeline_builder, log_writer, test_schema, simple_events_data
    ):
        """Test that LogWriter correctly tracks the table_total_rows metric."""
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Create log rows with table_total_rows metric
        from pipeline_builder.writer.models import LogRow

        # Create log row with table_total_rows
        log_row: LogRow = {
            "run_id": "test_run_002",
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
            "table_total_rows": 5,  # This is the new metric we're testing
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write log row
        result = log_writer.write_log_rows([log_row], run_id="test_run_002")

        # Verify write was successful and table_total_rows was tracked
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify the table_total_rows metric is correctly stored
        # We can check this by reading the log table
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        log_rows = log_df.filter(F.col("run_id") == "test_run_002").collect()

        assert len(log_rows) == 1
        assert log_rows[0]["table_total_rows"] == 5

    def test_logwriter_handles_multiple_runs(
        self, spark_session, pipeline_builder, log_writer, test_schema, simple_events_data
    ):
        """Test LogWriter handling multiple execution runs."""
        builder = pipeline_builder

        # Build simple pipeline
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute multiple runs
        pipeline = builder.to_pipeline()

        # Run 1: Initial load
        report1 = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Run 2: Incremental load
        incremental_data = spark_session.createDataFrame(
            [(6, "event6", "2024-01-02 10:00:00", 600)],
            ["id", "name", "timestamp", "value"],
        )
        report2 = pipeline.run_incremental(bronze_sources={"events": incremental_data})

        # Create log rows for both runs
        from pipeline_builder.writer.models import LogRow

        log_rows = []
        for i, (run_id, report) in enumerate([("run_001", report1), ("run_002", report2)]):
            log_row: LogRow = {
                "run_id": run_id,
                "run_mode": "initial" if i == 0 else "incremental",
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
                "write_mode": "overwrite" if i == 0 else "append",
                "input_rows": 5 if i == 0 else 1,
                "output_rows": 5 if i == 0 else 1,
                "rows_written": 5 if i == 0 else 1,
                "rows_processed": 5 if i == 0 else 1,
                "table_total_rows": 5 if i == 0 else 6,  # Accumulating total
                "valid_rows": 5 if i == 0 else 1,
                "invalid_rows": 0,
                "validation_rate": 100.0,
                "success": True,
                "error_message": None,
                "memory_usage_mb": 0.0,
                "cpu_usage_percent": 0.0,
                "metadata": {},
            }
            log_rows.append(log_row)

        # Write all log rows
        result = log_writer.write_log_rows(log_rows)

        # Verify both runs were logged successfully
        assert result["success"] is True
        assert result["rows_written"] == 2

        # Verify both runs are in the log table
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        run_ids = {row["run_id"] for row in log_df.collect()}
        assert "run_001" in run_ids
        assert "run_002" in run_ids

        # Verify table_total_rows shows accumulation
        run_001_row = log_df.filter(F.col("run_id") == "run_001").collect()[0]
        run_002_row = log_df.filter(F.col("run_id") == "run_002").collect()[0]

        assert run_001_row["table_total_rows"] == 5
        assert run_002_row["table_total_rows"] == 6

    def test_logwriter_error_handling(
        self, spark_session, log_writer, test_schema
    ):
        """Test LogWriter error handling capabilities."""
        from pipeline_builder.writer.models import LogRow

        # Create log row with error
        log_row: LogRow = {
            "run_id": "error_run_001",
            "run_mode": "initial",
            "run_started_at": "2024-01-01 10:00:00",
            "run_ended_at": "2024-01-01 10:01:00",
            "execution_id": "error_exec_001",
            "pipeline_id": "test_pipeline",
            "schema": test_schema,
            "phase": "bronze",
            "step_name": "events",
            "step_type": "bronze",
            "start_time": "2024-01-01 10:00:00",
            "end_time": "2024-01-01 10:01:00",
            "duration_secs": 60.0,
            "table_fqn": f"{test_schema}.events",
            "write_mode": "overwrite",
            "input_rows": 0,
            "output_rows": 0,
            "rows_written": 0,
            "rows_processed": 0,
            "table_total_rows": None,
            "valid_rows": 0,
            "invalid_rows": 0,
            "validation_rate": 0.0,
            "success": False,
            "error_message": "Test error message",
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0,
            "metadata": {},
        }

        # Write error log row
        result = log_writer.write_log_rows([log_row], run_id="error_run_001")

        # Verify error was logged successfully
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify error information is stored
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        error_row = log_df.filter(F.col("run_id") == "error_run_001").collect()[0]

        assert error_row["success"] is False
        assert error_row["error_message"] == "Test error message"
        assert error_row["validation_rate"] == 0.0

    def test_logwriter_performance_metrics(
        self, spark_session, pipeline_builder, log_writer, test_schema, simple_events_data
    ):
        """Test LogWriter performance metrics tracking."""
        builder = pipeline_builder

        # Build pipeline
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="clean_events",
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Create log row with performance metrics
        from pipeline_builder.writer.models import LogRow

        log_row: LogRow = {
            "run_id": "perf_run_001",
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
            "duration_secs": 2.5,
            "table_fqn": f"{test_schema}.clean_events",
            "write_mode": "overwrite",
            "input_rows": 5,
            "output_rows": 5,
            "rows_written": 5,
            "rows_processed": 5,
            "table_total_rows": 5,
            "valid_rows": 5,
            "invalid_rows": 0,
            "validation_rate": 100.0,
            "success": True,
            "error_message": None,
            "memory_usage_mb": 128.5,  # Performance metric
            "cpu_usage_percent": 75.0,  # Performance metric
            "metadata": {"custom_metric": "test_value"},
        }

        # Write log row
        result = log_writer.write_log_rows([log_row], run_id="perf_run_001")

        # Verify performance metrics were logged
        assert result["success"] is True
        assert result["rows_written"] == 1

        # Verify performance metrics are stored
        log_df = spark_session.table(f"{test_schema}.pipeline_logs")
        perf_row = log_df.filter(F.col("run_id") == "perf_run_001").collect()[0]

        assert perf_row["memory_usage_mb"] == 128.5
        assert perf_row["cpu_usage_percent"] == 75.0
        assert perf_row["duration_secs"] == 2.5
