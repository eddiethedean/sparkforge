"""
Tests for SQL log writer persistence behavior.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pipeline_builder_base.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    PipelineMetrics,
    PipelinePhase,
    StepResult,
)
from pipeline_builder_base.writer.models import WriteMode

from sql_pipeline_builder.writer import LogWriter, SqlLogWriter


def _make_log_row(run_id: str, *, metadata: dict | None = None) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "run_id": run_id,
        "run_mode": "initial",
        "run_started_at": now,
        "run_ended_at": now,
        "execution_id": f"exec-{run_id}",
        "pipeline_id": "pipeline-1",
        "schema": "main",
        "phase": "pipeline",
        "step_name": "pipeline_execution",
        "step_type": "pipeline",
        "start_time": now,
        "end_time": now,
        "duration_secs": 0.1,
        "table_fqn": None,
        "write_mode": None,
        "input_rows": None,
        "output_rows": None,
        "rows_written": None,
        "rows_processed": 0,
        "table_total_rows": None,
        "valid_rows": 0,
        "invalid_rows": 0,
        "validation_rate": 100.0,
        "success": True,
        "error_message": None,
        "memory_usage_mb": None,
        "cpu_usage_percent": None,
        "metadata": metadata or {},
    }


def _make_execution_result() -> ExecutionResult:
    now = datetime.now(timezone.utc)
    context = ExecutionContext(
        mode=ExecutionMode.INITIAL,
        start_time=now,
        end_time=now,
        duration_secs=0.0,
        run_id="run-exec",
        execution_id="exec-1",
        pipeline_id="pipeline-1",
        schema="main",
        run_mode="initial",
    )
    step = StepResult.create_success(
        step_name="test_step",
        phase=PipelinePhase.SILVER,
        start_time=now,
        end_time=now,
        rows_processed=10,
        rows_written=10,
        validation_rate=100.0,
        step_type="silver",
        table_fqn="main.test_table",
        write_mode="append",
        input_rows=10,
    )
    return ExecutionResult(
        context=context,
        step_results=[step],
        metrics=PipelineMetrics.from_step_results([step]),
        success=True,
    )


def test_sql_log_writer_creates_table_and_writes_rows(sqlite_session):
    writer = SqlLogWriter(sqlite_session, schema="main", table_name="pipeline_logs")
    assert not writer._table_exists()

    writer._write_log_rows([_make_log_row("run-1")], WriteMode.APPEND)

    assert writer._table_exists()
    rows = writer._read_log_table()
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-1"


def test_sql_log_writer_overwrite_mode_replaces_rows(sqlite_session):
    writer = SqlLogWriter(sqlite_session, schema="main", table_name="pipeline_logs_overwrite")
    writer._write_log_rows([_make_log_row("run-1")], WriteMode.APPEND)

    writer._write_log_rows([_make_log_row("run-2")], WriteMode.OVERWRITE)

    rows = writer._read_log_table()
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-2"


def test_sql_log_writer_reads_metadata_json(sqlite_session):
    writer = SqlLogWriter(sqlite_session, schema="main", table_name="pipeline_logs_metadata")
    row = _make_log_row("run-metadata", metadata={"source": "unit-test", "count": 2})
    writer._write_log_rows([row], WriteMode.APPEND)

    rows = writer._read_log_table(limit=1)
    assert len(rows) == 1
    assert rows[0]["metadata"]["source"] == "unit-test"
    assert rows[0]["metadata"]["count"] == 2


def test_sql_log_writer_rolls_back_on_insert_failure(sqlite_session):
    writer = SqlLogWriter(sqlite_session, schema="main", table_name="pipeline_logs_rollback")
    duplicate_pk_rows = [_make_log_row("dup-id"), _make_log_row("dup-id")]

    with pytest.raises(Exception):
        writer._write_log_rows(duplicate_pk_rows, WriteMode.APPEND)

    # Insert should be rolled back after primary-key conflict.
    rows = writer._read_log_table()
    assert rows == []


def test_logwriter_alias_matches_sql_log_writer(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_alias")
    assert isinstance(writer, SqlLogWriter)


def test_logwriter_write_execution_result_api(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_result")
    execution_result = _make_execution_result()

    outcome = writer.write_execution_result(execution_result)

    assert outcome["success"] is True
    assert outcome["rows_written"] == 2  # pipeline row + step row
    assert outcome["table_fqn"] == "main.pipeline_logs_result"
    assert len(writer._read_log_table()) == 2


def test_logwriter_create_table_and_append_api(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_create")
    execution_result = _make_execution_result()

    created = writer.create_table(execution_result)
    appended = writer.append(execution_result)

    assert created["success"] is True
    assert appended["success"] is True
    assert len(writer._read_log_table()) == 4


def test_logwriter_create_table_overwrites_existing_rows(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_overwrite_api")
    execution_result = _make_execution_result()

    writer.append(execution_result, run_id="append-run-1")
    writer.append(execution_result, run_id="append-run-2")
    assert len(writer._read_log_table()) == 4

    # create_table uses overwrite semantics via WriteMode.OVERWRITE
    writer.create_table(execution_result, run_id="overwrite-run")
    rows = writer._read_log_table()
    assert len(rows) == 2
    assert all(str(r["run_id"]).startswith("overwrite-run") for r in rows)


def test_logwriter_write_log_rows_public_api(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_public_rows")
    rows = [_make_log_row("public-1"), _make_log_row("public-2")]

    result = writer.write_log_rows(rows, mode=WriteMode.APPEND)

    assert result["success"] is True
    assert result["rows_written"] == 2
    stored = writer._read_log_table()
    assert len(stored) == 2
    assert {r["run_id"] for r in stored} == {"public-1", "public-2"}


def test_logwriter_write_step_results_api(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_step_results")
    now = datetime.now(timezone.utc)
    step_result = StepResult.create_success(
        step_name="silver_step",
        phase=PipelinePhase.SILVER,
        start_time=now,
        end_time=now,
        rows_processed=7,
        rows_written=7,
        validation_rate=100.0,
        step_type="silver",
        table_fqn="main.silver_step",
        write_mode="append",
        input_rows=7,
    )
    step_results = {"silver_step": step_result}

    outcome = writer.write_step_results(step_results, run_mode="incremental")

    assert outcome["success"] is True
    assert outcome["rows_written"] == 2  # pipeline + step
    stored = writer._read_log_table()
    assert len(stored) == 2
    run_modes = {row["run_mode"] for row in stored}
    assert run_modes == {"incremental"}


def test_logwriter_generates_unique_run_ids_when_not_provided(sqlite_session):
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_unique_run_ids")
    execution_result = _make_execution_result()

    first = writer.write_execution_result(execution_result)
    second = writer.write_execution_result(execution_result)

    assert first["success"] is True
    assert second["success"] is True
    assert first["run_id"] != second["run_id"]
    assert len(writer._read_log_table()) == 4


def test_logwriter_multi_run_mixed_modes_and_metadata(sqlite_session):
    """
    Multi-run scenario:
    - initial run
    - incremental run
    - ensure metadata round-trips and run_mode is persisted
    """
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_multi_run")
    execution_result = _make_execution_result()

    out1 = writer.append(execution_result, run_id="run-initial", run_mode="initial", metadata={"env": "test"})
    out2 = writer.append(
        execution_result, run_id="run-incremental", run_mode="incremental", metadata={"env": "test", "batch": 2}
    )

    assert out1["success"] is True
    assert out2["success"] is True
    rows = writer._read_log_table()
    assert len(rows) == 4  # 2 rows per run (pipeline + step)

    by_mode = {row["run_mode"] for row in rows}
    assert by_mode == {"initial", "incremental"}

    # Metadata should be parsed as dict and preserved
    initial_meta = [r["metadata"] for r in rows if str(r["run_id"]).startswith("run-initial")]
    # Metadata may be present only on the pipeline-level row depending on row generation.
    assert any(m.get("env") == "test" for m in initial_meta)


def test_logwriter_recovery_after_failed_append_does_not_corrupt_table(sqlite_session):
    """
    Error recovery:
    - first append succeeds
    - second append reuses same run_id and fails due to PK conflict
    - third append with new run_id succeeds
    - verify table contains only successful runs (no partial writes)
    """
    writer = LogWriter(sqlite_session, schema="main", table_name="pipeline_logs_recovery")
    execution_result = _make_execution_result()

    ok1 = writer.append(execution_result, run_id="run-1", run_mode="initial")
    assert ok1["success"] is True
    assert len(writer._read_log_table()) == 2

    # Force a PK conflict by reusing the same run_id: this will generate the same PKs
    with pytest.raises(Exception):
        writer.append(execution_result, run_id="run-1", run_mode="initial")

    # Table should still have only the first run's two rows
    rows_after_fail = writer._read_log_table()
    assert len(rows_after_fail) == 2
    assert all(str(r["run_id"]).startswith("run-1") for r in rows_after_fail)

    ok2 = writer.append(execution_result, run_id="run-2", run_mode="incremental")
    assert ok2["success"] is True

    rows_final = writer._read_log_table()
    assert len(rows_final) == 4
    run_prefixes = {str(r["run_id"]).split(":")[0] for r in rows_final}
    assert run_prefixes == {"run-1", "run-2"}
