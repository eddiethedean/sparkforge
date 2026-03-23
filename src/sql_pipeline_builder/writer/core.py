"""
Public SQL LogWriter facade aligned with pipeline_builder naming.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    PipelineMetrics,
    StepResult,
)
from pipeline_builder_base.writer.models import WriterConfig
from pipeline_builder_base.writer.models import (
    LogRow,
    WriteMode,
    create_log_rows_from_execution_result,
)

from .sql_storage import SqlLogWriter


class LogWriter(SqlLogWriter):
    """
    SQL LogWriter with the same public naming as pipeline_builder.LogWriter.

    This class is a thin facade over SqlLogWriter so users can import:
        from sql_pipeline_builder import LogWriter
    """

    def __init__(
        self,
        session: Any,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        config: Optional[WriterConfig] = None,
        logger: Optional[PipelineLogger] = None,
    ) -> None:
        super().__init__(
            session=session,
            schema=schema,
            table_name=table_name,
            config=config,
            logger=logger,
        )

    def write_execution_result(
        self,
        execution_result: ExecutionResult,
        run_id: Optional[str] = None,
        run_mode: str = "initial",
        metadata: Optional[Dict[str, Any]] = None,
        mode: WriteMode = WriteMode.APPEND,
    ) -> Dict[str, Any]:
        """
        Write an ExecutionResult with a pipeline_builder-like API.
        """
        resolved_run_id = run_id or str(uuid.uuid4())
        resolved_run_mode = run_mode
        if not run_mode and execution_result.context.run_mode:
            resolved_run_mode = execution_result.context.run_mode

        log_rows = create_log_rows_from_execution_result(
            execution_result=execution_result,
            run_id=resolved_run_id,
            run_mode=resolved_run_mode,  # type: ignore[arg-type]
            metadata=metadata,
        )
        # SQL table uses run_id as primary key, so each row needs uniqueness.
        if log_rows:
            log_rows[0]["run_id"] = resolved_run_id
            for idx in range(1, len(log_rows)):
                log_rows[idx]["run_id"] = f"{resolved_run_id}:{idx}"
        self._write_log_rows(log_rows, mode)
        return {
            "success": True,
            "run_id": resolved_run_id,
            "rows_written": len(log_rows),
            "table_fqn": self.table_fqn,
        }

    def write_step_results(
        self,
        step_results: Dict[str, StepResult],
        run_id: Optional[str] = None,
        run_mode: str = "initial",
        metadata: Optional[Dict[str, Any]] = None,
        mode: WriteMode = WriteMode.APPEND,
    ) -> Dict[str, Any]:
        """
        Write step results by wrapping them into an ExecutionResult.
        """
        now = datetime.now(timezone.utc)
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=now,
            end_time=now,
            duration_secs=0.0,
            run_id=run_id or str(uuid.uuid4()),
            execution_id=str(uuid.uuid4()),
            pipeline_id="sql_pipeline",
            schema=self.schema,
            run_mode=run_mode,
        )
        execution_result = ExecutionResult(
            context=context,
            step_results=list(step_results.values()),
            metrics=PipelineMetrics.from_step_results(list(step_results.values())),
            success=all(s.success for s in step_results.values()),
        )
        return self.write_execution_result(
            execution_result=execution_result,
            run_id=context.run_id,
            run_mode=run_mode,
            metadata=metadata,
            mode=mode,
        )

    def write_log_rows(
        self,
        log_rows: list[LogRow],
        mode: WriteMode = WriteMode.APPEND,
    ) -> Dict[str, Any]:
        """
        Write already-prepared log rows.
        """
        self._write_log_rows(log_rows, mode)
        return {
            "success": True,
            "rows_written": len(log_rows),
            "table_fqn": self.table_fqn,
        }

    def create_table(
        self,
        execution_result: ExecutionResult,
        run_id: Optional[str] = None,
        run_mode: str = "initial",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create or replace the log table from an execution result.
        """
        return self.write_execution_result(
            execution_result=execution_result,
            run_id=run_id,
            run_mode=run_mode,
            metadata=metadata,
            mode=WriteMode.OVERWRITE,
        )

    def append(
        self,
        execution_result: ExecutionResult,
        run_id: Optional[str] = None,
        run_mode: str = "initial",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Append an execution result to the log table.
        """
        return self.write_execution_result(
            execution_result=execution_result,
            run_id=run_id,
            run_mode=run_mode,
            metadata=metadata,
            mode=WriteMode.APPEND,
        )
