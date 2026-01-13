"""Execution reporter for creating execution reports.

This module provides a service for creating reports from execution results.
The ExecutionReporter separates reporting logic from execution, making it
easy to generate summaries and reports from pipeline execution results.
"""

from __future__ import annotations

from typing import Optional

from pipeline_builder_base.logging import PipelineLogger

from ..execution import ExecutionResult, StepExecutionResult


class ExecutionReporter:
    """Service for creating execution reports.

    Creates reports from execution results, separating reporting from execution.
    Provides methods to generate summary dictionaries from ExecutionResult and
    StepExecutionResult objects.

    Attributes:
        logger: PipelineLogger instance for logging.

    Example:
        >>> from pipeline_builder.reporting.execution_reporter import ExecutionReporter
        >>>
        >>> reporter = ExecutionReporter()
        >>> summary = reporter.create_execution_summary(execution_result)
        >>> step_summary = reporter.create_step_summary(step_result)
        >>> print(f"Pipeline status: {summary['status']}")
        >>> print(f"Total rows processed: {summary['total_rows_processed']}")
    """

    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the execution reporter.

        Args:
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.logger = logger or PipelineLogger()

    def create_execution_summary(
        self,
        result: ExecutionResult,
    ) -> dict:
        """Create a summary of execution results.

        Generates a dictionary summary of pipeline execution results including
        status, timing, step counts, and row metrics.

        Args:
            result: ExecutionResult from pipeline execution.

        Returns:
            Dictionary containing:
            - execution_id: Unique execution identifier
            - mode: Execution mode used
            - status: Overall pipeline status
            - duration: Total execution duration in seconds
            - steps_count: Total number of steps
            - execution_groups_count: Number of dependency groups
            - completed_steps: Number of successfully completed steps
            - failed_steps: Number of failed steps
            - total_rows_processed: Sum of rows processed across all steps
            - total_rows_written: Sum of rows written across all steps
            - error: Error message if pipeline failed (optional)
        """
        summary = {
            "execution_id": result.execution_id,
            "mode": result.mode.value
            if hasattr(result.mode, "value")
            else str(result.mode),
            "status": result.status,
            "duration": result.duration,
            "steps_count": len(result.steps) if result.steps else 0,
            "execution_groups_count": result.execution_groups_count,
        }

        if result.steps:
            completed_steps = [s for s in result.steps if s.status.value == "completed"]
            failed_steps = [s for s in result.steps if s.status.value == "failed"]

            summary["completed_steps"] = len(completed_steps)
            summary["failed_steps"] = len(failed_steps)
            summary["total_rows_processed"] = sum(
                s.rows_processed or 0 for s in completed_steps
            )
            summary["total_rows_written"] = sum(
                s.rows_written or 0 for s in completed_steps
            )

        if result.error:
            summary["error"] = result.error

        return summary

    def create_step_summary(
        self,
        result: StepExecutionResult,
    ) -> dict:
        """Create a summary of a step execution result.

        Generates a dictionary summary of individual step execution results
        including status, timing, row counts, and validation metrics.

        Args:
            result: StepExecutionResult from step execution.

        Returns:
            Dictionary containing:
            - step_name: Name of the step
            - step_type: Type of step (BRONZE, SILVER, GOLD)
            - status: Step execution status
            - duration: Step execution duration in seconds
            - rows_processed: Number of rows processed
            - rows_written: Number of rows written (None for Bronze steps)
            - validation_rate: Percentage of rows that passed validation
            - output_table: Fully qualified output table name (None for Bronze steps)
            - error: Error message if step failed (optional)
        """
        return {
            "step_name": result.step_name,
            "step_type": result.step_type.value
            if hasattr(result.step_type, "value")
            else str(result.step_type),
            "status": result.status.value
            if hasattr(result.status, "value")
            else str(result.status),
            "duration": result.duration,
            "rows_processed": result.rows_processed,
            "rows_written": result.rows_written,
            "validation_rate": result.validation_rate,
            "output_table": result.output_table,
            "error": result.error,
        }
