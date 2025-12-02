"""
Base runner with common runner patterns.

This module provides a base BaseRunner class that can be used
by all pipeline runner implementations to reduce code duplication.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from ..logging import PipelineLogger
from ..models import ExecutionResult, PipelineConfig, StepResult


class BaseRunner:
    """
    Base runner with common runner patterns.

    This class provides shared runner functionality that can be used
    by all pipeline runner implementations.
    """

    def __init__(
        self,
        config: PipelineConfig,
        logger: Optional[PipelineLogger] = None,
    ):
        """
        Initialize the base runner.

        Args:
            config: Pipeline configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or PipelineLogger()

    def _handle_step_error(
        self, step: Any, error: Exception, step_type: str = "step"
    ) -> None:
        """
        Handle step execution error with logging.

        Args:
            step: Step object that failed
            error: Exception that occurred
            step_type: Type of step (bronze/silver/gold) for logging
        """
        step_name = getattr(step, "name", "unknown")
        self.logger.error(
            f"❌ {step_type.capitalize()} step '{step_name}' failed: {str(error)}"
        )
        self.logger.debug(f"Error details: {error}", exc_info=True)

    def _collect_step_results(self, results: List[StepResult]) -> ExecutionResult:
        """
        Collect step results into an execution result.

        Args:
            results: List of step results

        Returns:
            ExecutionResult with aggregated results
        """
        from datetime import datetime, timezone

        from ..models import ExecutionContext, ExecutionMode, PipelineMetrics

        # Aggregate metrics from step results
        metrics = PipelineMetrics.from_step_results(results)

        # Determine overall status
        all_succeeded = all(result.success for result in results)

        # Create execution context
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
        )

        return ExecutionResult(
            context=context,
            step_results=results,
            metrics=metrics,
            success=all_succeeded,
        )

    def _create_pipeline_report(
        self,
        status: str,
        start_time: datetime,
        end_time: datetime,
        results: Optional[ExecutionResult] = None,
        errors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a pipeline execution report.

        Args:
            status: Pipeline execution status
            start_time: Pipeline start time
            end_time: Pipeline end time
            results: Optional execution results
            errors: Optional list of error messages

        Returns:
            Dictionary representing pipeline report
        """
        duration = (end_time - start_time).total_seconds()

        report: Dict[str, Any] = {
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
        }

        if results:
            report["results"] = results
            report["step_count"] = (
                len(results.step_results) if results.step_results else 0
            )
            report["success_count"] = (
                sum(1 for r in results.step_results if r.success)
                if results.step_results
                else 0
            )

        if errors:
            report["errors"] = errors
            report["error_count"] = len(errors)

        return report

    def _aggregate_step_reports(self, reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate multiple step reports into a single report.

        Args:
            reports: List of step reports

        Returns:
            Aggregated report dictionary
        """
        if not reports:
            return {
                "status": "unknown",
                "step_count": 0,
                "success_count": 0,
            }

        all_succeeded = all(r.get("status") == "success" for r in reports)
        status = "success" if all_succeeded else "partial_failure"

        total_duration = sum(r.get("duration_seconds", 0.0) for r in reports)

        return {
            "status": status,
            "step_count": len(reports),
            "success_count": sum(1 for r in reports if r.get("status") == "success"),
            "total_duration_seconds": total_duration,
            "step_reports": reports,
        }
