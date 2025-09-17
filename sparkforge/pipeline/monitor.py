"""
Simplified pipeline monitoring for SparkForge.

This module provides basic monitoring and reporting for pipeline execution.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from ..logging import PipelineLogger
from .models import PipelineMode, PipelineReport, PipelineStatus


class SimplePipelineMonitor:
    """
    Simplified pipeline monitoring.
    
    This monitor provides basic execution tracking and reporting
    without complex metrics collection.
    """

    def __init__(self, logger: PipelineLogger | None = None):
        """Initialize the simplified monitor."""
        self.logger = logger or PipelineLogger()
        self._current_report: PipelineReport | None = None

    def start_execution(
        self,
        pipeline_id: str,
        mode: PipelineMode,
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> PipelineReport:
        """Start monitoring a pipeline execution."""
        start_time = datetime.now()
        
        self._current_report = PipelineReport(
            pipeline_id=pipeline_id,
            status=PipelineStatus.RUNNING,
            mode=mode,
            start_time=start_time,
            end_time=None,
            duration=0.0,
            total_steps=len(bronze_steps) + len(silver_steps) + len(gold_steps),
            successful_steps=0,
            failed_steps=0,
            errors=[],
            warnings=[],
            metrics={
                "total_duration": 0.0,
                "steps_executed": 0,
                "success_rate": 0.0,
            }
        )
        
        self.logger.info(f"Started monitoring pipeline: {pipeline_id}")
        return self._current_report

    def update_step_execution(
        self,
        step_name: str,
        step_type: str,
        success: bool,
        duration: float,
        error_message: str | None = None,
        rows_processed: int = 0,
        rows_written: int = 0,
    ) -> None:
        """Update step execution metrics."""
        if not self._current_report:
            return

        if success:
            self._current_report.successful_steps += 1
        else:
            self._current_report.failed_steps += 1
            if error_message:
                self._current_report.errors.append(f"{step_name}: {error_message}")

        self.logger.debug(f"Updated step {step_name}: success={success}, duration={duration:.2f}s")

    def finish_execution(self, success: bool) -> PipelineReport:
        """Finish monitoring and return final report."""
        if not self._current_report:
            raise RuntimeError("No active execution to finish")

        end_time = datetime.now()
        total_duration = (end_time - self._current_report.start_time).total_seconds()

        # Update final metrics
        self._current_report.end_time = end_time
        self._current_report.duration = total_duration
        self._current_report.status = PipelineStatus.COMPLETED if success else PipelineStatus.FAILED
        self._current_report.metrics["total_duration"] = total_duration
        self._current_report.metrics["steps_executed"] = self._current_report.total_steps
        self._current_report.metrics["success_rate"] = (
            self._current_report.successful_steps / self._current_report.total_steps
            if self._current_report.total_steps > 0 else 0.0
        )

        self.logger.info(f"Finished monitoring pipeline: {self._current_report.pipeline_id}")
        return self._current_report


# Backward compatibility alias
PipelineMonitor = SimplePipelineMonitor
