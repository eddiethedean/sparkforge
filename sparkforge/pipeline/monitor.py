# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

"""
Pipeline monitoring and metrics system for SparkForge.

This module provides comprehensive monitoring, metrics collection, and reporting
for pipeline execution, enabling better observability and performance optimization.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from ..logger import PipelineLogger
from ..models import PipelineMetrics
from .models import PipelineMode, PipelineReport, PipelineStatus


class PipelineMonitor:
    """
    Comprehensive pipeline monitoring and metrics collection.

    This class provides real-time monitoring, metrics collection, and reporting
    for pipeline execution, enabling better observability and performance optimization.

    Features:
    - Real-time metrics collection
    - Performance monitoring
    - Error tracking and analysis
    - Execution reporting
    - Optimization recommendations
    """

    def __init__(self, logger: PipelineLogger | None = None):
        self.logger = logger or PipelineLogger()
        self._current_report: PipelineReport | None = None
        self._execution_start_time: datetime | None = None
        self._step_timings: dict[str, list[float]] = {}
        self._error_counts: dict[str, int] = {}

    def start_execution(
        self,
        pipeline_id: str,
        mode: PipelineMode,
        bronze_steps: dict[str, Any],
        silver_steps: dict[str, Any],
        gold_steps: dict[str, Any],
    ) -> PipelineReport:
        """Start monitoring a pipeline execution."""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        self._execution_start_time = start_time
        self._step_timings.clear()
        self._error_counts.clear()

        # Initialize metrics
        metrics = PipelineMetrics(
            total_steps=len(bronze_steps) + len(silver_steps) + len(gold_steps)
        )

        # Create initial report
        self._current_report = PipelineReport(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            mode=mode,
            status=PipelineStatus.RUNNING,
            start_time=start_time,
            metrics=metrics,
        )

        self.logger.info(
            f"Started monitoring pipeline {pipeline_id} (execution: {execution_id})"
        )
        return self._current_report

    def update_step_execution(
        self,
        step_name: str,
        step_type: str,
        success: bool,
        duration: float,
        rows_processed: int = 0,
        rows_written: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Update metrics for a step execution."""
        if not self._current_report:
            return

        # Update step timings
        if step_name not in self._step_timings:
            self._step_timings[step_name] = []
        self._step_timings[step_name].append(duration)

        # Update metrics
        if success:
            self._current_report.metrics.successful_steps += 1
        else:
            self._current_report.metrics.failed_steps += 1
            if error_message:
                self._current_report.errors.append(f"{step_name}: {error_message}")

        # Update step-specific metrics
        self._current_report.metrics.total_rows_processed += rows_processed
        self._current_report.metrics.total_rows_written += rows_written

        # Update duration by step type
        if step_type == "bronze":
            self._current_report.metrics.bronze_duration += duration
        elif step_type == "silver":
            self._current_report.metrics.silver_duration += duration
        elif step_type == "gold":
            self._current_report.metrics.gold_duration += duration

        # Track errors
        if not success:
            if step_name not in self._error_counts:
                self._error_counts[step_name] = 0
            self._error_counts[step_name] += 1
            self._current_report.metrics.error_count += 1

        self.logger.debug(
            f"Updated metrics for {step_name}: success={success}, duration={duration:.2f}s"
        )

    def finish_execution(self, success: bool) -> PipelineReport:
        """Finish monitoring and return final report."""
        if not self._current_report:
            raise RuntimeError("No active execution to finish")

        end_time = datetime.now()
        total_duration = (end_time - self._current_report.start_time).total_seconds()

        # Update final metrics
        self._current_report.end_time = end_time
        self._current_report.duration_seconds = total_duration
        self._current_report.metrics.total_duration = total_duration
        self._current_report.status = (
            PipelineStatus.COMPLETED if success else PipelineStatus.FAILED
        )

        # Recalculate metrics from actual step results for accuracy
        total_rows_written = 0
        successful_steps = 0
        failed_steps = 0
        total_rows_processed = 0

        # Count bronze results
        if self._current_report.bronze_results:
            for result in self._current_report.bronze_results.values():
                if result.get("success", False):
                    successful_steps += 1
                else:
                    failed_steps += 1
                total_rows_written += result.get("rows_written", 0)
                total_rows_processed += result.get("rows_processed", 0)

        # Count silver results
        if self._current_report.silver_results:
            for result in self._current_report.silver_results.values():
                if result.get("success", False):
                    successful_steps += 1
                else:
                    failed_steps += 1
                total_rows_written += result.get("rows_written", 0)
                total_rows_processed += result.get("rows_processed", 0)

        # Count gold results
        if self._current_report.gold_results:
            for result in self._current_report.gold_results.values():
                if result.get("success", False):
                    successful_steps += 1
                else:
                    failed_steps += 1
                total_rows_written += result.get("rows_written", 0)
                total_rows_processed += result.get("rows_processed", 0)

        # Update metrics with recalculated values
        self._current_report.metrics.successful_steps = successful_steps
        self._current_report.metrics.failed_steps = failed_steps
        self._current_report.metrics.total_rows_written = total_rows_written
        self._current_report.metrics.total_rows_processed = total_rows_processed

        # Calculate parallel efficiency
        if self._step_timings:
            sequential_time = sum(
                sum(timings) for timings in self._step_timings.values()
            )
            if sequential_time > 0:
                self._current_report.metrics.parallel_efficiency = (
                    sequential_time / total_duration
                )

        # Calculate average step duration
        (
            self._current_report.metrics.successful_steps
            + self._current_report.metrics.failed_steps
        )
        # Average step duration is calculated on-demand in _generate_recommendations

        # Generate recommendations
        self._current_report.recommendations = self._generate_recommendations()

        # Log completion
        if success:
            self.logger.info(
                f"Pipeline execution completed successfully in {total_duration:.2f}s"
            )
        else:
            self.logger.error(f"Pipeline execution failed after {total_duration:.2f}s")

        return self._current_report

    def get_current_metrics(self) -> PipelineMetrics | None:
        """Get current execution metrics."""
        return self._current_report.metrics if self._current_report else None

    def get_current_report(self) -> PipelineReport | None:
        """Get current execution report."""
        return self._current_report

    def get_step_performance(self, step_name: str) -> dict[str, Any]:
        """Get performance metrics for a specific step."""
        if step_name not in self._step_timings:
            return {}

        timings = self._step_timings[step_name]
        return {
            "execution_count": len(timings),
            "total_duration": sum(timings),
            "average_duration": sum(timings) / len(timings),
            "min_duration": min(timings),
            "max_duration": max(timings),
            "error_count": self._error_counts.get(step_name, 0),
        }

    def get_performance_summary(self) -> dict[str, Any]:
        """Get overall performance summary."""
        if not self._current_report:
            return {}

        metrics = self._current_report.metrics

        return {
            "total_duration": metrics.total_duration,
            "success_rate": metrics.success_rate,
            "parallel_efficiency": metrics.parallel_efficiency,
            "average_step_duration": (
                metrics.total_duration / metrics.total_steps
                if metrics.total_steps > 0
                else 0
            ),
            "total_rows_processed": metrics.total_rows_processed,
            "total_rows_written": metrics.total_rows_written,
            "error_count": metrics.error_count,
            "retry_count": metrics.retry_count,
        }

    def _generate_recommendations(self) -> list[str]:
        """Generate optimization recommendations based on execution metrics."""
        if not self._current_report:
            return []

        recommendations = []
        metrics = self._current_report.metrics

        # Performance recommendations
        if metrics.parallel_efficiency < 0.5:
            recommendations.append(
                "Consider optimizing parallel execution - low efficiency detected"
            )

        # Calculate average step duration
        if metrics.total_steps > 0:
            avg_duration = metrics.total_duration / metrics.total_steps
            if avg_duration > 60:
                recommendations.append(
                    "Consider optimizing step execution - long average duration"
                )

        if metrics.error_count > 0:
            recommendations.append(
                "Review error logs and consider improving error handling"
            )

        # Data volume recommendations
        if metrics.total_rows_processed > 1000000 and metrics.parallel_efficiency < 0.8:
            recommendations.append(
                "Consider increasing parallel workers for large datasets"
            )

        # Success rate recommendations
        if metrics.success_rate < 95:
            recommendations.append(
                "Consider improving data quality or validation rules"
            )

        return recommendations

    def reset(self) -> None:
        """Reset the monitor state."""
        self._current_report = None
        self._execution_start_time = None
        self._step_timings.clear()
        self._error_counts.clear()
        self.logger.info("Pipeline monitor reset")
