"""
Pipeline models and data structures for SparkForge.

This module defines the core data structures used throughout the pipeline system,
providing a clean separation of concerns and better type safety.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict

from ..models import PipelineMetrics


class PipelineMode(Enum):
    """Pipeline execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    VALIDATION_ONLY = "validation_only"


class PipelineStatus(Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


# PipelineMetrics moved to main models.py to avoid duplication


@dataclass
class PipelineReport:
    """Comprehensive pipeline execution report."""

    pipeline_id: str
    execution_id: str
    mode: PipelineMode
    status: PipelineStatus
    start_time: datetime
    end_time: datetime | None = None
    duration_seconds: float = 0.0
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)
    bronze_results: Dict[str, Any] = field(default_factory=dict)
    silver_results: Dict[str, Any] = field(default_factory=dict)
    gold_results: Dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Whether the pipeline executed successfully."""
        return self.status == PipelineStatus.COMPLETED and len(self.errors) == 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "mode": self.mode.value,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "metrics": {
                "total_steps": self.metrics.total_steps,
                "successful_steps": self.metrics.successful_steps,
                "failed_steps": self.metrics.failed_steps,
                "skipped_steps": self.metrics.skipped_steps,
                "total_duration": self.metrics.total_duration,
                "bronze_duration": self.metrics.bronze_duration,
                "silver_duration": self.metrics.silver_duration,
                "gold_duration": self.metrics.gold_duration,
                "total_rows_processed": self.metrics.total_rows_processed,
                "total_rows_written": self.metrics.total_rows_written,
                "parallel_efficiency": self.metrics.parallel_efficiency,
                "cache_hit_rate": self.metrics.cache_hit_rate,
                "error_count": self.metrics.error_count,
                "retry_count": self.metrics.retry_count,
            },
            "bronze_results": self.bronze_results,
            "silver_results": self.silver_results,
            "gold_results": self.gold_results,
            "errors": self.errors,
            "warnings": self.warnings,
            "recommendations": self.recommendations,
        }


# ParallelConfig and PipelineConfig moved to main models.py to avoid duplication


@dataclass
class StepExecutionContext:
    """Context for step execution."""

    step_name: str
    step_type: str
    mode: PipelineMode
    start_time: datetime
    dependencies: list[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """Duration of step execution in seconds."""
        return (datetime.now() - self.start_time).total_seconds()
