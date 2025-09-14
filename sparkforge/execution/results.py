"""
Execution results and statistics for SparkForge.

This module defines standardized result classes for execution operations,
providing consistent data structures across all execution strategies.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from pyspark.sql import DataFrame


class StepStatus(Enum):
    """Status of a pipeline step execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class StepType(Enum):
    """Types of pipeline steps."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepExecutionResult:
    """Result of executing a single pipeline step."""
    step_name: str
    step_type: StepType
    status: StepStatus
    duration_seconds: float
    rows_processed: int = 0
    rows_written: int = 0
    error_message: Optional[str] = None
    output_data: Optional[DataFrame] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success(self) -> bool:
        """Whether the step executed successfully."""
        return self.status == StepStatus.COMPLETED
    
    @property
    def failed(self) -> bool:
        """Whether the step failed."""
        return self.status == StepStatus.FAILED


@dataclass
class ExecutionStats:
    """Statistics for execution performance."""
    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration: float = 0.0
    parallel_efficiency: float = 0.0
    average_step_duration: float = 0.0
    retry_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        if self.total_steps == 0:
            return 0.0
        return (self.successful_steps / self.total_steps) * 100.0


@dataclass
class ExecutionResult:
    """Result of executing a pipeline or group of steps."""
    step_results: Dict[str, StepExecutionResult]
    execution_groups: List[List[str]]
    total_duration: float
    parallel_efficiency: float
    successful_steps: int
    failed_steps: int
    total_rows_processed: int
    total_rows_written: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def success(self) -> bool:
        """Whether all steps executed successfully."""
        return self.failed_steps == 0
    
    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        total = self.successful_steps + self.failed_steps
        if total == 0:
            return 0.0
        return (self.successful_steps / total) * 100.0
    
    def get_step_result(self, step_name: str) -> Optional[StepExecutionResult]:
        """Get the result for a specific step."""
        return self.step_results.get(step_name)
    
    def get_failed_steps(self) -> List[str]:
        """Get names of failed steps."""
        return [
            name for name, result in self.step_results.items()
            if result.failed
        ]
    
    def get_successful_steps(self) -> List[str]:
        """Get names of successful steps."""
        return [
            name for name, result in self.step_results.items()
            if result.success
        ]
