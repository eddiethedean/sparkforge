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
Execution results and statistics for SparkForge.

This module defines standardized result classes for execution operations,
providing consistent data structures across all execution strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

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
    error_message: str | None = None
    output_data: DataFrame | None = None
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

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

    step_results: dict[str, StepExecutionResult]
    execution_groups: list[list[str]]
    total_duration: float
    parallel_efficiency: float
    successful_steps: int
    failed_steps: int
    total_rows_processed: int
    total_rows_written: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None

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

    def get_step_result(self, step_name: str) -> StepExecutionResult | None:
        """Get the result for a specific step."""
        return self.step_results.get(step_name)

    def get_failed_steps(self) -> list[str]:
        """Get names of failed steps."""
        return [name for name, result in self.step_results.items() if result.failed]

    def get_successful_steps(self) -> list[str]:
        """Get names of successful steps."""
        return [name for name, result in self.step_results.items() if result.success]
