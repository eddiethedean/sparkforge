"""
Pipeline configuration models.

# Depends on:
#   errors
#   models.base
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..errors import PipelineValidationError
from .base import BaseModel, ValidationThresholds


@dataclass
class PipelineConfig(BaseModel):
    """
    Main pipeline configuration.

    Attributes:
        schema: Database schema name
        thresholds: Validation thresholds for each phase
        verbose: Whether to enable verbose logging
    """

    schema: str
    thresholds: ValidationThresholds
    verbose: bool = True

    @property
    def min_bronze_rate(self) -> float:
        """Get bronze validation threshold."""
        return self.thresholds.bronze

    @property
    def min_silver_rate(self) -> float:
        """Get silver validation threshold."""
        return self.thresholds.silver

    @property
    def min_gold_rate(self) -> float:
        """Get gold validation threshold."""
        return self.thresholds.gold

    def validate(self) -> None:
        """Validate pipeline configuration."""
        if not self.schema or not isinstance(self.schema, str):
            raise PipelineValidationError("Schema name must be a non-empty string")
        self.thresholds.validate()

    @classmethod
    def create_default(cls, schema: str) -> PipelineConfig:
        """Create default pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_default(),
            verbose=True,
        )

    @classmethod
    def create_high_performance(cls, schema: str) -> PipelineConfig:
        """Create high-performance pipeline configuration with strict validation."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            verbose=False,
        )

    @classmethod
    def create_conservative(cls, schema: str) -> PipelineConfig:
        """Create conservative pipeline configuration with strict validation."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            verbose=True,
        )


@dataclass
class PipelineMetrics(BaseModel):
    """
    Overall pipeline execution metrics.

    Attributes:
        total_steps: Total number of steps
        successful_steps: Number of successful steps
        failed_steps: Number of failed steps
        skipped_steps: Number of skipped steps
        total_duration: Total execution duration
        bronze_duration: Bronze layer duration
        silver_duration: Silver layer duration
        gold_duration: Gold layer duration
        total_rows_processed: Total rows processed
        total_rows_written: Total rows written
        avg_validation_rate: Average validation rate
        parallel_efficiency: Parallel execution efficiency
        cache_hit_rate: Cache hit rate
        error_count: Number of errors
        retry_count: Number of retries
    """

    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration: float = 0.0
    bronze_duration: float = 0.0
    silver_duration: float = 0.0
    gold_duration: float = 0.0
    total_rows_processed: int = 0
    total_rows_written: int = 0
    avg_validation_rate: float = 0.0
    parallel_efficiency: float = 0.0
    cache_hit_rate: float = 0.0
    error_count: int = 0
    retry_count: int = 0

    def validate(self) -> None:
        """Validate the pipeline metrics."""
        if self.total_steps < 0:
            raise ValueError("Total steps cannot be negative")
        if self.successful_steps < 0:
            raise ValueError("Successful steps cannot be negative")
        if self.failed_steps < 0:
            raise ValueError("Failed steps cannot be negative")
        if self.skipped_steps < 0:
            raise ValueError("Skipped steps cannot be negative")
        if self.total_duration < 0:
            raise ValueError("Total duration cannot be negative")
        if not 0 <= self.avg_validation_rate <= 100:
            raise ValueError("Average validation rate must be between 0 and 100")

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        return (
            (self.successful_steps / self.total_steps * 100)
            if self.total_steps > 0
            else 0.0
        )

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        return 100.0 - self.success_rate

    @classmethod
    def from_step_results(cls, step_results: list[Any]) -> PipelineMetrics:
        """Create metrics from step results."""
        total_steps = len(step_results)
        successful_steps = sum(1 for result in step_results if result.success)
        failed_steps = total_steps - successful_steps
        total_duration_secs = sum(result.duration_secs for result in step_results)
        total_rows_processed = sum(result.rows_processed for result in step_results)
        total_rows_written = sum(result.rows_written for result in step_results)
        avg_validation_rate = (
            sum(result.validation_rate for result in step_results) / total_steps
            if total_steps > 0
            else 0.0
        )

        return cls(
            total_steps=total_steps,
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            total_duration=total_duration_secs,
            total_rows_processed=total_rows_processed,
            total_rows_written=total_rows_written,
            avg_validation_rate=avg_validation_rate,
        )
