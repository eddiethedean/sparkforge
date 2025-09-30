"""
Execution models for the Pipeline Builder.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict

from .base import BaseModel
from .enums import ExecutionMode, PipelinePhase
from .exceptions import PipelineConfigurationError
from .pipeline import PipelineMetrics


@dataclass
class ExecutionContext(BaseModel):
    """
    Context for pipeline execution.

    Attributes:
        mode: Execution mode (initial/incremental)
        start_time: When execution started
        end_time: When execution ended
        duration_secs: Total execution duration
        run_id: Unique run identifier
        execution_id: Unique identifier for this execution
        pipeline_id: Identifier for the pipeline being executed
        schema: Target schema for data storage
        started_at: When execution started (alias for start_time)
        ended_at: When execution ended (alias for end_time)
        run_mode: Mode of execution (alias for mode)
        config: Pipeline configuration as dictionary
    """

    mode: ExecutionMode
    start_time: datetime
    end_time: datetime | None = None
    duration_secs: float | None = None
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Additional fields for writer compatibility
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = "unknown"
    schema: str = "default"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    run_mode: str = "initial"
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize aliases and defaults."""
        if self.started_at is None:
            self.started_at = self.start_time
        if self.ended_at is None:
            self.ended_at = self.end_time
        if self.run_mode == "initial":
            # Map mode to run_mode string
            if hasattr(self.mode, "value"):
                self.run_mode = self.mode.value
            elif hasattr(self.mode, "name"):
                self.run_mode = self.mode.name.lower()

    def validate(self) -> None:
        """Validate the execution context."""
        if not self.run_id:
            raise ValueError("Run ID cannot be empty")
        if self.duration_secs is not None and self.duration_secs < 0:
            raise ValueError("Duration cannot be negative")

    def finish(self) -> None:
        """Mark execution as finished and calculate duration."""
        self.end_time = datetime.utcnow()
        if self.start_time:
            self.duration_secs = (self.end_time - self.start_time).total_seconds()

    @property
    def is_finished(self) -> bool:
        """Check if execution is finished."""
        return self.end_time is not None

    @property
    def is_running(self) -> bool:
        """Check if execution is currently running."""
        return not self.is_finished


@dataclass
class StageStats(BaseModel):
    """
    Statistics for a pipeline stage.

    Attributes:
        stage: Stage name (bronze/silver/gold)
        step: Step name
        total_rows: Total number of rows processed
        valid_rows: Number of valid rows
        invalid_rows: Number of invalid rows
        validation_rate: Validation success rate (0-100)
        duration_secs: Processing duration in seconds
        start_time: When processing started
        end_time: When processing ended
    """

    stage: str
    step: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    validation_rate: float
    duration_secs: float
    start_time: datetime | None = None
    end_time: datetime | None = None

    def validate(self) -> None:
        """Validate stage statistics."""
        if self.total_rows != self.valid_rows + self.invalid_rows:
            raise PipelineConfigurationError(
                f"Total rows ({self.total_rows}) must equal valid ({self.valid_rows}) + invalid ({self.invalid_rows})"
            )
        if not 0 <= self.validation_rate <= 100:
            raise PipelineConfigurationError(
                f"Validation rate must be between 0 and 100, got {self.validation_rate}"
            )
        if self.duration_secs < 0:
            raise PipelineConfigurationError(
                f"Duration must be non-negative, got {self.duration_secs}"
            )

    @property
    def is_valid(self) -> bool:
        """Check if the stage passed validation."""
        return self.validation_rate >= 95.0  # Default threshold

    @property
    def error_rate(self) -> float:
        """Calculate error rate."""
        if self.total_rows == 0:
            return 0.0
        return (self.invalid_rows / self.total_rows) * 100

    @property
    def throughput_rows_per_sec(self) -> float:
        """Calculate throughput in rows per second."""
        if self.duration_secs == 0:
            return 0.0
        return self.total_rows / self.duration_secs


@dataclass
class StepResult(BaseModel):
    """
    Result of a pipeline step execution.

    Attributes:
        step_name: Name of the step
        phase: Pipeline phase
        success: Whether the step succeeded
        start_time: When execution started
        end_time: When execution ended
        duration_secs: Execution duration in seconds
        rows_processed: Number of rows processed
        rows_written: Number of rows written
        validation_rate: Validation success rate
        error_message: Error message if failed
        step_type: Type of step (bronze, silver, gold)
        table_fqn: Fully qualified table name if step writes to table
        write_mode: Write mode used (overwrite, append)
        input_rows: Number of input rows processed
    """

    step_name: str
    phase: PipelinePhase
    success: bool
    start_time: datetime
    end_time: datetime
    duration_secs: float
    rows_processed: int
    rows_written: int
    validation_rate: float
    error_message: str | None = None
    step_type: str | None = None
    table_fqn: str | None = None
    write_mode: str | None = None
    input_rows: int | None = None

    def validate(self) -> None:
        """Validate the step result."""
        if not self.step_name:
            raise ValueError("Step name cannot be empty")
        if self.duration_secs < 0:
            raise ValueError("Duration cannot be negative")
        if self.rows_processed < 0:
            raise ValueError("Rows processed cannot be negative")
        if self.rows_written < 0:
            raise ValueError("Rows written cannot be negative")
        if not 0 <= self.validation_rate <= 100:
            raise ValueError("Validation rate must be between 0 and 100")

    @property
    def is_valid(self) -> bool:
        """Check if the step result is valid."""
        return self.success and self.validation_rate >= 95.0

    @property
    def is_high_quality(self) -> bool:
        """Check if the step result is high quality."""
        return self.success and self.validation_rate >= 98.0

    @property
    def throughput_rows_per_sec(self) -> float:
        """Calculate throughput in rows per second."""
        if self.duration_secs == 0:
            return 0.0
        return self.rows_processed / self.duration_secs

    @classmethod
    def create_success(
        cls,
        step_name: str,
        phase: PipelinePhase,
        start_time: datetime,
        end_time: datetime,
        rows_processed: int,
        rows_written: int,
        validation_rate: float,
        step_type: str | None = None,
        table_fqn: str | None = None,
        write_mode: str | None = None,
        input_rows: int | None = None,
    ) -> "StepResult":
        """Create a successful step result."""
        duration_secs = (end_time - start_time).total_seconds()
        return cls(
            step_name=step_name,
            phase=phase,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=duration_secs,
            rows_processed=rows_processed,
            rows_written=rows_written,
            validation_rate=validation_rate,
            error_message=None,
            step_type=step_type,
            table_fqn=table_fqn,
            write_mode=write_mode,
            input_rows=input_rows,
        )

    @classmethod
    def create_failure(
        cls,
        step_name: str,
        phase: PipelinePhase,
        start_time: datetime,
        end_time: datetime,
        error_message: str,
        step_type: str | None = None,
        table_fqn: str | None = None,
        write_mode: str | None = None,
        input_rows: int | None = None,
    ) -> "StepResult":
        """Create a failed step result."""
        duration_secs = (end_time - start_time).total_seconds()
        return cls(
            step_name=step_name,
            phase=phase,
            success=False,
            start_time=start_time,
            end_time=end_time,
            duration_secs=duration_secs,
            rows_processed=0,
            rows_written=0,
            validation_rate=0.0,
            error_message=error_message,
            step_type=step_type,
            table_fqn=table_fqn,
            write_mode=write_mode,
            input_rows=input_rows,
        )

    @property
    def error_rate(self) -> float:
        """Calculate error rate."""
        if self.rows_processed == 0:
            return 0.0
        return 100.0 - self.validation_rate


@dataclass
class ExecutionResult(BaseModel):
    """
    Result of pipeline execution.

    Attributes:
        context: Execution context
        step_results: Results for each step
        metrics: Overall execution metrics
        success: Whether the entire pipeline succeeded
    """

    context: ExecutionContext
    step_results: list[StepResult]
    metrics: PipelineMetrics
    success: bool

    def validate(self) -> None:
        """Validate execution result."""
        if not isinstance(self.context, ExecutionContext):
            raise PipelineConfigurationError(
                "Context must be an ExecutionContext instance"
            )
        if not isinstance(self.step_results, list):
            raise PipelineConfigurationError("Step results must be a list")
        if not isinstance(self.metrics, PipelineMetrics):
            raise PipelineConfigurationError(
                "Metrics must be a PipelineMetrics instance"
            )
        if not isinstance(self.success, bool):
            raise PipelineConfigurationError("Success must be a boolean")

    @classmethod
    def from_context_and_results(
        cls, context: ExecutionContext, step_results: list[StepResult]
    ) -> "ExecutionResult":
        """Create execution result from context and step results."""
        metrics = PipelineMetrics.from_step_results(step_results)
        success = all(result.success for result in step_results)
        return cls(
            context=context, step_results=step_results, metrics=metrics, success=success
        )
