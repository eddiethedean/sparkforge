"""
Execution models for the Pipeline Builder.

This module provides models for tracking pipeline execution state and results,
including execution contexts, step results, stage statistics, and overall
execution results.

Key Components:
    - **ExecutionContext**: Tracks execution state, timing, and metadata
    - **StageStats**: Statistics for individual pipeline stages
    - **StepResult**: Results from individual step execution
    - **ExecutionResult**: Aggregated results from entire pipeline execution

Dependencies:
    - models.base: BaseModel
    - models.enums: ExecutionMode, PipelinePhase
    - models.exceptions: PipelineConfigurationError
    - models.pipeline: PipelineMetrics

Example:
    >>> from pipeline_builder.models.execution import (
    ...     ExecutionContext,
    ...     StepResult,
    ...     ExecutionResult
    ... )
    >>> from pipeline_builder.models.enums import ExecutionMode, PipelinePhase
    >>> from datetime import datetime, timezone
    >>>
    >>> # Create execution context
    >>> context = ExecutionContext(
    ...     mode=ExecutionMode.INITIAL,
    ...     start_time=datetime.now(timezone.utc)
    ... )
    >>>
    >>> # Create step result
    >>> result = StepResult.create_success(
    ...     step_name="bronze_step",
    ...     phase=PipelinePhase.BRONZE,
    ...     start_time=datetime.now(timezone.utc),
    ...     end_time=datetime.now(timezone.utc),
    ...     rows_processed=1000,
    ...     rows_written=950,
    ...     validation_rate=95.0
    ... )
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .base import BaseModel
from .enums import ExecutionMode, PipelinePhase
from .exceptions import PipelineConfigurationError
from .pipeline import PipelineMetrics


@dataclass
class ExecutionContext(BaseModel):
    """Context for pipeline execution.

    Tracks the state and metadata of a pipeline execution run, including
    timing information, execution mode, and identifiers. Provides both
    primary fields and aliases for compatibility with different parts of
    the system.

    **Validation Rules:**
        - `run_id`: Must be a non-empty string
        - `duration_secs`: Must be non-negative if set

    Attributes:
        mode: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH,
            VALIDATION_ONLY). Determines how the pipeline is executed.
        start_time: When execution started. Required field.
        end_time: When execution ended. None if execution is still running.
        duration_secs: Total execution duration in seconds. None if execution
            is still running. Automatically calculated when `finish()` is called.
        run_id: Unique run identifier (UUID string). Automatically generated
            if not provided.
        execution_id: Unique identifier for this execution (UUID string).
            Used for tracking and logging. Automatically generated if not provided.
        pipeline_id: Identifier for the pipeline being executed. Defaults
            to "unknown" if not provided.
        schema: Target schema for data storage. Defaults to "default" if
            not provided.
        started_at: When execution started (alias for start_time). Set
            automatically from start_time if not provided.
        ended_at: When execution ended (alias for end_time). Set automatically
            from end_time if not provided.
        run_mode: Mode of execution as string (alias for mode.value).
            Automatically set from mode if not provided.
        config: Pipeline configuration as dictionary. Defaults to empty dict.

    Example:
        >>> from pipeline_builder.models.execution import ExecutionContext
        >>> from pipeline_builder.models.enums import ExecutionMode
        >>> from datetime import datetime, timezone
        >>>
        >>> # Create context
        >>> context = ExecutionContext(
        ...     mode=ExecutionMode.INITIAL,
        ...     start_time=datetime.now(timezone.utc)
        ... )
        >>> print(context.run_id)  # Unique UUID
        >>>
        >>> # Finish execution
        >>> context.finish()
        >>> print(context.duration_secs)  # Execution duration
    """

    mode: ExecutionMode
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_secs: Optional[float] = None
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Additional fields for writer compatibility
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = "unknown"
    schema: str = "default"
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    run_mode: str = "initial"
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize aliases and defaults.

        Sets up alias fields (started_at, ended_at, run_mode) from primary
        fields if they are not explicitly provided. This ensures backward
        compatibility with code that uses the alias fields.
        """
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
        """Validate the execution context.

        Ensures the context has valid values for required fields and that
        numeric fields are within valid ranges.

        Raises:
            ValueError: If run_id is empty or duration_secs is negative.

        Example:
            >>> context = ExecutionContext(
            ...     mode=ExecutionMode.INITIAL,
            ...     start_time=datetime.now(timezone.utc)
            ... )
            >>> context.validate()  # Passes
        """
        if not self.run_id:
            raise ValueError("Run ID cannot be empty")
        if self.duration_secs is not None and self.duration_secs < 0:
            raise ValueError("Duration cannot be negative")

    def finish(self) -> None:
        """Mark execution as finished and calculate duration.

        Sets the end_time to the current timestamp and calculates the
        execution duration. Also updates the ended_at alias field.

        Example:
            >>> context = ExecutionContext(
            ...     mode=ExecutionMode.INITIAL,
            ...     start_time=datetime.now(timezone.utc)
            ... )
            >>> # ... execution happens ...
            >>> context.finish()
            >>> print(context.duration_secs)  # Execution duration in seconds
        """
        self.end_time = datetime.now(timezone.utc)
        if self.start_time:
            self.duration_secs = (self.end_time - self.start_time).total_seconds()

    @property
    def is_finished(self) -> bool:
        """Check if execution is finished.

        Returns:
            True if end_time is set, False otherwise.

        Example:
            >>> context = ExecutionContext(...)
            >>> print(context.is_finished)  # False
            >>> context.finish()
            >>> print(context.is_finished)  # True
        """
        return self.end_time is not None

    @property
    def is_running(self) -> bool:
        """Check if execution is currently running.

        Returns:
            True if execution is still running (end_time is None),
            False otherwise.

        Example:
            >>> context = ExecutionContext(...)
            >>> print(context.is_running)  # True
            >>> context.finish()
            >>> print(context.is_running)  # False
        """
        return not self.is_finished


@dataclass
class StageStats(BaseModel):
    """Statistics for a pipeline stage.

    Tracks detailed statistics for a single pipeline stage (Bronze, Silver,
    or Gold), including row counts, validation rates, and timing information.

    **Validation Rules:**
        - `total_rows` must equal `valid_rows + invalid_rows`
        - `validation_rate` must be between 0 and 100
        - `duration_secs` must be non-negative

    Attributes:
        stage: Stage name (bronze, silver, or gold). Identifies which
            Medallion Architecture layer this stage belongs to.
        step: Step name within the stage. Identifies the specific step
            these statistics are for.
        total_rows: Total number of rows processed in this stage.
        valid_rows: Number of rows that passed validation.
        invalid_rows: Number of rows that failed validation.
        validation_rate: Validation success rate (0-100). Percentage of
            rows that passed validation.
        duration_secs: Processing duration in seconds for this stage.
        start_time: When processing started. Optional timestamp.
        end_time: When processing ended. Optional timestamp.

    Example:
        >>> from pipeline_builder.models.execution import StageStats
        >>> from datetime import datetime, timezone
        >>>
        >>> stats = StageStats(
        ...     stage="bronze",
        ...     step="user_events",
        ...     total_rows=1000,
        ...     valid_rows=950,
        ...     invalid_rows=50,
        ...     validation_rate=95.0,
        ...     duration_secs=10.5
        ... )
        >>> stats.validate()
        >>> print(f"Error rate: {stats.error_rate}%")  # 5.0%
    """

    stage: str
    step: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    validation_rate: float
    duration_secs: float
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def validate(self) -> None:
        """Validate stage statistics.

        Ensures row counts are consistent and all values are within valid
        ranges. Raises an error if validation fails.

        Raises:
            PipelineConfigurationError: If row counts are inconsistent or
                values are outside valid ranges.

        Example:
            >>> stats = StageStats(
            ...     stage="bronze",
            ...     step="test",
            ...     total_rows=1000,
            ...     valid_rows=950,
            ...     invalid_rows=50,
            ...     validation_rate=95.0,
            ...     duration_secs=10.5
            ... )
            >>> stats.validate()  # Passes
        """
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
        """Check if the stage passed validation.

        Returns:
            True if validation_rate >= 95.0%, False otherwise.

        Example:
            >>> stats = StageStats(..., validation_rate=96.0, ...)
            >>> print(stats.is_valid)  # True
        """
        return self.validation_rate >= 95.0  # Default threshold

    @property
    def error_rate(self) -> float:
        """Calculate error rate.

        Returns:
            Percentage of rows that failed validation (0-100). Returns 0.0
            if total_rows is 0.

        Example:
            >>> stats = StageStats(
            ...     total_rows=1000,
            ...     invalid_rows=50,
            ...     ...
            ... )
            >>> print(f"Error rate: {stats.error_rate}%")  # 5.0%
        """
        if self.total_rows == 0:
            return 0.0
        return (self.invalid_rows / self.total_rows) * 100

    @property
    def throughput_rows_per_sec(self) -> float:
        """Calculate throughput in rows per second.

        Returns:
            Processing throughput in rows per second. Returns 0.0 if
            duration_secs is 0.

        Example:
            >>> stats = StageStats(
            ...     total_rows=10000,
            ...     duration_secs=10.0,
            ...     ...
            ... )
            >>> print(f"Throughput: {stats.throughput_rows_per_sec} rows/sec")  # 1000.0
        """
        if self.duration_secs == 0:
            return 0.0
        return self.total_rows / self.duration_secs


@dataclass
class StepResult(BaseModel):
    """Result of a pipeline step execution.

    Tracks the outcome and metrics of a single pipeline step execution,
    including success status, timing, row counts, and validation rates.

    **Validation Rules:**
        - `step_name`: Must be a non-empty string
        - `duration_secs`: Must be non-negative
        - `rows_processed`: Must be non-negative
        - `rows_written`: Must be non-negative
        - `validation_rate`: Must be between 0 and 100

    Attributes:
        step_name: Name of the step that was executed. Identifies which
            step these results are for.
        phase: Pipeline phase (BRONZE, SILVER, or GOLD) that this step
            belongs to.
        success: Whether the step execution succeeded. True if the step
            completed without errors, False otherwise.
        start_time: When step execution started. Required timestamp.
        end_time: When step execution ended. Required timestamp.
        duration_secs: Execution duration in seconds. Calculated from
            start_time and end_time.
        rows_processed: Number of rows processed during step execution.
            Includes both valid and invalid rows.
        rows_written: Number of rows written to the target table. May be
            less than rows_processed if validation filtered out some rows.
        validation_rate: Validation success rate (0-100). Percentage of
            processed rows that passed validation.
        error_message: Error message if the step failed. None if the step
            succeeded.
        step_type: Type of step (bronze, silver, gold) as string. Optional
            for compatibility.
        table_fqn: Fully qualified table name if step writes to a table
            (e.g., "schema.table_name"). None if step doesn't write to a table.
        write_mode: Write mode used (overwrite, append). None if step
            doesn't write to a table.
        input_rows: Number of input rows processed. Optional field for
            tracking input data size.

    Example:
        >>> from pipeline_builder.models.execution import StepResult
        >>> from pipeline_builder.models.enums import PipelinePhase
        >>> from datetime import datetime, timezone
        >>>
        >>> # Create success result
        >>> result = StepResult.create_success(
        ...     step_name="bronze_step",
        ...     phase=PipelinePhase.BRONZE,
        ...     start_time=datetime.now(timezone.utc),
        ...     end_time=datetime.now(timezone.utc),
        ...     rows_processed=1000,
        ...     rows_written=950,
        ...     validation_rate=95.0
        ... )
        >>> print(f"Success: {result.success}")  # True
        >>> print(f"Throughput: {result.throughput_rows_per_sec} rows/sec")
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
    error_message: Optional[str] = None
    step_type: Optional[str] = None
    table_fqn: Optional[str] = None
    write_mode: Optional[str] = None
    input_rows: Optional[int] = None

    def validate(self) -> None:
        """Validate the step result.

        Ensures all fields are within valid ranges and required fields are
        present. Raises an error if validation fails.

        Raises:
            ValueError: If any field is invalid or out of range.

        Example:
            >>> result = StepResult.create_success(...)
            >>> result.validate()  # Passes
        """
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
        """Check if the step result is valid.

        Returns:
            True if the step succeeded and validation_rate >= 95.0%,
            False otherwise.

        Example:
            >>> result = StepResult(..., success=True, validation_rate=96.0)
            >>> print(result.is_valid)  # True
        """
        return self.success and self.validation_rate >= 95.0

    @property
    def is_high_quality(self) -> bool:
        """Check if the step result is high quality.

        Returns:
            True if the step succeeded and validation_rate >= 98.0%,
            False otherwise.

        Example:
            >>> result = StepResult(..., success=True, validation_rate=99.0)
            >>> print(result.is_high_quality)  # True
        """
        return self.success and self.validation_rate >= 98.0

    @property
    def throughput_rows_per_sec(self) -> float:
        """Calculate throughput in rows per second.

        Returns:
            Processing throughput in rows per second. Returns 0.0 if
            duration_secs is 0.

        Example:
            >>> result = StepResult(
            ...     rows_processed=10000,
            ...     duration_secs=10.0,
            ...     ...
            ... )
            >>> print(f"Throughput: {result.throughput_rows_per_sec} rows/sec")  # 1000.0
        """
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
        step_type: Optional[str] = None,
        table_fqn: Optional[str] = None,
        write_mode: Optional[str] = None,
        input_rows: Optional[int] = None,
    ) -> StepResult:
        """Create a successful step result.

        Factory method for creating a StepResult representing a successful
        step execution. Automatically calculates duration and sets success=True.

        Args:
            step_name: Name of the step that was executed.
            phase: Pipeline phase (BRONZE, SILVER, or GOLD).
            start_time: When step execution started.
            end_time: When step execution ended.
            rows_processed: Number of rows processed.
            rows_written: Number of rows written to table.
            validation_rate: Validation success rate (0-100).
            step_type: Optional step type string (bronze, silver, gold).
            table_fqn: Optional fully qualified table name.
            write_mode: Optional write mode (overwrite, append).
            input_rows: Optional number of input rows.

        Returns:
            StepResult instance with success=True and calculated duration.

        Example:
            >>> from datetime import datetime, timezone
            >>> result = StepResult.create_success(
            ...     step_name="bronze_step",
            ...     phase=PipelinePhase.BRONZE,
            ...     start_time=datetime.now(timezone.utc),
            ...     end_time=datetime.now(timezone.utc),
            ...     rows_processed=1000,
            ...     rows_written=950,
            ...     validation_rate=95.0
            ... )
        """
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
        step_type: Optional[str] = None,
        table_fqn: Optional[str] = None,
        write_mode: Optional[str] = None,
        input_rows: Optional[int] = None,
    ) -> StepResult:
        """Create a failed step result.

        Factory method for creating a StepResult representing a failed
        step execution. Automatically calculates duration and sets success=False,
        with zero rows processed/written and zero validation rate.

        Args:
            step_name: Name of the step that was executed.
            phase: Pipeline phase (BRONZE, SILVER, or GOLD).
            start_time: When step execution started.
            end_time: When step execution ended.
            error_message: Error message describing the failure.
            step_type: Optional step type string (bronze, silver, gold).
            table_fqn: Optional fully qualified table name.
            write_mode: Optional write mode (overwrite, append).
            input_rows: Optional number of input rows.

        Returns:
            StepResult instance with success=False and zero metrics.

        Example:
            >>> from datetime import datetime, timezone
            >>> result = StepResult.create_failure(
            ...     step_name="bronze_step",
            ...     phase=PipelinePhase.BRONZE,
            ...     start_time=datetime.now(timezone.utc),
            ...     end_time=datetime.now(timezone.utc),
            ...     error_message="Validation failed: threshold not met"
            ... )
        """
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
        """Calculate error rate.

        Returns:
            Percentage of rows that failed validation (0-100). Returns 0.0
            if rows_processed is 0.

        Example:
            >>> result = StepResult(..., rows_processed=1000, validation_rate=95.0)
            >>> print(f"Error rate: {result.error_rate}%")  # 5.0%
        """
        if self.rows_processed == 0:
            return 0.0
        return 100.0 - self.validation_rate


@dataclass
class ExecutionResult(BaseModel):
    """Result of pipeline execution.

    Aggregates results from an entire pipeline execution, including the
    execution context, individual step results, overall metrics, and
    overall success status.

    **Validation Rules:**
        - `context`: Must be an ExecutionContext instance
        - `step_results`: Must be a list
        - `metrics`: Must be a PipelineMetrics instance
        - `success`: Must be a boolean

    Attributes:
        context: ExecutionContext instance containing execution metadata,
            timing, and configuration.
        step_results: List of StepResult instances, one for each step
            executed in the pipeline.
        metrics: PipelineMetrics instance with aggregated metrics from
            all steps (total rows, durations, validation rates, etc.).
        success: Whether the entire pipeline succeeded. True if all steps
            succeeded, False if any step failed.

    Example:
        >>> from pipeline_builder.models.execution import ExecutionResult
        >>> from pipeline_builder.models.enums import ExecutionMode
        >>>
        >>> # Create execution result from context and step results
        >>> context = ExecutionContext(mode=ExecutionMode.INITIAL, ...)
        >>> step_results = [step_result1, step_result2, ...]
        >>> result = ExecutionResult.from_context_and_results(context, step_results)
        >>> print(f"Pipeline success: {result.success}")
        >>> print(f"Total rows: {result.metrics.total_rows_processed}")
    """

    context: ExecutionContext
    step_results: list[StepResult]
    metrics: PipelineMetrics
    success: bool

    def validate(self) -> None:
        """Validate execution result.

        Ensures all fields are of the correct types. Raises an error if
        validation fails.

        Raises:
            PipelineConfigurationError: If any field has an invalid type.

        Example:
            >>> result = ExecutionResult(...)
            >>> result.validate()  # Passes
        """
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
    ) -> ExecutionResult:
        """Create execution result from context and step results.

        Factory method that aggregates step results into pipeline metrics
        and determines overall success. This is the recommended way to
        create an ExecutionResult after pipeline execution.

        Args:
            context: ExecutionContext from the pipeline execution.
            step_results: List of StepResult instances from all executed steps.

        Returns:
            ExecutionResult instance with aggregated metrics and success status.

        Example:
            >>> context = ExecutionContext(mode=ExecutionMode.INITIAL, ...)
            >>> step_results = [
            ...     StepResult.create_success(...),
            ...     StepResult.create_success(...)
            ... ]
            >>> result = ExecutionResult.from_context_and_results(context, step_results)
            >>> print(f"Success: {result.success}")  # True
            >>> print(f"Total steps: {result.metrics.total_steps}")  # 2
        """
        metrics = PipelineMetrics.from_step_results(step_results)
        success = all(result.success for result in step_results)
        return cls(
            context=context, step_results=step_results, metrics=metrics, success=success
        )
