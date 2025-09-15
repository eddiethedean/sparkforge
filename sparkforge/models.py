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

# models.py
"""
Enhanced data models and type definitions for the Pipeline Builder.

This module contains all the dataclasses and type definitions used throughout
the pipeline system. All models include comprehensive validation, type safety,
and clear documentation.

Key Features:
- Type-safe dataclasses with comprehensive validation
- Enhanced error handling with custom exceptions
- Clear separation of concerns with proper abstractions
- Immutable data structures where appropriate
- Rich metadata and documentation
- Protocol definitions for better type checking
- Factory methods for common object creation
"""

from __future__ import annotations

import json
import uuid
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Protocol, TypeVar

from pyspark.sql import DataFrame

from .errors.pipeline import PipelineValidationError

# ============================================================================
# Custom Exceptions
# ============================================================================


class PipelineConfigurationError(ValueError):
    """Raised when pipeline configuration is invalid."""

    pass


class PipelineExecutionError(RuntimeError):
    """Raised when pipeline execution fails."""

    pass


# ============================================================================
# Enums
# ============================================================================


class PipelinePhase(Enum):
    """Enumeration of pipeline phases."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ExecutionMode(Enum):
    """Enumeration of execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"


class WriteMode(Enum):
    """Enumeration of write modes."""

    OVERWRITE = "overwrite"
    APPEND = "append"


class ValidationResult(Enum):
    """Enumeration of validation results."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


# ============================================================================
# Type Aliases and Protocols
# ============================================================================

# Type aliases for better readability
ColumnRules = Dict[str, List[Any]]
TransformFunction = Callable[[DataFrame], DataFrame]
SilverTransformFunction = Callable[[DataFrame], DataFrame]
GoldTransformFunction = Callable[[Dict[str, DataFrame]], DataFrame]

# Generic type for pipeline results
T = TypeVar("T")


class Validatable(Protocol):
    """Protocol for objects that can be validated."""

    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid."""
        ...


class Serializable(Protocol):
    """Protocol for objects that can be serialized."""

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dictionary."""
        ...

    def to_json(self) -> str:
        """Convert object to JSON string."""
        ...


# ============================================================================
# Base Classes
# ============================================================================


@dataclass
class BaseModel(ABC):
    """
    Base class for all pipeline models with common functionality.

    Provides standard validation, serialization, and representation methods
    for all pipeline data models. All models in the pipeline system inherit
    from this base class to ensure consistent behavior.

    Features:
    - Automatic validation support
    - JSON serialization and deserialization
    - Dictionary conversion for easy data exchange
    - String representation for debugging
    - Type-safe field access

    Example:
        >>> @dataclass
        >>> class MyStep(BaseModel):
        ...     name: str
        ...     rules: Dict[str, List[Any]]
        ...
        ...     def validate(self) -> None:
        ...         if not self.name:
        ...             raise ValueError("Name cannot be empty")
        ...         if not self.rules:
        ...             raise ValueError("Rules cannot be empty")
        >>>
        >>> step = MyStep(name="test", rules={"id": [F.col("id").isNotNull()]})
        >>> step.validate()
        >>> print(step.to_json())
    """

    def validate(self) -> None:
        """Validate the model. Override in subclasses."""
        pass

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        result = {}
        for field in self.__dataclass_fields__.values():
            value = getattr(self, field.name)
            if hasattr(value, "to_dict"):
                result[field.name] = value.to_dict()
            else:
                result[field.name] = value
        return result

    def to_json(self) -> str:
        """Convert model to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)

    def __str__(self) -> str:
        """String representation of the model."""
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.to_dict().items())})"


# ============================================================================
# Configuration Models
# ============================================================================


@dataclass
class ValidationThresholds(BaseModel):
    """
    Validation thresholds for different pipeline phases.

    Attributes:
        bronze: Bronze layer validation threshold (0-100)
        silver: Silver layer validation threshold (0-100)
        gold: Gold layer validation threshold (0-100)
    """

    bronze: float
    silver: float
    gold: float

    def validate(self) -> None:
        """Validate threshold values."""
        for phase, threshold in [
            ("bronze", self.bronze),
            ("silver", self.silver),
            ("gold", self.gold),
        ]:
            if not 0 <= threshold <= 100:
                raise PipelineValidationError(
                    f"{phase} threshold must be between 0 and 100, got {threshold}"
                )

    def get_threshold(self, phase: PipelinePhase) -> float:
        """Get threshold for a specific phase."""
        phase_map = {
            PipelinePhase.BRONZE: self.bronze,
            PipelinePhase.SILVER: self.silver,
            PipelinePhase.GOLD: self.gold,
        }
        return phase_map[phase]

    @classmethod
    def create_default(cls) -> ValidationThresholds:
        """Create default validation thresholds."""
        return cls(bronze=95.0, silver=98.0, gold=99.0)

    @classmethod
    def create_strict(cls) -> ValidationThresholds:
        """Create strict validation thresholds."""
        return cls(bronze=99.0, silver=99.5, gold=99.9)

    @classmethod
    def create_loose(cls) -> ValidationThresholds:
        """Create loose validation thresholds."""
        return cls(bronze=80.0, silver=85.0, gold=90.0)


@dataclass
class ParallelConfig(BaseModel):
    """
    Configuration for parallel execution.

    Attributes:
        enabled: Whether parallel execution is enabled
        max_workers: Maximum number of parallel workers
        timeout_secs: Timeout for parallel operations in seconds
    """

    enabled: bool
    max_workers: int
    timeout_secs: int = 300

    def validate(self) -> None:
        """Validate parallel configuration."""
        if self.max_workers < 1:
            raise PipelineValidationError(
                f"max_workers must be at least 1, got {self.max_workers}"
            )
        if self.max_workers > 32:
            raise PipelineValidationError(
                f"max_workers should not exceed 32, got {self.max_workers}"
            )
        if self.timeout_secs < 1:
            raise PipelineValidationError(
                f"timeout_secs must be at least 1, got {self.timeout_secs}"
            )

    @classmethod
    def create_default(cls) -> ParallelConfig:
        """Create default parallel configuration."""
        return cls(enabled=True, max_workers=4, timeout_secs=300)

    @classmethod
    def create_sequential(cls) -> ParallelConfig:
        """Create sequential execution configuration."""
        return cls(enabled=False, max_workers=1, timeout_secs=600)

    @classmethod
    def create_high_performance(cls) -> ParallelConfig:
        """Create high-performance parallel configuration."""
        return cls(enabled=True, max_workers=16, timeout_secs=1200)


@dataclass
class PipelineConfig(BaseModel):
    """
    Main pipeline configuration.

    Attributes:
        schema: Database schema name
        thresholds: Validation thresholds for each phase
        parallel: Parallel execution configuration
        verbose: Whether to enable verbose logging
    """

    schema: str
    thresholds: ValidationThresholds
    parallel: ParallelConfig
    verbose: bool = True

    def validate(self) -> None:
        """Validate pipeline configuration."""
        if not self.schema or not isinstance(self.schema, str):
            raise PipelineValidationError("Schema name must be a non-empty string")
        self.thresholds.validate()
        self.parallel.validate()

    @classmethod
    def create_default(cls, schema: str) -> PipelineConfig:
        """Create default pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_default(),
            parallel=ParallelConfig.create_default(),
            verbose=True,
        )

    @classmethod
    def create_high_performance(cls, schema: str) -> PipelineConfig:
        """Create high-performance pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            parallel=ParallelConfig.create_high_performance(),
            verbose=False,
        )

    @classmethod
    def create_conservative(cls, schema: str) -> PipelineConfig:
        """Create conservative pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            parallel=ParallelConfig.create_sequential(),
            verbose=True,
        )


# ============================================================================
# Step Models
# ============================================================================


@dataclass
class BronzeStep(BaseModel):
    """
    Bronze layer step configuration for raw data validation and ingestion.

    Bronze steps represent the first layer of the Medallion Architecture,
    handling raw data validation and establishing the foundation for downstream
    processing. They define validation rules and incremental processing capabilities.

    Attributes:
        name: Unique identifier for this Bronze step
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        incremental_col: Column name for incremental processing (e.g., "timestamp").
                        If provided, enables watermarking for efficient updates.
                        If None, forces full refresh mode for downstream steps.

    Example:
        >>> from pyspark.sql import functions as F
        >>>
        >>> bronze_step = BronzeStep(
        ...     name="user_events",
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
        ...         "timestamp": [F.col("timestamp").isNotNull(), F.col("timestamp") > "2020-01-01"]
        ...     },
        ...     incremental_col="timestamp"
        ... )
        >>>
        >>> # Validate configuration
        >>> bronze_step.validate()
        >>> print(f"Supports incremental: {bronze_step.has_incremental_capability}")
    """

    name: str
    rules: ColumnRules
    incremental_col: str | None = None
    schema: str | None = None

    def validate(self) -> None:
        """Validate bronze step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if self.incremental_col is not None and not isinstance(
            self.incremental_col, str
        ):
            raise PipelineValidationError("Incremental column must be a string")

    @property
    def has_incremental_capability(self) -> bool:
        """Check if this Bronze step supports incremental processing."""
        return self.incremental_col is not None


@dataclass
class SilverStep(BaseModel):
    """
    Silver layer step configuration for data cleaning and enrichment.

    Silver steps represent the second layer of the Medallion Architecture,
    transforming raw Bronze data into clean, business-ready datasets.
    They apply data quality rules, business logic, and data transformations.

    Attributes:
        name: Unique identifier for this Silver step
        source_bronze: Name of the Bronze step providing input data
        transform: Transformation function with signature:
                 (spark: SparkSession, bronze_df: DataFrame, prior_silvers: Dict[str, DataFrame]) -> DataFrame
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                      If provided, enables incremental processing with append mode.
                      If None, uses overwrite mode for full refresh.
        existing: Whether this represents an existing table (for validation-only steps)

    Example:
        >>> def clean_user_events(spark, bronze_df, prior_silvers):
        ...     return (bronze_df
        ...         .filter(F.col("user_id").isNotNull())
        ...         .withColumn("event_date", F.date_trunc("day", "timestamp"))
        ...         .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
        ...     )
        >>>
        >>> silver_step = SilverStep(
        ...     name="clean_events",
        ...     source_bronze="user_events",
        ...     transform=clean_user_events,
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "event_date": [F.col("event_date").isNotNull()]
        ...     },
        ...     table_name="clean_user_events",
        ...     watermark_col="timestamp"
        ... )
    """

    name: str
    source_bronze: str
    transform: SilverTransformFunction
    rules: ColumnRules
    table_name: str
    watermark_col: str | None = None
    existing: bool = False
    schema: str | None = None

    def validate(self) -> None:
        """Validate silver step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError(
                "Source bronze step name must be a non-empty string"
            )
        if not callable(self.transform):
            raise PipelineValidationError("Transform must be a callable function")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise PipelineValidationError("Table name must be a non-empty string")


@dataclass
class GoldStep(BaseModel):
    """
    Gold layer step configuration for business analytics and reporting.

    Gold steps represent the third layer of the Medallion Architecture,
    creating business-ready datasets for analytics, reporting, and dashboards.
    They aggregate and transform Silver layer data into meaningful business insights.

    Attributes:
        name: Unique identifier for this Gold step
        transform: Transformation function with signature:
                 (spark: SparkSession, silvers: Dict[str, DataFrame]) -> DataFrame
                 - spark: Active SparkSession for operations
                 - silvers: Dictionary of all Silver DataFrames by step name
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        source_silvers: List of Silver step names to use as input sources.
                       If None, uses all available Silver steps.
                       Allows selective consumption of Silver data.

    Example:
        >>> def user_daily_metrics(spark, silvers):
        ...     events_df = silvers["clean_events"]
        ...     return (events_df
        ...         .groupBy("user_id", "event_date")
        ...         .agg(
        ...             F.count("*").alias("total_events"),
        ...             F.countDistinct("event_type").alias("unique_event_types"),
        ...             F.max("timestamp").alias("last_activity"),
        ...             F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
        ...         )
        ...         .withColumn("is_active_user", F.col("total_events") > 5)
        ...     )
        >>>
        >>> gold_step = GoldStep(
        ...     name="user_metrics",
        ...     transform=user_daily_metrics,
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "total_events": [F.col("total_events") > 0]
        ...     },
        ...     table_name="user_daily_metrics",
        ...     source_silvers=["clean_events"]
        ... )
    """

    name: str
    transform: GoldTransformFunction
    rules: ColumnRules
    table_name: str
    source_silvers: list[str] | None = None
    schema: str | None = None

    def validate(self) -> None:
        """Validate gold step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not callable(self.transform):
            raise PipelineValidationError("Transform must be a callable function")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise PipelineValidationError("Table name must be a non-empty string")
        if self.source_silvers is not None and not isinstance(
            self.source_silvers, list
        ):
            raise PipelineValidationError("Source silvers must be a list or None")


# ============================================================================
# Execution Models
# ============================================================================


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
    """

    mode: ExecutionMode
    start_time: datetime
    end_time: datetime | None = None
    duration_secs: float | None = None
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

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
            raise PipelineValidationError(
                f"Total rows ({self.total_rows}) must equal valid ({self.valid_rows}) + invalid ({self.invalid_rows})"
            )
        if not 0 <= self.validation_rate <= 100:
            raise PipelineValidationError(
                f"Validation rate must be between 0 and 100, got {self.validation_rate}"
            )
        if self.duration_secs < 0:
            raise PipelineValidationError(
                f"Duration must be non-negative, got {self.duration_secs}"
            )

    @property
    def is_valid(self) -> bool:
        """Check if the stage passed validation."""
        return self.validation_rate >= 95.0  # Default threshold

    @property
    def error_rate(self) -> float:
        """Calculate error rate."""
        return 100.0 - self.validation_rate

    @classmethod
    def create_bronze_stats(
        cls,
        step: str,
        total_rows: int,
        valid_rows: int,
        duration_secs: float,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> StageStats:
        """Create bronze stage statistics."""
        invalid_rows = total_rows - valid_rows
        validation_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0
        return cls(
            stage="bronze",
            step=step,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            validation_rate=validation_rate,
            duration_secs=duration_secs,
            start_time=start_time,
            end_time=end_time,
        )

    @classmethod
    def create_silver_stats(
        cls,
        step: str,
        total_rows: int,
        valid_rows: int,
        duration_secs: float,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> StageStats:
        """Create silver stage statistics."""
        invalid_rows = total_rows - valid_rows
        validation_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0
        return cls(
            stage="silver",
            step=step,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            validation_rate=validation_rate,
            duration_secs=duration_secs,
            start_time=start_time,
            end_time=end_time,
        )

    @classmethod
    def create_gold_stats(
        cls,
        step: str,
        total_rows: int,
        valid_rows: int,
        duration_secs: float,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> StageStats:
        """Create gold stage statistics."""
        invalid_rows = total_rows - valid_rows
        validation_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0
        return cls(
            stage="gold",
            step=step,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            validation_rate=validation_rate,
            duration_secs=duration_secs,
            start_time=start_time,
            end_time=end_time,
        )


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

    @property
    def is_valid(self) -> bool:
        """Check if the step result is valid."""
        return self.success and self.validation_rate >= 95.0

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
    ) -> StepResult:
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
        )

    @classmethod
    def create_failure(
        cls,
        step_name: str,
        phase: PipelinePhase,
        start_time: datetime,
        end_time: datetime,
        error_message: str,
    ) -> StepResult:
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
        total_duration_secs: Total execution duration
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
    def from_step_results(cls, step_results: list[StepResult]) -> PipelineMetrics:
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
            total_duration_secs=total_duration_secs,
            total_rows_processed=total_rows_processed,
            total_rows_written=total_rows_written,
            avg_validation_rate=avg_validation_rate,
        )


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

    @classmethod
    def from_context_and_results(
        cls, context: ExecutionContext, step_results: list[StepResult]
    ) -> ExecutionResult:
        """Create execution result from context and step results."""
        metrics = PipelineMetrics.from_step_results(step_results)
        success = all(result.success for result in step_results)
        return cls(
            context=context, step_results=step_results, metrics=metrics, success=success
        )


# ============================================================================
# Dependency Models
# ============================================================================


@dataclass
class SilverDependencyInfo(BaseModel):
    """
    Dependency information for Silver steps.

    Attributes:
        step_name: Name of the silver step
        source_bronze: Source bronze step name
        depends_on_silvers: Set of silver step names this step depends on
        can_run_parallel: Whether this step can run in parallel
        execution_group: Execution group for parallel processing
    """

    step_name: str
    source_bronze: str
    depends_on_silvers: set[str]
    can_run_parallel: bool
    execution_group: int

    def validate(self) -> None:
        """Validate dependency information."""
        if not self.step_name or not isinstance(self.step_name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError(
                "Source bronze step name must be a non-empty string"
            )
        if not isinstance(self.depends_on_silvers, set):
            raise PipelineValidationError("Depends on silvers must be a set")
        if self.execution_group < 0:
            raise PipelineValidationError("Execution group must be non-negative")


# ============================================================================
# Cross-Layer Dependency Models
# ============================================================================


@dataclass
class CrossLayerDependency(BaseModel):
    """
    Represents a dependency between steps across different layers.

    Attributes:
        source_step: Name of the source step
        target_step: Name of the target step
        dependency_type: Type of dependency (data, validation, etc.)
        is_required: Whether this dependency is required for execution
    """

    source_step: str
    target_step: str
    dependency_type: str = "data"
    is_required: bool = True

    def validate(self) -> None:
        """Validate dependency information."""
        if not self.source_step or not isinstance(self.source_step, str):
            raise PipelineValidationError("Source step must be a non-empty string")
        if not self.target_step or not isinstance(self.target_step, str):
            raise PipelineValidationError("Target step must be a non-empty string")
        if self.source_step == self.target_step:
            raise PipelineValidationError("Source and target steps cannot be the same")


@dataclass
class UnifiedStepConfig(BaseModel):
    """
    Unified configuration for any pipeline step type.

    Attributes:
        name: Step name
        step_type: Type of step (bronze, silver, gold)
        dependencies: List of step names this step depends on
        estimated_duration: Estimated execution time in seconds
        priority: Execution priority (higher = more important)
        can_run_parallel: Whether this step can run in parallel
        resource_requirements: Resource requirements for execution
    """

    name: str
    step_type: str
    dependencies: list[str] = field(default_factory=list)
    estimated_duration: float = 1.0
    priority: int = 0
    can_run_parallel: bool = True
    resource_requirements: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if self.step_type not in ["bronze", "silver", "gold"]:
            raise PipelineValidationError(
                "Step type must be 'bronze', 'silver', or 'gold'"
            )
        if self.estimated_duration < 0:
            raise PipelineValidationError("Estimated duration must be non-negative")
        if not isinstance(self.dependencies, list):
            raise PipelineValidationError("Dependencies must be a list")


@dataclass
class UnifiedExecutionPlan(BaseModel):
    """
    Unified execution plan for cross-layer parallel execution.

    Attributes:
        execution_groups: Groups of steps that can run in parallel
        total_estimated_duration: Total estimated execution time
        parallel_efficiency: Percentage of steps that can run in parallel
        critical_path: Steps that are on the critical path
        recommendations: Optimization recommendations
    """

    execution_groups: list[list[str]] = field(default_factory=list)
    total_estimated_duration: float = 0.0
    parallel_efficiency: float = 0.0
    critical_path: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    def validate(self) -> None:
        """Validate execution plan."""
        if self.total_estimated_duration < 0:
            raise PipelineValidationError(
                "Total estimated duration must be non-negative"
            )
        if not 0 <= self.parallel_efficiency <= 100:
            raise PipelineValidationError(
                "Parallel efficiency must be between 0 and 100"
            )


# ============================================================================
# Factory Functions
# ============================================================================


def create_pipeline_config(
    schema: str,
    bronze_threshold: float = 95.0,
    silver_threshold: float = 98.0,
    gold_threshold: float = 99.0,
    enable_parallel: bool = True,
    max_workers: int = 4,
    verbose: bool = True,
) -> PipelineConfig:
    """Factory function to create pipeline configuration."""
    thresholds = ValidationThresholds(
        bronze=bronze_threshold, silver=silver_threshold, gold=gold_threshold
    )
    parallel = ParallelConfig(enabled=enable_parallel, max_workers=max_workers)
    return PipelineConfig(
        schema=schema, thresholds=thresholds, parallel=parallel, verbose=verbose
    )


def create_execution_context(mode: ExecutionMode) -> ExecutionContext:
    """Factory function to create execution context."""
    return ExecutionContext(mode=mode, start_time=datetime.utcnow())


# ============================================================================
# Validation Utilities
# ============================================================================


def validate_pipeline_config(config: PipelineConfig) -> None:
    """Validate a pipeline configuration."""
    try:
        config.validate()
    except PipelineValidationError as e:
        raise PipelineConfigurationError(f"Invalid pipeline configuration: {e}")


def validate_step_config(step: BronzeStep | SilverStep | GoldStep) -> None:
    """Validate a step configuration."""
    try:
        step.validate()
    except PipelineValidationError as e:
        raise PipelineConfigurationError(f"Invalid step configuration: {e}")


# ============================================================================
# Serialization Utilities
# ============================================================================


def serialize_pipeline_config(config: PipelineConfig) -> str:
    """Serialize pipeline configuration to JSON."""
    return config.to_json()


def deserialize_pipeline_config(json_str: str) -> PipelineConfig:
    """Deserialize pipeline configuration from JSON."""
    data = json.loads(json_str)
    return PipelineConfig(
        schema=data["schema"],
        thresholds=ValidationThresholds(
            bronze=data["thresholds"]["bronze"],
            silver=data["thresholds"]["silver"],
            gold=data["thresholds"]["gold"],
        ),
        parallel=ParallelConfig(
            enabled=data["parallel"]["enabled"],
            max_workers=data["parallel"]["max_workers"],
            timeout_secs=data["parallel"].get("timeout_secs", 300),
        ),
        verbose=data.get("verbose", True),
    )
