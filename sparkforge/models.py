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
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union, Protocol, TypeVar
from datetime import datetime
from enum import Enum
import uuid
import json
from abc import ABC

from .exceptions import PipelineValidationError

# DataFrame will be imported from pyspark.sql when available
try:
    from pyspark.sql import DataFrame
except ImportError:
    # Mock DataFrame for testing
    class DataFrame:
        pass


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
T = TypeVar('T')


class Validatable(Protocol):
    """Protocol for objects that can be validated."""
    
    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid."""
        ...


class Serializable(Protocol):
    """Protocol for objects that can be serialized."""
    
    def to_dict(self) -> Dict[str, Any]:
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
    """Base class for all pipeline models with common functionality."""
    
    def validate(self) -> None:
        """Validate the model. Override in subclasses."""
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        result = {}
        for field in self.__dataclass_fields__.values():
            value = getattr(self, field.name)
            if hasattr(value, 'to_dict'):
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
        for phase, threshold in [("bronze", self.bronze), ("silver", self.silver), ("gold", self.gold)]:
            if not 0 <= threshold <= 100:
                raise PipelineValidationError(f"{phase} threshold must be between 0 and 100, got {threshold}")
    
    def get_threshold(self, phase: PipelinePhase) -> float:
        """Get threshold for a specific phase."""
        phase_map = {
            PipelinePhase.BRONZE: self.bronze,
            PipelinePhase.SILVER: self.silver,
            PipelinePhase.GOLD: self.gold
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
            raise PipelineValidationError(f"max_workers must be at least 1, got {self.max_workers}")
        if self.max_workers > 32:
            raise PipelineValidationError(f"max_workers should not exceed 32, got {self.max_workers}")
        if self.timeout_secs < 1:
            raise PipelineValidationError(f"timeout_secs must be at least 1, got {self.timeout_secs}")
    
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
            verbose=True
        )
    
    @classmethod
    def create_high_performance(cls, schema: str) -> PipelineConfig:
        """Create high-performance pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            parallel=ParallelConfig.create_high_performance(),
            verbose=False
        )
    
    @classmethod
    def create_conservative(cls, schema: str) -> PipelineConfig:
        """Create conservative pipeline configuration."""
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            parallel=ParallelConfig.create_sequential(),
            verbose=True
        )


# ============================================================================
# Step Models
# ============================================================================

@dataclass
class BronzeStep(BaseModel):
    """
    Bronze layer step configuration.
    
    Attributes:
        name: Step name
        rules: Validation rules for the step
        incremental_col: Column used for incremental processing (optional)
                        If None, forces full refresh of downstream Silver tables
    """
    name: str
    rules: ColumnRules
    incremental_col: Optional[str] = None
    
    def validate(self) -> None:
        """Validate bronze step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if self.incremental_col is not None and not isinstance(self.incremental_col, str):
            raise PipelineValidationError("Incremental column must be a string")
    
    @property
    def has_incremental_capability(self) -> bool:
        """Check if this Bronze step supports incremental processing."""
        return self.incremental_col is not None


@dataclass
class SilverStep(BaseModel):
    """
    Silver layer step configuration.
    
    Attributes:
        name: Step name
        source_bronze: Source bronze step name
        transform: Transform function
        rules: Validation rules for the step
        table_name: Target table name
        watermark_col: Watermark column for incremental processing
    """
    name: str
    source_bronze: str
    transform: SilverTransformFunction
    rules: ColumnRules
    table_name: str
    watermark_col: Optional[str] = None
    existing: bool = False
    
    def validate(self) -> None:
        """Validate silver step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError("Source bronze step name must be a non-empty string")
        if not callable(self.transform):
            raise PipelineValidationError("Transform must be a callable function")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise PipelineValidationError("Table name must be a non-empty string")


@dataclass
class GoldStep(BaseModel):
    """
    Gold layer step configuration.
    
    Attributes:
        name: Step name
        transform: Transform function
        rules: Validation rules for the step
        table_name: Target table name
        source_silvers: List of source silver step names, or None to use all available silvers
    """
    name: str
    transform: GoldTransformFunction
    rules: ColumnRules
    table_name: str
    source_silvers: Optional[List[str]] = None
    
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
        if self.source_silvers is not None and not isinstance(self.source_silvers, list):
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
    end_time: Optional[datetime] = None
    duration_secs: Optional[float] = None
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
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def validate(self) -> None:
        """Validate stage statistics."""
        if self.total_rows != self.valid_rows + self.invalid_rows:
            raise PipelineValidationError(
                f"Total rows ({self.total_rows}) must equal valid ({self.valid_rows}) + invalid ({self.invalid_rows})"
            )
        if not 0 <= self.validation_rate <= 100:
            raise PipelineValidationError(f"Validation rate must be between 0 and 100, got {self.validation_rate}")
        if self.duration_secs < 0:
            raise PipelineValidationError(f"Duration must be non-negative, got {self.duration_secs}")
    
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
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
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
            end_time=end_time
        )
    
    @classmethod
    def create_silver_stats(
        cls,
        step: str,
        total_rows: int,
        valid_rows: int,
        duration_secs: float,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
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
            end_time=end_time
        )
    
    @classmethod
    def create_gold_stats(
        cls,
        step: str,
        total_rows: int,
        valid_rows: int,
        duration_secs: float,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
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
            end_time=end_time
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
    error_message: Optional[str] = None
    
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
        validation_rate: float
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
            validation_rate=validation_rate
        )
    
    @classmethod
    def create_failure(
        cls,
        step_name: str,
        phase: PipelinePhase,
        start_time: datetime,
        end_time: datetime,
        error_message: str
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
            error_message=error_message
        )


@dataclass
class PipelineMetrics(BaseModel):
    """
    Overall pipeline execution metrics.
    
    Attributes:
        total_steps: Total number of steps
        successful_steps: Number of successful steps
        failed_steps: Number of failed steps
        total_duration_secs: Total execution duration
        total_rows_processed: Total rows processed
        total_rows_written: Total rows written
        avg_validation_rate: Average validation rate
    """
    total_steps: int
    successful_steps: int
    failed_steps: int
    total_duration_secs: float
    total_rows_processed: int
    total_rows_written: int
    avg_validation_rate: float
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        return (self.successful_steps / self.total_steps * 100) if self.total_steps > 0 else 0.0
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        return 100.0 - self.success_rate

    @classmethod
    def from_step_results(cls, step_results: List[StepResult]) -> PipelineMetrics:
        """Create metrics from step results."""
        total_steps = len(step_results)
        successful_steps = sum(1 for result in step_results if result.success)
        failed_steps = total_steps - successful_steps
        total_duration_secs = sum(result.duration_secs for result in step_results)
        total_rows_processed = sum(result.rows_processed for result in step_results)
        total_rows_written = sum(result.rows_written for result in step_results)
        avg_validation_rate = (
            sum(result.validation_rate for result in step_results) / total_steps
            if total_steps > 0 else 0.0
        )
        
        return cls(
            total_steps=total_steps,
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            total_duration_secs=total_duration_secs,
            total_rows_processed=total_rows_processed,
            total_rows_written=total_rows_written,
            avg_validation_rate=avg_validation_rate
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
    step_results: List[StepResult]
    metrics: PipelineMetrics
    success: bool
    
    @classmethod
    def from_context_and_results(
        cls,
        context: ExecutionContext,
        step_results: List[StepResult]
    ) -> ExecutionResult:
        """Create execution result from context and step results."""
        metrics = PipelineMetrics.from_step_results(step_results)
        success = all(result.success for result in step_results)
        return cls(
            context=context,
            step_results=step_results,
            metrics=metrics,
            success=success
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
    depends_on_silvers: Set[str]
    can_run_parallel: bool
    execution_group: int
    
    def validate(self) -> None:
        """Validate dependency information."""
        if not self.step_name or not isinstance(self.step_name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError("Source bronze step name must be a non-empty string")
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
    dependencies: List[str] = field(default_factory=list)
    estimated_duration: float = 1.0
    priority: int = 0
    can_run_parallel: bool = True
    resource_requirements: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> None:
        """Validate step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if self.step_type not in ["bronze", "silver", "gold"]:
            raise PipelineValidationError("Step type must be 'bronze', 'silver', or 'gold'")
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
    execution_groups: List[List[str]] = field(default_factory=list)
    total_estimated_duration: float = 0.0
    parallel_efficiency: float = 0.0
    critical_path: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    
    def validate(self) -> None:
        """Validate execution plan."""
        if self.total_estimated_duration < 0:
            raise PipelineValidationError("Total estimated duration must be non-negative")
        if not 0 <= self.parallel_efficiency <= 100:
            raise PipelineValidationError("Parallel efficiency must be between 0 and 100")


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
    verbose: bool = True
) -> PipelineConfig:
    """Factory function to create pipeline configuration."""
    thresholds = ValidationThresholds(
        bronze=bronze_threshold,
        silver=silver_threshold,
        gold=gold_threshold
    )
    parallel = ParallelConfig(
        enabled=enable_parallel,
        max_workers=max_workers
    )
    return PipelineConfig(
        schema=schema,
        thresholds=thresholds,
        parallel=parallel,
        verbose=verbose
    )


def create_execution_context(mode: ExecutionMode) -> ExecutionContext:
    """Factory function to create execution context."""
    return ExecutionContext(
        mode=mode,
        start_time=datetime.utcnow()
    )


# ============================================================================
# Validation Utilities
# ============================================================================

def validate_pipeline_config(config: PipelineConfig) -> None:
    """Validate a pipeline configuration."""
    try:
        config.validate()
    except PipelineValidationError as e:
        raise PipelineConfigurationError(f"Invalid pipeline configuration: {e}")


def validate_step_config(step: Union[BronzeStep, SilverStep, GoldStep]) -> None:
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
            gold=data["thresholds"]["gold"]
        ),
        parallel=ParallelConfig(
            enabled=data["parallel"]["enabled"],
            max_workers=data["parallel"]["max_workers"],
            timeout_secs=data["parallel"].get("timeout_secs", 300)
        ),
        verbose=data.get("verbose", True)
    )