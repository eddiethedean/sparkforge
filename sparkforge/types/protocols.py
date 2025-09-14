"""
Protocol definitions for SparkForge.

This module defines comprehensive protocols for better type checking
and interface definition across all SparkForge modules.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable, TypeVar
from abc import ABC, abstractmethod

from .aliases import (
    DataFrame, SparkSession, StepName, StepType, PipelineId, ExecutionId,
    StepResult, PipelineResult, ValidationResult, ExecutionResult,
    QualityRate, Duration, RowCount, ErrorMessage
)

# Type variables
T = TypeVar('T')
R = TypeVar('R')

# ============================================================================
# Core Protocols
# ============================================================================

@runtime_checkable
class Validatable(Protocol):
    """Protocol for objects that can be validated."""
    
    def validate(self) -> None:
        """Validate the object and raise ValidationError if invalid."""
        ...

@runtime_checkable
class Serializable(Protocol):
    """Protocol for objects that can be serialized."""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert object to dictionary."""
        ...
    
    def to_json(self) -> str:
        """Convert object to JSON string."""
        ...

@runtime_checkable
class Executable(Protocol):
    """Protocol for objects that can be executed."""
    
    def execute(self) -> Any:
        """Execute the object and return result."""
        ...

@runtime_checkable
class Monitorable(Protocol):
    """Protocol for objects that can be monitored."""
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics."""
        ...
    
    def get_status(self) -> str:
        """Get current status."""
        ...

# ============================================================================
# Pipeline Protocols
# ============================================================================

@runtime_checkable
class PipelineStep(Protocol):
    """Protocol for pipeline steps."""
    
    name: StepName
    step_type: StepType
    
    def execute(self, context: Dict[str, Any]) -> StepResult:
        """Execute the step with given context."""
        ...
    
    def validate(self) -> ValidationResult:
        """Validate the step configuration."""
        ...

@runtime_checkable
class PipelineValidator(Protocol):
    """Protocol for pipeline validators."""
    
    def validate_pipeline(self, pipeline: Any) -> ValidationResult:
        """Validate entire pipeline."""
        ...
    
    def validate_step(self, step: PipelineStep) -> ValidationResult:
        """Validate individual step."""
        ...

@runtime_checkable
class PipelineMonitor(Protocol):
    """Protocol for pipeline monitors."""
    
    def start_monitoring(self, pipeline_id: PipelineId) -> None:
        """Start monitoring pipeline execution."""
        ...
    
    def stop_monitoring(self) -> None:
        """Stop monitoring pipeline execution."""
        ...
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        ...

@runtime_checkable
class PipelineExecutor(Protocol):
    """Protocol for pipeline executors."""
    
    def execute_pipeline(self, pipeline: Any, sources: Dict[str, DataFrame]) -> PipelineResult:
        """Execute entire pipeline."""
        ...
    
    def execute_step(self, step: PipelineStep, context: Dict[str, Any]) -> StepResult:
        """Execute individual step."""
        ...

# ============================================================================
# Data Protocols
# ============================================================================

@runtime_checkable
class DataSource(Protocol):
    """Protocol for data sources."""
    
    def read(self) -> DataFrame:
        """Read data from source."""
        ...
    
    def get_schema(self) -> Any:
        """Get data schema."""
        ...

@runtime_checkable
class DataSink(Protocol):
    """Protocol for data sinks."""
    
    def write(self, data: DataFrame) -> None:
        """Write data to sink."""
        ...
    
    def get_schema(self) -> Any:
        """Get expected schema."""
        ...

@runtime_checkable
class DataTransformer(Protocol):
    """Protocol for data transformers."""
    
    def transform(self, data: DataFrame) -> DataFrame:
        """Transform data."""
        ...
    
    def get_output_schema(self, input_schema: Any) -> Any:
        """Get output schema from input schema."""
        ...

@runtime_checkable
class DataValidator(Protocol):
    """Protocol for data validators."""
    
    def validate(self, data: DataFrame) -> ValidationResult:
        """Validate data quality."""
        ...
    
    def get_quality_rate(self, data: DataFrame) -> QualityRate:
        """Get data quality rate."""
        ...

# ============================================================================
# System Protocols
# ============================================================================

@runtime_checkable
class Configurable(Protocol):
    """Protocol for configurable objects."""
    
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure object with given config."""
        ...
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration."""
        ...

@runtime_checkable
class Loggable(Protocol):
    """Protocol for loggable objects."""
    
    def log(self, message: str, level: str = "INFO") -> None:
        """Log message with given level."""
        ...
    
    def get_logs(self) -> List[str]:
        """Get all log messages."""
        ...

@runtime_checkable
class Cacheable(Protocol):
    """Protocol for cacheable objects."""
    
    def cache(self, key: str, value: Any) -> None:
        """Cache value with given key."""
        ...
    
    def get_cached(self, key: str) -> Optional[Any]:
        """Get cached value by key."""
        ...
    
    def clear_cache(self) -> None:
        """Clear all cached values."""
        ...

@runtime_checkable
class Retryable(Protocol):
    """Protocol for retryable objects."""
    
    def retry(self, func: callable, *args, **kwargs) -> Any:
        """Retry function execution."""
        ...
    
    def should_retry(self, error: Exception) -> bool:
        """Check if error should be retried."""
        ...

# ============================================================================
# Execution Protocols
# ============================================================================

@runtime_checkable
class ExecutionStrategy(Protocol):
    """Protocol for execution strategies."""
    
    def execute_steps(self, steps: Dict[str, Any]) -> ExecutionResult:
        """Execute steps using this strategy."""
        ...
    
    def can_parallelize(self, steps: List[str]) -> bool:
        """Check if steps can be parallelized."""
        ...

@runtime_checkable
class DependencyAnalyzer(Protocol):
    """Protocol for dependency analyzers."""
    
    def analyze_dependencies(self, steps: Dict[str, Any]) -> Dict[str, List[str]]:
        """Analyze step dependencies."""
        ...
    
    def detect_cycles(self, dependencies: Dict[str, List[str]]) -> List[List[str]]:
        """Detect circular dependencies."""
        ...

@runtime_checkable
class ErrorHandler(Protocol):
    """Protocol for error handlers."""
    
    def handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Handle error with given context."""
        ...
    
    def should_retry(self, error: Exception) -> bool:
        """Check if error should be retried."""
        ...

# ============================================================================
# Monitoring Protocols
# ============================================================================

@runtime_checkable
class MetricsCollector(Protocol):
    """Protocol for metrics collectors."""
    
    def collect_metrics(self) -> Dict[str, Any]:
        """Collect current metrics."""
        ...
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        ...

@runtime_checkable
class PerformanceMonitor(Protocol):
    """Protocol for performance monitors."""
    
    def start_timing(self, operation: str) -> None:
        """Start timing operation."""
        ...
    
    def stop_timing(self, operation: str) -> Duration:
        """Stop timing operation and return duration."""
        ...
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary."""
        ...

# ============================================================================
# Validation Protocols
# ============================================================================

@runtime_checkable
class QualityValidator(Protocol):
    """Protocol for quality validators."""
    
    def validate_quality(self, data: DataFrame, rules: Dict[str, Any]) -> QualityRate:
        """Validate data quality against rules."""
        ...
    
    def get_quality_report(self, data: DataFrame) -> Dict[str, Any]:
        """Get detailed quality report."""
        ...

@runtime_checkable
class SchemaValidator(Protocol):
    """Protocol for schema validators."""
    
    def validate_schema(self, data: DataFrame, expected_schema: Any) -> bool:
        """Validate data schema."""
        ...
    
    def get_schema_differences(self, actual: Any, expected: Any) -> List[str]:
        """Get schema differences."""
        ...

# ============================================================================
# Utility Protocols
# ============================================================================

@runtime_checkable
class Factory(Protocol[T]):
    """Protocol for factory objects."""
    
    def create(self, **kwargs) -> T:
        """Create object with given parameters."""
        ...

@runtime_checkable
class Builder(Protocol[T]):
    """Protocol for builder objects."""
    
    def build(self) -> T:
        """Build and return object."""
        ...
    
    def reset(self) -> None:
        """Reset builder state."""
        ...

@runtime_checkable
class Iterator(Protocol[T]):
    """Protocol for iterator objects."""
    
    def __iter__(self) -> Iterator[T]:
        """Return iterator."""
        ...
    
    def __next__(self) -> T:
        """Return next item."""
        ...

# ============================================================================
# Type Checking Utilities
# ============================================================================

def implements_protocol(obj: Any, protocol: type) -> bool:
    """Check if object implements protocol."""
    return isinstance(obj, protocol)

def get_protocol_methods(protocol: type) -> List[str]:
    """Get method names from protocol."""
    return [name for name, method in protocol.__dict__.items() 
            if callable(method) and not name.startswith('_')]

def validate_protocol_implementation(obj: Any, protocol: type) -> List[str]:
    """Validate object implements protocol and return missing methods."""
    missing = []
    for name, method in protocol.__dict__.items():
        if callable(method) and not name.startswith('_'):
            if not hasattr(obj, name):
                missing.append(name)
    return missing
