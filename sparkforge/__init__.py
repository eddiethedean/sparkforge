"""
SparkForge - A powerful data pipeline builder for Apache Spark and Databricks.

SparkForge provides a fluent API for building robust data pipelines with
Bronze → Silver → Gold architecture, featuring:

- Fluent pipeline building API
- Advanced parallel execution with dynamic worker allocation
- Enhanced data validation with security and performance optimization
- Enterprise security features (input validation, SQL injection protection)
- Intelligent caching and performance optimization
- Delta Lake integration
- Performance monitoring and logging
- Error handling and recovery
 
Example:
    from sparkforge import PipelineBuilder, PipelineRunner
    from sparkforge.models import ExecutionMode, ValidationThresholds
    
    # Create a pipeline
    builder = PipelineBuilder(spark=spark, schema="my_schema")
    builder.with_bronze_rules(
        name="events", 
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    builder.add_silver_transform(
        name="enriched_events", 
        source_bronze="events", 
        transform=lambda spark, df, prior_silvers: df.withColumn("processed_at", F.current_timestamp()),
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="enriched_events"
    )
    builder.add_gold_transform(
        name="daily_analytics", 
        transform=lambda spark, silvers: silvers["enriched_events"].groupBy("date").agg(F.count("*").alias("events")),
        rules={"date": [F.col("date").isNotNull()]},
        table_name="daily_analytics",
        source_silvers=["enriched_events"]
    )
    
    # Run the pipeline
    pipeline = builder.to_pipeline()
    result = pipeline.initial_load(bronze_sources={"events": source_df})
"""

__version__ = "0.4.1"
__author__ = "Odos Matthews"
__email__ = "odosmattthewsm@gmail.com"
__description__ = "A powerful data pipeline builder for Apache Spark and Databricks"

# Import main classes for easy access
from .pipeline import PipelineBuilder, PipelineRunner
from .step_executor import StepExecutor, StepExecutionResult, StepValidationResult, StepType, StepStatus

# Import security and performance modules
from .security import SecurityManager, SecurityConfig, get_security_manager
from .performance_cache import PerformanceCache, CacheConfig, get_performance_cache

# Import parallel execution modules
from .parallel_execution import (
    DynamicWorkerPool, DynamicParallelExecutor, ExecutionTask, TaskPriority,
    get_dynamic_executor, create_execution_task
)
from .execution.dynamic_strategy import DynamicExecutionStrategy

# Import unified execution system
from .execution import (
    ExecutionEngine as UnifiedExecutionEngine,
    ExecutionConfig,
    ExecutionMode,
    RetryStrategy,
    ExecutionResult,
    ExecutionStats,
    StepExecutionResult as UnifiedStepExecutionResult
)

# Import unified dependency analysis
from .dependencies import (
    DependencyAnalyzer as UnifiedDependencyAnalyzer,
    DependencyAnalysisResult,
    DependencyGraph,
    StepNode
)

from .models import (
    PipelinePhase,
    ValidationResult,
    WriteMode,
    ValidationThresholds,
    ParallelConfig,
    PipelineConfig,
    BronzeStep,
    SilverStep,
    GoldStep,
    ExecutionContext,
    StageStats,
    StepResult,
    PipelineMetrics,
    SilverDependencyInfo
)
from .log_writer import LogWriter, PIPELINE_LOG_SCHEMA
from .reporting import create_validation_dict, create_write_dict
from .errors.data import ValidationError

# Import standardized error handling
from .errors import (
    SparkForgeError,
    ConfigurationError,
    ValidationError as DataValidationError,
    ExecutionError,
    DataQualityError,
    ResourceError,
    PipelineError,
    PipelineConfigurationError,
    PipelineExecutionError,
    PipelineValidationError,
    StepError,
    StepExecutionError,
    StepValidationError,
    DependencyError,
    CircularDependencyError,
    InvalidDependencyError
)

# Import type system
from .types import (
    StepName, StepType, PipelineId, ExecutionId, TableName, SchemaName,
    TransformFunction, BronzeTransformFunction, SilverTransformFunction, GoldTransformFunction,
    ColumnRules, ValidationRules, QualityThresholds, ExecutionContext,
    StepResult, PipelineResult, ValidationResult, ExecutionResult,
    PipelineConfig, ExecutionConfig, ValidationConfig, MonitoringConfig,
    ErrorCode, ErrorContext, ErrorSuggestions,
    OptionalDict, OptionalList, StringDict, AnyDict, NumericDict
)

# Make key classes available at package level
__all__ = [
    # Main classes
    "PipelineBuilder",
    "PipelineRunner",
    "LogWriter",
    
    # Step execution classes
    "StepExecutor",
    "StepExecutionResult",
    "StepValidationResult",
    "StepType",
    "StepStatus",
    
    # Unified execution system
    "UnifiedExecutionEngine",
    "ExecutionConfig",
    "ExecutionMode",
    "RetryStrategy",
    "ExecutionResult",
    "ExecutionStats",
    "UnifiedStepExecutionResult",
    
    # Unified dependency analysis
    "UnifiedDependencyAnalyzer",
    "DependencyAnalysisResult",
    "DependencyGraph",
    "StepNode",
    
    # Models
    "PipelinePhase", 
    "ValidationResult",
    "WriteMode",
    "ValidationThresholds",
    "ParallelConfig",
    "PipelineConfig",
    "BronzeStep",
    "SilverStep", 
    "GoldStep",
    "ExecutionContext",
    "StageStats",
    "StepResult",
    "PipelineMetrics",
    "SilverDependencyInfo",
    
    # Utilities
    "create_validation_dict",
    "create_write_dict", 
    "ValidationError",
    "PIPELINE_LOG_SCHEMA",
    
    # Error handling
    "SparkForgeError",
    "ConfigurationError",
    "DataValidationError",
    "ExecutionError",
    "DataQualityError",
    "ResourceError",
    "PipelineError",
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "PipelineValidationError",
    "StepError",
    "StepExecutionError",
    "StepValidationError",
    "DependencyError",
    "CircularDependencyError",
    "InvalidDependencyError",
    
    # Type system
    "StepName",
    "StepType",
    "PipelineId",
    "ExecutionId",
    "TableName",
    "SchemaName",
    "TransformFunction",
    "BronzeTransformFunction",
    "SilverTransformFunction",
    "GoldTransformFunction",
    "ColumnRules",
    "ValidationRules",
    "QualityThresholds",
    "ExecutionContext",
    "StepResult",
    "PipelineResult",
    "ValidationResult",
    "ExecutionResult",
    "PipelineConfig",
    "ExecutionConfig",
    "ValidationConfig",
    "MonitoringConfig",
    "ErrorCode",
    "ErrorContext",
    "ErrorSuggestions",
    "OptionalDict",
    "OptionalList",
    "StringDict",
    "AnyDict",
    "NumericDict",
    
    # Package info
    "__version__",
    "__author__",
    "__email__",
    "__description__"
]