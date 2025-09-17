
"""
SparkForge - A simplified, production-ready data pipeline builder for Apache Spark and Delta Lake.

SparkForge provides a clean, maintainable API for building robust data pipelines with
Bronze → Silver → Gold architecture, featuring:

- Simplified pipeline building API
- Clean execution engine with step-by-step processing
- Enhanced data validation with configurable thresholds
- Delta Lake integration with ACID transactions
- Multi-schema support for enterprise environments
- Step-by-step debugging capabilities
- Comprehensive error handling and logging
- Auto-inference of dependencies

Example:
    from sparkforge import PipelineBuilder
    from pyspark.sql import functions as F

    # Create a pipeline
    builder = PipelineBuilder(spark=spark, schema="my_schema")
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    builder.add_silver_transform(
        name="enriched_events",
        source_bronze="events",
        transform=lambda spark, df, silvers: df.withColumn("processed_at", F.current_timestamp()),
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
    result = pipeline.run_initial_load(bronze_sources={"events": source_df})
"""

__version__ = "0.6.0"
__author__ = "Odos Matthews"
__email__ = "odosmattthewsm@gmail.com"
__description__ = "A simplified, production-ready data pipeline builder for Apache Spark and Delta Lake"

# Import unified dependency analysis
from .dependencies import DependencyAnalysisResult, DependencyGraph, StepNode
from .dependencies import DependencyAnalyzer as UnifiedDependencyAnalyzer

# Import simplified error handling
from .errors import (
    ConfigurationError,
    DataError,
    ExecutionError,
    PerformanceError,
    ResourceError,
    SparkForgeError,
    SystemError,
    ValidationError,
)

# Create aliases for backward compatibility
PipelineError = ExecutionError
DependencyError = ConfigurationError
IngestionError = DataError
StorageError = SystemError
StrategyError = ConfigurationError
TableOperationError = DataError
TimeoutError = SystemError

# Import simplified execution system
from .execution import (
    ExecutionEngine,
    ExecutionMode,
    ExecutionResult,
    StepExecutionResult,
    StepStatus,
    StepType,
    UnifiedExecutionEngine,
    UnifiedStepExecutionResult,
)
from .logging import PipelineLogger, get_logger, create_logger
from .models import (
    BronzeStep,
    ExecutionContext,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    PipelineMetrics,
    PipelinePhase,
    SilverDependencyInfo,
    SilverStep,
    StageStats,
    StepResult,
    ValidationResult,
    ValidationThresholds,
    WriteMode,
)

# Import parallel execution modules
# Import main classes for easy access
from .pipeline import PipelineBuilder, PipelineRunner
from .reporting import create_validation_dict, create_write_dict

# Import security and performance modules
# Step executor functionality moved to execution module

# Import simplified type system
from .types import (
    BronzeTransformFunction,
    ColumnRules,
    ErrorContext,
    ErrorSuggestions,
    ExecutionConfig,
    GenericDict,
    GoldTransformFunction,
    MonitoringConfig,
    NumericDict,
    OptionalDict,
    OptionalList,
    PipelineResult,
    QualityThresholds,
    SchemaName,
    SilverTransformFunction,
    StepName,
    StringDict,
    TableName,
    TransformFunction,
    ValidationConfig,
)

# Make key classes available at package level
__all__ = [
    # Main classes
    "PipelineBuilder",
    "PipelineRunner",
    "PipelineLogger",
    "get_logger",
    "create_logger",
    # Execution system
    "ExecutionEngine",
    "ExecutionMode",
    "ExecutionResult",
    "StepExecutionResult",
    "StepStatus",
    "StepType",
    "UnifiedExecutionEngine",
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
    # Error handling
    "SparkForgeError",
    "ConfigurationError",
    "DataError", 
    "ExecutionError",
    "PerformanceError",
    "ResourceError",
    "SystemError",
    "ValidationError",
    # Backward compatibility aliases
    "PipelineError",
    "DependencyError",
    "IngestionError",
    "StorageError",
    "StrategyError",
    "TableOperationError",
    "TimeoutError",
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
    "GenericDict",
    "NumericDict",
    # Package info
    "__version__",
    "__author__",
    "__email__",
    "__description__",
]
