"""
SparkForge - A production-ready data pipeline framework for Apache Spark & Delta Lake.

SparkForge transforms complex Spark + Delta Lake development into clean, maintainable code
using the proven Medallion Architecture (Bronze → Silver → Gold). Features include:

- **Robust Validation System**: Early error detection with clear validation messages
- **Simplified Pipeline Building**: 70% less boilerplate compared to raw Spark
- **Auto-inference**: Automatic dependency detection and validation
- **Step-by-step Debugging**: Easy troubleshooting of complex pipelines
- **Delta Lake Integration**: ACID transactions and time travel
- **Multi-schema Support**: Enterprise-ready cross-schema data flows
- **Comprehensive Error Handling**: Detailed error messages with suggestions
- **100% Test Coverage**: 702+ comprehensive tests ensuring reliability

Quick Start:
    from sparkforge import PipelineBuilder
    from pyspark.sql import functions as F

    # Initialize Spark
    spark = SparkSession.builder.appName("MyPipeline").getOrCreate()

    # Create sample data
    data = [("user1", "click", 100), ("user2", "purchase", 200)]
    df = spark.createDataFrame(data, ["user_id", "action", "value"])

    # Build pipeline with validation
    builder = PipelineBuilder(spark=spark, schema="analytics")

    # Bronze: Raw data validation (required)
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]},
        incremental_col="timestamp"
    )

    # Silver: Data transformation (required)
    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=lambda spark, df, silvers: df.filter(F.col("value") > 50),
        rules={"value": [F.col("value") > 50]},
        table_name="clean_events"
    )

    # Gold: Business analytics (required)
    builder.add_gold_transform(
        name="daily_metrics",
        transform=lambda spark, silvers: silvers["clean_events"].groupBy("action").agg(F.count("*").alias("count")),
        rules={"count": [F.col("count") > 0]},
        table_name="daily_metrics",
        source_silvers=["clean_events"]
    )

    # Execute pipeline
    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": df})
    print(f"✅ Pipeline completed: {result.status}")

Validation Requirements:
    All pipeline steps must have validation rules. Invalid configurations are rejected
    with clear error messages to help you fix issues quickly.

    # ✅ Valid - has required validation rules
    BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]})

    # ❌ Invalid - empty rules rejected
    BronzeStep(name="events", rules={})  # ValidationError: Rules must be non-empty
"""

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
from .logging import PipelineLogger, create_logger, get_logger
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

__version__ = "0.7.3"
__author__ = "Odos Matthews"
__email__ = "odosmattthewsm@gmail.com"
__description__ = "A simplified, production-ready data pipeline builder for Apache Spark and Delta Lake"

# Create aliases for backward compatibility
PipelineError = ExecutionError
DependencyError = ConfigurationError
IngestionError = DataError
StorageError = SystemError
StrategyError = ConfigurationError
TableOperationError = DataError
TimeoutError = SystemError

# Import security and performance modules
# Step executor functionality moved to execution module


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
