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

__version__ = "0.4.5"
__author__ = "Odos Matthews"
__email__ = "odosmattthewsm@gmail.com"
__description__ = "A powerful data pipeline builder for Apache Spark and Databricks"

# Import unified dependency analysis
from .dependencies import DependencyAnalysisResult, DependencyGraph, StepNode
from .dependencies import DependencyAnalyzer as UnifiedDependencyAnalyzer

# Import standardized error handling
from .errors import (
    CircularDependencyError,
    ConfigurationError,
    DataQualityError,
    DependencyError,
    ExecutionError,
    InvalidDependencyError,
    PipelineConfigurationError,
    PipelineError,
    PipelineExecutionError,
    PipelineValidationError,
    ResourceError,
    SparkForgeError,
    StepError,
    StepExecutionError,
    StepValidationError,
)
from .errors import ValidationError as DataValidationError
from .errors.data import ValidationError

# Import unified execution system
from .execution import (
    ExecutionConfig,
    ExecutionMode,
    ExecutionResult,
    ExecutionStats,
    RetryStrategy,
)
from .execution import ExecutionEngine as UnifiedExecutionEngine
from .execution import StepExecutionResult as UnifiedStepExecutionResult
from .log_writer import PIPELINE_LOG_SCHEMA, LogWriter
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
from .step_executor import (
    StepExecutionResult,
    StepExecutor,
    StepStatus,
    StepType,
    StepValidationResult,
)

# Import type system
from .types import (
    AnyDict,
    BronzeTransformFunction,
    ColumnRules,
    ErrorCode,
    ErrorContext,
    ErrorSuggestions,
    ExecutionConfig,
    ExecutionContext,
    ExecutionId,
    ExecutionResult,
    GoldTransformFunction,
    MonitoringConfig,
    NumericDict,
    OptionalDict,
    OptionalList,
    PipelineConfig,
    PipelineId,
    PipelineResult,
    QualityThresholds,
    SchemaName,
    SilverTransformFunction,
    StepName,
    StepResult,
    StepType,
    StringDict,
    TableName,
    TransformFunction,
    ValidationConfig,
    ValidationResult,
    ValidationRules,
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
    "__description__",
]
