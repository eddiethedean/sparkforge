"""
SparkForge - A powerful data pipeline builder for Apache Spark and Databricks.

SparkForge provides a fluent API for building robust data pipelines with
Bronze → Silver → Gold architecture, featuring:

- Fluent pipeline building API
- Concurrent execution of independent steps
- Comprehensive data validation
- Delta Lake integration
- Performance monitoring and logging
- Error handling and recovery

Example:
    from sparkforge import PipelineBuilder, PipelineRunner
    from sparkforge.models import ExecutionMode, ValidationThresholds
    
    # Create a pipeline
    builder = PipelineBuilder(spark=spark, schema="my_schema")
    builder.with_bronze_rules("events", rules={"user_id": [F.col("user_id").isNotNull()]})
    builder.add_silver_transform("enriched_events", source_bronze="events", 
                                transform=lambda df: df.withColumn("processed_at", F.current_timestamp()))
    builder.add_gold_transform("daily_analytics", source_silvers=["enriched_events"],
                              transform=lambda df: df.groupBy("date").agg(F.count("*").alias("events")))
    
    # Run the pipeline
    pipeline = builder.to_pipeline()
    runner = PipelineRunner(pipeline)
    result = runner.initial_load(bronze_sources={"events": source_df})
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"
__description__ = "A powerful data pipeline builder for Apache Spark and Databricks"

# Import main classes for easy access
from .pipeline_builder import PipelineBuilder, PipelineRunner
from .models import (
    ExecutionMode,
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
    ExecutionResult,
    SilverDependencyInfo
)
from .log_writer import LogWriter, PIPELINE_LOG_SCHEMA
from .utils import (
    create_validation_dict,
    create_write_dict,
    ValidationError
)

# Make key classes available at package level
__all__ = [
    # Main classes
    "PipelineBuilder",
    "PipelineRunner",
    "LogWriter",
    
    # Models
    "ExecutionMode",
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
    "ExecutionResult",
    "SilverDependencyInfo",
    
    # Utilities
    "create_validation_dict",
    "create_write_dict", 
    "ValidationError",
    "PIPELINE_LOG_SCHEMA",
    
    # Package info
    "__version__",
    "__author__",
    "__email__",
    "__description__"
]