"""
Pipeline Builder Base - Shared models, utilities, and interfaces.

This package contains code shared between pipeline_builder (Spark) and
sql_pipeline_builder (SQLAlchemy) implementations.
"""

__version__ = "2.8.0"

# Export main components
from .builder import (
    BasePipelineBuilder,
    StepClassifier,
    create_bronze_step_dict,
    create_gold_step_dict,
    create_silver_step_dict,
)
from .config import (
    create_development_config,
    create_production_config,
    create_test_config,
    validate_pipeline_config,
    validate_thresholds,
)
from .errors import (
    ErrorCategory,
    ErrorContext,
    ErrorSeverity,
    PipelineValidationError,
    SparkForgeError,
    SuggestionGenerator,
    ValidationError,
    build_execution_context,
    build_validation_context,
)
from .logging import PipelineLogger
from .models import (
    BaseModel,
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    PipelineConfig,
    PipelineMetrics,
    PipelinePhase,
    StageStats,
    StepResult,
    ValidationThresholds,
)
from .reporting import (
    DataMetrics,
    ExecutionSummary,
    PerformanceMetrics,
    SummaryReport,
    TransformReport,
    ValidationReport,
    WriteReport,
)
from .runner import (
    BaseRunner,
    determine_execution_mode,
    prepare_sources_for_execution,
    should_run_incremental,
    validate_bronze_sources,
)
from .steps import (
    StepManager,
    classify_step_type,
    extract_step_dependencies,
    get_step_target,
    normalize_step_name,
)
from .validation import (
    PipelineValidator,
    StepValidator,
    check_duplicate_names,
    safe_divide,
    validate_dependency_chain,
    validate_schema_name,
    validate_step_name,
)

__all__ = [
    # Errors
    "SparkForgeError",
    "ValidationError",
    "PipelineValidationError",
    "ErrorSeverity",
    "ErrorCategory",
    # Error Context
    "ErrorContext",
    "SuggestionGenerator",
    "build_execution_context",
    "build_validation_context",
    # Logging
    "PipelineLogger",
    # Models
    "BaseModel",
    "PipelineConfig",
    "ValidationThresholds",
    "PipelineMetrics",
    "StageStats",
    "StepResult",
    "ExecutionContext",
    "ExecutionResult",
    "PipelinePhase",
    "ExecutionMode",
    # Reporting
    "ValidationReport",
    "TransformReport",
    "WriteReport",
    "ExecutionSummary",
    "PerformanceMetrics",
    "DataMetrics",
    "SummaryReport",
    # Validation
    "PipelineValidator",
    "StepValidator",
    "check_duplicate_names",
    "safe_divide",
    "validate_dependency_chain",
    "validate_schema_name",
    "validate_step_name",
    # Builder
    "BasePipelineBuilder",
    "StepClassifier",
    "create_bronze_step_dict",
    "create_gold_step_dict",
    "create_silver_step_dict",
    # Config
    "create_development_config",
    "create_production_config",
    "create_test_config",
    "validate_pipeline_config",
    "validate_thresholds",
    # Runner
    "BaseRunner",
    "determine_execution_mode",
    "prepare_sources_for_execution",
    "should_run_incremental",
    "validate_bronze_sources",
    # Steps
    "StepManager",
    "classify_step_type",
    "extract_step_dependencies",
    "get_step_target",
    "normalize_step_name",
]
