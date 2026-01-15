# mypy: ignore-errors
"""Production-ready execution system for pipeline execution.

This module provides a robust execution engine that handles pipeline execution
with comprehensive error handling, step-by-step processing, and detailed reporting.
The engine uses a service-oriented architecture with dedicated step executors,
validation services, and storage services for clean separation of concerns.

Key Features:
    - Step-by-Step Execution: Process pipeline steps individually with detailed tracking
    - Comprehensive Error Handling: Detailed error messages with context and suggestions
    - Multiple Execution Modes: Initial load, incremental, full refresh, and validation-only
    - Dependency-Aware Execution: Automatically analyzes step dependencies and executes
      in correct order
    - Detailed Reporting: Comprehensive execution reports with metrics and timing
    - Validation Integration: Built-in validation with configurable thresholds
    - Service-Oriented Architecture: Clean separation with step executors, validators,
      and storage services

Execution Modes:
    INITIAL: First-time pipeline execution with full data processing. Allows schema
        changes and creates tables from scratch.
    INCREMENTAL: Process only new data based on watermark columns. Requires exact
        schema matching with existing tables.
    FULL_REFRESH: Reprocess all data, overwriting existing results. Requires exact
        schema matching.
    VALIDATION_ONLY: Validate data without writing results. Useful for testing
        validation rules.

Dependency Analysis:
    The engine automatically analyzes step dependencies and executes steps
    sequentially in the correct order based on their dependencies. Steps are
    grouped by dependencies, and groups are executed sequentially to respect
    dependency constraints.

Service Architecture:
    The execution engine delegates to specialized services:
    - Step Executors: BronzeStepExecutor, SilverStepExecutor, GoldStepExecutor
        handle step-specific execution logic
    - ExecutionValidator: Validates data according to step rules
    - TableService: Manages table operations and schema management
    - WriteService: Handles all write operations to Delta Lake

Example:
    Basic usage with a single step:

    >>> from pipeline_builder.execution import ExecutionEngine
    >>> from pipeline_builder_base.models import ExecutionMode, PipelineConfig
    >>> from pipeline_builder.models import BronzeStep
    >>> from pipeline_builder.functions import get_default_functions
    >>> F = get_default_functions()
    >>>
    >>> # Create execution engine
    >>> config = PipelineConfig.create_default(schema="my_schema")
    >>> engine = ExecutionEngine(spark, config)
    >>>
    >>> # Execute a single step
    >>> result = engine.execute_step(
    ...     step=BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]}),
    ...     context={"events": source_df},
    ...     mode=ExecutionMode.INITIAL
    ... )
    >>> print(f"Step completed: {result.status}, rows: {result.rows_processed}")

    Full pipeline execution:

    >>> result = engine.execute_pipeline(
    ...     steps=[bronze_step, silver_step, gold_step],
    ...     mode=ExecutionMode.INITIAL,
    ...     context={"events": source_df}
    ... )
    >>> print(f"Pipeline completed: {result.status}")
    >>> print(f"Execution groups: {result.execution_groups_count}")

Note:
    This module depends on:
    - compat: Spark compatibility layer
    - dependencies: Dependency analysis
    - errors: Error handling
    - functions: PySpark function protocols
    - logging: Pipeline logging
    - models.pipeline: Pipeline configuration models
    - models.steps: Step models
    - table_operations: Table utility functions
    - validation.data_validation: Data validation logic
"""

from __future__ import annotations

import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union, cast

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    psutil = None  # type: ignore[assignment, unused-ignore]

from pipeline_builder_base.dependencies import DependencyAnalyzer
from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionMode,
    PipelineConfig,
)

from .compat import DataFrame, F, SparkSession
from .functions import FunctionsProtocol
from .models import BronzeStep, GoldStep, SilverStep
from .step_executors import (
    BronzeStepExecutor,
    GoldStepExecutor,
    SilverStepExecutor,
)
from .storage import TableService, WriteService
from .table_operations import fqn, table_exists, table_schema_is_empty
from .validation.execution_validator import ExecutionValidator

# Handle optional Delta Lake dependency
try:
    from delta.tables import DeltaTable

    HAS_DELTA = True
except (ImportError, AttributeError, RuntimeError):
    DeltaTable = None  # type: ignore[misc, assignment]
    HAS_DELTA = False

# Cache for Delta Lake availability per Spark session
_delta_availability_cache_execution: Dict[str, bool] = {}


def _is_delta_lake_available_execution(spark: SparkSession) -> bool:  # type: ignore[valid-type]
    """Check if Delta Lake is available and working in the Spark session.

    This function checks Spark configuration and optionally tests Delta
    functionality by attempting to write a test DataFrame. Results are cached
    per Spark session for performance.

    Args:
        spark: SparkSession instance to test for Delta Lake availability.

    Returns:
        True if Delta Lake is available and working, False otherwise.

    Note:
        The function checks:
        1. If delta package is installed
        2. Spark configuration for Delta extensions and catalog
        3. Actual Delta write capability via test write operation

        Results are cached per Spark session using the session's JVM ID.
    """
    # Use Spark session's underlying SparkContext ID as cache key
    try:
        spark_id = (
            str(id(spark._jsparkSession))
            if hasattr(spark, "_jsparkSession")
            else str(id(spark))
        )
    except Exception:
        spark_id = str(id(spark))

    # Check cache first
    if spark_id in _delta_availability_cache_execution:
        return _delta_availability_cache_execution[spark_id]

    # If delta package is not installed, can't be available
    if not HAS_DELTA:
        _delta_availability_cache_execution[spark_id] = False
        return False

    # Check Spark configuration first (fast check)
    try:
        extensions = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")  # type: ignore[attr-defined]

        # If both extensions and catalog are configured for Delta, assume it works
        if (
            extensions
            and catalog
            and "DeltaSparkSessionExtension" in extensions
            and "DeltaCatalog" in catalog
        ):
            _delta_availability_cache_execution[spark_id] = True
            return True
    except Exception:
        pass

    # If only extensions are configured, do a lightweight test
    try:
        extensions = spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
        if extensions and "DeltaSparkSessionExtension" in extensions:
            # Try a simple test - create a minimal DataFrame and try to write it
            test_df = spark.createDataFrame([(1, "test")], ["id", "name"])
            # Use a unique temp directory to avoid conflicts
            with tempfile.TemporaryDirectory() as temp_dir:
                test_path = os.path.join(temp_dir, "delta_test")
                try:
                    test_df.write.format("delta").mode("overwrite").save(test_path)
                    _delta_availability_cache_execution[spark_id] = True
                    return True
                except Exception:
                    # Delta format failed - not available
                    pass
    except Exception:
        pass

    # Delta is not available in this Spark session
    _delta_availability_cache_execution[spark_id] = False
    return False


# Removed _check_batch_mode_with_delta() - Delta Lake does support batch operations
# in real Spark mode. The previous restriction was incorrect.


def _create_dataframe_writer(
    df: DataFrame,
    spark: SparkSession,  # type: ignore[valid-type]
    mode: str,
    table_name: Optional[str] = None,
    **options: Any,
) -> Any:
    """Create a DataFrameWriter using the standardized Delta write pattern.

    Creates a DataFrameWriter configured for Delta Lake format with appropriate
    options based on the write mode. For overwrite mode, includes
    overwriteSchema option to allow schema evolution.

    Args:
        df: DataFrame to write.
        spark: SparkSession instance (used for Delta overwrite preparation).
        mode: Write mode ("overwrite", "append", etc.).
        table_name: Optional fully qualified table name for preparing Delta
            overwrite operations.
        **options: Additional write options to apply to the writer.

    Returns:
        Configured DataFrameWriter instance ready for saveAsTable().

    Note:
        Always uses Delta format. Failures will propagate if Delta is not
        available. For overwrite mode, uses format("delta").mode("overwrite")
        .option("overwriteSchema", "true").
    """
    # Use standardized overwrite pattern: overwrite + overwriteSchema
    if mode == "overwrite":
        writer = (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        )
    else:
        # Append or other modes - always use Delta
        writer = df.write.format("delta").mode(mode)

    for key, value in options.items():
        writer = writer.option(key, value)

    return writer


def _get_existing_schema_safe(spark: Any, table_name: str) -> Optional[Any]:
    """Safely get the schema of an existing table.

    Attempts multiple methods to retrieve the table schema, handling catalog
    sync issues where Spark may report empty schemas. Tries progressively
    more expensive methods until schema is found or all methods are exhausted.

    Args:
        spark: SparkSession instance.
        table_name: Fully qualified table name (schema.table).

    Returns:
        StructType schema if table exists and schema is readable (may be
        empty struct<>), None if table doesn't exist or schema can't be read.

    Note:
        Tries methods in order:
        1. Direct schema from spark.table()
        2. If empty schema (catalog sync issue), try DESCRIBE TABLE
        3. If still empty, try reading a sample row to infer schema

        Returns empty struct<> if table exists but schema cannot be determined,
        allowing callers to handle catalog sync issues appropriately.
    """
    try:
        table_df = spark.table(table_name)  # type: ignore[attr-defined]
        schema = table_df.schema  # type: ignore[attr-defined]

        # If schema is empty (catalog sync issue), try DESCRIBE TABLE as fallback
        if not schema.fields or len(schema.fields) == 0:
            try:
                # Try DESCRIBE TABLE to get schema information
                describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")  # type: ignore[attr-defined]
                describe_rows = describe_df.collect()  # type: ignore[attr-defined]

                # If DESCRIBE returns rows with column info, try to read schema from data
                if describe_rows and len(describe_rows) > 0:
                    # Try reading a sample row to infer schema
                    try:
                        sample_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 1")  # type: ignore[attr-defined]
                        inferred_schema = sample_df.schema  # type: ignore[attr-defined]
                        if inferred_schema.fields and len(inferred_schema.fields) > 0:
                            return inferred_schema
                    except Exception:
                        pass
            except Exception:
                pass

        # Return schema even if empty (struct<>) - caller will handle empty schemas specially
        return schema
    except Exception:
        pass
    return None


def _schemas_match(existing_schema: Any, output_schema: Any) -> tuple[bool, list[str]]:
    """Compare two schemas and determine if they match exactly.

    Compares field names, types, and nullability between existing and output
    schemas. Returns detailed information about any mismatches found.

    Args:
        existing_schema: StructType schema of the existing table.
        output_schema: StructType schema of the output DataFrame.

    Returns:
        Tuple of (matches: bool, differences: list[str]) where:
        - matches: True if schemas match exactly, False otherwise
        - differences: List of human-readable descriptions of mismatches

    Note:
        Checks for:
        - Missing columns in output
        - New columns in output
        - Type mismatches in common columns
        - Nullable changes (informational only - doesn't fail validation)

        Column order differences are noted but don't affect the match result.
    """
    differences = []

    # Extract field dictionaries
    existing_fields = (
        {f.name: f for f in existing_schema.fields} if existing_schema.fields else {}
    )
    output_fields = (
        {f.name: f for f in output_schema.fields} if output_schema.fields else {}
    )

    existing_columns = set(existing_fields.keys())
    output_columns = set(output_fields.keys())

    # Check for missing columns in output
    missing_in_output = existing_columns - output_columns
    if missing_in_output:
        differences.append(f"Missing columns in output: {sorted(missing_in_output)}")

    # Check for new columns in output
    new_in_output = output_columns - existing_columns
    if new_in_output:
        differences.append(
            f"New columns in output (not in existing table): {sorted(new_in_output)}"
        )

    # Check for type mismatches and nullable changes in common columns
    common_columns = existing_columns & output_columns
    type_mismatches = []
    nullable_changes = []
    for col in common_columns:
        existing_field = existing_fields[col]
        output_field = output_fields[col]

        # Check type mismatch
        if existing_field.dataType != output_field.dataType:
            type_mismatches.append(
                f"{col}: existing={existing_field.dataType}, "
                f"output={output_field.dataType}"
            )

        # Check nullable changes (nullable -> non-nullable is stricter, non-nullable -> nullable is more lenient)
        existing_nullable = getattr(existing_field, "nullable", True)
        output_nullable = getattr(output_field, "nullable", True)
        if existing_nullable != output_nullable:
            if not existing_nullable and output_nullable:
                # Existing is non-nullable, output is nullable - this is usually OK (more lenient)
                nullable_changes.append(
                    f"{col}: nullable changed from False to True (more lenient - usually OK)"
                )
            else:
                # Existing is nullable, output is non-nullable - this is stricter and may cause issues
                nullable_changes.append(
                    f"{col}: nullable changed from True to False (stricter - may cause issues if data has nulls)"
                )

    if type_mismatches:
        differences.append(f"Type mismatches: {', '.join(type_mismatches)}")

    if nullable_changes:
        # Note nullable changes but don't fail validation for them (Delta Lake handles this)
        differences.append(
            f"Nullable changes (informational): {', '.join(nullable_changes)}"
        )

    # Check for column order differences (informational only - order doesn't affect functionality)
    existing_order = list(existing_fields.keys())
    output_order = list(output_fields.keys())
    if (
        existing_order != output_order
        and common_columns == existing_columns == output_columns
    ):
        # All columns match, just order is different
        differences.append(
            f"Column order differs (informational - order doesn't affect functionality): "
            f"existing={existing_order}, output={output_order}"
        )

    return len(
        [d for d in differences if "informational" not in d.lower()]
    ) == 0, differences


def _recover_table_schema(spark: Any, table_name: str) -> Optional[Any]:
    """Attempt to recover table schema when catalog shows empty schema.

    Attempts to recover schema information when Spark catalog reports an
    empty schema (struct<>), which can occur due to catalog sync issues
    with Delta Lake tables.

    Args:
        spark: SparkSession instance.
        table_name: Fully qualified table name (schema.table).

    Returns:
        Recovered StructType schema if recovery succeeds, None if all
        recovery methods fail.

    Note:
        Tries methods in order:
        1. REFRESH TABLE and re-read schema
        2. DESCRIBE TABLE to get column information
        3. Read sample data to force schema resolution
        4. Force schema evaluation by reading a row

        This is a best-effort recovery - may return None if table doesn't
        exist or all recovery methods fail.
    """
    try:
        # Method 1: Try DESCRIBE TABLE and REFRESH to force catalog sync
        try:
            spark.sql(f"REFRESH TABLE {table_name}")  # type: ignore[attr-defined]
        except Exception:
            pass  # Ignore refresh errors

        # Re-read table after refresh
        table_df = spark.table(table_name)  # type: ignore[attr-defined]
        df_schema = table_df.schema  # type: ignore[attr-defined]

        # Check if schema now has fields
        if (
            hasattr(df_schema, "fields")
            and df_schema.fields
            and len(df_schema.fields) > 0
        ):
            return df_schema

        # Method 2: Even if schema.fields is empty, check if DataFrame has columns
        # This indicates the table exists and has data, just catalog is out of sync
        if table_df.columns and len(table_df.columns) > 0:
            # DataFrame has columns - try DESCRIBE TABLE to get schema information
            try:
                describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")  # type: ignore[attr-defined]
                describe_rows = describe_df.collect()  # type: ignore[attr-defined]

                # If DESCRIBE returns column information, try to re-read the table
                # Sometimes DESCRIBE helps Spark refresh its understanding of the schema
                if describe_rows and len(describe_rows) > 0:
                    # Try reading the table again after DESCRIBE
                    table_df_retry = spark.table(table_name)  # type: ignore[attr-defined]
                    df_schema_retry = table_df_retry.schema  # type: ignore[attr-defined]
                    if (
                        hasattr(df_schema_retry, "fields")
                        and df_schema_retry.fields
                        and len(df_schema_retry.fields) > 0
                    ):
                        return df_schema_retry
            except Exception:
                # DESCRIBE or re-read failed
                pass

            # If DESCRIBE didn't help, try to force schema resolution by reading data
            try:
                # Attempt to read a row to force Spark to resolve schema
                sample = table_df.limit(1)
                sample.collect()  # Force execution

                # Re-read schema after forcing execution
                df_schema_retry = table_df.schema  # type: ignore[attr-defined]
                if (
                    hasattr(df_schema_retry, "fields")
                    and df_schema_retry.fields
                    and len(df_schema_retry.fields) > 0
                ):
                    return df_schema_retry
            except Exception:
                # Schema recovery via data read failed
                pass
    except Exception:
        # Schema recovery failed
        pass

    return None


class StepStatus(Enum):
    """Execution status of a pipeline step.

    Attributes:
        PENDING: Step is queued but not yet started.
        RUNNING: Step is currently executing.
        COMPLETED: Step completed successfully.
        FAILED: Step execution failed with an error.
        SKIPPED: Step was skipped (e.g., due to dependencies).
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepType(Enum):
    """Types of pipeline steps in the Medallion architecture.

    Attributes:
        BRONZE: Raw data ingestion and validation step.
        SILVER: Cleaned and enriched data step.
        GOLD: Business analytics and aggregation step.
    """

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepExecutionResult:
    """Result of a single pipeline step execution.

    Contains comprehensive information about step execution including timing,
    validation metrics, resource usage, and output details.

    Attributes:
        step_name: Name of the executed step.
        step_type: Type of step (BRONZE, SILVER, or GOLD).
        status: Execution status (PENDING, RUNNING, COMPLETED, FAILED, SKIPPED).
        start_time: Timestamp when step execution started.
        end_time: Timestamp when step execution completed (None if still running).
        duration: Execution duration in seconds (calculated from start/end times).
        error: Error message if step failed (None if successful).
        rows_processed: Number of rows processed by the step.
        output_table: Fully qualified name of output table (None for Bronze steps).
        write_mode: Write mode used ("overwrite", "append", or None).
        validation_rate: Percentage of rows that passed validation (0-100).
        rows_written: Number of rows written to output table (None for Bronze steps).
        input_rows: Number of input rows (same as rows_processed for most steps).
        memory_usage_mb: Peak memory usage in megabytes (if psutil available).
        cpu_usage_percent: CPU usage percentage (if psutil available).

    Note:
        Duration is automatically calculated in __post_init__ if both start_time
        and end_time are provided. Bronze steps don't write to tables, so
        output_table and rows_written will be None for them.
    """

    step_name: str
    step_type: StepType
    status: StepStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    error: Optional[str] = None
    rows_processed: Optional[int] = None
    output_table: Optional[str] = None
    write_mode: Optional[str] = None
    validation_rate: float = 100.0
    rows_written: Optional[int] = None
    input_rows: Optional[int] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None

    def __post_init__(self) -> None:
        """Calculate duration if both start and end times are available."""
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


@dataclass
class ExecutionResult:
    """Result of complete pipeline execution.

    Contains comprehensive information about pipeline execution including
    all step results, timing, dependency analysis results, and overall status.

    Attributes:
        execution_id: Unique identifier for this execution run.
        mode: Execution mode used (INITIAL, INCREMENTAL, FULL_REFRESH, VALIDATION_ONLY).
        start_time: Timestamp when pipeline execution started.
        end_time: Timestamp when pipeline execution completed (None if still running).
        duration: Total execution duration in seconds (calculated from start/end times).
        status: Overall pipeline status ("running", "completed", "failed").
        steps: List of StepExecutionResult for each step in the pipeline.
        error: Error message if pipeline failed (None if successful).
        execution_groups_count: Number of dependency groups executed.
        max_group_size: Maximum number of steps in any execution group.

    Note:
        Duration is automatically calculated in __post_init__ if both start_time
        and end_time are provided. Steps list is initialized to empty list if None.
        The status field tracks overall pipeline status based on individual step results.
    """

    execution_id: str
    mode: ExecutionMode
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    status: str = "running"
    steps: Optional[list[StepExecutionResult]] = None
    error: Optional[str] = None
    execution_groups_count: int = 0
    max_group_size: int = 0

    def __post_init__(self) -> None:
        """Initialize steps list and calculate duration if times are available."""
        if self.steps is None:
            self.steps = []
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


class ExecutionEngine:
    """Execution engine for pipeline execution with service-oriented architecture.

    This engine orchestrates pipeline execution using specialized services for
    clean separation of concerns. It handles both individual step execution and
    full pipeline execution with dependency-aware sequential processing.

    The engine uses a service-oriented architecture:
    - Step Executors: BronzeStepExecutor, SilverStepExecutor, GoldStepExecutor
      handle step-specific execution logic
    - ExecutionValidator: Validates data according to step rules
    - TableService: Manages table operations and schema management
    - WriteService: Handles all write operations to Delta Lake

    Key Features:
        - Dependency-aware execution: Automatically analyzes and respects step
          dependencies
        - Sequential execution: Steps execute in correct order within dependency groups
        - Comprehensive validation: Built-in validation with configurable thresholds
        - Error handling: Detailed error messages with context and suggestions
        - Resource tracking: Monitors memory and CPU usage (if psutil available)

    Example:
        >>> from pipeline_builder.execution import ExecutionEngine
        >>> from pipeline_builder_base.models import PipelineConfig, ExecutionMode
        >>>
        >>> config = PipelineConfig.create_default(schema="analytics")
        >>> engine = ExecutionEngine(spark, config)
        >>>
        >>> # Execute a single step
        >>> result = engine.execute_step(
        ...     step=bronze_step,
        ...     context={"events": source_df},
        ...     mode=ExecutionMode.INITIAL
        ... )
        >>>
        >>> # Execute full pipeline
        >>> result = engine.execute_pipeline(
        ...     steps=[bronze, silver, gold],
        ...     mode=ExecutionMode.INITIAL,
        ...     context={"events": source_df}
        ... )
    """

    def __init__(
        self,
        spark: SparkSession,  # type: ignore[valid-type]
        config: PipelineConfig,
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
    ):
        """Initialize the execution engine.

        Creates an ExecutionEngine instance with all required services. The
        engine initializes step executors, validation service, and storage
        services for handling pipeline execution.

        Args:
            spark: Active SparkSession instance for DataFrame operations.
            config: PipelineConfig containing pipeline configuration including
                schema, validation thresholds, and other settings.
            logger: Optional PipelineLogger instance. If None, creates a default
                logger.
            functions: Optional FunctionsProtocol instance for PySpark operations.
                If None, uses get_default_functions() to get appropriate functions
                based on engine configuration.

        Note:
            All services are initialized during construction. The engine is ready
            to execute steps immediately after initialization.
        """
        self.spark: SparkSession = spark  # type: ignore[valid-type]
        self.config = config
        if logger is None:
            self.logger = PipelineLogger()
        else:
            self.logger = logger

        # Store functions for validation
        if functions is None:
            from .functions import get_default_functions

            self.functions = get_default_functions()
        else:
            self.functions = functions

        # Initialize step executors
        self.bronze_executor = BronzeStepExecutor(spark, self.logger, self.functions)
        self.silver_executor = SilverStepExecutor(spark, self.logger, self.functions)
        self.gold_executor = GoldStepExecutor(spark, self.logger, self.functions)

        # Initialize validation service
        self.validator = ExecutionValidator(self.logger, self.functions)

        # Initialize storage services
        self.table_service = TableService(spark, self.logger)
        self.write_service = WriteService(spark, self.table_service, self.logger)

    def _ensure_schema_exists(self, schema: str) -> None:
        """Ensure a schema exists, creating it if necessary.

        Attempts to create the specified schema if it doesn't already exist.
        Uses SQL CREATE SCHEMA IF NOT EXISTS for idempotent creation.

        Args:
            schema: Schema name to create or verify.

        Raises:
            ExecutionError: If schema creation fails after all attempts.

        Note:
            First checks if schema exists in catalog, then attempts creation
            using SQL. If creation fails, raises ExecutionError with context.
        """
        # Check if schema already exists
        try:
            databases = [db.name for db in self.spark.catalog.listDatabases()]
            if schema in databases:
                return  # Schema already exists, nothing to do
        except Exception:
            pass  # If we can't check, try to create anyway

        try:
            # Use SQL CREATE SCHEMA (works for both PySpark and mock-spark)
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")  # type: ignore[attr-defined]
            # Verify it was created
            databases = [db.name for db in self.spark.catalog.listDatabases()]  # type: ignore[attr-defined]
            if schema not in databases:
                raise ExecutionError(
                    f"Schema '{schema}' creation via SQL failed - schema not in catalog. "
                    f"Available databases: {databases}"
                )
        except ExecutionError:
            raise  # Re-raise ExecutionError
        except Exception as e:
            # Wrap other exceptions
            raise ExecutionError(f"Failed to create schema '{schema}': {str(e)}") from e

    @staticmethod
    def _collect_resource_metrics() -> tuple[Optional[float], Optional[float]]:
        """Collect current memory and CPU usage metrics.

        Uses psutil to collect resource usage metrics for the current process.
        Returns None values if psutil is not available.

        Returns:
            Tuple of (memory_usage_mb, cpu_usage_percent) where:
            - memory_usage_mb: Memory usage in megabytes (RSS)
            - cpu_usage_percent: CPU usage percentage

            Returns (None, None) if psutil is unavailable or metrics collection fails.

        Note:
            Memory is measured as RSS (Resident Set Size) in megabytes.
            CPU usage is measured over a 0.1 second interval.
        """
        if not HAS_PSUTIL:
            return None, None

        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert bytes to MB
            cpu_percent = process.cpu_percent(interval=0.1)
            return memory_mb, cpu_percent
        except Exception:
            # If metrics collection fails, return None values
            return None, None

    def execute_step(
        self,
        step: Union[BronzeStep, SilverStep, GoldStep],
        context: Dict[str, DataFrame],  # type: ignore[valid-type]
        mode: ExecutionMode = ExecutionMode.INITIAL,
    ) -> StepExecutionResult:
        """Execute a single pipeline step.

        Executes a single step (Bronze, Silver, or Gold) with validation,
        transformation, and optional table writing. Uses specialized step
        executors for step-specific logic.

        Args:
            step: The step to execute (BronzeStep, SilverStep, or GoldStep).
            context: Dictionary mapping step names to DataFrames. Must contain
                required source data for the step (e.g., bronze data for Silver
                steps, silver data for Gold steps).
            mode: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH,
                VALIDATION_ONLY). Defaults to INITIAL.

        Returns:
            StepExecutionResult containing execution details including:
            - Status (COMPLETED, FAILED)
            - Timing information
            - Row counts (processed, written)
            - Validation metrics
            - Resource usage (if available)

        Raises:
            ExecutionError: If step execution fails for any reason.

        Example:
            >>> result = engine.execute_step(
            ...     step=silver_step,
            ...     context={"events": bronze_df},
            ...     mode=ExecutionMode.INITIAL
            ... )
            >>> print(f"Status: {result.status}, Rows: {result.rows_processed}")

        Note:
            - Bronze steps only validate data, they don't write to tables
            - Silver and Gold steps write to Delta Lake tables
            - Validation is applied according to step rules
            - Schema validation is performed for INCREMENTAL and FULL_REFRESH modes
        """
        start_time = datetime.now()
        # Collect initial resource metrics
        start_memory, start_cpu = self._collect_resource_metrics()

        # Determine step type using step_type property (avoids isinstance issues in Python 3.8)
        # Initialize step_type to None to ensure it's always defined for exception handling
        step_type = None
        try:
            phase = step.step_type
            if phase.value == "bronze":
                step_type = StepType.BRONZE
            elif phase.value == "silver":
                step_type = StepType.SILVER
            elif phase.value == "gold":
                step_type = StepType.GOLD
            else:
                raise ValueError(f"Unknown step type: {phase.value}")
        except AttributeError as err:
            raise ValueError(
                f"Unknown step type: Step must have step_type property (BronzeStep, SilverStep, or GoldStep), got {type(step)}"
            ) from err

        result = StepExecutionResult(
            step_name=step.name,
            step_type=step_type,
            status=StepStatus.RUNNING,
            start_time=start_time,
        )

        try:
            # Use logger's step_start method for consistent formatting with emoji and uppercase
            self.logger.step_start(step_type.value, step.name)

            # Execute the step based on type using executors
            output_df: DataFrame  # type: ignore[valid-type]
            if step_type == StepType.BRONZE:
                output_df = self.bronze_executor.execute(step, context, mode)  # type: ignore[arg-type]
            elif step_type == StepType.SILVER:
                output_df = self.silver_executor.execute(step, context, mode)  # type: ignore[arg-type]
                # Store output DataFrame in context immediately after execution for downstream steps
                # This ensures prior_silvers is populated for subsequent silver steps
                context[step.name] = output_df  # type: ignore[assignment]
            elif step_type == StepType.GOLD:
                output_df = self.gold_executor.execute(step, context, mode)  # type: ignore[arg-type]
                # Store output DataFrame in context immediately after execution for downstream steps
                context[step.name] = output_df  # type: ignore[assignment]
            else:
                raise ExecutionError(f"Unknown step type: {step_type}")

            # Apply validation if not in validation-only mode
            validation_rate = 100.0
            invalid_rows = 0
            if mode != ExecutionMode.VALIDATION_ONLY:
                # All step types (Bronze, Silver, Gold) have rules attribute
                if step.rules:
                    # Use validation service for validation
                    output_df, _, validation_stats = (
                        self.validator.validate_step_output(
                            output_df,
                            step.name,
                            step.rules,
                            "pipeline",
                        )
                    )
                    # Extract validation metrics
                    validation_rate, invalid_rows = (
                        self.validator.get_validation_metrics(validation_stats)
                    )

            # Write output if not in validation-only mode
            # Note: Bronze steps only validate data, they don't write to tables
            if mode != ExecutionMode.VALIDATION_ONLY and step_type != StepType.BRONZE:
                # Use table_name attribute for SilverStep and GoldStep
                table_name = getattr(step, "table_name", step.name)
                schema = getattr(step, "schema", None)

                # Validate schema is provided
                if schema is None:
                    raise ExecutionError(
                        f"Step '{step.name}' requires a schema to be specified. "
                        f"Silver and Gold steps must have a valid schema for table operations. "
                        f"Please provide a schema when creating the step."
                    )

                output_table = fqn(schema, table_name)

                # Ensure schema exists before creating table
                # Use SQL CREATE SCHEMA (works for both PySpark and mock-spark)
                try:
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")  # type: ignore[attr-defined]
                except Exception as e:
                    # Schema might already exist, continue
                    # If all methods fail, raise error - schema creation is critical
                    # Check if it's a schema already exists error
                    error_msg = str(e).lower()
                    if (
                        "already exists" not in error_msg
                        and "duplicate" not in error_msg
                    ):
                        raise ExecutionError(
                            f"Failed to create schema '{schema}' before table creation: {e}"
                        ) from e

                # Determine write mode
                # - Gold steps always use overwrite to prevent duplicate aggregates
                # - Silver steps append during incremental runs to preserve history
                # - INITIAL mode uses overwrite
                # - FULL_REFRESH uses overwrite
                if step_type == StepType.GOLD:
                    write_mode_str = "overwrite"
                elif mode == ExecutionMode.INCREMENTAL:
                    write_mode_str = "append"
                else:  # INITIAL or FULL_REFRESH
                    write_mode_str = "overwrite"

                # Validate schema based on execution mode
                # For INCREMENTAL and FULL_REFRESH modes, schema must match exactly
                # For INITIAL mode, schema changes are allowed
                if mode in (ExecutionMode.INCREMENTAL, ExecutionMode.FULL_REFRESH):
                    if table_exists(self.spark, output_table):
                        # Refresh table metadata to ensure catalog is in sync (especially important for Delta tables)
                        try:
                            self.spark.sql(f"REFRESH TABLE {output_table}")  # type: ignore[attr-defined]
                        except Exception as refresh_error:
                            # Refresh might fail for some table types - log but continue
                            self.logger.debug(
                                f"Could not refresh table {output_table} before schema validation: {refresh_error}"
                            )

                        existing_schema = _get_existing_schema_safe(
                            self.spark, output_table
                        )
                        if existing_schema is None:
                            # Cannot read schema - raise error
                            raise ExecutionError(
                                f"Cannot read schema for table '{output_table}' in {mode} mode. "
                                "Schema validation is required for INCREMENTAL and FULL_REFRESH modes.",
                                context={
                                    "step_name": step.name,
                                    "table": output_table,
                                    "mode": mode.value,
                                },
                                suggestions=[
                                    "Ensure the table exists and is accessible",
                                    "Check that the table schema is readable",
                                    "Use INITIAL mode if you need to recreate the table",
                                ],
                            )

                        # If catalog reports empty schema, treat as mismatch with explicit guidance
                        schema_is_empty = (
                            not existing_schema.fields
                            or len(existing_schema.fields) == 0
                        )
                        if schema_is_empty:
                            output_schema = output_df.schema  # type: ignore[attr-defined]
                            raise ExecutionError(
                                f"Schema mismatch for table '{output_table}' in {mode} mode. "
                                f"Catalog reports empty schema (struct<>), but output schema has {len(output_schema.fields)} fields: {[f.name for f in output_schema.fields]}. "
                                f"Use INITIAL mode to recreate the table or provide schema_override explicitly.",
                                context={
                                    "step_name": step.name,
                                    "table": output_table,
                                    "mode": mode.value,
                                    "existing_schema": "struct<> (empty - catalog sync issue)",
                                    "output_schema": str(output_schema),
                                },
                                suggestions=[
                                    "Run initial_load/full_refresh to recreate the table with the desired schema",
                                    "Provide schema_override to force the schema in allowed modes",
                                ],
                            )

                        output_schema = output_df.schema  # type: ignore[attr-defined]
                        schemas_match, differences = _schemas_match(
                            existing_schema, output_schema
                        )

                        if not schemas_match:
                            raise ExecutionError(
                                f"Schema mismatch for table '{output_table}' in {mode} mode. "
                                f"Schema changes are only allowed in INITIAL mode.\n"
                                f"{chr(10).join(differences)}\n\n"
                                f"Existing table schema: {existing_schema}\n"
                                f"Output DataFrame schema: {output_schema}",
                                context={
                                    "step_name": step.name,
                                    "table": output_table,
                                    "mode": mode.value,
                                    "existing_schema": str(existing_schema),
                                    "output_schema": str(output_schema),
                                },
                                suggestions=[
                                    "Ensure the output schema matches the existing table schema exactly",
                                    "Run with INITIAL mode to recreate the table with the new schema",
                                    "Manually update the existing table schema to match the new schema",
                                ],
                            )

                # For INITIAL runs, ensure target tables start clean to avoid lingering catalog state
                if mode == ExecutionMode.INITIAL and step_type in (
                    StepType.SILVER,
                    StepType.GOLD,
                ):
                    try:
                        if table_exists(self.spark, output_table):
                            self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                    except Exception:
                        pass

                # Handle schema override if provided
                schema_override = getattr(step, "schema_override", None)
                should_apply_schema_override = False

                if schema_override is not None:
                    # Determine when to apply schema override:
                    # - Gold steps: Always apply (always use overwrite mode)
                    # - Silver steps in initial/full refresh: Always apply
                    # - Silver steps in incremental: Only if table doesn't exist
                    if step_type == StepType.GOLD:
                        should_apply_schema_override = True
                    elif step_type == StepType.SILVER:
                        if mode != ExecutionMode.INCREMENTAL:
                            should_apply_schema_override = True
                        else:
                            should_apply_schema_override = not table_exists(
                                self.spark, output_table
                            )

                # Apply schema override if needed
                if should_apply_schema_override:
                    try:
                        # Cast DataFrame to the override schema
                        output_df = self.spark.createDataFrame(  # type: ignore[attr-defined]
                            output_df.rdd, schema_override
                        )  # type: ignore[attr-defined]
                        # For overwrite mode, use DELETE + INSERT pattern
                        if write_mode_str == "overwrite":
                            # Delete existing data if table exists
                            delete_succeeded = False
                            if table_exists(self.spark, output_table):
                                try:
                                    self.spark.sql(f"DELETE FROM {output_table}")  # type: ignore[attr-defined]
                                    delete_succeeded = True
                                except Exception as e:
                                    # DELETE might fail for non-Delta tables
                                    error_msg = str(e).lower()
                                    if (
                                        "does not support delete" in error_msg
                                        or "unsupported_feature" in error_msg
                                    ):
                                        self.logger.info(
                                            f"Table '{output_table}' does not support DELETE. "
                                            f"Using overwrite mode instead."
                                        )
                                    else:
                                        self.logger.warning(
                                            f"Could not delete from table '{output_table}' before overwrite: {e}"
                                        )

                            if delete_succeeded:
                                # Write with overwrite mode and overwriteSchema option
                                # Note: Delta Lake doesn't support append in batch mode
                                try:
                                    (
                                        output_df.write.format("delta")
                                        .mode("overwrite")
                                        .option("overwriteSchema", "true")
                                        .saveAsTable(output_table)  # type: ignore[attr-defined]
                                    )
                                except Exception as write_error:
                                    # Handle race condition where table might be created by another thread
                                    error_msg = str(write_error).lower()
                                    if (
                                        "already exists" in error_msg
                                        or "table_or_view_already_exists" in error_msg
                                    ):
                                        # Table was created by another thread - verify it exists and retry with overwrite mode
                                        if table_exists(self.spark, output_table):
                                            self.logger.debug(
                                                f"Table {output_table} was created by another thread, retrying with overwrite mode"
                                            )
                                            # Retry with overwrite mode (original mode) and overwriteSchema
                                            retry_writer = _create_dataframe_writer(
                                                output_df,
                                                self.spark,
                                                "overwrite",
                                                table_name=output_table,
                                                overwriteSchema="true",
                                            )
                                            retry_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                        else:
                                            raise
                                    else:
                                        raise
                            else:
                                # DELETE failed - use overwrite mode directly
                                try:
                                    writer = _create_dataframe_writer(
                                        output_df,
                                        self.spark,
                                        "overwrite",
                                        table_name=output_table,
                                        overwriteSchema="true",
                                    )
                                    writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                except Exception as write_error:
                                    # Handle race condition where table might be created by another thread
                                    error_msg = str(write_error).lower()
                                    if (
                                        "already exists" in error_msg
                                        or "table_or_view_already_exists" in error_msg
                                    ):
                                        # Table was created by another thread - verify it exists and retry with overwrite mode
                                        if table_exists(self.spark, output_table):
                                            self.logger.debug(
                                                f"Table {output_table} was created by another thread, retrying with overwrite mode"
                                            )
                                            # Retry with overwrite mode (original mode) and overwriteSchema
                                            retry_writer = _create_dataframe_writer(
                                                output_df,
                                                self.spark,
                                                "overwrite",
                                                table_name=output_table,
                                                overwriteSchema="true",
                                            )
                                            retry_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                        else:
                                            raise
                                    else:
                                        raise
                        else:
                            # For append mode, use normal write
                            try:
                                writer = _create_dataframe_writer(
                                    output_df,
                                    self.spark,
                                    write_mode_str,
                                    table_name=output_table,
                                    overwriteSchema="true",
                                )
                                writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                            except Exception as write_error:
                                # Handle race condition where table might be created by another thread
                                error_msg = str(write_error).lower()
                                if (
                                    "already exists" in error_msg
                                    or "table_or_view_already_exists" in error_msg
                                ):
                                    # Table was created by another thread - verify it exists and retry
                                    if table_exists(self.spark, output_table):
                                        self.logger.debug(
                                            f"Table {output_table} was created by another thread, retrying with overwrite mode"
                                        )
                                        # Retry with overwrite mode (append not supported in batch mode for Delta)
                                        retry_writer = _create_dataframe_writer(
                                            output_df,
                                            self.spark,
                                            "overwrite",
                                            overwriteSchema="true",
                                        )
                                        retry_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                    else:
                                        raise
                                else:
                                    raise
                    except Exception as e:
                        raise ExecutionError(
                            f"Failed to write table '{output_table}' with schema override: {e}",
                            context={
                                "step_name": step.name,
                                "table": output_table,
                                "schema_override": str(schema_override),
                            },
                            suggestions=[
                                "Verify that the schema_override matches the DataFrame structure",
                                "Check that all required columns are present in the DataFrame",
                                "Ensure data types are compatible",
                            ],
                        ) from e
                else:
                    # Normal write without schema override
                    # Handle INITIAL mode schema changes - allow schema changes via CREATE OR REPLACE TABLE
                    # For INCREMENTAL/FULL_REFRESH, schema validation already done above
                    if mode == ExecutionMode.INITIAL and write_mode_str == "append":
                        existing_schema = _get_existing_schema_safe(
                            self.spark, output_table
                        )
                        if existing_schema is not None:
                            # Check if schema is empty (catalog sync issue)
                            schema_is_empty = (
                                not existing_schema.fields
                                or len(existing_schema.fields) == 0
                            )
                            output_schema = output_df.schema  # type: ignore[attr-defined]

                            if schema_is_empty:
                                # Catalog reports empty schema but table exists - use CREATE OR REPLACE TABLE
                                self.logger.info(
                                    f"Table '{output_table}' exists but catalog reports empty schema. "
                                    f"Using CREATE OR REPLACE TABLE for atomic schema replacement."
                                )
                                temp_view_name = (
                                    f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                )
                                output_df.createOrReplaceTempView(temp_view_name)  # type: ignore[attr-defined]

                                # For Delta tables, DROP then CREATE (CREATE OR REPLACE doesn't work with Delta)
                                self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                                self.spark.sql(f"""
                                    CREATE TABLE {output_table}
                                    USING DELTA
                                    AS SELECT * FROM {temp_view_name}
                                """)  # type: ignore[attr-defined]

                                try:
                                    self.spark.sql(
                                        f"DROP VIEW IF EXISTS {temp_view_name}"
                                    )  # type: ignore[attr-defined]
                                except Exception:
                                    pass

                                # Skip normal write path - table already written
                                writer = None
                            else:
                                # Schema exists and is not empty - check if it matches
                                schemas_match, differences = _schemas_match(
                                    existing_schema, output_schema
                                )
                                if not schemas_match:
                                    # Schema differs - INITIAL mode allows schema changes via CREATE OR REPLACE TABLE
                                    self.logger.info(
                                        f"Schema change detected for '{output_table}' in INITIAL mode. "
                                        f"Using CREATE OR REPLACE TABLE for atomic schema replacement."
                                    )
                                    temp_view_name = (
                                        f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                    )
                                    output_df.createOrReplaceTempView(temp_view_name)  # type: ignore[attr-defined]

                                    # For Delta tables, DROP then CREATE (CREATE OR REPLACE doesn't work with Delta)
                                    self.spark.sql(
                                        f"DROP TABLE IF EXISTS {output_table}"
                                    )  # type: ignore[attr-defined]
                                    self.spark.sql(f"""
                                        CREATE TABLE {output_table}
                                        USING DELTA
                                        AS SELECT * FROM {temp_view_name}
                                    """)  # type: ignore[attr-defined]

                                    try:
                                        self.spark.sql(
                                            f"DROP VIEW IF EXISTS {temp_view_name}"
                                        )  # type: ignore[attr-defined]
                                    except Exception:
                                        pass

                                    # Skip normal write path - table already written
                                    writer = None
                                else:
                                    # Schema matches - use DELETE + overwrite for Delta tables
                                    # or CREATE OR REPLACE TABLE for atomic replacement
                                    # Note: Delta Lake doesn't support append in batch mode, so we use overwrite
                                    if _is_delta_lake_available_execution(self.spark):
                                        # For Delta tables, use DELETE + overwrite (append not supported in batch mode)
                                        try:
                                            self.spark.sql(
                                                f"DELETE FROM {output_table}"
                                            )  # type: ignore[attr-defined]
                                            # DELETE succeeded, use overwrite mode (append not supported in batch mode)
                                            writer = _create_dataframe_writer(
                                                output_df,
                                                self.spark,
                                                "overwrite",
                                                table_name=output_table,
                                            )
                                        except Exception as delete_error:
                                            # If DELETE fails, fall back to CREATE OR REPLACE TABLE
                                            self.logger.warning(
                                                f"DELETE FROM failed for '{output_table}': {delete_error}. "
                                                f"Using CREATE OR REPLACE TABLE instead."
                                            )
                                            temp_view_name = f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                            output_df.createOrReplaceTempView(
                                                temp_view_name
                                            )  # type: ignore[attr-defined]
                                            self.spark.sql(f"""
                                                CREATE OR REPLACE TABLE {output_table}
                                                USING DELTA
                                                AS SELECT * FROM {temp_view_name}
                                            """)  # type: ignore[attr-defined]
                                            try:
                                                self.spark.sql(
                                                    f"DROP VIEW IF EXISTS {temp_view_name}"
                                                )  # type: ignore[attr-defined]
                                            except Exception:
                                                pass
                                            writer = None
                                    else:
                                        # Not Delta table, use normal overwrite
                                        writer = _create_dataframe_writer(
                                            output_df,
                                            self.spark,
                                            "overwrite",
                                            table_name=output_table,
                                        )
                        else:
                            # Table doesn't exist - proceed with normal write
                            writer = _create_dataframe_writer(
                                output_df,
                                self.spark,
                                write_mode_str,
                                table_name=output_table,
                            )
                    # Heal catalog entries that report empty schema (struct<>) before choosing writer
                    if table_exists(self.spark, output_table) and table_schema_is_empty(
                        self.spark, output_table
                    ):
                        self.logger.warning(
                            f"Catalog reports empty schema for '{output_table}'. Dropping table to recreate with correct schema."
                        )
                        try:
                            self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                        except Exception:
                            pass

                    # Create writer with appropriate mode
                    writer = _create_dataframe_writer(
                        output_df, self.spark, write_mode_str, table_name=output_table
                    )

                    # Execute write
                    if writer is not None:
                        try:
                            writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                        except Exception as write_error:
                            error_msg = str(write_error).lower()
                            # Handle catalog sync issues where Spark reports empty schema (struct<>)
                            if (
                                "struct<>" in error_msg
                                or "column number of the existing table" in error_msg
                            ):
                                # This is a catalog sync issue - try refreshing the table and retrying
                                self.logger.warning(
                                    f"Catalog sync issue detected for table '{output_table}'. "
                                    f"Refreshing table and retrying write."
                                )
                                try:
                                    # Refresh table and force schema re-read by reading actual data
                                    self.spark.sql(f"REFRESH TABLE {output_table}")  # type: ignore[attr-defined]
                                    # Force Spark to re-read schema by reading a sample row
                                    try:
                                        sample_df = self.spark.sql(
                                            f"SELECT * FROM {output_table} LIMIT 1"
                                        )  # type: ignore[attr-defined]
                                        _ = sample_df.schema  # Force schema evaluation
                                    except Exception:
                                        pass  # Ignore errors when reading sample

                                    # Try SQL-based INSERT for Delta tables (works even with catalog sync issues)
                                    if (
                                        _is_delta_lake_available_execution(self.spark)
                                        and write_mode_str == "append"
                                    ):
                                        temp_view_name = (
                                            f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                        )
                                        output_df.createOrReplaceTempView(
                                            temp_view_name
                                        )  # type: ignore[attr-defined]
                                        try:
                                            self.spark.sql(
                                                f"INSERT INTO {output_table} SELECT * FROM {temp_view_name}"
                                            )  # type: ignore[attr-defined]
                                            # Success - clean up and skip normal write path
                                            try:
                                                self.spark.sql(
                                                    f"DROP VIEW IF EXISTS {temp_view_name}"
                                                )  # type: ignore[attr-defined]
                                            except Exception:
                                                pass
                                            writer = None  # Mark writer as None to skip normal write
                                            # Exit the retry block - write succeeded via SQL INSERT
                                        except Exception:
                                            # SQL INSERT also failed - try normal write as fallback
                                            try:
                                                self.spark.sql(
                                                    f"DROP VIEW IF EXISTS {temp_view_name}"
                                                )  # type: ignore[attr-defined]
                                            except Exception:
                                                pass
                                            writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                    else:
                                        # Retry the write after refresh
                                        writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                except Exception as retry_error:
                                    # Attempt to heal catalog by recreating table with existing + new data
                                    healed = False
                                    # For Gold steps, we can safely rebuild the table from the fresh output
                                    if step_type == StepType.GOLD:
                                        try:
                                            self.spark.sql(
                                                f"DROP TABLE IF EXISTS {output_table}"
                                            )  # type: ignore[attr-defined]
                                            direct_writer = _create_dataframe_writer(
                                                output_df,
                                                self.spark,
                                                "overwrite",
                                                table_name=output_table,
                                                overwriteSchema="true",
                                            )
                                            direct_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                            healed = True
                                            writer = None
                                        except Exception:
                                            healed = False

                                    if not healed:
                                        try:
                                            base_df = None
                                            if _is_delta_lake_available_execution(
                                                self.spark
                                            ):
                                                try:
                                                    delta_tbl = DeltaTable.forName(
                                                        self.spark, output_table
                                                    )  # type: ignore[attr-defined]
                                                    base_df = delta_tbl.toDF()
                                                except Exception:
                                                    base_df = None
                                            if base_df is not None:
                                                combined_df = base_df.unionByName(  # type: ignore[attr-defined]
                                                    output_df, allowMissingColumns=True
                                                )
                                            else:
                                                combined_df = output_df

                                            temp_view_name = f"_heal_{step.name}_{uuid.uuid4().hex[:8]}"
                                            combined_df.createOrReplaceTempView(
                                                temp_view_name
                                            )  # type: ignore[attr-defined]
                                            try:
                                                # Always drop then create to avoid truncate/replace limitations
                                                self.spark.sql(
                                                    f"DROP TABLE IF EXISTS {output_table}"
                                                )  # type: ignore[attr-defined]
                                                if _is_delta_lake_available_execution(
                                                    self.spark
                                                ):
                                                    self.spark.sql(  # type: ignore[attr-defined]
                                                        f"CREATE TABLE {output_table} USING DELTA AS SELECT * FROM {temp_view_name}"
                                                    )
                                                else:
                                                    self.spark.sql(  # type: ignore[attr-defined]
                                                        f"CREATE TABLE {output_table} USING PARQUET AS SELECT * FROM {temp_view_name}"
                                                    )
                                                healed = True
                                            finally:
                                                try:
                                                    self.spark.sql(
                                                        f"DROP VIEW IF EXISTS {temp_view_name}"
                                                    )  # type: ignore[attr-defined]
                                                except Exception:
                                                    pass
                                        except Exception:
                                            healed = False

                                    if not healed:
                                        # Last-resort fix for catalog sync issues: drop and recreate the table when safe.
                                        try:
                                            if mode == ExecutionMode.INITIAL:
                                                self.spark.sql(
                                                    f"DROP TABLE IF EXISTS {output_table}"
                                                )  # type: ignore[attr-defined]
                                                writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                            else:
                                                raise retry_error
                                        except Exception as drop_error:
                                            # If drop+rewrite fails, convert to ExecutionError with helpful message
                                            raise ExecutionError(
                                                f"Schema validation failed for table '{output_table}' in {mode} mode. "
                                                f"Catalog reports empty schema (struct<>), indicating a catalog sync issue. "
                                                f"Original error: {write_error}",
                                                context={
                                                    "step_name": step.name,
                                                    "table": output_table,
                                                    "mode": mode.value,
                                                    "original_error": str(write_error),
                                                    "retry_error": str(retry_error),
                                                    "drop_error": str(drop_error),
                                                },
                                                suggestions=[
                                                    "This may be a Spark/Delta Lake catalog sync issue",
                                                    "Try running the pipeline again",
                                                    "If the issue persists, use INITIAL mode to recreate the table",
                                                ],
                                            ) from drop_error
                                    else:
                                        # Table healed via recreate; skip normal writer path
                                        writer = None
                            # Handle race condition where table might be created by another thread
                            elif (
                                "already exists" in error_msg
                                or "table_or_view_already_exists" in error_msg
                            ):
                                # Table was created by another thread - verify it exists and retry with overwrite mode
                                if table_exists(self.spark, output_table):
                                    self.logger.debug(
                                        f"Table {output_table} was created by another thread, retrying with overwrite mode"
                                    )
                                    # Retry with overwrite mode (append not supported in batch mode for Delta)
                                    retry_writer = _create_dataframe_writer(
                                        output_df,
                                        self.spark,
                                        "overwrite",
                                        table_name=output_table,
                                    )
                                    retry_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                else:
                                    raise
                            else:
                                # Different error - re-raise
                                raise
                        finally:
                            pass

                        # Refresh table metadata after write to ensure subsequent reads see the latest data
                        try:
                            self.spark.sql(f"REFRESH TABLE {output_table}")  # type: ignore[attr-defined]
                        except Exception as refresh_error:
                            # Refresh might fail for some table types or if table doesn't exist - log but don't fail
                            self.logger.debug(
                                f"Could not refresh table {output_table} after write: {refresh_error}"
                            )

                result.output_table = output_table
                result.rows_processed = output_df.count()  # type: ignore[attr-defined]

                # Set write mode in result for tracking
                result.write_mode = write_mode_str  # type: ignore[attr-defined]
            elif step_type == StepType.BRONZE:
                # Bronze steps only validate data, don't write to tables
                result.rows_processed = output_df.count()  # type: ignore[attr-defined]
                result.write_mode = None  # type: ignore[attr-defined]
            else:  # VALIDATION_ONLY mode
                # Validation-only mode doesn't write to tables
                result.rows_processed = output_df.count()  # type: ignore[attr-defined]
                result.write_mode = None  # type: ignore[attr-defined]

            # Collect final resource metrics
            end_memory, end_cpu = self._collect_resource_metrics()

            # Calculate metrics (use end values, or delta if both available)
            if end_memory is not None:
                if start_memory is not None:
                    # Use peak memory (difference) or end memory
                    result.memory_usage_mb = max(end_memory - start_memory, end_memory)
                else:
                    result.memory_usage_mb = end_memory
            if end_cpu is not None:
                result.cpu_usage_percent = end_cpu

            result.status = StepStatus.COMPLETED
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()

            # Populate result fields
            rows_processed = result.rows_processed or 0
            # For Silver/Gold steps, rows_written equals rows_processed (since we write the output)
            # For Bronze steps, rows_written is None (they don't write to tables)
            rows_written = rows_processed if step_type != StepType.BRONZE else None

            result.rows_written = rows_written
            result.input_rows = rows_processed
            result.validation_rate = (
                validation_rate if validation_rate is not None else 100.0
            )

            # Note: output_df is already stored in context immediately after execution
            # (for Silver and Gold steps) to ensure prior_silvers is populated for downstream steps

            # Use logger's step_complete method for consistent formatting with emoji and uppercase
            # rows_written can be None for Bronze steps, but logger expects int, so use 0 as fallback
            self.logger.step_complete(
                step_type.value,
                step.name,
                result.duration,
                rows_processed=rows_processed,
                rows_written=rows_written if rows_written is not None else 0,
                invalid_rows=invalid_rows,
                validation_rate=validation_rate,
            )

        except Exception as e:
            # Collect final resource metrics even on failure
            end_memory, end_cpu = self._collect_resource_metrics()

            # Calculate metrics (use end values, or delta if both available)
            if end_memory is not None:
                if start_memory is not None:
                    # Use peak memory (difference) or end memory
                    result.memory_usage_mb = max(end_memory - start_memory, end_memory)
                else:
                    result.memory_usage_mb = end_memory
            if end_cpu is not None:
                result.cpu_usage_percent = end_cpu

            result.status = StepStatus.FAILED
            result.error = str(e)
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()

            # Log step failure
            self.logger.error(
                f"❌ Failed {step_type.value.upper()} step: {step.name} ({result.duration:.2f}s) - {str(e)}"
            )
            raise ExecutionError(f"Step execution failed: {e}") from e

        return result

    def execute_pipeline(
        self,
        steps: list[Union[BronzeStep, SilverStep, GoldStep]],
        mode: ExecutionMode = ExecutionMode.INITIAL,
        context: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
    ) -> ExecutionResult:
        """Execute a complete pipeline with dependency-aware sequential execution.

        Analyzes step dependencies and executes steps sequentially in the correct
        order. Steps are grouped by dependencies, and groups are executed
        sequentially to respect dependency constraints.

        Args:
            steps: List of steps to execute. Can include Bronze, Silver, and
                Gold steps in any order - dependencies are automatically analyzed.
            mode: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH,
                VALIDATION_ONLY). Defaults to INITIAL.
            context: Optional initial execution context dictionary mapping step
                names to DataFrames. Must contain bronze source data. If None,
                empty dictionary is used.

        Returns:
            ExecutionResult containing:
            - Overall pipeline status
            - List of StepExecutionResult for each step
            - Execution timing
            - Dependency analysis results (execution_groups_count, max_group_size)

        Raises:
            ExecutionError: If pipeline execution fails.
            TypeError: If context is not a dictionary.

        Example:
            >>> config = PipelineConfig.create_default(schema="my_schema")
            >>> engine = ExecutionEngine(spark, config)
            >>> result = engine.execute_pipeline(
            ...     steps=[bronze, silver1, silver2, gold],
            ...     mode=ExecutionMode.INITIAL,
            ...     context={"events": source_df}
            ... )
            >>> print(f"Status: {result.status}")
            >>> print(f"Execution groups: {result.execution_groups_count}")
            >>> print(f"Steps completed: {len([s for s in result.steps if s.status == StepStatus.COMPLETED])}")

        Note:
            - All required schemas are created upfront before execution
            - Steps are grouped by dependencies using DependencyAnalyzer
            - Groups execute sequentially, steps within groups execute sequentially
            - Context is updated after each step completion for downstream steps
            - Failed steps are recorded but don't stop execution of remaining steps
        """
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        result = ExecutionResult(
            execution_id=execution_id,
            mode=mode,
            start_time=start_time,
            status="running",
        )

        try:
            # Logging is handled by the runner to avoid duplicate messages
            # Ensure all required schemas exist before execution
            # Collect unique schemas from all steps
            required_schemas = set()
            for step in steps:
                if hasattr(step, "schema") and step.schema:  # type: ignore[attr-defined]
                    schema_value = step.schema  # type: ignore[attr-defined]
                    # Handle both string schemas and Mock objects (for tests)
                    if isinstance(schema_value, str):
                        required_schemas.add(schema_value)
            # Create all required schemas upfront - always try to create, don't rely on catalog checks
            for schema in required_schemas:
                try:
                    # Always try to create schema - CREATE SCHEMA IF NOT EXISTS is idempotent
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")  # type: ignore[attr-defined]
                    # Also use _ensure_schema_exists as backup (tries multiple methods)
                    self._ensure_schema_exists(schema)
                except Exception as e:
                    # Log but don't fail - schema might already exist or creation might work later
                    self.logger.debug(f"Schema '{schema}' pre-creation attempt: {e}")

            # Validate context parameter
            if context is None:
                context = {}
            elif not isinstance(context, dict):
                raise TypeError(f"context must be a dictionary, got {type(context)}")

            # Group steps by type for dependency analysis
            bronze_steps = [s for s in steps if s.step_type.value == "bronze"]
            silver_steps = [s for s in steps if s.step_type.value == "silver"]
            gold_steps = [s for s in steps if s.step_type.value == "gold"]

            # Build dependency graph and get execution groups
            analyzer = DependencyAnalyzer()
            analysis = analyzer.analyze_dependencies(
                bronze_steps={s.name: s for s in bronze_steps},
                silver_steps={s.name: s for s in silver_steps},
                gold_steps={s.name: s for s in gold_steps},
            )

            execution_groups = analysis.execution_groups
            result.execution_groups_count = len(execution_groups)
            result.max_group_size = (
                max(len(group) for group in execution_groups) if execution_groups else 0
            )

            # Log dependency analysis results
            self.logger.info(
                f"Dependency analysis complete: {len(execution_groups)} execution groups, "
                f"max group size: {result.max_group_size}"
            )

            # Create a mapping of step names to step objects
            step_map = {s.name: s for s in steps}

            # Execute each group sequentially
            for group_idx, group in enumerate(execution_groups):
                group_start = datetime.now()
                self.logger.info(
                    f"Executing group {group_idx + 1}/{len(execution_groups)}: "
                    f"{len(group)} steps - {', '.join(group)}"
                )

                # Execute steps sequentially within each group
                for step_name in group:
                    if step_name not in step_map:
                        self.logger.warning(
                            f"Step {step_name} in execution group but not found in step list"
                        )
                        continue

                    step = step_map[step_name]
                    try:
                        step_result = self.execute_step(
                            step,
                            context,
                            mode,
                        )
                        if result.steps is not None:
                            result.steps.append(step_result)

                        # Update context with step output for downstream steps
                        # Note: output_df is already stored in context by execute_step after completion
                        # This table read is a fallback/refresh to ensure we have the latest data from the table
                        if (
                            step_result.status == StepStatus.COMPLETED
                            and step_result.step_type != StepType.BRONZE
                        ):
                            table_name = getattr(step, "table_name", step.name)
                            schema = getattr(step, "schema", None)
                            if schema is not None:
                                table_fqn = fqn(schema, table_name)
                                try:
                                    # Refresh table first to ensure we see the latest data
                                    try:
                                        self.spark.sql(f"REFRESH TABLE {table_fqn}")  # type: ignore[attr-defined]
                                    except Exception:
                                        pass  # Refresh might fail for some table types - continue anyway
                                    # Read table and add to context (overwrites output_df with table data)
                                    # Only update if read succeeds - if it fails, keep the output_df from execute_step
                                    table_df = self.spark.table(table_fqn)  # type: ignore[attr-defined,valid-type]
                                    context[step.name] = table_df  # type: ignore[valid-type]
                                except Exception as e:
                                    # If reading fails, the output_df stored by execute_step is still in context
                                    # This is fine - we'll use the output_df that was stored during execution
                                    # Only log if the step name is not already in context (meaning execute_step didn't store it)
                                    if step.name not in context:
                                        self.logger.warning(
                                            f"Could not read table '{table_fqn}' to add to context, "
                                            f"and execute_step did not store output_df. "
                                            f"Downstream steps may not have access to this step's output. Error: {e}"
                                        )
                                    else:
                                        self.logger.debug(
                                            f"Could not read table '{table_fqn}' to refresh context. "
                                            f"Using output_df stored during execution. Error: {e}"
                                        )

                        if step_result.status == StepStatus.FAILED:
                            self.logger.error(
                                f"Step {step_name} failed: {step_result.error}"
                            )
                    except Exception as e:
                        self.logger.error(f"Exception executing step {step_name}: {e}")
                        # Determine correct step type
                        step_obj = step_map.get(step_name)
                        if step_obj is not None:
                            phase = step_obj.step_type
                            if phase.value == "bronze":
                                step_type_enum = StepType.BRONZE
                            elif phase.value == "silver":
                                step_type_enum = StepType.SILVER
                            elif phase.value == "gold":
                                step_type_enum = StepType.GOLD
                            else:
                                step_type_enum = StepType.BRONZE  # fallback
                        else:
                            step_type_enum = StepType.BRONZE  # fallback

                        step_result = StepExecutionResult(
                            step_name=step_name,
                            step_type=step_type_enum,
                            status=StepStatus.FAILED,
                            error=str(e),
                            start_time=datetime.now(),
                            end_time=datetime.now(),
                            duration=0.0,
                        )
                        if result.steps is not None:
                            result.steps.append(step_result)

                group_end = datetime.now()
                group_duration = (group_end - group_start).total_seconds()
                self.logger.info(
                    f"Group {group_idx + 1} completed in {group_duration:.2f}s"
                )

            # Determine overall pipeline status based on step results
            if result.steps is None:
                result.steps = []
            step_results: list[StepExecutionResult] = result.steps
            failed_steps = [s for s in step_results if s.status == StepStatus.FAILED]

            if failed_steps:
                result.status = "failed"
                self.logger.error(
                    f"Pipeline execution failed: {len(failed_steps)} steps failed"
                )
            else:
                result.status = "completed"
                self.logger.info(f"Completed pipeline execution: {execution_id}")

            result.end_time = datetime.now()

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            result.end_time = datetime.now()
            self.logger.error(f"Pipeline execution failed: {e}")
            raise ExecutionError(f"Pipeline execution failed: {e}") from e

        return result

    def _execute_bronze_step(
        self,
        step: BronzeStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]  # type: ignore[valid-type]
    ) -> DataFrame:  # type: ignore[valid-type]
        """Execute a bronze step.

        Bronze steps validate existing data but don't transform or write it.
        The step name must exist in the context dictionary with the source DataFrame.

        Args:
            step: BronzeStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                the step name as a key.

        Returns:
            DataFrame from context (validated but unchanged).

        Raises:
            ExecutionError: If step name not found in context or DataFrame is
                invalid.

        Note:
            Bronze steps are for validating raw data. They don't write to tables
            or perform transformations. Validation is applied separately by the
            execution engine.
        """
        # Bronze steps require data to be provided in context
        # This is the expected behavior - bronze steps validate existing data
        if step.name not in context:
            raise ExecutionError(
                f"Bronze step '{step.name}' requires data to be provided in context. "
                f"Bronze steps are for validating existing data, not creating it. "
                f"Please provide data using bronze_sources parameter or context dictionary. "
                f"Available context keys: {list(context.keys())}"
            )

        df: DataFrame = context[step.name]  # type: ignore[valid-type]

        # Validate that the DataFrame is not empty (optional check)
        if df.count() == 0:  # type: ignore[attr-defined]
            self.logger.warning(
                f"Bronze step '{step.name}' received empty DataFrame. "
                f"This may indicate missing or invalid data source."
            )

        return df

    def _execute_silver_step(
        self,
        step: SilverStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]
        mode: ExecutionMode,
    ) -> DataFrame:  # type: ignore[valid-type]
        """Execute a silver step.

        Silver steps transform bronze data into cleaned and enriched data.
        For INCREMENTAL mode, filters bronze input to only process new rows.

        Args:
            step: SilverStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                the source bronze step name.
            mode: Execution mode. INCREMENTAL mode triggers incremental filtering.

        Returns:
            Transformed DataFrame ready for validation and writing.

        Raises:
            ExecutionError: If source bronze step not found in context.

        Note:
            - Applies incremental filtering if mode is INCREMENTAL
            - Calls step.transform() with bronze DataFrame and empty silvers dict
            - Transformation logic is defined in the step's transform function
        """

        # Get source bronze data
        if step.source_bronze not in context:
            raise ExecutionError(
                f"Source bronze step {step.source_bronze} not found in context"
            )

        bronze_df: DataFrame = context[step.source_bronze]  # type: ignore[valid-type]

        if mode == ExecutionMode.INCREMENTAL:
            bronze_df = self._filter_incremental_bronze_input(step, bronze_df)

        # Build prior_silvers dict from context
        # If source_silvers is specified, only include those steps
        # Otherwise, include all previously executed steps (excluding bronze and current step)
        prior_silvers: Dict[str, DataFrame] = {}  # type: ignore[valid-type]
        source_silvers = getattr(step, "source_silvers", None)
        
        if source_silvers:
            # Only include explicitly specified silver steps
            for silver_name in source_silvers:
                if silver_name in context and silver_name != step.name:
                    prior_silvers[silver_name] = context[silver_name]  # type: ignore[assignment]
                elif silver_name not in context:
                    # Log warning if expected silver step is not in context
                    # This helps debug dependency issues
                    available_keys = [k for k in context.keys() if k != step.name and k != step.source_bronze]
                    self.logger.warning(
                        f"Silver step {step.name} expects {silver_name} in prior_silvers "
                        f"(via source_silvers), but it's not in context. "
                        f"Available keys: {list(context.keys())}, "
                        f"Other silver steps in context: {available_keys}"
                    )
        else:
            # Include all previously executed steps (excluding bronze and current step)
            # This allows backward compatibility for silver steps that access prior_silvers
            # without explicitly declaring dependencies
            for key, value in context.items():
                if key != step.name and key != step.source_bronze:
                    prior_silvers[key] = value  # type: ignore[assignment]

        # Apply transform with source bronze data and prior silvers dict
        return step.transform(self.spark, bronze_df, prior_silvers)

    def _filter_incremental_bronze_input(
        self,
        step: SilverStep,
        bronze_df: DataFrame,  # type: ignore[valid-type]  # type: ignore[valid-type]
    ) -> DataFrame:  # type: ignore[valid-type]
        """Filter bronze input rows already processed in previous incremental runs.

        Filters bronze DataFrame to only include rows that haven't been processed
        yet. Uses the source bronze step's incremental column and the silver step's
        watermark column to determine which rows to exclude.

        Args:
            step: SilverStep instance with incremental configuration.
            bronze_df: Bronze DataFrame to filter.

        Returns:
            Filtered DataFrame containing only new rows to process. Returns
            original DataFrame if filtering cannot be performed (missing columns,
            table doesn't exist, etc.).

        Raises:
            ExecutionError: If filtering fails due to column or type issues.

        Note:
            Filtering logic:
            1. Reads existing silver table to get maximum watermark value
            2. Filters bronze rows where incremental_col > max_watermark
            3. Returns original DataFrame if table doesn't exist (first run)

            Requires:
            - step.source_incremental_col: Column in bronze DataFrame
            - step.watermark_col: Column in existing silver table
            - step.schema and step.table_name: To locate existing table

            Skips filtering gracefully if requirements not met (returns original DataFrame).
        """

        incremental_col = getattr(step, "source_incremental_col", None)
        watermark_col = getattr(step, "watermark_col", None)
        schema = getattr(step, "schema", None)
        table_name = getattr(step, "table_name", step.name)

        if not incremental_col or not watermark_col or schema is None:
            return bronze_df

        if incremental_col not in getattr(bronze_df, "columns", []):
            self.logger.debug(
                f"Silver step {step.name}: incremental column '{incremental_col}' "
                f"not present in bronze DataFrame; skipping incremental filter"
            )
            return bronze_df

        # Validate that incremental column type is appropriate for filtering
        try:
            schema = bronze_df.schema  # type: ignore[attr-defined]
            col_field = schema[incremental_col]  # type: ignore[index]
            col_type = col_field.dataType  # type: ignore[attr-defined]
            col_type_name = str(col_type)

            # Check if type is comparable (numeric, date, timestamp, string)
            # Non-comparable types: boolean, array, map, struct
            non_comparable_types = ["boolean", "array", "map", "struct", "binary"]
            if any(
                non_comp in col_type_name.lower() for non_comp in non_comparable_types
            ):
                self.logger.warning(
                    f"Silver step {step.name}: incremental column '{incremental_col}' "
                    f"has type '{col_type_name}' which may not be suitable for comparison operations. "
                    f"Filtering may fail or produce unexpected results. "
                    f"Consider using a numeric, date, timestamp, or string column for incremental processing."
                )
        except (KeyError, AttributeError, Exception) as e:
            # If we can't inspect the schema, log a warning but continue
            self.logger.debug(
                f"Silver step {step.name}: could not validate incremental column type: {e}"
            )

        output_table = fqn(schema, table_name)

        try:
            existing_table = self.spark.table(output_table)  # type: ignore[attr-defined]
        except Exception as exc:
            self.logger.debug(
                f"Silver step {step.name}: unable to read existing table {output_table} "
                f"for incremental filter: {exc}"
            )
            return bronze_df

        if watermark_col not in getattr(existing_table, "columns", []):
            self.logger.debug(
                f"Silver step {step.name}: watermark column '{watermark_col}' "
                f"not present in existing table {output_table}; skipping incremental filter"
            )
            return bronze_df

        try:
            watermark_rows = existing_table.select(watermark_col).collect()  # type: ignore[attr-defined]
        except Exception as exc:
            self.logger.warning(
                f"Silver step {step.name}: failed to collect watermark values "
                f"from {output_table}: {exc}"
            )
            return bronze_df

        if not watermark_rows:
            return bronze_df

        cutoff_value = None
        for row in watermark_rows:
            value = None
            if hasattr(row, "__getitem__"):
                try:
                    value = row[watermark_col]
                except Exception:
                    try:
                        value = row[0]
                    except Exception:
                        value = None
            if value is None and hasattr(row, "asDict"):
                value = row.asDict().get(watermark_col)
            if value is None:
                continue
            cutoff_value = value if cutoff_value is None else max(cutoff_value, value)

        if cutoff_value is None:
            return bronze_df

        try:
            filtered_df = bronze_df.filter(F.col(incremental_col) > F.lit(cutoff_value))  # type: ignore[attr-defined]
        except Exception as exc:
            # Provide detailed error context for incremental filtering failures
            error_msg = str(exc).lower()
            if "cannot resolve" in error_msg or "column" in error_msg:
                # Column-related error - provide schema context
                available_cols = sorted(getattr(bronze_df, "columns", []))
                raise ExecutionError(
                    f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                    f"Error: {exc!r}. "
                    f"Available columns in bronze DataFrame: {available_cols}. "
                    f"This may indicate that the incremental column was dropped or renamed in a previous transform. "
                    f"Please ensure the incremental column '{incremental_col}' exists in the bronze DataFrame."
                ) from exc
            elif "type" in error_msg or "cast" in error_msg:
                # Type-related error - provide type information
                try:
                    col_type = bronze_df.schema[incremental_col].dataType  # type: ignore[attr-defined]
                    raise ExecutionError(
                        f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                        f"Error: {exc!r}. "
                        f"Column type: {col_type}. "
                        f"Cutoff value type: {type(cutoff_value).__name__}. "
                        f"Incremental columns must be comparable types (numeric, date, timestamp). "
                        f"Please ensure the incremental column type is compatible with the cutoff value."
                    ) from exc
                except (KeyError, AttributeError, Exception):
                    # If we can't get type info, provide generic error
                    raise ExecutionError(
                        f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                        f"Error: {exc!r}. "
                        f"This may be a type mismatch between the incremental column and the cutoff value. "
                        f"Please ensure the incremental column type is compatible with the cutoff value type."
                    ) from exc
            else:
                # Generic error with context
                raise ExecutionError(
                    f"Silver step {step.name}: failed to filter bronze rows using "
                    f"{incremental_col} > {cutoff_value}: {exc!r}. "
                    f"Please check that the incremental column exists and is of a comparable type."
                ) from exc

        self.logger.info(
            f"Silver step {step.name}: filtering bronze rows where "
            f"{incremental_col} <= {cutoff_value}"
        )
        return filtered_df

    @staticmethod
    def _extract_row_value(row: Any, column: str) -> Optional[object]:
        """Safely extract a column value from a Row-like object.

        Attempts multiple methods to extract a column value from Spark Row objects,
        handling different Row implementations and access patterns.

        Args:
            row: Row-like object (Spark Row, dict, or similar).
            column: Column name to extract.

        Returns:
            Extracted value if found, None otherwise.

        Note:
            Tries methods in order:
            1. Direct indexing: row[column]
            2. Positional indexing: row[0]
            3. Dictionary access: row.asDict().get(column)

            Returns None if all methods fail or value is not found.
        """
        if hasattr(row, "__getitem__"):
            try:
                result: Optional[object] = row[column]  # type: ignore[assignment]
                return result
            except Exception:
                try:
                    result = row[0]  # type: ignore[assignment]
                    return cast(Optional[object], result)
                except Exception:
                    pass
        if hasattr(row, "asDict"):
            try:
                result = row.asDict().get(column)  # type: ignore[assignment]
                return cast(Optional[object], result)
            except Exception:
                return None
        return None

    def _execute_gold_step(
        self,
        step: GoldStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]  # type: ignore[valid-type]
    ) -> DataFrame:  # type: ignore[valid-type]
        """Execute a gold step.

        Gold steps transform silver data into business analytics and aggregations.
        Builds a dictionary of source silver DataFrames from step.source_silvers.

        Args:
            step: GoldStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                all source silver step names listed in step.source_silvers.

        Returns:
            Transformed DataFrame ready for validation and writing.

        Raises:
            ExecutionError: If any source silver step not found in context.

        Note:
            - Builds silvers dictionary from step.source_silvers
            - Calls step.transform() with SparkSession and silvers dictionary
            - Transformation logic is defined in the step's transform function
            - Gold steps typically perform aggregations and business metrics
        """

        # Build silvers dict from source_silvers
        silvers = {}
        if step.source_silvers is not None:
            for silver_name in step.source_silvers:
                if silver_name not in context:
                    raise ExecutionError(
                        f"Source silver {silver_name} not found in context"
                    )
                silvers[silver_name] = context[silver_name]  # type: ignore[valid-type]

        return step.transform(self.spark, silvers)


# Backward compatibility aliases
UnifiedExecutionEngine = ExecutionEngine
UnifiedStepExecutionResult = StepExecutionResult
