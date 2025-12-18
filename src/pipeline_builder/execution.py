# mypy: ignore-errors
"""
Production-ready execution system for the framework pipelines.

This module provides a robust execution engine that handles pipeline execution
with comprehensive error handling, step-by-step processing, and detailed reporting.

Key Features:
- **Step-by-Step Execution**: Process pipeline steps individually with detailed tracking
- **Comprehensive Error Handling**: Detailed error messages with context and suggestions
- **Multiple Execution Modes**: Initial load, incremental, full refresh, and validation-only
- **Parallel Processing**: Smart dependency-aware parallel execution of independent steps
- **Detailed Reporting**: Comprehensive execution reports with metrics and timing
- **Validation Integration**: Built-in validation with configurable thresholds

Execution Modes:
    - INITIAL: First-time pipeline execution with full data processing
    - INCREMENTAL: Process only new data based on watermark columns
    - FULL_REFRESH: Reprocess all data, overwriting existing results
    - VALIDATION_ONLY: Validate data without writing results

Parallel Execution:
    The engine automatically analyzes step dependencies and executes independent steps
    in parallel across all layers (Bronze, Silver, Gold). Configure parallelism via
    PipelineConfig.parallel settings:

    - parallel.enabled: Enable/disable parallel execution (default: True)
    - parallel.max_workers: Maximum concurrent workers (default: 4)

    Example with parallel execution:
        >>> config = PipelineConfig.create_default(schema="my_schema")
        >>> # Parallel enabled by default with 4 workers
        >>> engine = ExecutionEngine(spark, config)

        >>> # For high-performance scenarios
        >>> config = PipelineConfig.create_high_performance(schema="my_schema")
        >>> # Uses 16 workers

        >>> # For sequential execution
        >>> config = PipelineConfig.create_conservative(schema="my_schema")
        >>> # Disables parallel execution

Example:
    >>> from the framework.execution import ExecutionEngine, ExecutionMode
    >>> from the framework.models import BronzeStep, PipelineConfig
    >>> from pipeline_builder.functions import get_default_functions
    >>>
    >>> # Create execution engine
    >>> engine = ExecutionEngine(spark, config)
    >>>
    >>> # Execute a single step
    >>> result = engine.execute_step(
    ...     step=BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]}),
    ...     sources={"events": source_df},
    ...     mode=ExecutionMode.INITIAL
    ... )
    >>>
    >>> # Execute entire pipeline with parallel execution
    >>> result = engine.execute_pipeline(
    ...     steps=[bronze_step, silver_step, gold_step],
    ...     sources={"events": source_df},
    ...     mode=ExecutionMode.INITIAL
    ... )

# Depends on:
#   compat
#   dependencies
#   errors
#   functions
#   logging
#   models.pipeline
#   models.steps
#   table_operations
#   validation.data_validation
"""

from __future__ import annotations

import os
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union, cast

from pipeline_builder_base.dependencies import DependencyAnalyzer
from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionMode,
    PipelineConfig,
)

from .compat import AnalysisException, DataFrame, F, SparkSession
from .functions import FunctionsProtocol
from .models import BronzeStep, GoldStep, SilverStep
from .table_operations import fqn, prepare_delta_overwrite, table_exists, table_schema_is_empty
from .validation import apply_column_rules

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
    """
    Check if Delta Lake is actually available and working in the Spark session.

    This function checks configuration and optionally tests Delta functionality.
    Results are cached per Spark session for performance.

    Args:
        spark: Spark session to test

    Returns:
        True if Delta Lake is available and working, False otherwise
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


# Legacy function - use prepare_delta_overwrite from table_operations instead
def _prepare_delta_overwrite(
    spark: SparkSession,  # type: ignore[valid-type]
    table_name: str,
) -> None:
    """
    Legacy function - delegates to prepare_delta_overwrite() from table_operations.
    
    This function is kept for backward compatibility but now uses the centralized
    prepare_delta_overwrite() function from table_operations module.
    """
    # Only prepare if Delta is available
    if HAS_DELTA and _is_delta_lake_available_execution(spark):
        prepare_delta_overwrite(spark, table_name)


def _create_dataframe_writer(
    df: DataFrame,
    spark: SparkSession,  # type: ignore[valid-type]
    mode: str,
    table_name: Optional[str] = None,
    **options: Any,
) -> Any:
    """
    Create a DataFrameWriter using the standardized Delta overwrite pattern.
    
    For overwrite mode: uses format("delta").mode("overwrite").option("overwriteSchema", "true")
    Always uses Delta format - failures will propagate if Delta is not available.
    
    Args:
        df: DataFrame to write
        spark: Spark session
        mode: Write mode ("overwrite", "append", etc.)
        table_name: Optional table name for preparing Delta overwrite
        **options: Additional write options
    """
    # Use standardized overwrite pattern: overwrite + overwriteSchema
    if mode == "overwrite":
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
    else:
        # Append or other modes - always use Delta
        writer = df.write.format("delta").mode(mode)

    for key, value in options.items():
        writer = writer.option(key, value)

    return writer


def _get_existing_schema_safe(spark: Any, table_name: str) -> Optional[Any]:
    """
    Safely get the schema of an existing table.

    Tries multiple methods to get the schema:
    1. Direct schema from spark.table()
    2. If empty schema (catalog sync issue), try DESCRIBE TABLE
    3. If still empty, try reading a sample of data to infer schema

    Args:
        spark: Spark session
        table_name: Fully qualified table name

    Returns:
        StructType schema if table exists and schema is readable (may be empty struct<>), None if table doesn't exist or schema can't be read
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
    """
    Compare two schemas and determine if they match exactly.

    Args:
        existing_schema: Schema of the existing table
        output_schema: Schema of the output DataFrame

    Returns:
        Tuple of (matches: bool, differences: list[str])
        differences contains descriptions of any mismatches
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

    # Check for type mismatches in common columns
    common_columns = existing_columns & output_columns
    type_mismatches = []
    for col in common_columns:
        if existing_fields[col].dataType != output_fields[col].dataType:
            type_mismatches.append(
                f"{col}: existing={existing_fields[col].dataType}, "
                f"output={output_fields[col].dataType}"
            )
    if type_mismatches:
        differences.append(f"Type mismatches: {', '.join(type_mismatches)}")

    return len(differences) == 0, differences


def _recover_table_schema(spark: Any, table_name: str) -> Optional[Any]:
    """
    Attempt to recover table schema when catalog shows empty schema.

    Tries multiple methods:
    1. DESCRIBE TABLE and refresh, then re-read schema
    2. Read table DataFrame and use its schema (even if catalog shows empty)

    Args:
        spark: Spark session
        table_name: Fully qualified table name

    Returns:
        Recovered StructType schema or None if recovery fails
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
    """Step execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepType(Enum):
    """Types of pipeline steps."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class StepExecutionResult:
    """Result of step execution."""

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

    def __post_init__(self) -> None:
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


@dataclass
class ExecutionResult:
    """Result of pipeline execution."""

    execution_id: str
    mode: ExecutionMode
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    status: str = "running"
    steps: Optional[list[StepExecutionResult]] = None
    error: Optional[str] = None
    parallel_efficiency: float = 0.0
    execution_groups_count: int = 0
    max_group_size: int = 0

    def __post_init__(self) -> None:
        if self.steps is None:
            self.steps = []
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


class ExecutionEngine:
    """
    Simplified execution engine for the framework pipelines.

    This engine handles both individual step execution and full pipeline execution
    with a clean, unified interface.
    """

    def __init__(
        self,
        spark: SparkSession,  # type: ignore[valid-type]
        config: PipelineConfig,
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
    ):
        """
        Initialize the execution engine.

        Args:
            spark: Active SparkSession instance
            config: Pipeline configuration
            logger: Optional logger instance
            functions: Optional functions object for PySpark operations
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

    def _ensure_schema_exists(self, schema: str) -> None:
        """
        Ensure a schema exists, creating it if necessary.

        Args:
            schema: Schema name to create

        Raises:
            ExecutionError: If schema creation fails
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

    def _ensure_materialized_for_validation(
        self,
        df: DataFrame,  # type: ignore[valid-type]
        rules: Dict[str, Any],
    ) -> DataFrame:  # type: ignore[valid-type]
        """
        Force DataFrame materialization before validation to avoid CTE optimization issues.

        Mock-spark's CTE optimization can fail when validation rules reference columns
        created by transforms (via withColumn). By materializing the DataFrame first,
        we ensure all columns are available in the validation context.

        Args:
            df: DataFrame to potentially materialize
            rules: Validation rules dictionary

        Returns:
            Materialized DataFrame (or original if materialization not needed/available)
        """
        # Check if rules reference columns that might be new (not in original input)
        # Materialize before validation so downstream rules see all columns.
        if not rules:
            return df

        try:
            if hasattr(df, "cache"):
                df = df.cache()  # type: ignore[assignment]
            _ = df.count()  # type: ignore[attr-defined]
        except Exception as e:
            # Surface materialization problems instead of masking them
            self.logger.debug(f"Could not materialize DataFrame before validation: {e}")

        return df

    def execute_step(
        self,
        step: Union[BronzeStep, SilverStep] | GoldStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]
        mode: ExecutionMode = ExecutionMode.INITIAL,
        table_locks: Optional[Dict[str, threading.Lock]] = None,
        table_locks_lock: Optional[threading.Lock] = None,
    ) -> StepExecutionResult:
        """
        Execute a single pipeline step.

        Args:
            step: The step to execute
            context: Execution context with available DataFrames
            mode: Execution mode

        Returns:
            StepExecutionResult with execution details
        """
        start_time = datetime.now()
        # Determine step type based on class
        if isinstance(step, BronzeStep):
            step_type = StepType.BRONZE
        elif isinstance(step, SilverStep):
            step_type = StepType.SILVER
        elif isinstance(step, GoldStep):
            step_type = StepType.GOLD
        else:
            raise ValueError(f"Unknown step type: {type(step)}")

        result = StepExecutionResult(
            step_name=step.name,
            step_type=step_type,
            status=StepStatus.RUNNING,
            start_time=start_time,
        )

        try:
            # Use logger's step_start method for consistent formatting with emoji and uppercase
            self.logger.step_start(step_type.value, step.name)

            # Execute the step based on type
            output_df: DataFrame  # type: ignore[valid-type]
            if isinstance(step, BronzeStep):
                output_df = self._execute_bronze_step(step, context)
            elif isinstance(step, SilverStep):
                output_df = self._execute_silver_step(step, context, mode)
            elif isinstance(step, GoldStep):
                output_df = self._execute_gold_step(step, context)
            else:
                raise ExecutionError(f"Unknown step type: {type(step)}")

            # Apply validation if not in validation-only mode
            validation_rate = 100.0
            invalid_rows = 0
            if mode != ExecutionMode.VALIDATION_ONLY:
                # All step types (Bronze, Silver, Gold) have rules attribute
                if step.rules:
                    # CRITICAL: Force materialization before validation to avoid CTE optimization issues
                    # When transforms create new columns with withColumn(), mock-spark's CTE optimization
                    # can fail because those columns aren't visible in CTE context during validation.
                    # Materializing ensures all columns are available.
                    output_df = self._ensure_materialized_for_validation(
                        output_df, step.rules
                    )
                    output_df, _, validation_stats = apply_column_rules(
                        output_df,
                        step.rules,
                        "pipeline",
                        step.name,
                        functions=self.functions,
                    )
                    # Capture validation stats for logging (handle different return types for test mocking)
                    if validation_stats is not None:
                        validation_rate = getattr(
                            validation_stats, "validation_rate", 100.0
                        )
                        invalid_rows = getattr(validation_stats, "invalid_rows", 0)

            # Write output if not in validation-only mode
            # Note: Bronze steps only validate data, they don't write to tables
            if mode != ExecutionMode.VALIDATION_ONLY and not isinstance(
                step, BronzeStep
            ):
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
                # - INITIAL mode uses append (Delta tables don't support overwrite with saveAsTable)
                # - FULL_REFRESH uses overwrite (will use DELETE + append for Delta tables)
                if isinstance(step, GoldStep):
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
                if mode == ExecutionMode.INITIAL and isinstance(
                    step, (SilverStep, GoldStep)
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
                    if isinstance(step, GoldStep):
                        should_apply_schema_override = True
                    elif isinstance(step, SilverStep):
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
                                # Write with append mode and overwriteSchema option
                                try:
                                    (
                                        output_df.write.mode("append")
                                        .option("overwriteSchema", "true")
                                        .saveAsTable(output_table)  # type: ignore[attr-defined]
                                    )
                                except Exception as write_error:
                                    # Handle race condition where table might be created by another thread
                                    error_msg = str(write_error).lower()
                                    if (
                                        "already exists" in error_msg
                                        or "table_or_view_already_exists" in error_msg
                                        or isinstance(write_error, AnalysisException)
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
                                        or isinstance(write_error, AnalysisException)
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
                                    or isinstance(write_error, AnalysisException)
                                ):
                                    # Table was created by another thread - verify it exists and retry
                                    if table_exists(self.spark, output_table):
                                        self.logger.debug(
                                            f"Table {output_table} was created by another thread, retrying with append mode"
                                        )
                                        # Retry with append mode and overwriteSchema
                                        retry_writer = _create_dataframe_writer(
                                            output_df,
                                            self.spark,
                                            "append",
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
                                    # Schema matches - use DELETE + append for Delta tables
                                    # or CREATE OR REPLACE TABLE for atomic replacement
                                    if _is_delta_lake_available_execution(self.spark):
                                        # For Delta tables, use DELETE + append
                                        try:
                                            self.spark.sql(
                                                f"DELETE FROM {output_table}"
                                            )  # type: ignore[attr-defined]
                                            # DELETE succeeded, use append mode
                                            writer = _create_dataframe_writer(
                                                output_df, self.spark, "append", table_name=output_table
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
                                            output_df, self.spark, "overwrite", table_name=output_table
                                        )
                        else:
                            # Table doesn't exist - proceed with normal write
                            writer = _create_dataframe_writer(
                                output_df, self.spark, write_mode_str, table_name=output_table
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

                    if isinstance(step, GoldStep) and write_mode_str == "overwrite":
                        # Gold steps always use overwrite, but Delta tables don't support overwrite with saveAsTable
                        # Use DELETE + append or DROP + CREATE for Delta tables
                        if _is_delta_lake_available_execution(self.spark):
                            if table_exists(self.spark, output_table):
                                # Table exists - use DELETE + append
                                try:
                                    self.spark.sql(f"DELETE FROM {output_table}")  # type: ignore[attr-defined]
                                    # DELETE succeeded, use append mode
                                    writer = _create_dataframe_writer(
                                        output_df, self.spark, "append", table_name=output_table
                                    )
                                except Exception as delete_error:
                                    # If DELETE fails, fall back to DROP + CREATE
                                    self.logger.warning(
                                        f"DELETE FROM failed for '{output_table}': {delete_error}. "
                                        f"Using DROP + CREATE instead."
                                    )
                                    temp_view_name = (
                                        f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                    )
                                    output_df.createOrReplaceTempView(temp_view_name)  # type: ignore[attr-defined]
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
                                    writer = None
                            else:
                                # Table doesn't exist - use append to create it
                                writer = _create_dataframe_writer(
                                    output_df, self.spark, "append", table_name=output_table
                                )
                        else:
                            # Not Delta table, use normal overwrite
                            writer = _create_dataframe_writer(
                                output_df, self.spark, "overwrite", table_name=output_table
                            )
                    else:
                        # For INCREMENTAL and FULL_REFRESH modes (non-Gold), schema validation already done above
                        # Just create writer with appropriate mode
                        writer = _create_dataframe_writer(
                            output_df, self.spark, write_mode_str, table_name=output_table
                        )

                    # Execute write
                    if writer is not None:
                        # Use table-level lock to serialize writes to the same table
                        # This ensures that when multiple steps write to the same table, they do so sequentially
                        table_lock = None
                        if table_locks is not None and table_locks_lock is not None:
                            # Get or create lock for this table
                            with table_locks_lock:
                                if output_table not in table_locks:
                                    table_locks[output_table] = threading.Lock()
                                table_lock = table_locks[output_table]

                        # Acquire table lock before writing (if available)
                        if table_lock is not None:
                            table_lock.acquire()

                        try:
                            # For overwrite mode with Delta, always drop existing table right before write
                            # This avoids "truncate in batch mode" errors
                            # We drop unconditionally because Spark may create the table during the write
                            # and then try to use truncate semantics, which Delta doesn't support
                            if write_mode_str == "overwrite" and _is_delta_lake_available_execution(self.spark):
                                # Clear catalog cache to ensure we have fresh table metadata
                                # This prevents stale metadata from causing conflicts in parallel execution
                                try:
                                    self.spark.catalog.clearCache()  # type: ignore[attr-defined]
                                except Exception:
                                    pass  # Ignore cache clearing errors
                                
                                # Always try to drop - if table doesn't exist, this is a no-op
                                try:
                                    self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                                    # Clear cache again after drop to ensure catalog is updated
                                    try:
                                        self.spark.catalog.clearCache()  # type: ignore[attr-defined]
                                    except Exception:
                                        pass
                                except Exception:
                                    # If drop fails, try the more sophisticated check
                                    _prepare_delta_overwrite(self.spark, output_table)
                            
                            writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                        except Exception as write_error:
                            # Handle "truncate in batch mode" errors for Delta tables
                            error_msg = str(write_error).lower()
                            truncate_handled = False
                            if "truncate in batch mode" in error_msg and _is_delta_lake_available_execution(self.spark):
                                # Delta table doesn't support truncate - drop and recreate
                                self.logger.warning(
                                    f"Delta table '{output_table}' doesn't support truncate. "
                                    f"Dropping and recreating with fresh data."
                                )
                                try:
                                    # Clear catalog cache before retry to ensure fresh metadata
                                    try:
                                        self.spark.catalog.clearCache()  # type: ignore[attr-defined]
                                    except Exception:
                                        pass
                                    
                                    # Drop table and recreate using CREATE TABLE AS SELECT
                                    # This avoids truncate semantics entirely
                                    self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                                    
                                    # Clear cache again after drop
                                    try:
                                        self.spark.catalog.clearCache()  # type: ignore[attr-defined]
                                    except Exception:
                                        pass
                                    
                                    temp_view_name = f"_temp_{step.name}_{uuid.uuid4().hex[:8]}"
                                    output_df.createOrReplaceTempView(temp_view_name)  # type: ignore[attr-defined]
                                    self.spark.sql(f"""
                                        CREATE TABLE {output_table}
                                        USING DELTA
                                        AS SELECT * FROM {temp_view_name}
                                    """)  # type: ignore[attr-defined]
                                    try:
                                        self.spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")  # type: ignore[attr-defined]
                                    except Exception:
                                        pass
                                    # Success - truncate error handled, don't process other errors
                                    truncate_handled = True
                                except Exception as retry_error:
                                    # If retry also fails, fall through to other error handling
                                    write_error = retry_error
                                    error_msg = str(retry_error).lower()
                            
                            # If we successfully handled the truncate error, skip other error handling
                            if truncate_handled:
                                pass  # Write succeeded, continue normally
                            # Handle catalog sync issues where Spark reports empty schema (struct<>)
                            elif (
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
                                    if isinstance(step, GoldStep):
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
                                or isinstance(write_error, AnalysisException)
                            ):
                                # Table was created by another thread - verify it exists and retry with append mode
                                if table_exists(self.spark, output_table):
                                    self.logger.debug(
                                        f"Table {output_table} was created by another thread, retrying with append mode"
                                    )
                                    # Retry with append mode to preserve data from both steps
                                    retry_writer = _create_dataframe_writer(
                                        output_df,
                                        self.spark,
                                        "append",
                                        table_name=output_table,
                                    )
                                    retry_writer.saveAsTable(output_table)  # type: ignore[attr-defined]
                                else:
                                    raise
                            else:
                                # Different error - re-raise
                                raise
                        finally:
                            # Release table lock after writing
                            if table_lock is not None:
                                table_lock.release()

                        # Refresh table metadata after write to ensure subsequent reads see the latest data
                        # This is especially important in parallel execution where multiple steps might write to the same table
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
            elif isinstance(step, BronzeStep):
                # Bronze steps only validate data, don't write to tables
                result.rows_processed = output_df.count()  # type: ignore[attr-defined]
                result.write_mode = None  # type: ignore[attr-defined]
            else:  # VALIDATION_ONLY mode
                # Validation-only mode doesn't write to tables
                result.rows_processed = output_df.count()  # type: ignore[attr-defined]
                result.write_mode = None  # type: ignore[attr-defined]

            result.status = StepStatus.COMPLETED
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()

            # Populate result fields
            rows_processed = result.rows_processed or 0
            # For Silver/Gold steps, rows_written equals rows_processed (since we write the output)
            # For Bronze steps, rows_written is None (they don't write to tables)
            rows_written = rows_processed if not isinstance(step, BronzeStep) else None

            result.rows_written = rows_written
            result.input_rows = rows_processed
            result.validation_rate = (
                validation_rate if validation_rate is not None else 100.0
            )

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
        steps: list[Union[BronzeStep, SilverStep] | GoldStep],
        mode: ExecutionMode = ExecutionMode.INITIAL,
        max_workers: int = 4,
        context: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
    ) -> ExecutionResult:
        """
        Execute a complete pipeline with smart dependency-aware parallel execution.

        This method automatically analyzes step dependencies and executes independent
        steps in parallel across all layers (Bronze, Silver, Gold). Steps within each
        execution group run concurrently using ThreadPoolExecutor, while groups are
        executed sequentially to respect dependencies.

        Parallelism is controlled by the PipelineConfig.parallel settings:
        - If parallel.enabled is True, uses parallel.max_workers (default: 4)
        - If parallel.enabled is False, executes sequentially (max_workers=1)
        - The max_workers parameter is ignored; config settings take precedence

        Args:
            steps: List of steps to execute
            mode: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH, VALIDATION_ONLY)
            max_workers: Deprecated - use PipelineConfig.parallel.max_workers instead
            context: Optional initial execution context with DataFrames

        Returns:
            ExecutionResult with execution details and parallel execution metrics

        Example:
            >>> # Default config enables parallel execution with 4 workers
            >>> config = PipelineConfig.create_default(schema="my_schema")
            >>> engine = ExecutionEngine(spark, config)
            >>> result = engine.execute_pipeline(steps=[bronze, silver1, silver2, gold])
            >>> print(f"Parallel efficiency: {result.parallel_efficiency:.2f}")
            >>> print(f"Execution groups: {result.execution_groups_count}")
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
            # Ensure all required schemas exist before parallel execution
            # This MUST happen in the main thread before any worker threads start
            # Collect unique schemas from all steps
            required_schemas = set()
            for step in steps:
                if hasattr(step, "schema") and step.schema:  # type: ignore[attr-defined]
                    schema_value = step.schema  # type: ignore[attr-defined]
                    # Handle both string schemas and Mock objects (for tests)
                    if isinstance(schema_value, str):
                        required_schemas.add(schema_value)
            # Create all required schemas upfront - always try to create, don't rely on catalog checks
            # This ensures schemas are available before worker threads start
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
            bronze_steps = [s for s in steps if isinstance(s, BronzeStep)]
            silver_steps = [s for s in steps if isinstance(s, SilverStep)]
            gold_steps = [s for s in steps if isinstance(s, GoldStep)]

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

            # Determine worker count from config
            # After PipelineConfig.__post_init__, parallel is always ParallelConfig
            # But handle mocked configs gracefully
            from .models import ParallelConfig

            if isinstance(self.config.parallel, ParallelConfig):
                if self.config.parallel.enabled:
                    workers = self.config.parallel.max_workers
                    self.logger.info(
                        f"Parallel execution enabled with {workers} workers"
                    )
                else:
                    workers = 1
                    self.logger.info("Sequential execution mode")
            elif hasattr(self.config.parallel, "enabled"):
                # Handle Mock or other types with enabled attribute
                enabled = getattr(self.config.parallel, "enabled", True)
                if enabled:
                    workers = getattr(self.config.parallel, "max_workers", 4)
                else:
                    workers = 1
            else:
                # Fallback for tests with mock configs
                workers = 1
                self.logger.info("Sequential execution mode (default)")

            # Thread-safe context management
            context_lock = threading.Lock()

            # Table-level locks to serialize writes to the same table
            # This prevents race conditions when multiple steps write to the same table in parallel
            table_locks: Dict[str, threading.Lock] = {}
            table_locks_lock = (
                threading.Lock()
            )  # Lock for the table_locks dictionary itself

            # Create a mapping of step names to step objects
            step_map = {s.name: s for s in steps}

            # Track timing for parallel efficiency calculation
            group_timings = []

            # Execute each group in parallel
            for group_idx, group in enumerate(execution_groups):
                group_start = datetime.now()
                self.logger.info(
                    f"Executing group {group_idx + 1}/{len(execution_groups)}: "
                    f"{len(group)} steps - {', '.join(group)}"
                )

                if workers > 1:
                    # Parallel execution
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {}
                        for step_name in group:
                            if step_name not in step_map:
                                self.logger.warning(
                                    f"Step {step_name} in execution group but not found in step list"
                                )
                                continue

                            step = step_map[step_name]
                            future = executor.submit(
                                self._execute_step_safe,
                                step,
                                context,
                                mode,
                                context_lock,
                                table_locks,
                                table_locks_lock,
                            )
                            futures[future] = step_name

                        # Wait for all steps in group to complete
                        for future in as_completed(futures):
                            step_name = futures[future]
                            try:
                                step_result = future.result()
                                if result.steps is not None:
                                    result.steps.append(step_result)

                                if step_result.status == StepStatus.FAILED:
                                    self.logger.error(
                                        f"Step {step_name} failed: {step_result.error}"
                                    )
                            except Exception as e:
                                self.logger.error(
                                    f"Exception executing step {step_name}: {e}"
                                )
                                # Determine correct step type
                                step_obj = step_map.get(step_name)
                                if step_obj is not None and isinstance(
                                    step_obj, BronzeStep
                                ):
                                    step_type_enum = StepType.BRONZE
                                elif step_obj is not None and isinstance(
                                    step_obj, SilverStep
                                ):
                                    step_type_enum = StepType.SILVER
                                elif step_obj is not None and isinstance(
                                    step_obj, GoldStep
                                ):
                                    step_type_enum = StepType.GOLD
                                else:
                                    step_type_enum = StepType.BRONZE  # fallback

                                # Create failed step result
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
                else:
                    # Sequential execution (workers == 1)
                    for step_name in group:
                        if step_name not in step_map:
                            self.logger.warning(
                                f"Step {step_name} in execution group but not found in step list"
                            )
                            continue

                        step = step_map[step_name]
                        try:
                            step_result = self._execute_step_safe(
                                step,
                                context,
                                mode,
                                context_lock,
                                table_locks,
                                table_locks_lock,
                            )
                            if result.steps is not None:
                                result.steps.append(step_result)

                            if step_result.status == StepStatus.FAILED:
                                self.logger.error(
                                    f"Step {step_name} failed: {step_result.error}"
                                )
                        except Exception as e:
                            self.logger.error(
                                f"Exception executing step {step_name}: {e}"
                            )
                            # Determine correct step type
                            step_obj = step_map.get(step_name)
                            if step_obj is not None and isinstance(
                                step_obj, BronzeStep
                            ):
                                step_type_enum = StepType.BRONZE
                            elif step_obj is not None and isinstance(
                                step_obj, SilverStep
                            ):
                                step_type_enum = StepType.SILVER
                            elif step_obj is not None and isinstance(
                                step_obj, GoldStep
                            ):
                                step_type_enum = StepType.GOLD
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
                group_timings.append((len(group), group_duration))
                self.logger.info(
                    f"Group {group_idx + 1} completed in {group_duration:.2f}s"
                )

            # Calculate parallel efficiency
            if result.steps:
                total_step_time = sum(
                    s.duration for s in result.steps if s.duration is not None
                )
                total_wall_time = (datetime.now() - start_time).total_seconds()

                if total_wall_time > 0 and workers > 1:
                    # Efficiency = (total sequential time / total parallel time) / workers
                    # This gives a ratio of how well we utilized parallelism
                    ideal_parallel_time = total_step_time / workers
                    result.parallel_efficiency = min(
                        (ideal_parallel_time / total_wall_time) * 100, 100.0
                    )
                else:
                    result.parallel_efficiency = (
                        100.0  # Sequential execution is 100% efficient
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
                self.logger.info(
                    f"Completed pipeline execution: {execution_id} - "
                    f"Parallel efficiency: {result.parallel_efficiency:.1f}%"
                )

            result.end_time = datetime.now()

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            result.end_time = datetime.now()
            self.logger.error(f"Pipeline execution failed: {e}")
            raise ExecutionError(f"Pipeline execution failed: {e}") from e

        return result

    def _execute_step_safe(
        self,
        step: Union[BronzeStep, SilverStep] | GoldStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]
        mode: ExecutionMode,
        context_lock: threading.Lock,
        table_locks: Dict[str, threading.Lock],
        table_locks_lock: threading.Lock,
    ) -> StepExecutionResult:
        """
        Execute a step with thread-safe context access.

        This method wraps execute_step() to provide thread-safe access to the
        shared execution context when running steps in parallel.

        Args:
            step: The step to execute
            context: Shared execution context with available DataFrames
            mode: Execution mode
            context_lock: Threading lock for thread-safe context access

        Returns:
            StepExecutionResult with execution details
        """
        # Ensure schema exists in THIS worker thread before execution
        # Schema creation is serialized with a lock, and schemas are created right before saveAsTable.
        # This is just a safety check.
        if hasattr(step, "schema") and step.schema:  # type: ignore[attr-defined]
            with context_lock:
                # Try to ensure schema exists (serialized to avoid race conditions)
                schema_name = step.schema  # type: ignore[attr-defined]
                try:
                    # Use SQL to ensure schema exists (reliable for both PySpark and mock-spark)
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")  # type: ignore[attr-defined]
                except Exception as e:
                    self.logger.debug(
                        f"Schema '{schema_name}' creation in worker thread (non-critical): {e}"
                    )

        # Read from context with lock to get a snapshot
        with context_lock:
            local_context = dict(context)

        # Execute step (this can happen in parallel without lock)
        # Pass table locks to serialize writes to the same table
        result = self.execute_step(
            step, local_context, mode, table_locks, table_locks_lock
        )

        # Write to context with lock (for Silver/Gold steps that write tables)
        if result.status == StepStatus.COMPLETED and not isinstance(step, BronzeStep):
            with context_lock:
                # Get table name and schema from step
                table_name = getattr(step, "table_name", step.name)
                schema = getattr(step, "schema", None)

                if schema is not None:
                    # Try to use the output DataFrame directly from the step execution
                    # This avoids reading from table files which might be in flux during parallel execution
                    table_fqn = fqn(schema, table_name)

                    # First, try to get the output DataFrame from the step execution
                    # We need to re-execute the step to get the output DataFrame, but that's expensive
                    # Instead, try to read from table with refresh, and cache it
                    try:
                        # Refresh table first to ensure we see the latest data
                        try:
                            self.spark.sql(f"REFRESH TABLE {table_fqn}")  # type: ignore[attr-defined]
                        except Exception:
                            pass  # Refresh might fail for some table types - continue anyway

                        # Read table and cache it to avoid re-reading from files
                        table_df = self.spark.table(table_fqn)  # type: ignore[attr-defined,valid-type]
                        # Cache the DataFrame so it doesn't try to re-read from files that might be deleted
                        table_df.cache()  # type: ignore[attr-defined]
                        context[step.name] = table_df  # type: ignore[valid-type]
                    except Exception as e:
                        # If reading fails, log warning but don't fail the step
                        # The table was written successfully, it just can't be added to context
                        self.logger.warning(
                            f"Could not read table '{table_fqn}' immediately after writing. "
                            f"The table was written successfully but is not yet accessible. "
                            f"This may happen in parallel execution when multiple steps write to the same table. Error: {e}"
                        )
                        # Don't add to context - downstream steps will need to read the table directly
                else:
                    self.logger.warning(
                        f"Step '{step.name}' completed but has no schema. "
                        f"Cannot add to context for downstream steps."
                    )

        return result

    def _execute_bronze_step(
        self,
        step: BronzeStep,
        context: Dict[str, DataFrame],  # type: ignore[valid-type]  # type: ignore[valid-type]
    ) -> DataFrame:  # type: ignore[valid-type]
        """Execute a bronze step."""
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
        """Execute a silver step."""

        # Get source bronze data
        if step.source_bronze not in context:
            raise ExecutionError(
                f"Source bronze step {step.source_bronze} not found in context"
            )

        bronze_df: DataFrame = context[step.source_bronze]  # type: ignore[valid-type]

        if mode == ExecutionMode.INCREMENTAL:
            bronze_df = self._filter_incremental_bronze_input(step, bronze_df)

        # Apply transform with source bronze data and empty silvers dict
        return step.transform(self.spark, bronze_df, {})

    def _filter_incremental_bronze_input(
        self,
        step: SilverStep,
        bronze_df: DataFrame,  # type: ignore[valid-type]  # type: ignore[valid-type]
    ) -> DataFrame:  # type: ignore[valid-type]
        """
        Filter bronze input rows that were already processed in previous incremental runs.

        Uses the source bronze step's incremental column and the silver step's watermark
        column to eliminate rows whose incremental value is less than or equal to the
        last processed watermark.
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
            raise ExecutionError(
                f"Silver step {step.name}: failed to filter bronze rows using "
                f"{incremental_col} > {cutoff_value}: {exc!r}"
            ) from exc

        self.logger.info(
            f"Silver step {step.name}: filtering bronze rows where "
            f"{incremental_col} <= {cutoff_value}"
        )
        return filtered_df

    @staticmethod
    def _extract_row_value(row: Any, column: str) -> Optional[object]:
        """Safely extract a column value from a Row-like object."""
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
        """Execute a gold step."""

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
