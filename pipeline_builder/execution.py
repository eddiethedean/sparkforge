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
    >>> from pyspark.sql import functions as F
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

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict

from .compat import DataFrame, F, SparkSession, is_mock_spark
from .functions import FunctionsProtocol
from .models import BronzeStep, GoldStep, SilverStep
from .table_operations import fqn
from .validation import apply_column_rules
from pipeline_builder_base.dependencies import DependencyAnalyzer
from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionContext,
    ExecutionMode,
    PipelineConfig,
    PipelineMetrics,
    StepResult,
)


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
    end_time: datetime | None = None
    duration: float | None = None
    error: str | None = None
    rows_processed: int | None = None
    output_table: str | None = None
    write_mode: str | None = None
    validation_rate: float = 100.0
    rows_written: int | None = None
    input_rows: int | None = None

    def __post_init__(self) -> None:
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


@dataclass
class ExecutionResult:
    """Result of pipeline execution."""

    execution_id: str
    mode: ExecutionMode
    start_time: datetime
    end_time: datetime | None = None
    duration: float | None = None
    status: str = "running"
    steps: list[StepExecutionResult] | None = None
    error: str | None = None
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
        spark: SparkSession,
        config: PipelineConfig,
        logger: PipelineLogger | None = None,
        functions: FunctionsProtocol | None = None,
    ):
        """
        Initialize the execution engine.

        Args:
            spark: Active SparkSession instance
            config: Pipeline configuration
            logger: Optional logger instance
            functions: Optional functions object for PySpark operations
        """
        self.spark = spark
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
            # Try using mock-spark storage API if available (for mock-spark compatibility)
            if hasattr(self.spark, "storage") and hasattr(
                self.spark.storage, "create_schema"
            ):
                try:
                    self.spark.storage.create_schema(schema)
                    # Verify it was created
                    databases = [db.name for db in self.spark.catalog.listDatabases()]
                    if schema in databases:
                        return  # Success
                    else:
                        raise ExecutionError(
                            f"Schema '{schema}' creation via storage API failed - schema not in catalog. "
                            f"Available databases: {databases}"
                        )
                except Exception as storage_error:
                    # If storage API fails, fall through to SQL approach
                    self.logger.debug(
                        f"Storage API schema creation failed: {storage_error}, trying SQL"
                    )

            # Fall back to SQL for real Spark or if storage API not available
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            # Verify it was created
            databases = [db.name for db in self.spark.catalog.listDatabases()]
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
        self, df: DataFrame, rules: Dict[str, Any]
    ) -> DataFrame:
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
        # For now, we'll materialize if rules exist and we're in mock-spark mode
        # This is a conservative approach to avoid CTE issues
        try:
            # Check if we're using mock-spark
            from pipeline_builder.compat import is_mock_spark

            if is_mock_spark() and rules:
                # Force full materialization by collecting and recreating DataFrame
                # This bypasses CTE optimization entirely
                try:
                    # Get schema first
                    schema = df.schema

                    # Collect data to force full materialization
                    # This bypasses CTE optimization in mock-spark
                    collected_data = df.collect()

                    # Convert Row objects to dictionaries to preserve column names
                    # This fixes a bug in mock-spark's Polars backend where Row objects
                    # lose column names during materialization after filter operations
                    if collected_data and hasattr(collected_data[0], "asDict"):
                        # Convert Row objects to dictionaries
                        dict_data = [row.asDict() for row in collected_data]
                    elif collected_data:
                        # Fallback: try to convert to dict if possible
                        try:
                            dict_data = [dict(row) for row in collected_data]
                        except (TypeError, ValueError):
                            # If conversion fails, use original data
                            dict_data = collected_data
                    else:
                        dict_data = collected_data

                    # Recreate DataFrame from dictionary data
                    # This ensures all columns are fully materialized with correct names
                    df = self.spark.createDataFrame(dict_data, schema)
                except Exception as e:
                    # If materialization fails, try alternative: just cache and count
                    try:
                        if hasattr(df, "cache"):
                            df = df.cache()
                        _ = df.count()  # Force evaluation
                    except Exception:
                        # If all materialization attempts fail, return original
                        # Validation will still be attempted
                        self.logger.debug(f"Could not materialize DataFrame: {e}")
                        pass
        except Exception:
            # If we can't determine mock-spark status or materialization fails,
            # return original DataFrame
            pass

        return df

    def execute_step(
        self,
        step: BronzeStep | SilverStep | GoldStep,
        context: Dict[str, DataFrame],
        mode: ExecutionMode = ExecutionMode.INITIAL,
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
            if isinstance(step, BronzeStep):
                output_df = self._execute_bronze_step(step, context)
            elif isinstance(step, SilverStep):
                output_df = self._execute_silver_step(step, context, mode)
            elif isinstance(step, GoldStep):
                output_df = self._execute_gold_step(step, context)

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

                # CRITICAL FIX for mock-spark threading issue:
                # DuckDB connections in worker threads don't see schemas created in other threads.
                # This issue persists in mock-spark 3.1.0 and earlier versions.
                # We MUST create schema in THIS thread's connection right before saveAsTable.
                # Try multiple methods to ensure schema is visible to this DuckDB connection.
                try:
                    # Method 1: Try SQL (most reliable for DuckDB in mock-spark)
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    # Method 2: Also try storage API if available (redundancy for mock-spark)
                    if hasattr(self.spark, "storage") and hasattr(
                        self.spark.storage, "create_schema"
                    ):
                        try:
                            self.spark.storage.create_schema(schema)
                        except Exception:
                            pass  # SQL might be enough, continue
                    # Method 3: Try catalog API as well
                    try:
                        self.spark.catalog.createDatabase(schema, ignoreIfExists=True)
                    except Exception:
                        pass  # SQL might be enough, continue
                except Exception as e:
                    # If all methods fail, raise error - schema creation is critical
                    raise ExecutionError(
                        f"Failed to create schema '{schema}' before table creation: {e}"
                    ) from e

                # Determine write mode
                # - Gold steps always use overwrite to prevent duplicate aggregates
                # - Silver steps append during incremental runs to preserve history
                # - All other modes overwrite
                if isinstance(step, GoldStep):
                    write_mode_str = "overwrite"
                elif mode == ExecutionMode.INCREMENTAL:
                    write_mode_str = "append"
                else:  # INITIAL or FULL_REFRESH
                    write_mode_str = "overwrite"

                output_df.write.mode(write_mode_str).saveAsTable(output_table)
                result.output_table = output_table
                result.rows_processed = output_df.count()

                # Set write mode in result for tracking
                result.write_mode = write_mode_str
            elif isinstance(step, BronzeStep):
                # Bronze steps only validate data, don't write to tables
                result.rows_processed = output_df.count()
                result.write_mode = None
            else:  # VALIDATION_ONLY mode
                # Validation-only mode doesn't write to tables
                result.rows_processed = output_df.count()
                result.write_mode = None

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
            self.logger.step_complete(
                step_type.value,
                step.name,
                result.duration,
                rows_processed=rows_processed,
                rows_written=rows_written,
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
        steps: list[BronzeStep | SilverStep | GoldStep],
        mode: ExecutionMode = ExecutionMode.INITIAL,
        max_workers: int = 4,
        context: Dict[str, DataFrame] | None = None,
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
            # Ensure all required schemas exist before parallel execution (required in mock-spark due to DuckDB threading)
            # This MUST happen in the main thread before any worker threads start
            # Collect unique schemas from all steps
            required_schemas = set()
            for step in steps:
                if hasattr(step, "schema") and step.schema:
                    schema_value = step.schema
                    # Handle both string schemas and Mock objects (for tests)
                    if isinstance(schema_value, str):
                        required_schemas.add(schema_value)
            # Create all required schemas upfront - always try to create, don't rely on catalog checks
            # This is critical for mock-spark where DuckDB connections in worker threads
            # don't see schemas created via catalog API, so we must create them in main thread first
            for schema in required_schemas:
                try:
                    # Always try to create schema - CREATE SCHEMA IF NOT EXISTS is idempotent
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
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
                                step, context, mode, context_lock
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
        step: BronzeStep | SilverStep | GoldStep,
        context: Dict[str, DataFrame],
        mode: ExecutionMode,
        context_lock: threading.Lock,
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
        # CRITICAL: Ensure schema exists in THIS worker thread before execution
        # mock-spark has DuckDB threading issues where schemas created in one thread
        # are not visible to DuckDB connections in other threads. We serialize schema creation
        # with a lock, but the real fix is in execute_step() where we CREATE SCHEMA right before saveAsTable.
        # This is just a safety check.
        if hasattr(step, "schema") and step.schema:
            with context_lock:
                # Try to ensure schema exists (serialized to avoid race conditions)
                try:
                    # Use SQL to ensure schema exists (more reliable than storage API in threads)
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {step.schema}")
                except Exception as e:
                    self.logger.debug(
                        f"Schema '{step.schema}' creation in worker thread (non-critical): {e}"
                    )

        # Read from context with lock to get a snapshot
        with context_lock:
            local_context = dict(context)

        # Execute step (this can happen in parallel without lock)
        result = self.execute_step(step, local_context, mode)

        # Write to context with lock (for Silver/Gold steps that write tables)
        if result.status == StepStatus.COMPLETED and not isinstance(step, BronzeStep):
            with context_lock:
                # Get table name and schema from step
                table_name = getattr(step, "table_name", step.name)
                schema = getattr(step, "schema", None)

                if schema is not None:
                    # Add the step's output table to context for downstream steps
                    context[step.name] = self.spark.table(fqn(schema, table_name))
                else:
                    self.logger.warning(
                        f"Step '{step.name}' completed but has no schema. "
                        f"Cannot add to context for downstream steps."
                    )

        return result

    def _execute_bronze_step(
        self, step: BronzeStep, context: Dict[str, DataFrame]
    ) -> DataFrame:
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

        df = context[step.name]

        # Validate that the DataFrame is not empty (optional check)
        if df.count() == 0:
            self.logger.warning(
                f"Bronze step '{step.name}' received empty DataFrame. "
                f"This may indicate missing or invalid data source."
            )

        return df

    def _execute_silver_step(
        self,
        step: SilverStep,
        context: Dict[str, DataFrame],
        mode: ExecutionMode,
    ) -> DataFrame:
        """Execute a silver step."""

        # Get source bronze data
        if step.source_bronze not in context:
            raise ExecutionError(
                f"Source bronze step {step.source_bronze} not found in context"
            )

        bronze_df = context[step.source_bronze]

        if mode == ExecutionMode.INCREMENTAL:
            bronze_df = self._filter_incremental_bronze_input(step, bronze_df)

        # Apply transform with source bronze data and empty silvers dict
        return step.transform(self.spark, bronze_df, {})

    def _filter_incremental_bronze_input(
        self, step: SilverStep, bronze_df: DataFrame
    ) -> DataFrame:
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
            existing_table = self.spark.table(output_table)
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
            watermark_rows = existing_table.select(watermark_col).collect()
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
            filtered_df = bronze_df.filter(F.col(incremental_col) > F.lit(cutoff_value))
        except Exception as exc:
            if self._using_mock_spark():
                mock_df = self._filter_bronze_rows_mock(
                    bronze_df, incremental_col, cutoff_value
                )
                if mock_df is not None:
                    self.logger.debug(
                        f"Silver step {step.name}: applied mock fallback filter "
                        f"for {incremental_col} > {cutoff_value}"
                    )
                    filtered_df = mock_df
                else:
                    self.logger.warning(
                        f"Silver step {step.name}: failed to filter bronze rows using "
                        f"{incremental_col} > {cutoff_value}: {exc!r}"
                    )
                    return bronze_df
            else:
                self.logger.warning(
                    f"Silver step {step.name}: failed to filter bronze rows using "
                    f"{incremental_col} > {cutoff_value}: {exc!r}"
                )
                return bronze_df

        self.logger.info(
            f"Silver step {step.name}: filtering bronze rows where "
            f"{incremental_col} <= {cutoff_value}"
        )
        return filtered_df

    def _using_mock_spark(self) -> bool:
        """Determine if current spark session is backed by mock-spark."""

        try:
            spark_module = type(self.spark).__module__
        except Exception:
            spark_module = ""
        return is_mock_spark() or "mock_spark" in spark_module

    def _filter_bronze_rows_mock(
        self, bronze_df: DataFrame, incremental_col: str, cutoff_value: object
    ) -> DataFrame | None:
        """
        Mock-spark fallback: collect rows and filter in-memory when column operations fail.
        """

        try:
            rows = bronze_df.collect()
            schema = bronze_df.schema
        except Exception:
            return None

        filtered_rows = []
        for row in rows:
            value = self._extract_row_value(row, incremental_col)
            if value is None:
                continue
            try:
                if value > cutoff_value:
                    filtered_rows.append(row)
            except Exception:
                continue

        if not filtered_rows:
            try:
                return bronze_df.limit(0)
            except Exception:
                pass
            return self.spark.createDataFrame([], schema)

        try:
            column_order: list[str] = []
            if hasattr(schema, "__iter__"):
                column_order = [getattr(field, "name", field) for field in schema]
            if not column_order and hasattr(schema, "fieldNames"):
                column_order = list(schema.fieldNames())
            if not column_order and hasattr(schema, "names"):
                column_order = list(schema.names)
            if not column_order:
                column_order = list(getattr(bronze_df, "columns", []))
            structured_rows = []
            for row in filtered_rows:
                if hasattr(row, "asDict"):
                    row_dict = row.asDict()
                    structured_rows.append(
                        tuple(row_dict.get(col) for col in column_order)
                    )
                else:
                    structured_rows.append(tuple(row))
            return self.spark.createDataFrame(
                structured_rows, schema, verifySchema=False
            )
        except Exception:
            return None

    @staticmethod
    def _extract_row_value(row: Any, column: str) -> object | None:
        """Safely extract a column value from a Row-like object."""
        if hasattr(row, "__getitem__"):
            try:
                return row[column]
            except Exception:
                try:
                    return row[0]
                except Exception:
                    pass
        if hasattr(row, "asDict"):
            try:
                return row.asDict().get(column)
            except Exception:
                return None
        return None

    def _execute_gold_step(
        self, step: GoldStep, context: Dict[str, DataFrame]
    ) -> DataFrame:
        """Execute a gold step."""

        # Build silvers dict from source_silvers
        silvers = {}
        if step.source_silvers is not None:
            for silver_name in step.source_silvers:
                if silver_name not in context:
                    raise ExecutionError(
                        f"Source silver {silver_name} not found in context"
                    )
                silvers[silver_name] = context[silver_name]

        return step.transform(self.spark, silvers)


# Backward compatibility aliases
UnifiedExecutionEngine = ExecutionEngine
UnifiedStepExecutionResult = StepExecutionResult
