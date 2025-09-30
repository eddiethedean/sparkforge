"""
Production-ready execution system for SparkForge pipelines.

This module provides a robust execution engine that handles pipeline execution
with comprehensive error handling, step-by-step processing, and detailed reporting.

Key Features:
- **Step-by-Step Execution**: Process pipeline steps individually with detailed tracking
- **Comprehensive Error Handling**: Detailed error messages with context and suggestions
- **Multiple Execution Modes**: Initial load, incremental, full refresh, and validation-only
- **Parallel Processing**: Support for parallel execution of independent steps
- **Detailed Reporting**: Comprehensive execution reports with metrics and timing
- **Validation Integration**: Built-in validation with configurable thresholds

Execution Modes:
    - INITIAL: First-time pipeline execution with full data processing
    - INCREMENTAL: Process only new data based on watermark columns
    - FULL_REFRESH: Reprocess all data, overwriting existing results
    - VALIDATION_ONLY: Validate data without writing results

Example:
    >>> from sparkforge.execution import ExecutionEngine, ExecutionMode
    >>> from sparkforge.models import BronzeStep, PipelineConfig
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
    >>> # Execute entire pipeline
    >>> result = engine.execute_pipeline(
    ...     steps=[bronze_step, silver_step, gold_step],
    ...     sources={"events": source_df},
    ...     mode=ExecutionMode.INITIAL
    ... )
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from .errors import ExecutionError
from .logging import PipelineLogger
from .models import BronzeStep, GoldStep, PipelineConfig, SilverStep
from .table_operations import fqn
from .validation import apply_column_rules


class ExecutionMode(Enum):
    """Pipeline execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    VALIDATION_ONLY = "validation_only"


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

    def __post_init__(self) -> None:
        if self.steps is None:
            self.steps = []
        if self.end_time and self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


class ExecutionEngine:
    """
    Simplified execution engine for SparkForge pipelines.

    This engine handles both individual step execution and full pipeline execution
    with a clean, unified interface.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        logger: PipelineLogger | None = None,
    ):
        """
        Initialize the execution engine.

        Args:
            spark: Active SparkSession instance
            config: Pipeline configuration
            logger: Optional logger instance
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = PipelineLogger()
        else:
            self.logger = logger

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
            self.logger.info(f"Executing {step_type.value} step: {step.name}")

            # Execute the step based on type
            if isinstance(step, BronzeStep):
                output_df = self._execute_bronze_step(step, context)
            elif isinstance(step, SilverStep):
                output_df = self._execute_silver_step(step, context)
            elif isinstance(step, GoldStep):
                output_df = self._execute_gold_step(step, context)

            # Apply validation if not in validation-only mode
            if mode != ExecutionMode.VALIDATION_ONLY:
                # All step types (Bronze, Silver, Gold) have rules attribute
                if step.rules:
                    output_df, _, _ = apply_column_rules(
                        output_df, step.rules, "pipeline", step.name
                    )

            # Write output if not in validation-only mode
            # Note: Bronze steps only validate data, they don't write to tables
            if mode != ExecutionMode.VALIDATION_ONLY and not isinstance(step, BronzeStep):
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
                output_df.write.mode("overwrite").saveAsTable(output_table)
                result.output_table = output_table
                result.rows_processed = output_df.count()
            elif isinstance(step, BronzeStep):
                # Bronze steps only validate data, don't write to tables
                result.rows_processed = output_df.count()

            result.status = StepStatus.COMPLETED
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()

            self.logger.info(f"Completed {step_type.value} step: {step.name}")

        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = str(e)
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()
            self.logger.error(f"Failed {step_type.value} step {step.name}: {e}")
            raise ExecutionError(f"Step execution failed: {e}") from e

        return result

    def execute_pipeline(
        self,
        steps: list[BronzeStep | SilverStep | GoldStep],
        mode: ExecutionMode = ExecutionMode.INITIAL,
        max_workers: int = 4,
        context: dict[str, DataFrame] | None = None,
    ) -> ExecutionResult:
        """
        Execute a complete pipeline.

        Args:
            steps: List of steps to execute
            mode: Execution mode
            max_workers: Maximum number of parallel workers

        Returns:
            ExecutionResult with execution details
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
            self.logger.info(f"Starting pipeline execution: {execution_id}")

            # Group steps by type for execution
            bronze_steps = [s for s in steps if isinstance(s, BronzeStep)]
            silver_steps = [s for s in steps if isinstance(s, SilverStep)]
            gold_steps = [s for s in steps if isinstance(s, GoldStep)]

            # Validate context parameter
            if context is None:
                context = {}
            elif not isinstance(context, dict):
                raise TypeError(f"context must be a dictionary, got {type(context)}")

            # Execute bronze steps first
            for step in bronze_steps:
                try:
                    step_result = self.execute_step(step, context, mode)
                except Exception as e:
                    # Create a failed step result for tracking
                    step_result = StepExecutionResult(
                        step_name=step.name,
                        step_type=StepType.BRONZE,
                        status=StepStatus.FAILED,
                        error=str(e),
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                        duration=0.0,
                    )
                
                if result.steps is not None:
                    result.steps.append(step_result)
                    
                if step_result.status == StepStatus.COMPLETED:
                    # Bronze steps don't write to tables, they only validate data
                    # The validated data is available in the step result's output_df
                    # For now, we'll skip adding to context since bronze steps are validation-only
                    # In a real pipeline, you might want to store the validated data somewhere
                    pass

            # Execute silver steps
            for silver_step in silver_steps:
                try:
                    step_result = self.execute_step(silver_step, context, mode)
                except Exception as e:
                    # Create a failed step result for tracking
                    step_result = StepExecutionResult(
                        step_name=silver_step.name,
                        step_type=StepType.SILVER,
                        status=StepStatus.FAILED,
                        error=str(e),
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                        duration=0.0,
                    )
                
                if result.steps is not None:
                    result.steps.append(step_result)
                    
                if step_result.status == StepStatus.COMPLETED:
                    table_name = getattr(silver_step, "table_name", silver_step.name)
                    schema = getattr(silver_step, "schema", None)
                    
                    # Validate schema is provided
                    if schema is None:
                        self.logger.error(
                            f"Silver step '{silver_step.name}' completed but has no schema. "
                            f"Cannot read from table. Skipping context update."
                        )
                    else:
                        context[silver_step.name] = self.spark.table(
                            fqn(schema, table_name)
                        )

            # Execute gold steps
            for gold_step in gold_steps:
                try:
                    step_result = self.execute_step(gold_step, context, mode)
                except Exception as e:
                    # Create a failed step result for tracking
                    step_result = StepExecutionResult(
                        step_name=gold_step.name,
                        step_type=StepType.GOLD,
                        status=StepStatus.FAILED,
                        error=str(e),
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                        duration=0.0,
                    )
                
                if result.steps is not None:
                    result.steps.append(step_result)
                    
                if step_result.status == StepStatus.COMPLETED:
                    table_name = getattr(gold_step, "table_name", gold_step.name)
                    schema = getattr(gold_step, "schema", None)
                    
                    # Validate schema is provided
                    if schema is None:
                        self.logger.error(
                            f"Gold step '{gold_step.name}' completed but has no schema. "
                            f"Cannot read from table. Skipping context update."
                        )
                    else:
                        context[gold_step.name] = self.spark.table(fqn(schema, table_name))

            # Determine overall pipeline status based on step results
            if result.steps is None:
                result.steps = []
            steps = result.steps
            failed_steps = [s for s in steps if s.status == StepStatus.FAILED]
            
            if failed_steps:
                result.status = "failed"
                self.logger.error(f"Pipeline execution failed: {len(failed_steps)} steps failed")
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
        self, step: BronzeStep, context: dict[str, DataFrame]
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
        self, step: SilverStep, context: dict[str, DataFrame]
    ) -> DataFrame:
        """Execute a silver step."""

        # Get source bronze data
        if step.source_bronze not in context:
            raise ExecutionError(
                f"Source bronze step {step.source_bronze} not found in context"
            )

        # Apply transform with source bronze data and empty silvers dict
        return step.transform(self.spark, context[step.source_bronze], {})

    def _execute_gold_step(
        self, step: GoldStep, context: dict[str, DataFrame]
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
