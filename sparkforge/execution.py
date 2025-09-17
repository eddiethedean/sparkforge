"""
Simplified execution system for SparkForge pipelines.

This module provides a clean, consolidated execution engine that replaces
the complex multi-file execution system with a single, maintainable solution.
"""

from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession

from .errors import ExecutionError, ValidationError
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
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    error: Optional[str] = None
    rows_processed: Optional[int] = None
    output_table: Optional[str] = None

    def __post_init__(self):
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
    steps: List[StepExecutionResult] = None
    error: Optional[str] = None

    def __post_init__(self):
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
        logger: Optional[PipelineLogger] = None
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
        self.logger = logger or PipelineLogger()

    def execute_step(
        self,
        step: Union[BronzeStep, SilverStep, GoldStep],
        context: Dict[str, DataFrame],
        mode: ExecutionMode = ExecutionMode.INITIAL
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
            start_time=start_time
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
            else:
                raise ExecutionError(f"Unknown step type: {type(step)}")
            
            # Apply validation if not in validation-only mode
            if mode != ExecutionMode.VALIDATION_ONLY:
                if hasattr(step, 'column_rules') and step.column_rules:
                    output_df = apply_column_rules(output_df, step.column_rules)
            
            # Write output if not in validation-only mode
            if mode != ExecutionMode.VALIDATION_ONLY:
                output_table = fqn(step.schema, step.table)
                output_df.write.mode("overwrite").saveAsTable(output_table)
                result.output_table = output_table
                result.rows_processed = output_df.count()
            
            result.status = StepStatus.COMPLETED
            result.end_time = datetime.now()
            
            self.logger.info(f"Completed {step_type.value} step: {step.name}")
            
        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = str(e)
            result.end_time = datetime.now()
            self.logger.error(f"Failed {step_type.value} step {step.name}: {e}")
            raise ExecutionError(f"Step execution failed: {e}") from e
        
        return result

    def execute_pipeline(
        self,
        steps: List[Union[BronzeStep, SilverStep, GoldStep]],
        mode: ExecutionMode = ExecutionMode.INITIAL,
        max_workers: int = 4
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
            status="running"
        )
        
        try:
            self.logger.info(f"Starting pipeline execution: {execution_id}")
            
            # Group steps by type for execution
            bronze_steps = [s for s in steps if isinstance(s, BronzeStep)]
            silver_steps = [s for s in steps if isinstance(s, SilverStep)]
            gold_steps = [s for s in steps if isinstance(s, GoldStep)]
            
            context = {}
            
            # Execute bronze steps first
            for step in bronze_steps:
                step_result = self.execute_step(step, context, mode)
                result.steps.append(step_result)
                if step_result.status == StepStatus.COMPLETED:
                    context[step.name] = self.spark.table(fqn(step.schema, step.table))
            
            # Execute silver steps
            for step in silver_steps:
                step_result = self.execute_step(step, context, mode)
                result.steps.append(step_result)
                if step_result.status == StepStatus.COMPLETED:
                    context[step.name] = self.spark.table(fqn(step.schema, step.table))
            
            # Execute gold steps
            for step in gold_steps:
                step_result = self.execute_step(step, context, mode)
                result.steps.append(step_result)
                if step_result.status == StepStatus.COMPLETED:
                    context[step.name] = self.spark.table(fqn(step.schema, step.table))
            
            result.status = "completed"
            result.end_time = datetime.now()
            
            self.logger.info(f"Completed pipeline execution: {execution_id}")
            
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            result.end_time = datetime.now()
            self.logger.error(f"Pipeline execution failed: {e}")
            raise ExecutionError(f"Pipeline execution failed: {e}") from e
        
        return result

    def _execute_bronze_step(self, step: BronzeStep, context: Dict[str, DataFrame]) -> DataFrame:
        """Execute a bronze step."""
        if not step.source_path:
            raise ExecutionError("Bronze step must have source_path")
        
        # Read from source
        df = self.spark.read.format(step.source_format or "parquet").load(step.source_path)
        
        # Apply transform if provided
        if step.transform:
            df = step.transform(self.spark, df)
        
        return df

    def _execute_silver_step(self, step: SilverStep, context: Dict[str, DataFrame]) -> DataFrame:
        """Execute a silver step."""
        if not step.dependencies:
            raise ExecutionError("Silver step must have dependencies")
        
        # Get dependency data
        dependency_dfs = {}
        for dep in step.dependencies:
            if dep.step_name not in context:
                raise ExecutionError(f"Dependency {dep.step_name} not found in context")
            dependency_dfs[dep.step_name] = context[dep.step_name]
        
        # Apply transform
        if not step.transform:
            raise ExecutionError("Silver step must have transform function")
        
        return step.transform(self.spark, dependency_dfs)

    def _execute_gold_step(self, step: GoldStep, context: Dict[str, DataFrame]) -> DataFrame:
        """Execute a gold step."""
        if not step.dependencies:
            raise ExecutionError("Gold step must have dependencies")
        
        # Get dependency data
        dependency_dfs = {}
        for dep in step.dependencies:
            if dep.step_name not in context:
                raise ExecutionError(f"Dependency {dep.step_name} not found in context")
            dependency_dfs[dep.step_name] = context[dep.step_name]
        
        # Apply transform
        if not step.transform:
            raise ExecutionError("Gold step must have transform function")
        
        return step.transform(self.spark, dependency_dfs)


# Backward compatibility aliases
UnifiedExecutionEngine = ExecutionEngine
UnifiedStepExecutionResult = StepExecutionResult
