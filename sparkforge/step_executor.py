#!/usr/bin/env python3
"""
Step-by-step execution utilities for troubleshooting.

This module provides the ability to run individual pipeline steps independently
for debugging and troubleshooting purposes.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
import time
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import DataFrame, SparkSession

from .models import (
    BronzeStep, SilverStep, GoldStep, PipelineConfig, ExecutionContext,
    StageStats, WriteMode
)
from .logger import PipelineLogger, ExecutionTimer
from .table_operations import fqn
from .performance import now_dt, time_write_operation
from .reporting import create_validation_dict, create_transform_dict, create_write_dict
from .validation import apply_column_rules
from .dependency_analyzer import DependencyAnalyzer


class StepType(Enum):
    """Types of pipeline steps."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class StepStatus(Enum):
    """Status of step execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StepValidationResult:
    """Result of step validation."""
    validation_passed: bool
    validation_rate: float
    total_rows: int
    valid_rows: int
    invalid_rows: int


@dataclass
class StepExecutionResult:
    """Result of executing a single step."""
    step_name: str
    step_type: StepType
    status: StepStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    input_data: Optional[DataFrame] = None
    output_data: Optional[DataFrame] = None
    output_count: Optional[int] = None
    validation_result: Optional[StepValidationResult] = None
    write_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    
    @property
    def is_successful(self) -> bool:
        """Check if step execution was successful."""
        return self.status == StepStatus.COMPLETED
    
    @property
    def is_failed(self) -> bool:
        """Check if step execution failed."""
        return self.status == StepStatus.FAILED
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            'step_name': self.step_name,
            'step_type': self.step_type.value,
            'status': self.status.value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'output_count': self.output_count,
            'validation_passed': self.validation_result.validation_passed if self.validation_result else None,
            'validation_rate': self.validation_result.validation_rate if self.validation_result else None,
            'error': self.error,
            'dependencies': self.dependencies
        }


class StepExecutor:
    """
    Executes individual pipeline steps for troubleshooting and debugging.
    
    Features:
    - Run individual Bronze, Silver, or Gold steps
    - Inspect intermediate outputs
    - Debug step configurations
    - Resume from any step
    - Dependency validation
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        logger: PipelineLogger,
        dependency_analyzer: DependencyAnalyzer
    ):
        self.spark = spark
        self.config = config
        self.bronze_steps = bronze_steps
        self.silver_steps = silver_steps
        self.gold_steps = gold_steps
        self.logger = logger
        self.dependency_analyzer = dependency_analyzer
        
        # Execution state for tracking
        self._execution_state: Dict[str, StepExecutionResult] = {}
        self._step_outputs: Dict[str, DataFrame] = {}
        
        self.logger.info("🔧 StepExecutor initialized for troubleshooting")
    
    def list_steps(self) -> Dict[str, List[str]]:
        """List all available steps by type."""
        return {
            'bronze': list(self.bronze_steps.keys()),
            'silver': list(self.silver_steps.keys()),
            'gold': list(self.gold_steps.keys())
        }
    
    def get_step_info(self, step_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a step."""
        # Check Bronze steps
        if step_name in self.bronze_steps:
            step = self.bronze_steps[step_name]
            return {
                'name': step_name,
                'type': 'bronze',
                'rules': step.rules,
                'incremental_col': step.incremental_col,
                'description': getattr(step, 'description', None),
                'dependencies': [],  # Bronze steps have no dependencies
                'dependents': self._get_bronze_dependents(step_name)
            }
        
        # Check Silver steps
        if step_name in self.silver_steps:
            step = self.silver_steps[step_name]
            return {
                'name': step_name,
                'type': 'silver',
                'rules': step.rules,
                'watermark_col': step.watermark_col,
                'table_name': step.table_name,
                'source_bronze': step.source_bronze,
                'description': getattr(step, 'description', None),
                'dependencies': [step.source_bronze] if step.source_bronze else [],
                'dependents': self._get_silver_dependents(step_name)
            }
        
        # Check Gold steps
        if step_name in self.gold_steps:
            step = self.gold_steps[step_name]
            return {
                'name': step_name,
                'type': 'gold',
                'rules': step.rules,
                'table_name': step.table_name,
                'source_silvers': step.source_silvers,
                'description': getattr(step, 'description', None),
                'dependencies': step.source_silvers or [],
                'dependents': []  # Gold steps are leaves
            }
        
        return None
    
    def execute_bronze_step(
        self,
        step_name: str,
        input_data: DataFrame,
        output_to_table: bool = True
    ) -> StepExecutionResult:
        """Execute a single Bronze step."""
        if step_name not in self.bronze_steps:
            raise ValueError(f"Bronze step '{step_name}' not found")
        
        step = self.bronze_steps[step_name]
        start_time = now_dt()
        
        try:
            self.logger.info(f"🔍 Executing Bronze step: {step_name}")
            
            # Apply validation rules
            valid_data, invalid_data, stats = apply_column_rules(
                df=input_data,
                rules=step.rules,
                stage="bronze",
                step=step_name
            )
            
            # Create validation result
            validation_result = StepValidationResult(
                validation_passed=stats.validation_rate >= 95.0,  # Use a reasonable threshold
                validation_rate=stats.validation_rate,
                total_rows=stats.total_rows,
                valid_rows=stats.valid_rows,
                invalid_rows=stats.invalid_rows
            )
            
            # For step-by-step execution, preserve all columns but filter to valid rows only
            # This ensures downstream steps have access to all necessary columns
            if validation_result.validation_passed:
                # If validation passed, use all columns from input but only valid rows
                valid_row_ids = valid_data.select("*").limit(0).union(valid_data.select("*"))
                pred = self._get_validation_predicate(step.rules)
                output_data = input_data.filter(pred)
            else:
                # If validation failed, still return the validated subset
                output_data = valid_data
            output_count = output_data.count()
            
            # Optionally write to table
            write_result = None
            if output_to_table:
                table_name = fqn(self.config.schema, step_name)
                write_result = self._write_bronze_data(output_data, table_name)
            
            # Create execution result
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.BRONZE,
                status=StepStatus.COMPLETED if validation_result.validation_passed else StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                output_data=output_data,
                output_count=output_count,
                validation_result=validation_result,
                write_result=write_result,
                dependencies=[]
            )
            
            # Store in execution state
            self._execution_state[step_name] = execution_result
            self._step_outputs[step_name] = output_data
            
            self.logger.info(f"✅ Bronze step '{step_name}' completed: {output_count} rows")
            return execution_result
            
        except Exception as e:
            error_msg = f"Bronze step '{step_name}' failed: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.BRONZE,
                status=StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                error=error_msg,
                dependencies=[]
            )
            
            self._execution_state[step_name] = execution_result
            return execution_result
    
    def execute_silver_step(
        self,
        step_name: str,
        input_data: Optional[DataFrame] = None,
        output_to_table: bool = True,
        force_input: bool = False
    ) -> StepExecutionResult:
        """Execute a single Silver step."""
        if step_name not in self.silver_steps:
            raise ValueError(f"Silver step '{step_name}' not found")
        
        step = self.silver_steps[step_name]
        start_time = now_dt()
        
        try:
            self.logger.info(f"🔍 Executing Silver step: {step_name}")
            
            # Get input data if not provided
            if input_data is None and not force_input:
                input_data = self._get_silver_input_data(step)
            
            if input_data is None:
                raise ValueError(f"No input data available for Silver step '{step_name}'. "
                               f"Dependencies: {step.source_bronze}")
            
            # Apply transform function if available
            output_data = input_data
            if hasattr(step, 'transform') and step.transform:
                try:
                    output_data = step.transform(self.spark, input_data, {})
                    if not isinstance(output_data, DataFrame):
                        raise ValueError("Transform function must return a DataFrame")
                except Exception as e:
                    self.logger.warning(f"Transform function failed for '{step_name}': {e}")
                    # Continue with input data
            
            # Apply validation rules
            valid_data, invalid_data, stats = apply_column_rules(
                df=output_data,
                rules=step.rules,
                stage="silver",
                step=step_name
            )
            
            # Create validation result
            validation_result = StepValidationResult(
                validation_passed=stats.validation_rate >= 98.0,  # Silver threshold
                validation_rate=stats.validation_rate,
                total_rows=stats.total_rows,
                valid_rows=stats.valid_rows,
                invalid_rows=stats.invalid_rows
            )
            
            # For step-by-step execution, preserve all columns but filter to valid rows only
            if validation_result.validation_passed:
                pred = self._get_validation_predicate(step.rules)
                output_data = output_data.filter(pred)
            else:
                # If validation failed, still return the validated subset
                output_data = valid_data
            output_count = output_data.count()
            
            # Optionally write to table
            write_result = None
            if output_to_table:
                table_name = fqn(self.config.schema, step.table_name)
                write_result = self._write_silver_data(output_data, table_name, step.watermark_col)
            
            # Create execution result
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.SILVER,
                status=StepStatus.COMPLETED if validation_result.validation_passed else StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                output_data=output_data,
                output_count=output_count,
                validation_result=validation_result,
                write_result=write_result,
                dependencies=[step.source_bronze] if step.source_bronze else []
            )
            
            # Store in execution state
            self._execution_state[step_name] = execution_result
            self._step_outputs[step_name] = output_data
            
            self.logger.info(f"✅ Silver step '{step_name}' completed: {output_count} rows")
            return execution_result
            
        except Exception as e:
            error_msg = f"Silver step '{step_name}' failed: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.SILVER,
                status=StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                error=error_msg,
                dependencies=[step.source_bronze] if step.source_bronze else []
            )
            
            self._execution_state[step_name] = execution_result
            return execution_result
    
    def execute_gold_step(
        self,
        step_name: str,
        input_data: Optional[DataFrame] = None,
        output_to_table: bool = True,
        force_input: bool = False
    ) -> StepExecutionResult:
        """Execute a single Gold step."""
        if step_name not in self.gold_steps:
            raise ValueError(f"Gold step '{step_name}' not found")
        
        step = self.gold_steps[step_name]
        start_time = now_dt()
        
        try:
            self.logger.info(f"🔍 Executing Gold step: {step_name}")
            
            # Get input data if not provided
            if input_data is None and not force_input:
                input_data = self._get_gold_input_data(step)
            
            if input_data is None:
                raise ValueError(f"No input data available for Gold step '{step_name}'. "
                               f"Dependencies: {step.source_silvers}")
            
            # Apply transform function if available
            output_data = input_data
            if hasattr(step, 'transform') and step.transform:
                try:
                    # For Gold steps, collect all Silver outputs as dict
                    silvers = {name: self._step_outputs[name] for name in (step.source_silvers or [])
                             if name in self._step_outputs}
                    output_data = step.transform(self.spark, silvers)
                    if not isinstance(output_data, DataFrame):
                        raise ValueError("Transform function must return a DataFrame")
                except Exception as e:
                    self.logger.warning(f"Transform function failed for '{step_name}': {e}")
                    # Continue with input data
            
            # Apply validation rules
            valid_data, invalid_data, stats = apply_column_rules(
                df=output_data,
                rules=step.rules,
                stage="gold",
                step=step_name
            )
            
            # Create validation result
            validation_result = StepValidationResult(
                validation_passed=stats.validation_rate >= 99.0,  # Gold threshold
                validation_rate=stats.validation_rate,
                total_rows=stats.total_rows,
                valid_rows=stats.valid_rows,
                invalid_rows=stats.invalid_rows
            )
            
            # For step-by-step execution, preserve all columns but filter to valid rows only
            if validation_result.validation_passed:
                pred = self._get_validation_predicate(step.rules)
                output_data = output_data.filter(pred)
            else:
                # If validation failed, still return the validated subset
                output_data = valid_data
            output_count = output_data.count()
            
            # Optionally write to table
            write_result = None
            if output_to_table:
                table_name = fqn(self.config.schema, step.table_name)
                write_result = self._write_gold_data(output_data, table_name)
            
            # Create execution result
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.GOLD,
                status=StepStatus.COMPLETED if validation_result.validation_passed else StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                output_data=output_data,
                output_count=output_count,
                validation_result=validation_result,
                write_result=write_result,
                dependencies=step.source_silvers or []
            )
            
            # Store in execution state
            self._execution_state[step_name] = execution_result
            self._step_outputs[step_name] = output_data
            
            self.logger.info(f"✅ Gold step '{step_name}' completed: {output_count} rows")
            return execution_result
            
        except Exception as e:
            error_msg = f"Gold step '{step_name}' failed: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            
            execution_result = StepExecutionResult(
                step_name=step_name,
                step_type=StepType.GOLD,
                status=StepStatus.FAILED,
                start_time=start_time,
                end_time=now_dt(),
                duration_seconds=(now_dt() - start_time).total_seconds(),
                input_data=input_data,
                error=error_msg,
                dependencies=step.source_silvers or []
            )
            
            self._execution_state[step_name] = execution_result
            return execution_result
    
    def execute_step(
        self,
        step_name: str,
        input_data: Optional[DataFrame] = None,
        output_to_table: bool = True,
        force_input: bool = False
    ) -> StepExecutionResult:
        """Execute any step by name (auto-detects type)."""
        step_type = self._detect_step_type(step_name)
        
        if step_type == StepType.BRONZE:
            return self.execute_bronze_step(step_name, input_data, output_to_table)
        elif step_type == StepType.SILVER:
            return self.execute_silver_step(step_name, input_data, output_to_table, force_input)
        elif step_type == StepType.GOLD:
            return self.execute_gold_step(step_name, input_data, output_to_table, force_input)
        else:
            raise ValueError(f"Unknown step type for '{step_name}'")
    
    def get_step_output(self, step_name: str) -> Optional[DataFrame]:
        """Get the output DataFrame from a previously executed step."""
        return self._step_outputs.get(step_name)
    
    def get_execution_state(self) -> Dict[str, StepExecutionResult]:
        """Get the current execution state."""
        return self._execution_state.copy()
    
    def clear_execution_state(self):
        """Clear all execution state."""
        self._execution_state.clear()
        self._step_outputs.clear()
        self.logger.info("🧹 Execution state cleared")
    
    def list_completed_steps(self) -> List[str]:
        """List all completed steps."""
        return [name for name, result in self._execution_state.items() 
                if result.is_successful]
    
    def list_failed_steps(self) -> List[str]:
        """List all failed steps."""
        return [name for name, result in self._execution_state.items() 
                if result.is_failed]
    
    def _detect_step_type(self, step_name: str) -> StepType:
        """Detect the type of a step by name."""
        if step_name in self.bronze_steps:
            return StepType.BRONZE
        elif step_name in self.silver_steps:
            return StepType.SILVER
        elif step_name in self.gold_steps:
            return StepType.GOLD
        else:
            raise ValueError(f"Step '{step_name}' not found")
    
    def _get_bronze_dependents(self, step_name: str) -> List[str]:
        """Get Silver steps that depend on this Bronze step."""
        dependents = []
        for name, step in self.silver_steps.items():
            if step.source_bronze == step_name:
                dependents.append(name)
        return dependents
    
    def _get_silver_dependents(self, step_name: str) -> List[str]:
        """Get Gold steps that depend on this Silver step."""
        dependents = []
        for name, step in self.gold_steps.items():
            if step_name in (step.source_silvers or []):
                dependents.append(name)
        return dependents
    
    def _get_silver_input_data(self, step: SilverStep) -> Optional[DataFrame]:
        """Get input data for a Silver step from its dependencies."""
        if not step.source_bronze:
            return None
        
        # Try to get from executed Bronze step
        if step.source_bronze in self._step_outputs:
            return self._step_outputs[step.source_bronze]
        
        # Try to read from table
        try:
            table_name = fqn(self.config.schema, step.source_bronze)
            return self.spark.table(table_name)
        except Exception:
            return None
    
    def _get_gold_input_data(self, step: GoldStep) -> Optional[DataFrame]:
        """Get input data for a Gold step from its dependencies."""
        if not step.source_silvers:
            return None
        
        # Collect all available Silver outputs
        available_silvers = []
        for silver_name in step.source_silvers:
            if silver_name in self._step_outputs:
                available_silvers.append(self._step_outputs[silver_name])
            else:
                try:
                    table_name = fqn(self.config.schema, silver_name)
                    available_silvers.append(self.spark.table(table_name))
                except Exception:
                    pass
        
        return available_silvers[0] if available_silvers else None
    
    def _write_bronze_data(self, data: DataFrame, table_name: str) -> Dict[str, Any]:
        """Write Bronze data to table."""
        rows_written, duration, start_time, end_time = time_write_operation(
            mode="overwrite",
            df=data,
            fqn=table_name
        )
        
        return {
            'table_name': table_name,
            'mode': 'overwrite',
            'rows_written': rows_written,
            'duration': duration,
            'start_time': start_time,
            'end_time': end_time
        }
    
    def _write_silver_data(self, data: DataFrame, table_name: str, watermark_col: str) -> Dict[str, Any]:
        """Write Silver data to table."""
        rows_written, duration, start_time, end_time = time_write_operation(
            mode="overwrite",
            df=data,
            fqn=table_name
        )
        
        return {
            'table_name': table_name,
            'mode': 'overwrite',
            'rows_written': rows_written,
            'duration': duration,
            'start_time': start_time,
            'end_time': end_time,
            'watermark_col': watermark_col
        }
    
    def _write_gold_data(self, data: DataFrame, table_name: str) -> Dict[str, Any]:
        """Write Gold data to table."""
        rows_written, duration, start_time, end_time = time_write_operation(
            mode="overwrite",
            df=data,
            fqn=table_name
        )
        
        return {
            'table_name': table_name,
            'mode': 'overwrite',
            'rows_written': rows_written,
            'duration': duration,
            'start_time': start_time,
            'end_time': end_time
        }
    
    def _get_validation_predicate(self, rules: Dict[str, List[Any]]) -> Any:
        """Create a validation predicate from rules."""
        from .validation import and_all_rules
        return and_all_rules(rules)