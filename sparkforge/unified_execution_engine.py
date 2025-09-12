#!/usr/bin/env python3
"""
Unified execution engine for dependency-aware parallel execution.

This module provides a unified execution engine that can execute Bronze, Silver, and Gold
steps in parallel based on their actual dependencies rather than layer boundaries.

Key Features:
- Cross-layer parallel execution
- Dependency-aware scheduling
- Optimal resource utilization
- Comprehensive error handling
- Performance monitoring
"""

from __future__ import annotations
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from enum import Enum
import time
import threading
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

from .models import SilverStep, BronzeStep, GoldStep, ExecutionContext
from .logger import PipelineLogger
from .unified_dependency_analyzer import UnifiedDependencyAnalyzer, UnifiedDependencyResult, StepType
from .performance import now_dt, time_write_operation
from .reporting import create_validation_dict, create_transform_dict, create_write_dict
from .validation import apply_column_rules


@dataclass
class UnifiedExecutionConfig:
    """Configuration for unified execution engine."""
    max_workers: int = 4
    timeout_seconds: int = 300
    retry_attempts: int = 3
    retry_delay: float = 1.0
    enable_parallel_execution: bool = True
    enable_dependency_optimization: bool = True
    verbose: bool = True


@dataclass
class StepExecutionResult:
    """Result of executing a single step."""
    step_name: str
    step_type: StepType
    success: bool
    duration_seconds: float
    rows_processed: int = 0
    rows_written: int = 0
    error_message: Optional[str] = None
    output_data: Optional[DataFrame] = None
    metadata: Dict[str, Any] = None


@dataclass
class UnifiedExecutionResult:
    """Result of unified execution across all step types."""
    step_results: Dict[str, StepExecutionResult]
    execution_groups: List[List[str]]
    total_duration: float
    parallel_efficiency: float
    successful_steps: int
    failed_steps: int
    total_rows_processed: int
    total_rows_written: int
    errors: List[str]


class UnifiedExecutionEngine:
    """
    Unified execution engine for dependency-aware parallel execution.
    
    This engine can execute Bronze, Silver, and Gold steps in parallel based on
    their actual dependencies, providing optimal performance and resource utilization.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[UnifiedExecutionConfig] = None,
        logger: Optional[PipelineLogger] = None
    ):
        self.spark = spark
        self.config = config or UnifiedExecutionConfig()
        self.logger = logger or PipelineLogger()
        self.dependency_analyzer = UnifiedDependencyAnalyzer(self.logger)
        
        # Execution state
        self._execution_lock = threading.Lock()
        self._step_results: Dict[str, StepExecutionResult] = {}
        self._available_data: Dict[str, DataFrame] = {}
        self._bronze_sources: Dict[str, DataFrame] = {}
    
    def execute_unified_pipeline(
        self,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        bronze_sources: Dict[str, DataFrame],
        mode: str = "incremental",
        context: Optional[ExecutionContext] = None
    ) -> UnifiedExecutionResult:
        """
        Execute the entire pipeline with dependency-aware parallel execution.
        
        Args:
            bronze_steps: Dictionary of Bronze step configurations
            silver_steps: Dictionary of Silver step configurations
            gold_steps: Dictionary of Gold step configurations
            bronze_sources: Dictionary of source DataFrames for Bronze steps
            mode: Execution mode (incremental, full_refresh, etc.)
            context: Execution context
            
        Returns:
            Unified execution result with comprehensive metrics
        """
        start_time = time.time()
        self.logger.info("🚀 Starting unified dependency-aware pipeline execution")
        
        try:
            # Step 1: Analyze dependencies
            dependency_result = self.dependency_analyzer.analyze_unified_dependencies(
                bronze_steps, silver_steps, gold_steps
            )
            
            # Step 2: Initialize available data with Bronze sources
            self._available_data.update(bronze_sources)
            self._bronze_sources = bronze_sources
            
            # Step 3: Execute steps in dependency order
            step_results = self._execute_steps_by_groups(
                dependency_result,
                bronze_steps,
                silver_steps,
                gold_steps,
                mode,
                context
            )
            
            # Step 4: Calculate final metrics
            total_duration = time.time() - start_time
            successful_steps = sum(1 for result in step_results.values() if result.success)
            failed_steps = len(step_results) - successful_steps
            total_rows_processed = sum(result.rows_processed for result in step_results.values())
            total_rows_written = sum(result.rows_written for result in step_results.values())
            errors = [result.error_message for result in step_results.values() if result.error_message]
            
            # Step 5: Create execution groups for reporting
            execution_groups = [group.step_names for group in dependency_result.execution_groups]
            
            result = UnifiedExecutionResult(
                step_results=step_results,
                execution_groups=execution_groups,
                total_duration=total_duration,
                parallel_efficiency=dependency_result.parallel_efficiency,
                successful_steps=successful_steps,
                failed_steps=failed_steps,
                total_rows_processed=total_rows_processed,
                total_rows_written=total_rows_written,
                errors=errors
            )
            
            self.logger.info(f"✅ Unified pipeline execution completed: {successful_steps} successful, "
                           f"{failed_steps} failed, {total_duration:.2f}s total")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Unified pipeline execution failed: {e}")
            raise
    
    def _execute_steps_by_groups(
        self,
        dependency_result: UnifiedDependencyResult,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        mode: str,
        context: Optional[ExecutionContext]
    ) -> Dict[str, StepExecutionResult]:
        """Execute steps in dependency groups with parallel execution within groups."""
        step_results = {}
        
        for group in dependency_result.execution_groups:
            self.logger.info(f"Executing group {group.group_id} with {len(group.step_names)} steps")
            
            if group.can_parallelize and self.config.enable_parallel_execution:
                # Execute steps in parallel
                group_results = self._execute_group_parallel(
                    group.step_names,
                    bronze_steps,
                    silver_steps,
                    gold_steps,
                    mode,
                    context
                )
            else:
                # Execute steps sequentially
                group_results = self._execute_group_sequential(
                    group.step_names,
                    bronze_steps,
                    silver_steps,
                    gold_steps,
                    mode,
                    context
                )
            
            step_results.update(group_results)
            
            # Update available data for next group
            for step_name, result in group_results.items():
                if result.success and result.output_data is not None:
                    self._available_data[step_name] = result.output_data
        
        return step_results
    
    def _execute_group_parallel(
        self,
        step_names: List[str],
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        mode: str,
        context: Optional[ExecutionContext]
    ) -> Dict[str, StepExecutionResult]:
        """Execute a group of steps in parallel."""
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all steps for parallel execution
            future_to_step = {}
            
            for step_name in step_names:
                future = executor.submit(
                    self._execute_single_step,
                    step_name,
                    bronze_steps,
                    silver_steps,
                    gold_steps,
                    mode,
                    context
                )
                future_to_step[future] = step_name
            
            # Collect results as they complete
            for future in as_completed(future_to_step, timeout=self.config.timeout_seconds):
                step_name = future_to_step[future]
                try:
                    result = future.result()
                    results[step_name] = result
                    self.logger.info(f"✅ Step {step_name} completed in {result.duration_seconds:.2f}s")
                except Exception as e:
                    self.logger.error(f"❌ Step {step_name} failed: {e}")
                    results[step_name] = StepExecutionResult(
                        step_name=step_name,
                        step_type=StepType.BRONZE,  # Default, will be corrected
                        success=False,
                        duration_seconds=0.0,
                        error_message=str(e)
                    )
        
        return results
    
    def _execute_group_sequential(
        self,
        step_names: List[str],
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        mode: str,
        context: Optional[ExecutionContext]
    ) -> Dict[str, StepExecutionResult]:
        """Execute a group of steps sequentially."""
        results = {}
        
        for step_name in step_names:
            try:
                result = self._execute_single_step(
                    step_name,
                    bronze_steps,
                    silver_steps,
                    gold_steps,
                    mode,
                    context
                )
                results[step_name] = result
                self.logger.info(f"✅ Step {step_name} completed in {result.duration_seconds:.2f}s")
                
                # Update available data for next step
                if result.success and result.output_data is not None:
                    self._available_data[step_name] = result.output_data
                    
            except Exception as e:
                self.logger.error(f"❌ Step {step_name} failed: {e}")
                results[step_name] = StepExecutionResult(
                    step_name=step_name,
                    step_type=StepType.BRONZE,  # Default, will be corrected
                    success=False,
                    duration_seconds=0.0,
                    error_message=str(e)
                )
        
        return results
    
    def _execute_single_step(
        self,
        step_name: str,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        mode: str,
        context: Optional[ExecutionContext]
    ) -> StepExecutionResult:
        """Execute a single step based on its type."""
        start_time = time.time()
        
        try:
            if step_name in bronze_steps:
                return self._execute_bronze_step(
                    step_name, bronze_steps[step_name], mode, context, start_time
                )
            elif step_name in silver_steps:
                return self._execute_silver_step(
                    step_name, silver_steps[step_name], mode, context, start_time
                )
            elif step_name in gold_steps:
                return self._execute_gold_step(
                    step_name, gold_steps[step_name], mode, context, start_time
                )
            else:
                raise ValueError(f"Unknown step: {step_name}")
                
        except Exception as e:
            duration = time.time() - start_time
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.BRONZE,  # Default
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            )
    
    def _execute_bronze_step(
        self,
        step_name: str,
        step: BronzeStep,
        mode: str,
        context: Optional[ExecutionContext],
        start_time: float
    ) -> StepExecutionResult:
        """Execute a Bronze step."""
        try:
            # Get source data from bronze sources
            source_data = self._bronze_sources.get(step_name)
            if source_data is None:
                raise ValueError(f"No source data available for Bronze step: {step_name}")
            
            # Apply validation rules
            validated_data, _, _ = apply_column_rules(
                source_data, step.rules, "bronze", step_name
            )
            
            # Calculate metrics
            rows_processed = source_data.count()
            rows_written = validated_data.count()
            
            duration = time.time() - start_time
            
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.BRONZE,
                success=True,
                duration_seconds=duration,
                rows_processed=rows_processed,
                rows_written=rows_written,
                output_data=validated_data,
                metadata={"validation_rules": len(step.rules)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.BRONZE,
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            )
    
    def _execute_silver_step(
        self,
        step_name: str,
        step: SilverStep,
        mode: str,
        context: Optional[ExecutionContext],
        start_time: float
    ) -> StepExecutionResult:
        """Execute a Silver step."""
        try:
            # Get source data
            source_data = self._available_data.get(step.source_bronze)
            if source_data is None:
                raise ValueError(f"No source data available for Silver step {step_name}: {step.source_bronze}")
            
            # Get prior Silver data if needed
            prior_silvers = {
                name: data for name, data in self._available_data.items()
                if name != step_name
            }
            
            # Apply transformation
            transformed_data = step.transform(self.spark, source_data, prior_silvers)
            
            # Apply validation rules
            validated_data, _, _ = apply_column_rules(
                transformed_data, step.rules, "silver", step_name
            )
            
            # Calculate metrics
            rows_processed = source_data.count()
            rows_written = validated_data.count()
            
            duration = time.time() - start_time
            
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.SILVER,
                success=True,
                duration_seconds=duration,
                rows_processed=rows_processed,
                rows_written=rows_written,
                output_data=validated_data,
                metadata={"source_bronze": step.source_bronze}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.SILVER,
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            )
    
    def _execute_gold_step(
        self,
        step_name: str,
        step: GoldStep,
        mode: str,
        context: Optional[ExecutionContext],
        start_time: float
    ) -> StepExecutionResult:
        """Execute a Gold step."""
        try:
            # Get source Silver data
            if step.source_silvers is None:
                # Use all available data as source
                source_silvers = self._available_data
            else:
                source_silvers = {
                    name: data for name, data in self._available_data.items()
                    if name in step.source_silvers
                }
            
            if not source_silvers:
                raise ValueError(f"No source Silver data available for Gold step {step_name}")
            
            # Apply transformation
            transformed_data = step.transform(self.spark, source_silvers)
            
            # Apply validation rules
            validated_data, _, _ = apply_column_rules(
                transformed_data, step.rules, "gold", step_name
            )
            
            # Calculate metrics
            rows_processed = sum(data.count() for data in source_silvers.values())
            rows_written = validated_data.count()
            
            duration = time.time() - start_time
            
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.GOLD,
                success=True,
                duration_seconds=duration,
                rows_processed=rows_processed,
                rows_written=rows_written,
                output_data=validated_data,
                metadata={"source_silvers": list(source_silvers.keys())}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            return StepExecutionResult(
                step_name=step_name,
                step_type=StepType.GOLD,
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            )
