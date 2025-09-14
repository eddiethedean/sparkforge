"""
Execution strategies for SparkForge pipelines.

This module defines pluggable execution strategies that can be used
by the unified execution engine for different execution patterns.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Protocol
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from .results import ExecutionResult, StepExecutionResult, StepStatus, StepType
from ..logger import PipelineLogger


class ExecutionStrategy(Protocol):
    """Protocol for execution strategies."""
    
    def execute_steps(
        self,
        steps: Dict[str, Any],
        step_executor: Any,
        max_workers: int,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute a group of steps using this strategy."""
        ...


class SequentialStrategy:
    """Sequential execution strategy - executes steps one at a time."""
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger()
    
    def execute_steps(
        self,
        steps: Dict[str, Any],
        step_executor: Any,
        max_workers: int = 1,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute steps sequentially."""
        self.logger.info(f"Executing {len(steps)} steps sequentially")
        
        step_results: Dict[str, StepExecutionResult] = {}
        execution_groups = [list(steps.keys())]
        errors: List[str] = []
        warnings: List[str] = []
        
        start_time = time.time()
        
        for step_name, step_config in steps.items():
            try:
                self.logger.info(f"Executing step: {step_name}")
                result = step_executor.execute_step(step_name, step_config)
                step_results[step_name] = result
                
                if result.failed:
                    errors.append(f"Step {step_name} failed: {result.error_message}")
                elif result.status == StepStatus.SKIPPED:
                    warnings.append(f"Step {step_name} was skipped")
                    
            except Exception as e:
                error_msg = f"Step {step_name} failed with exception: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
                
                # Create failed result
                step_results[step_name] = StepExecutionResult(
                    step_name=step_name,
                    step_type=step_config.get('step_type', StepType.SILVER),
                    status=StepStatus.FAILED,
                    duration_seconds=0.0,
                    error_message=str(e)
                )
        
        total_duration = time.time() - start_time
        
        return ExecutionResult(
            step_results=step_results,
            execution_groups=execution_groups,
            total_duration=total_duration,
            parallel_efficiency=1.0,  # Sequential is 100% efficient by definition
            successful_steps=sum(1 for r in step_results.values() if r.success),
            failed_steps=sum(1 for r in step_results.values() if r.failed),
            total_rows_processed=sum(r.rows_processed for r in step_results.values()),
            total_rows_written=sum(r.rows_written for r in step_results.values()),
            errors=errors,
            warnings=warnings
        )


class ParallelStrategy:
    """Parallel execution strategy - executes independent steps concurrently."""
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger()
    
    def execute_steps(
        self,
        steps: Dict[str, Any],
        step_executor: Any,
        max_workers: int = 4,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute steps in parallel."""
        self.logger.info(f"Executing {len(steps)} steps in parallel with {max_workers} workers")
        
        step_results: Dict[str, StepExecutionResult] = {}
        execution_groups = [list(steps.keys())]
        errors: List[str] = []
        warnings: List[str] = []
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all steps for execution
            future_to_step = {
                executor.submit(step_executor.execute_step, step_name, step_config): step_name
                for step_name, step_config in steps.items()
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_step, timeout=timeout_seconds):
                step_name = future_to_step[future]
                try:
                    result = future.result()
                    step_results[step_name] = result
                    
                    if result.failed:
                        errors.append(f"Step {step_name} failed: {result.error_message}")
                    elif result.status == StepStatus.SKIPPED:
                        warnings.append(f"Step {step_name} was skipped")
                        
                except Exception as e:
                    error_msg = f"Step {step_name} failed with exception: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
                    
                    # Create failed result
                    step_results[step_name] = StepExecutionResult(
                        step_name=step_name,
                        step_type=steps[step_name].get('step_type', StepType.SILVER),
                        status=StepStatus.FAILED,
                        duration_seconds=0.0,
                        error_message=str(e)
                    )
        
        total_duration = time.time() - start_time
        
        # Calculate parallel efficiency
        sequential_time = sum(r.duration_seconds for r in step_results.values())
        parallel_efficiency = (sequential_time / total_duration) if total_duration > 0 else 1.0
        
        return ExecutionResult(
            step_results=step_results,
            execution_groups=execution_groups,
            total_duration=total_duration,
            parallel_efficiency=parallel_efficiency,
            successful_steps=sum(1 for r in step_results.values() if r.success),
            failed_steps=sum(1 for r in step_results.values() if r.failed),
            total_rows_processed=sum(r.rows_processed for r in step_results.values()),
            total_rows_written=sum(r.rows_written for r in step_results.values()),
            errors=errors,
            warnings=warnings
        )


class AdaptiveStrategy:
    """Adaptive execution strategy - chooses between sequential and parallel based on conditions."""
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger()
        self.sequential_strategy = SequentialStrategy(logger)
        self.parallel_strategy = ParallelStrategy(logger)
    
    def execute_steps(
        self,
        steps: Dict[str, Any],
        step_executor: Any,
        max_workers: int = 4,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute steps using adaptive strategy."""
        # Simple adaptive logic: use parallel for > 2 steps, sequential otherwise
        if len(steps) <= 2:
            self.logger.info("Using sequential strategy for small number of steps")
            return self.sequential_strategy.execute_steps(
                steps, step_executor, 1, timeout_seconds
            )
        else:
            self.logger.info("Using parallel strategy for multiple steps")
            return self.parallel_strategy.execute_steps(
                steps, step_executor, max_workers, timeout_seconds
            )
