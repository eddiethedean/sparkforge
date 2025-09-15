#!/usr/bin/env python3
"""
Simplified dynamic execution strategy for SparkForge pipelines.

This module provides a clean, reliable execution strategy that uses dynamic
worker allocation and adaptive optimization for optimal pipeline performance.

Key Features:
- Simple API for dynamic execution
- Dynamic worker allocation based on workload analysis
- Adaptive optimization based on performance metrics
- Intelligent task prioritization and scheduling
- Resource-aware execution planning
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
import time
import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from .results import ExecutionResult, StepExecutionResult, StepStatus, StepType
from .strategies import ExecutionStrategy
from ..logger import PipelineLogger
from ..parallel_execution import (
    DynamicParallelExecutor, ExecutionTask, TaskPriority, 
    get_dynamic_executor, create_execution_task
)
from ..models import PipelineConfig, ValidationThresholds, ParallelConfig


@dataclass
class StepComplexityAnalysis:
    """Analysis of step complexity for optimization."""
    step_name: str
    estimated_duration: float
    memory_requirement_mb: float
    cpu_requirement_percent: float
    dependencies_count: int
    fan_out: int
    critical_path: bool
    complexity_score: float
    
    @property
    def priority(self) -> TaskPriority:
        """Determine task priority based on complexity analysis."""
        if self.critical_path:
            return TaskPriority.CRITICAL
        elif self.complexity_score > 0.8:
            return TaskPriority.HIGH
        elif self.complexity_score > 0.5:
            return TaskPriority.NORMAL
        else:
            return TaskPriority.LOW


class DynamicExecutionStrategy:
    """
    Simplified dynamic execution strategy with intelligent worker allocation.
    
    This strategy analyzes pipeline steps, estimates resource requirements,
    and dynamically allocates workers for optimal performance.
    """
    
    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        logger: Optional[PipelineLogger] = None
    ):
        self.config = config or PipelineConfig(
            schema="default",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=8, timeout_secs=600)
        )
        self.logger = logger or PipelineLogger()
        
        # Initialize dynamic executor
        self.executor = get_dynamic_executor()
        
        # Performance tracking
        self.execution_history: List[Dict[str, Any]] = []
        self.optimization_enabled = True
        
        # Step analysis cache
        self.step_analysis_cache: Dict[str, StepComplexityAnalysis] = {}
    
    def execute_steps(
        self,
        steps: Dict[str, Any],
        step_executor: Any,
        max_workers: int = None,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute steps using dynamic parallel strategy."""
        if not self.config.parallel.enabled:
            self.logger.warning("Parallel execution is disabled, falling back to sequential")
            return self._execute_sequential(steps, step_executor)
        
        start_time = time.time()
        self.logger.info(f"Starting dynamic parallel execution of {len(steps)} steps")
        
        # Analyze step complexity and dependencies
        step_analysis = self._analyze_steps(steps)
        
        # Create execution plan
        execution_plan = self._create_execution_plan(step_analysis)
        
        # Execute steps according to plan
        step_results = self._execute_plan(execution_plan, step_executor, steps)
        
        # Calculate execution metrics
        total_duration = time.time() - start_time
        execution_groups = self._get_execution_groups(execution_plan)
        
        # Calculate parallel efficiency
        parallel_efficiency = self._calculate_parallel_efficiency(step_results, total_duration)
        
        # Create execution result
        result = ExecutionResult(
            step_results=step_results,
            execution_groups=execution_groups,
            total_duration=total_duration,
            parallel_efficiency=parallel_efficiency,
            successful_steps=sum(1 for r in step_results.values() if r.success),
            failed_steps=sum(1 for r in step_results.values() if r.failed),
            total_rows_processed=sum(r.rows_processed for r in step_results.values()),
            total_rows_written=sum(r.rows_written for r in step_results.values()),
            errors=[r.error_message for r in step_results.values() if r.failed and r.error_message],
            warnings=[]
        )
        
        # Store execution history for optimization
        self._store_execution_history(result, step_analysis)
        
        # Log results
        self.logger.info(
            f"Dynamic execution completed: {result.successful_steps}/{len(steps)} "
            f"steps successful in {total_duration:.2f}s (efficiency: {parallel_efficiency:.2f})"
        )
        
        return result
    
    def _analyze_steps(self, steps: Dict[str, Any]) -> Dict[str, StepComplexityAnalysis]:
        """Analyze step complexity and resource requirements."""
        analysis = {}
        
        for step_name, step_config in steps.items():
            if step_name in self.step_analysis_cache:
                analysis[step_name] = self.step_analysis_cache[step_name]
                continue
            
            # Analyze step characteristics
            step_type = step_config.get('step_type', StepType.SILVER)
            rules = step_config.get('rules', {})
            transform = step_config.get('transform')
            
            # Estimate duration based on step type and complexity
            estimated_duration = self._estimate_duration(step_type, rules, transform)
            
            # Estimate memory requirements
            memory_requirement = self._estimate_memory_requirement(step_type, rules)
            
            # Estimate CPU requirements
            cpu_requirement = self._estimate_cpu_requirement(step_type, rules, transform)
            
            # Count dependencies
            dependencies = step_config.get('dependencies', [])
            dependencies_count = len(dependencies)
            
            # Calculate fan-out (how many steps depend on this one)
            fan_out = sum(1 for other_step in steps.values() 
                         if step_name in other_step.get('dependencies', []))
            
            # Determine if on critical path
            critical_path = self._is_critical_path(step_name, steps)
            
            # Calculate complexity score
            complexity_score = self._calculate_complexity_score(
                estimated_duration, memory_requirement, cpu_requirement,
                dependencies_count, fan_out, critical_path
            )
            
            # Create analysis
            step_analysis = StepComplexityAnalysis(
                step_name=step_name,
                estimated_duration=estimated_duration,
                memory_requirement_mb=memory_requirement,
                cpu_requirement_percent=cpu_requirement,
                dependencies_count=dependencies_count,
                fan_out=fan_out,
                critical_path=critical_path,
                complexity_score=complexity_score
            )
            
            analysis[step_name] = step_analysis
            self.step_analysis_cache[step_name] = step_analysis
        
        return analysis
    
    def _estimate_duration(
        self, 
        step_type: StepType, 
        rules: Dict[str, Any], 
        transform: Optional[Any]
    ) -> float:
        """Estimate step execution duration in seconds."""
        base_duration = {
            StepType.BRONZE: 10.0,
            StepType.SILVER: 15.0,
            StepType.GOLD: 20.0
        }.get(step_type, 15.0)
        
        # Adjust based on rules complexity
        rules_complexity = len(rules) * 0.5
        
        # Adjust based on transform complexity
        transform_complexity = 0.0
        if transform:
            # Simple heuristic based on function name and source
            transform_name = getattr(transform, '__name__', 'unknown')
            if 'complex' in transform_name.lower():
                transform_complexity = 10.0
            elif 'simple' in transform_name.lower():
                transform_complexity = 2.0
            else:
                transform_complexity = 5.0
        
        return base_duration + rules_complexity + transform_complexity
    
    def _estimate_memory_requirement(self, step_type: StepType, rules: Dict[str, Any]) -> float:
        """Estimate memory requirement in MB."""
        base_memory = {
            StepType.BRONZE: 256.0,
            StepType.SILVER: 512.0,
            StepType.GOLD: 1024.0
        }.get(step_type, 512.0)
        
        # Adjust based on rules complexity
        rules_memory = len(rules) * 50.0
        
        return base_memory + rules_memory
    
    def _estimate_cpu_requirement(
        self, 
        step_type: StepType, 
        rules: Dict[str, Any], 
        transform: Optional[Any]
    ) -> float:
        """Estimate CPU requirement as percentage."""
        base_cpu = {
            StepType.BRONZE: 25.0,
            StepType.SILVER: 50.0,
            StepType.GOLD: 75.0
        }.get(step_type, 50.0)
        
        # Adjust based on rules complexity
        rules_cpu = len(rules) * 5.0
        
        # Adjust based on transform complexity
        transform_cpu = 0.0
        if transform:
            transform_name = getattr(transform, '__name__', 'unknown')
            if 'complex' in transform_name.lower():
                transform_cpu = 25.0
            elif 'simple' in transform_name.lower():
                transform_cpu = 5.0
            else:
                transform_cpu = 15.0
        
        return min(100.0, base_cpu + rules_cpu + transform_cpu)
    
    def _is_critical_path(self, step_name: str, steps: Dict[str, Any]) -> bool:
        """Determine if step is on critical path."""
        # Simple heuristic: steps with high fan-out or no dependencies are critical
        step_config = steps.get(step_name, {})
        dependencies = step_config.get('dependencies', [])
        
        # Count how many steps depend on this one
        dependent_count = sum(1 for other_step in steps.values() 
                             if step_name in other_step.get('dependencies', []))
        
        # Critical if no dependencies (start of pipeline) or high fan-out
        return len(dependencies) == 0 or dependent_count > 2
    
    def _calculate_complexity_score(
        self,
        duration: float,
        memory: float,
        cpu: float,
        dependencies: int,
        fan_out: int,
        critical_path: bool
    ) -> float:
        """Calculate overall complexity score (0.0 to 1.0)."""
        # Normalize factors
        duration_score = min(1.0, duration / 60.0)  # Normalize to 60 seconds
        memory_score = min(1.0, memory / 2048.0)    # Normalize to 2GB
        cpu_score = min(1.0, cpu / 100.0)           # Normalize to 100%
        dependencies_score = min(1.0, dependencies / 5.0)  # Normalize to 5 dependencies
        fan_out_score = min(1.0, fan_out / 5.0)     # Normalize to 5 fan-out
        
        # Weighted average
        weights = {
            'duration': 0.3,
            'memory': 0.2,
            'cpu': 0.2,
            'dependencies': 0.1,
            'fan_out': 0.1,
            'critical_path': 0.1
        }
        
        score = (
            duration_score * weights['duration'] +
            memory_score * weights['memory'] +
            cpu_score * weights['cpu'] +
            dependencies_score * weights['dependencies'] +
            fan_out_score * weights['fan_out'] +
            (1.0 if critical_path else 0.0) * weights['critical_path']
        )
        
        return min(1.0, score)
    
    def _create_execution_plan(
        self, 
        step_analysis: Dict[str, StepComplexityAnalysis]
    ) -> List[Tuple[List[str], TaskPriority]]:
        """Create execution plan with dependency-aware grouping."""
        # Group steps by priority and dependencies
        execution_groups = []
        
        # Sort steps by priority
        sorted_steps = sorted(
            step_analysis.items(),
            key=lambda x: (x[1].priority.value, x[1].complexity_score)
        )
        
        # Group steps that can run in parallel
        current_group = []
        current_priority = None
        
        for step_name, analysis in sorted_steps:
            if current_priority is None or analysis.priority == current_priority:
                current_group.append(step_name)
                current_priority = analysis.priority
            else:
                # Start new group
                if current_group:
                    execution_groups.append((current_group, current_priority))
                current_group = [step_name]
                current_priority = analysis.priority
        
        # Add final group
        if current_group:
            execution_groups.append((current_group, current_priority))
        
        return execution_groups
    
    def _execute_plan(
        self,
        execution_plan: List[Tuple[List[str], TaskPriority]],
        step_executor: Any,
        steps: Dict[str, Any]
    ) -> Dict[str, StepExecutionResult]:
        """Execute the execution plan using dynamic parallel execution."""
        step_results = {}
        
        for group_steps, priority in execution_plan:
            self.logger.info(f"Executing group of {len(group_steps)} steps with priority {priority.name}")
            
            # Create execution tasks for this group
            tasks = []
            for step_name in group_steps:
                step_config = steps[step_name]
                analysis = self.step_analysis_cache[step_name]
                
                # Create execution task
                task = create_execution_task(
                    task_id=step_name,
                    function=self._create_step_executor_wrapper(step_executor, step_name, step_config),
                    priority=priority,
                    estimated_duration=analysis.estimated_duration,
                    memory_requirement_mb=analysis.memory_requirement_mb,
                    timeout_seconds=self.config.parallel.timeout_secs
                )
                tasks.append(task)
            
            # Execute tasks in parallel
            execution_result = self.executor.execute_parallel(
                tasks=tasks,
                wait_for_completion=True,
                timeout=self.config.parallel.timeout_secs
            )
            
            # Collect results
            for task in tasks:
                if task.task_id in self.executor.worker_pool.completed_tasks:
                    task_metrics = self.executor.worker_pool.completed_tasks[task.task_id]
                    step_results[task.task_id] = self._convert_task_metrics_to_step_result(
                        task_metrics, step_config
                    )
                elif task.task_id in self.executor.worker_pool.failed_tasks:
                    task_metrics = self.executor.worker_pool.failed_tasks[task.task_id]
                    step_results[task.task_id] = self._convert_task_metrics_to_step_result(
                        task_metrics, step_config
                    )
        
        return step_results
    
    def _create_step_executor_wrapper(
        self, 
        step_executor: Any, 
        step_name: str, 
        step_config: Dict[str, Any]
    ):
        """Create a wrapper function for step execution."""
        def wrapper():
            return step_executor.execute_step(step_name, step_config)
        return wrapper
    
    def _convert_task_metrics_to_step_result(
        self, 
        task_metrics, 
        step_config: Dict[str, Any]
    ) -> StepExecutionResult:
        """Convert task metrics to step execution result."""
        status = StepStatus.COMPLETED if task_metrics.success else StepStatus.FAILED
        
        return StepExecutionResult(
            step_name=task_metrics.task_id,
            step_type=step_config.get('step_type', StepType.SILVER),
            status=status,
            duration_seconds=task_metrics.duration_seconds or 0.0,
            rows_processed=task_metrics.rows_processed,
            rows_written=task_metrics.rows_written,
            error_message=task_metrics.error_message
        )
    
    def _get_execution_groups(
        self, 
        execution_plan: List[Tuple[List[str], TaskPriority]]
    ) -> List[List[str]]:
        """Get execution groups from the plan."""
        return [group for group, _ in execution_plan]
    
    def _calculate_parallel_efficiency(
        self, 
        step_results: Dict[str, StepExecutionResult], 
        total_duration: float
    ) -> float:
        """Calculate parallel execution efficiency."""
        if not step_results:
            return 0.0
        
        # Calculate theoretical sequential time
        sequential_time = sum(r.duration_seconds for r in step_results.values())
        
        if sequential_time == 0:
            return 0.0
        
        # Calculate parallel efficiency
        efficiency = sequential_time / (total_duration * len(step_results))
        return min(1.0, efficiency)
    
    def _store_execution_history(
        self, 
        result: ExecutionResult, 
        step_analysis: Dict[str, StepComplexityAnalysis]
    ) -> None:
        """Store execution history for optimization."""
        history_record = {
            "timestamp": datetime.utcnow(),
            "total_duration": result.total_duration,
            "parallel_efficiency": result.parallel_efficiency,
            "successful_steps": result.successful_steps,
            "failed_steps": result.failed_steps,
            "step_analysis": {name: {
                "complexity_score": analysis.complexity_score,
                "estimated_duration": analysis.estimated_duration,
                "priority": analysis.priority.name
            } for name, analysis in step_analysis.items()}
        }
        
        self.execution_history.append(history_record)
        
        # Keep only last 100 executions
        if len(self.execution_history) > 100:
            self.execution_history = self.execution_history[-100:]
    
    def _execute_sequential(
        self, 
        steps: Dict[str, Any], 
        step_executor: Any
    ) -> ExecutionResult:
        """Fallback to sequential execution."""
        from .strategies import SequentialStrategy
        
        sequential_strategy = SequentialStrategy(self.logger)
        return sequential_strategy.execute_steps(steps, step_executor)
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get optimization recommendations based on execution history."""
        if len(self.execution_history) < 3:
            return ["Insufficient execution history for optimization recommendations"]
        
        recommendations = []
        recent_executions = self.execution_history[-10:]
        
        # Analyze parallel efficiency
        avg_efficiency = sum(e["parallel_efficiency"] for e in recent_executions) / len(recent_executions)
        if avg_efficiency < 0.7:
            recommendations.append(
                f"Low parallel efficiency ({avg_efficiency:.2f}). Consider optimizing step dependencies or reducing parallelism."
            )
        
        # Analyze execution time trends
        durations = [e["total_duration"] for e in recent_executions]
        if len(durations) >= 3:
            trend = (durations[-1] - durations[0]) / len(durations)
            if trend > 0:
                recommendations.append(
                    "Increasing execution times detected. Consider optimizing step functions or reducing complexity."
                )
        
        # Analyze step complexity distribution
        all_complexity_scores = []
        for execution in recent_executions:
            for step_analysis in execution["step_analysis"].values():
                all_complexity_scores.append(step_analysis["complexity_score"])
        
        if all_complexity_scores:
            avg_complexity = sum(all_complexity_scores) / len(all_complexity_scores)
            if avg_complexity > 0.8:
                recommendations.append(
                    "High step complexity detected. Consider breaking down complex steps into smaller ones."
                )
        
        return recommendations
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        executor_metrics = self.executor.get_performance_metrics()
        
        return {
            "executor_metrics": executor_metrics,
            "execution_history_count": len(self.execution_history),
            "step_analysis_cache_size": len(self.step_analysis_cache),
            "optimization_enabled": self.optimization_enabled
        }
    
    def shutdown(self) -> None:
        """Shutdown the dynamic execution strategy."""
        self.executor.shutdown()
        self.logger.info("Dynamic execution strategy shutdown complete")