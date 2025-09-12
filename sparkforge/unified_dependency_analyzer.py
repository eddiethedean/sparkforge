#!/usr/bin/env python3
"""
Unified dependency analyzer for cross-layer parallel execution.

This module provides dependency analysis across all pipeline step types (Bronze, Silver, Gold)
to enable optimal parallel execution based on actual dependencies rather than layer boundaries.

Key Features:
- Cross-layer dependency analysis
- Optimal execution ordering
- Parallel execution groups
- Cycle detection and resolution
- Performance optimization
"""

from __future__ import annotations
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
import time

from .models import SilverStep, BronzeStep, GoldStep
from .logger import PipelineLogger


class StepType(Enum):
    """Types of pipeline steps."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class UnifiedStepInfo:
    """Unified information about a pipeline step."""
    name: str
    step_type: StepType
    dependencies: Set[str]  # Names of steps this step depends on
    dependents: Set[str]    # Names of steps that depend on this step
    execution_group: int    # Group for parallel execution
    can_run_parallel: bool  # Whether this step can run in parallel
    estimated_duration: float = 0.0  # Estimated execution time in seconds


@dataclass
class ExecutionGroup:
    """A group of steps that can run in parallel."""
    group_id: int
    step_names: List[str]
    step_types: Dict[str, StepType]
    estimated_duration: float
    can_parallelize: bool


@dataclass
class UnifiedDependencyResult:
    """Result of unified dependency analysis."""
    step_info: Dict[str, UnifiedStepInfo]
    execution_groups: List[ExecutionGroup]
    execution_order: List[str]  # Ordered list of step names for execution
    parallel_efficiency: float
    total_estimated_duration: float
    cycles_detected: List[List[str]]
    conflicts_detected: List[str]
    recommendations: List[str]


class UnifiedDependencyAnalyzer:
    """
    Unified dependency analyzer for cross-layer parallel execution.
    
    This analyzer can determine optimal execution order and parallelization
    opportunities across Bronze, Silver, and Gold steps based on their
    actual dependencies rather than layer boundaries.
    """
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger()
        self._analysis_cache: Dict[str, UnifiedDependencyResult] = {}
    
    def analyze_unified_dependencies(
        self,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep],
        force_refresh: bool = False
    ) -> UnifiedDependencyResult:
        """
        Analyze dependencies across all step types for optimal execution.
        
        Args:
            bronze_steps: Dictionary of Bronze step configurations
            silver_steps: Dictionary of Silver step configurations
            gold_steps: Dictionary of Gold step configurations
            force_refresh: Force refresh of cached analysis
            
        Returns:
            Unified dependency analysis result with execution plan
        """
        cache_key = self._generate_cache_key(bronze_steps, silver_steps, gold_steps)
        
        if not force_refresh and cache_key in self._analysis_cache:
            self.logger.debug(f"Using cached unified dependency analysis for key: {cache_key}")
            return self._analysis_cache[cache_key]
        
        try:
            self.logger.info("Starting unified dependency analysis across all step types")
            
            # Step 1: Build unified step information
            step_info = self._build_unified_step_info(bronze_steps, silver_steps, gold_steps)
            
            # Step 2: Analyze cross-layer dependencies
            self._analyze_cross_layer_dependencies(step_info, silver_steps, gold_steps)
            
            # Step 3: Detect cycles
            cycles = self._detect_cycles_unified(step_info)
            if cycles:
                self.logger.warning(f"Detected {len(cycles)} circular dependencies")
                step_info = self._resolve_cycles_unified(step_info, cycles)
            
            # Step 4: Detect conflicts
            conflicts = self._detect_conflicts_unified(step_info)
            if conflicts:
                self.logger.warning(f"Detected {len(conflicts)} dependency conflicts")
            
            # Step 5: Create execution groups
            execution_groups = self._create_execution_groups(step_info)
            
            # Step 6: Generate execution order
            execution_order = self._generate_execution_order(step_info, execution_groups)
            
            # Step 7: Calculate performance metrics
            parallel_efficiency = self._calculate_parallel_efficiency(execution_groups)
            total_duration = self._calculate_total_duration(execution_groups)
            
            # Step 8: Generate recommendations
            recommendations = self._generate_recommendations(step_info, cycles, conflicts, execution_groups)
            
            # Create result
            result = UnifiedDependencyResult(
                step_info=step_info,
                execution_groups=execution_groups,
                execution_order=execution_order,
                parallel_efficiency=parallel_efficiency,
                total_estimated_duration=total_duration,
                cycles_detected=cycles,
                conflicts_detected=conflicts,
                recommendations=recommendations
            )
            
            # Cache result
            self._analysis_cache[cache_key] = result
            
            self.logger.info(f"Unified dependency analysis completed: {len(execution_groups)} execution groups, "
                           f"{parallel_efficiency:.2f}% parallel efficiency")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Unified dependency analysis failed: {e}")
            raise
    
    def _build_unified_step_info(
        self,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ) -> Dict[str, UnifiedStepInfo]:
        """Build unified step information from all step types."""
        step_info = {}
        
        # Add Bronze steps
        for name, step in bronze_steps.items():
            step_info[name] = UnifiedStepInfo(
                name=name,
                step_type=StepType.BRONZE,
                dependencies=set(),  # Bronze steps typically have no dependencies
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True,  # Bronze steps can typically run in parallel
                estimated_duration=1.0  # Default estimate
            )
        
        # Add Silver steps
        for name, step in silver_steps.items():
            dependencies = set()
            if hasattr(step, 'source_bronze') and step.source_bronze:
                dependencies.add(step.source_bronze)
            if hasattr(step, 'depends_on_silvers') and step.depends_on_silvers:
                dependencies.update(step.depends_on_silvers)
            
            step_info[name] = UnifiedStepInfo(
                name=name,
                step_type=StepType.SILVER,
                dependencies=dependencies,
                dependents=set(),
                execution_group=-1,
                can_run_parallel=True,  # Silver steps can run in parallel if no dependencies
                estimated_duration=2.0  # Default estimate
            )
        
        # Add Gold steps
        for name, step in gold_steps.items():
            dependencies = set()
            if hasattr(step, 'source_silvers') and step.source_silvers:
                dependencies.update(step.source_silvers)
            
            step_info[name] = UnifiedStepInfo(
                name=name,
                step_type=StepType.GOLD,
                dependencies=dependencies,
                dependents=set(),
                execution_group=-1,
                can_run_parallel=False,  # Gold steps typically depend on Silver steps
                estimated_duration=3.0  # Default estimate
            )
        
        return step_info
    
    def _analyze_cross_layer_dependencies(
        self,
        step_info: Dict[str, UnifiedStepInfo],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ):
        """Analyze dependencies across different layer types."""
        # Build dependency graph
        for name, info in step_info.items():
            for dep_name in info.dependencies:
                if dep_name in step_info:
                    # Add reverse dependency
                    step_info[dep_name].dependents.add(name)
        
        # Analyze Silver step dependencies more deeply
        for name, step in silver_steps.items():
            if name in step_info:
                info = step_info[name]
                # Check if Silver step depends on other Silver steps
                if hasattr(step, 'depends_on_silvers') and step.depends_on_silvers:
                    for dep_name in step.depends_on_silvers:
                        if dep_name in step_info and step_info[dep_name].step_type == StepType.SILVER:
                            info.dependencies.add(dep_name)
                            step_info[dep_name].dependents.add(name)
    
    def _detect_cycles_unified(self, step_info: Dict[str, UnifiedStepInfo]) -> List[List[str]]:
        """Detect circular dependencies in the unified step graph."""
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: List[str]) -> bool:
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return True
            
            if node in visited:
                return False
            
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for dependent in step_info[node].dependents:
                if dfs(dependent, path.copy()):
                    return True
            
            rec_stack.remove(node)
            path.pop()
            return False
        
        for step_name in step_info:
            if step_name not in visited:
                dfs(step_name, [])
        
        return cycles
    
    def _resolve_cycles_unified(
        self, 
        step_info: Dict[str, UnifiedStepInfo], 
        cycles: List[List[str]]
    ) -> Dict[str, UnifiedStepInfo]:
        """Resolve circular dependencies by breaking cycles."""
        for cycle in cycles:
            self.logger.warning(f"Breaking cycle: {' -> '.join(cycle)}")
            # Break cycle by removing the last dependency in the cycle
            if len(cycle) > 1:
                # Remove the dependency from the last step to the first step
                last_step = cycle[-1]
                first_step = cycle[0]
                if first_step in step_info[last_step].dependencies:
                    step_info[last_step].dependencies.remove(first_step)
                    step_info[first_step].dependents.remove(last_step)
                else:
                    # If the direct dependency doesn't exist, remove any dependency in the cycle
                    # Find the first dependency that exists in the cycle
                    for i in range(len(cycle) - 1):
                        current_step = cycle[i]
                        next_step = cycle[i + 1]
                        if next_step in step_info[current_step].dependencies:
                            step_info[current_step].dependencies.remove(next_step)
                            step_info[next_step].dependents.remove(current_step)
                            break
                    else:
                        # If no dependency was found in the forward direction, try the reverse
                        for i in range(len(cycle) - 1, 0, -1):
                            current_step = cycle[i]
                            prev_step = cycle[i - 1]
                            if prev_step in step_info[current_step].dependencies:
                                step_info[current_step].dependencies.remove(prev_step)
                                step_info[prev_step].dependents.remove(current_step)
                                break
        
        return step_info
    
    def _detect_conflicts_unified(self, step_info: Dict[str, UnifiedStepInfo]) -> List[str]:
        """Detect potential conflicts in the dependency graph."""
        conflicts = []
        
        # Check for steps that depend on themselves
        for name, info in step_info.items():
            if name in info.dependencies:
                conflicts.append(f"Step {name} depends on itself")
        
        # Check for impossible dependencies (e.g., Bronze depending on Silver)
        for name, info in step_info.items():
            for dep_name in info.dependencies:
                if dep_name in step_info:
                    dep_info = step_info[dep_name]
                    if info.step_type == StepType.BRONZE and dep_info.step_type in [StepType.SILVER, StepType.GOLD]:
                        conflicts.append(f"Bronze step {name} cannot depend on {dep_info.step_type.value} step {dep_name}")
        
        return conflicts
    
    def _create_execution_groups(self, step_info: Dict[str, UnifiedStepInfo]) -> List[ExecutionGroup]:
        """Create execution groups based on dependencies."""
        groups = []
        remaining_steps = set(step_info.keys())
        group_id = 0
        
        while remaining_steps:
            # Find steps that can run now (no unresolved dependencies)
            ready_steps = []
            for step_name in remaining_steps:
                info = step_info[step_name]
                if not info.dependencies or all(dep not in remaining_steps for dep in info.dependencies):
                    ready_steps.append(step_name)
            
            if not ready_steps:
                # This shouldn't happen if cycles were resolved, but handle gracefully
                self.logger.error("No ready steps found - possible unresolved cycle")
                break
            
            # Create execution group
            group = ExecutionGroup(
                group_id=group_id,
                step_names=ready_steps,
                step_types={name: step_info[name].step_type for name in ready_steps},
                estimated_duration=max(step_info[name].estimated_duration for name in ready_steps),
                can_parallelize=len(ready_steps) > 1
            )
            groups.append(group)
            
            # Update step info
            for step_name in ready_steps:
                step_info[step_name].execution_group = group_id
                step_info[step_name].can_run_parallel = len(ready_steps) > 1
                remaining_steps.remove(step_name)
            
            group_id += 1
        
        return groups
    
    def _generate_execution_order(
        self, 
        step_info: Dict[str, UnifiedStepInfo], 
        execution_groups: List[ExecutionGroup]
    ) -> List[str]:
        """Generate optimal execution order for all steps."""
        execution_order = []
        
        for group in execution_groups:
            # Within each group, order by step type (Bronze -> Silver -> Gold)
            # and then by estimated duration (shorter first)
            group_steps = sorted(
                group.step_names,
                key=lambda name: (
                    step_info[name].step_type.value,
                    step_info[name].estimated_duration
                )
            )
            execution_order.extend(group_steps)
        
        return execution_order
    
    def _calculate_parallel_efficiency(self, execution_groups: List[ExecutionGroup]) -> float:
        """Calculate parallel execution efficiency."""
        if not execution_groups:
            return 0.0
        
        total_steps = sum(len(group.step_names) for group in execution_groups)
        parallel_steps = sum(len(group.step_names) for group in execution_groups if group.can_parallelize)
        
        return (parallel_steps / total_steps) * 100.0 if total_steps > 0 else 0.0
    
    def _calculate_total_duration(self, execution_groups: List[ExecutionGroup]) -> float:
        """Calculate total estimated execution duration."""
        return sum(group.estimated_duration for group in execution_groups)
    
    def _generate_recommendations(
        self,
        step_info: Dict[str, UnifiedStepInfo],
        cycles: List[List[str]],
        conflicts: List[str],
        execution_groups: List[ExecutionGroup]
    ) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []
        
        # Parallelization opportunities
        parallel_groups = [g for g in execution_groups if g.can_parallelize]
        if parallel_groups:
            recommendations.append(f"Found {len(parallel_groups)} groups that can run in parallel")
        
        # Sequential bottlenecks
        sequential_groups = [g for g in execution_groups if not g.can_parallelize]
        if len(sequential_groups) > len(execution_groups) * 0.5:
            recommendations.append("Consider breaking dependencies to enable more parallel execution")
        
        # Step type distribution
        bronze_count = sum(1 for info in step_info.values() if info.step_type == StepType.BRONZE)
        silver_count = sum(1 for info in step_info.values() if info.step_type == StepType.SILVER)
        gold_count = sum(1 for info in step_info.values() if info.step_type == StepType.GOLD)
        
        if silver_count > bronze_count * 2:
            recommendations.append("Consider consolidating Silver steps to reduce complexity")
        
        if gold_count > silver_count:
            recommendations.append("Consider breaking Gold steps into smaller, more focused steps")
        
        return recommendations
    
    def _generate_cache_key(
        self,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ) -> str:
        """Generate cache key for dependency analysis."""
        step_names = sorted(list(bronze_steps.keys()) + list(silver_steps.keys()) + list(gold_steps.keys()))
        return f"unified_{hash(tuple(step_names))}"
