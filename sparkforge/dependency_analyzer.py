# dependency_analyzer.py
"""
Enhanced dependency analysis for parallel execution of Silver steps.

This module contains the DependencyAnalyzer class for analyzing Silver step
dependencies and determining which steps can run in parallel.

Key Features:
- Advanced dependency analysis with multiple strategies
- Cycle detection and resolution
- Performance optimization recommendations
- Detailed execution planning
- Dependency visualization and reporting
- Conflict resolution and validation
"""

from __future__ import annotations
from typing import Dict, List, Set, Optional, Tuple, Any
from collections import defaultdict, deque
import inspect
import ast
from dataclasses import dataclass
from enum import Enum

from .models import SilverStep, SilverDependencyInfo, BronzeStep, GoldStep
from .logger import PipelineLogger


# ============================================================================
# Custom Exceptions
# ============================================================================

class DependencyAnalysisError(Exception):
    """Base exception for dependency analysis errors."""
    pass


class CircularDependencyError(DependencyAnalysisError):
    """Raised when circular dependencies are detected."""
    pass


class InvalidDependencyError(DependencyAnalysisError):
    """Raised when invalid dependencies are detected."""
    pass


class DependencyConflictError(DependencyAnalysisError):
    """Raised when dependency conflicts are detected."""
    pass


# ============================================================================
# Analysis Strategies
# ============================================================================

class AnalysisStrategy(Enum):
    """Different strategies for dependency analysis."""
    CONSERVATIVE = "conservative"  # Assume all dependencies exist
    OPTIMISTIC = "optimistic"     # Assume minimal dependencies
    AST_BASED = "ast_based"       # Analyze function AST
    SIGNATURE_BASED = "signature_based"  # Analyze function signatures
    HYBRID = "hybrid"             # Combine multiple strategies


class ExecutionMode(Enum):
    """Execution modes for dependency analysis."""
    SEQUENTIAL = "sequential"     # Run all steps sequentially
    PARALLEL = "parallel"         # Run all possible steps in parallel
    OPTIMIZED = "optimized"       # Optimize based on dependencies


# ============================================================================
# Analysis Results
# ============================================================================

@dataclass
class DependencyAnalysisResult:
    """Result of dependency analysis."""
    dependency_info: Dict[str, SilverDependencyInfo]
    execution_groups: Dict[int, List[str]]
    execution_plan: List[Tuple[int, List[str]]]
    cycles: List[List[str]]
    conflicts: List[Tuple[str, str, str]]  # (step1, step2, conflict_type)
    performance_metrics: Dict[str, Any]
    recommendations: List[str]
    
    def get_total_execution_time(self) -> float:
        """Estimate total execution time based on groups."""
        return len(self.execution_groups) * 1.0  # Simplified estimation
    
    def get_parallelization_ratio(self) -> float:
        """Calculate parallelization ratio."""
        total_steps = sum(len(steps) for steps in self.execution_groups.values())
        if total_steps == 0:
            return 0.0
        parallel_steps = sum(len(steps) for steps in self.execution_groups.values() if len(steps) > 1)
        return parallel_steps / total_steps


@dataclass
class StepComplexity:
    """Complexity analysis for a step."""
    step_name: str
    complexity_score: float
    estimated_duration: float
    dependencies_count: int
    fan_out: int  # How many steps depend on this one
    critical_path: bool


# ============================================================================
# Enhanced Dependency Analyzer
# ============================================================================

class DependencyAnalyzer:
    """
    Enhanced dependency analyzer for parallel execution optimization.
    
    Features:
    - Multiple analysis strategies
    - Cycle detection and resolution
    - Performance optimization
    - Detailed reporting and visualization
    - Conflict resolution
    - Complexity analysis
    """
    
    def __init__(
        self, 
        logger: Optional[PipelineLogger] = None,
        strategy: AnalysisStrategy = AnalysisStrategy.HYBRID,
        execution_mode: ExecutionMode = ExecutionMode.OPTIMIZED
    ):
        self.logger = logger or PipelineLogger()
        self.strategy = strategy
        self.execution_mode = execution_mode
        self._analysis_cache: Dict[str, DependencyAnalysisResult] = {}
        self._complexity_cache: Dict[str, StepComplexity] = {}
    
    def analyze_dependencies(
        self, 
        silver_steps: Dict[str, SilverStep],
        bronze_steps: Optional[Dict[str, BronzeStep]] = None,
        gold_steps: Optional[Dict[str, GoldStep]] = None,
        force_refresh: bool = False
    ) -> DependencyAnalysisResult:
        """
        Analyze Silver step dependencies with enhanced features.
        
        Args:
            silver_steps: Dictionary of Silver step configurations
            bronze_steps: Optional dictionary of Bronze step configurations
            gold_steps: Optional dictionary of Gold step configurations
            force_refresh: Force refresh of cached analysis
            
        Returns:
            Comprehensive dependency analysis result
            
        Raises:
            DependencyAnalysisError: If analysis fails
            CircularDependencyError: If circular dependencies are detected
        """
        cache_key = self._generate_cache_key(silver_steps, bronze_steps, gold_steps)
        
        if not force_refresh and cache_key in self._analysis_cache:
            self.logger.debug(f"Using cached dependency analysis for key: {cache_key}")
            return self._analysis_cache[cache_key]
        
        try:
            self.logger.info(f"Starting dependency analysis with strategy: {self.strategy.value}")
            
            # Step 1: Analyze dependencies
            dependency_info = self._analyze_step_dependencies(silver_steps, bronze_steps, gold_steps)
            
            # Step 2: Detect cycles
            cycles = self._detect_cycles(dependency_info)
            if cycles:
                self.logger.warning(f"Detected {len(cycles)} circular dependencies")
                dependency_info = self._resolve_cycles(dependency_info, cycles)
            
            # Step 3: Detect conflicts
            conflicts = self._detect_conflicts(dependency_info, silver_steps)
            if conflicts:
                self.logger.warning(f"Detected {len(conflicts)} dependency conflicts")
            
            # Step 4: Assign execution groups
            self._assign_execution_groups(dependency_info)
            
            # Step 5: Generate execution plan
            execution_groups = self._group_steps_by_execution_order(dependency_info)
            execution_plan = self._generate_execution_plan(execution_groups)
            
            # Step 6: Analyze complexity
            complexity_analysis = self._analyze_complexity(dependency_info, silver_steps)
            
            # Step 7: Generate performance metrics
            performance_metrics = self._calculate_performance_metrics(
                dependency_info, execution_groups, complexity_analysis
            )
            
            # Step 8: Generate recommendations
            recommendations = self._generate_recommendations(
                dependency_info, cycles, conflicts, performance_metrics
            )
            
            # Create result
            result = DependencyAnalysisResult(
                dependency_info=dependency_info,
                execution_groups=execution_groups,
                execution_plan=execution_plan,
                cycles=cycles,
                conflicts=conflicts,
                performance_metrics=performance_metrics,
                recommendations=recommendations
            )
            
            # Cache result
            self._analysis_cache[cache_key] = result
            
            # Log results
            self._log_analysis_results(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Dependency analysis failed: {e}")
            raise DependencyAnalysisError(f"Failed to analyze dependencies: {e}")
    
    def _analyze_step_dependencies(
        self,
        silver_steps: Dict[str, SilverStep],
        bronze_steps: Optional[Dict[str, BronzeStep]] = None,
        gold_steps: Optional[Dict[str, GoldStep]] = None
    ) -> Dict[str, SilverDependencyInfo]:
        """Analyze dependencies using the configured strategy."""
        dependency_info = {}
        
        for sname, sstep in silver_steps.items():
            try:
                depends_on_silvers = self._get_step_dependencies(sname, sstep, silver_steps)
                
                dependency_info[sname] = SilverDependencyInfo(
                    step_name=sname,
                    source_bronze=sstep.source_bronze,
                    depends_on_silvers=depends_on_silvers,
                    can_run_parallel=len(depends_on_silvers) == 0,
                    execution_group=0  # Will be set later
                )
                
            except Exception as e:
                self.logger.error(f"Failed to analyze dependencies for step {sname}: {e}")
                raise InvalidDependencyError(f"Invalid dependency for step {sname}: {e}")
        
        return dependency_info
    
    def _get_step_dependencies(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Get dependencies for a step based on the analysis strategy."""
        if self.strategy == AnalysisStrategy.CONSERVATIVE:
            return self._conservative_analysis(step_name, step, all_silver_steps)
        elif self.strategy == AnalysisStrategy.OPTIMISTIC:
            return self._optimistic_analysis(step_name, step, all_silver_steps)
        elif self.strategy == AnalysisStrategy.AST_BASED:
            return self._ast_based_analysis(step_name, step, all_silver_steps)
        elif self.strategy == AnalysisStrategy.SIGNATURE_BASED:
            return self._signature_based_analysis(step_name, step, all_silver_steps)
        elif self.strategy == AnalysisStrategy.HYBRID:
            return self._hybrid_analysis(step_name, step, all_silver_steps)
        else:
            raise DependencyAnalysisError(f"Unknown analysis strategy: {self.strategy}")
    
    def _conservative_analysis(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Conservative analysis - assume all dependencies exist."""
        return set(all_silver_steps.keys()) - {step_name}
    
    def _optimistic_analysis(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Optimistic analysis - assume minimal dependencies."""
        return set()
    
    def _signature_based_analysis(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Analyze dependencies based on function signature."""
        depends_on_silvers = set()
        
        if step.transform and hasattr(step.transform, '__call__'):
            try:
                sig = inspect.signature(step.transform)
                params = list(sig.parameters.keys())
                
                # Check if function accepts prior_silvers parameter
                if 'prior_silvers' in params or 'silver_data' in params:
                    depends_on_silvers = set(all_silver_steps.keys()) - {step_name}
                elif len(params) >= 2:  # Function might depend on other silvers
                    depends_on_silvers = set(all_silver_steps.keys()) - {step_name}
                    
            except Exception as e:
                self.logger.warning(f"Could not analyze signature for {step_name}: {e}")
                depends_on_silvers = set(all_silver_steps.keys()) - {step_name}
        
        return depends_on_silvers
    
    def _ast_based_analysis(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Analyze dependencies by parsing function AST."""
        depends_on_silvers = set()
        
        if step.transform and hasattr(step.transform, '__code__'):
            try:
                # Get function source code
                source = inspect.getsource(step.transform)
                tree = ast.parse(source)
                
                # Find references to other silver steps
                for node in ast.walk(tree):
                    if isinstance(node, ast.Name):
                        if node.id in all_silver_steps and node.id != step_name:
                            depends_on_silvers.add(node.id)
                    elif isinstance(node, ast.Attribute):
                        if hasattr(node.value, 'id') and node.value.id in all_silver_steps:
                            depends_on_silvers.add(node.value.id)
                            
            except Exception as e:
                self.logger.warning(f"Could not analyze AST for {step_name}: {e}")
                # Fallback to signature-based analysis
                depends_on_silvers = self._signature_based_analysis(step_name, step, all_silver_steps)
        
        return depends_on_silvers
    
    def _hybrid_analysis(
        self,
        step_name: str,
        step: SilverStep,
        all_silver_steps: Dict[str, SilverStep]
    ) -> Set[str]:
        """Combine multiple analysis strategies."""
        signature_deps = self._signature_based_analysis(step_name, step, all_silver_steps)
        ast_deps = self._ast_based_analysis(step_name, step, all_silver_steps)
        
        # Union of all detected dependencies
        return signature_deps | ast_deps
    
    def _detect_cycles(self, dependency_info: Dict[str, SilverDependencyInfo]) -> List[List[str]]:
        """Detect circular dependencies using DFS."""
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: List[str]) -> None:
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return
            
            if node in visited:
                return
            
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            if node in dependency_info:
                for dep in dependency_info[node].depends_on_silvers:
                    dfs(dep, path.copy())
            
            rec_stack.remove(node)
            path.pop()
        
        for step_name in dependency_info:
            if step_name not in visited:
                dfs(step_name, [])
        
        return cycles
    
    def _resolve_cycles(
        self, 
        dependency_info: Dict[str, SilverDependencyInfo], 
        cycles: List[List[str]]
    ) -> Dict[str, SilverDependencyInfo]:
        """Resolve circular dependencies by breaking cycles."""
        resolved_info = dependency_info.copy()
        
        for cycle in cycles:
            self.logger.warning(f"Breaking cycle: {' -> '.join(cycle)}")
            
            # Break cycle by removing the last dependency
            if len(cycle) > 1:
                step_to_fix = cycle[-1]
                dependency_to_remove = cycle[-2]
                
                if step_to_fix in resolved_info:
                    resolved_info[step_to_fix].depends_on_silvers.discard(dependency_to_remove)
                    resolved_info[step_to_fix].can_run_parallel = len(resolved_info[step_to_fix].depends_on_silvers) == 0
        
        return resolved_info
    
    def _detect_conflicts(
        self, 
        dependency_info: Dict[str, SilverDependencyInfo],
        silver_steps: Dict[str, SilverStep]
    ) -> List[Tuple[str, str, str]]:
        """Detect dependency conflicts."""
        conflicts = []
        
        for step_name, info in dependency_info.items():
            if step_name not in silver_steps:
                continue
            
            step = silver_steps[step_name]
            
            # Check for missing source bronze
            if step.source_bronze not in [s.name for s in silver_steps.values() if hasattr(s, 'source_bronze')]:
                conflicts.append((step_name, step.source_bronze, "missing_source_bronze"))
            
            # Check for circular dependencies in dependencies
            for dep in info.depends_on_silvers:
                if dep in dependency_info and step_name in dependency_info[dep].depends_on_silvers:
                    conflicts.append((step_name, dep, "circular_dependency"))
        
        return conflicts
    
    def _assign_execution_groups(self, dependency_info: Dict[str, SilverDependencyInfo]) -> None:
        """Assign execution groups with enhanced logic."""
        # Topological sort to determine execution order
        in_degree = defaultdict(int)
        graph = defaultdict(list)
        
        for step_name, info in dependency_info.items():
            in_degree[step_name] = len(info.depends_on_silvers)
            for dep in info.depends_on_silvers:
                graph[dep].append(step_name)
        
        # BFS to assign groups
        queue = deque([step for step, degree in in_degree.items() if degree == 0])
        current_group = 0
        
        while queue:
            group_size = len(queue)
            current_group += 1
            
            for _ in range(group_size):
                step = queue.popleft()
                dependency_info[step].execution_group = current_group
                
                # Add dependent steps to queue
                for dependent in graph[step]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
    
    def _group_steps_by_execution_order(
        self, 
        dependency_info: Dict[str, SilverDependencyInfo]
    ) -> Dict[int, List[str]]:
        """Group steps by execution order."""
        execution_groups = defaultdict(list)
        
        for step_name, info in dependency_info.items():
            execution_groups[info.execution_group].append(step_name)
        
        return dict(execution_groups)
    
    def _generate_execution_plan(
        self, 
        execution_groups: Dict[int, List[str]]
    ) -> List[Tuple[int, List[str]]]:
        """Generate detailed execution plan."""
        return sorted(execution_groups.items())
    
    def _analyze_complexity(
        self, 
        dependency_info: Dict[str, SilverDependencyInfo],
        silver_steps: Dict[str, SilverStep]
    ) -> Dict[str, StepComplexity]:
        """Analyze step complexity."""
        complexity_analysis = {}
        
        for step_name, info in dependency_info.items():
            if step_name not in silver_steps:
                continue
            
            # Calculate complexity score
            complexity_score = self._calculate_complexity_score(step_name, silver_steps[step_name])
            
            # Estimate duration (simplified)
            estimated_duration = complexity_score * 0.1  # 0.1 seconds per complexity point
            
            # Calculate fan-out (how many steps depend on this one)
            fan_out = sum(1 for other_info in dependency_info.values() 
                         if step_name in other_info.depends_on_silvers)
            
            # Determine if on critical path
            critical_path = fan_out > 0 or len(info.depends_on_silvers) == 0
            
            complexity_analysis[step_name] = StepComplexity(
                step_name=step_name,
                complexity_score=complexity_score,
                estimated_duration=estimated_duration,
                dependencies_count=len(info.depends_on_silvers),
                fan_out=fan_out,
                critical_path=critical_path
            )
        
        return complexity_analysis
    
    def _calculate_complexity_score(self, step_name: str, step: SilverStep) -> float:
        """Calculate complexity score for a step."""
        score = 1.0  # Base score
        
        # Add score based on transform function complexity
        if step.transform and hasattr(step.transform, '__code__'):
            try:
                source = inspect.getsource(step.transform)
                # Simple complexity based on lines of code
                lines = len(source.split('\n'))
                score += lines * 0.1
                
                # Add score for control structures
                if 'if' in source or 'for' in source or 'while' in source:
                    score += 0.5
                    
            except Exception:
                score += 0.5  # Default complexity for unanalyzable functions
        
        # Add score based on validation rules
        if hasattr(step, 'rules') and step.rules:
            score += len(step.rules) * 0.1
        
        return score
    
    def _calculate_performance_metrics(
        self,
        dependency_info: Dict[str, SilverDependencyInfo],
        execution_groups: Dict[int, List[str]],
        complexity_analysis: Dict[str, StepComplexity]
    ) -> Dict[str, Any]:
        """Calculate performance metrics."""
        total_steps = len(dependency_info)
        parallel_groups = len([group for group in execution_groups.values() if len(group) > 1])
        sequential_groups = len(execution_groups) - parallel_groups
        
        total_complexity = sum(comp.complexity_score for comp in complexity_analysis.values())
        avg_complexity = total_complexity / total_steps if total_steps > 0 else 0
        
        critical_path_steps = sum(1 for comp in complexity_analysis.values() if comp.critical_path)
        
        return {
            "total_steps": total_steps,
            "execution_groups": len(execution_groups),
            "parallel_groups": parallel_groups,
            "sequential_groups": sequential_groups,
            "parallelization_ratio": parallel_groups / len(execution_groups) if execution_groups else 0,
            "total_complexity": total_complexity,
            "avg_complexity": avg_complexity,
            "critical_path_steps": critical_path_steps,
            "estimated_total_time": sum(comp.estimated_duration for comp in complexity_analysis.values())
        }
    
    def _generate_recommendations(
        self,
        dependency_info: Dict[str, SilverDependencyInfo],
        cycles: List[List[str]],
        conflicts: List[Tuple[str, str, str]],
        performance_metrics: Dict[str, Any]
    ) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []
        
        if cycles:
            recommendations.append(f"Consider refactoring to eliminate {len(cycles)} circular dependencies")
        
        if conflicts:
            recommendations.append(f"Resolve {len(conflicts)} dependency conflicts")
        
        if performance_metrics["parallelization_ratio"] < 0.5:
            recommendations.append("Consider optimizing dependencies to increase parallelization")
        
        if performance_metrics["avg_complexity"] > 5.0:
            recommendations.append("Consider breaking down complex steps into smaller ones")
        
        if performance_metrics["critical_path_steps"] > performance_metrics["total_steps"] * 0.8:
            recommendations.append("Consider optimizing critical path steps")
        
        return recommendations
    
    def _log_analysis_results(self, result: DependencyAnalysisResult) -> None:
        """Log analysis results."""
        self.logger.info(f"Dependency analysis completed:")
        self.logger.info(f"  - Total steps: {len(result.dependency_info)}")
        self.logger.info(f"  - Execution groups: {len(result.execution_groups)}")
        self.logger.info(f"  - Circular dependencies: {len(result.cycles)}")
        self.logger.info(f"  - Conflicts: {len(result.conflicts)}")
        self.logger.info(f"  - Parallelization ratio: {result.get_parallelization_ratio():.2%}")
        
        if result.recommendations:
            self.logger.info("Recommendations:")
            for rec in result.recommendations:
                self.logger.info(f"  - {rec}")
    
    def _generate_cache_key(
        self,
        silver_steps: Dict[str, SilverStep],
        bronze_steps: Optional[Dict[str, BronzeStep]] = None,
        gold_steps: Optional[Dict[str, GoldStep]] = None
    ) -> str:
        """Generate cache key for analysis results."""
        import hashlib
        
        key_data = {
            "silver_steps": list(silver_steps.keys()),
            "bronze_steps": list(bronze_steps.keys()) if bronze_steps else [],
            "gold_steps": list(gold_steps.keys()) if gold_steps else [],
            "strategy": self.strategy.value,
            "execution_mode": self.execution_mode.value
        }
        
        key_str = str(sorted(key_data.items()))
        return hashlib.md5(key_str.encode()).hexdigest()
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def get_execution_groups(self, dependency_info: Dict[str, SilverDependencyInfo]) -> Dict[int, List[str]]:
        """Get steps grouped by execution order."""
        return self._group_steps_by_execution_order(dependency_info)
    
    def can_run_parallel(self, step1: str, step2: str, dependency_info: Dict[str, SilverDependencyInfo]) -> bool:
        """Check if two steps can run in parallel."""
        if step1 not in dependency_info or step2 not in dependency_info:
            return False
        
        info1 = dependency_info[step1]
        info2 = dependency_info[step2]
        
        return info1.execution_group == info2.execution_group
    
    def get_dependencies(self, step_name: str, dependency_info: Dict[str, SilverDependencyInfo]) -> Set[str]:
        """Get direct dependencies for a step."""
        if step_name not in dependency_info:
            return set()
        return dependency_info[step_name].depends_on_silvers
    
    def get_all_dependencies(self, step_name: str, dependency_info: Dict[str, SilverDependencyInfo]) -> Set[str]:
        """Get all dependencies (including transitive) for a step."""
        if step_name not in dependency_info:
            return set()
        
        all_deps = set()
        to_process = {step_name}
        
        while to_process:
            current = to_process.pop()
            if current in dependency_info:
                for dep in dependency_info[current].depends_on_silvers:
                    if dep not in all_deps:
                        all_deps.add(dep)
                        to_process.add(dep)
        
        return all_deps
    
    def get_dependents(self, step_name: str, dependency_info: Dict[str, SilverDependencyInfo]) -> Set[str]:
        """Get all steps that depend on the given step."""
        dependents = set()
        
        for other_step, info in dependency_info.items():
            if step_name in info.depends_on_silvers:
                dependents.add(other_step)
        
        return dependents
    
    def clear_cache(self) -> None:
        """Clear analysis cache."""
        self._analysis_cache.clear()
        self._complexity_cache.clear()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "analysis_cache_size": len(self._analysis_cache),
            "complexity_cache_size": len(self._complexity_cache)
        }