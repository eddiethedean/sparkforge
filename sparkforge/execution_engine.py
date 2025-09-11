#!/usr/bin/env python3
"""
Execution engine for pipeline steps with parallel processing support.

This module provides comprehensive execution capabilities for pipeline steps,
including parallel processing, error handling, retry mechanisms, and performance monitoring.
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Tuple, Optional, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

from .models import SilverStep, StageStats, ExecutionContext
from .logger import PipelineLogger
from .table_operations import fqn
from .performance import now_dt, time_write_operation
from .reporting import create_validation_dict, create_transform_dict, create_write_dict
from .validation import apply_column_rules


class ExecutionMode(Enum):
    """Execution modes for pipeline steps."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ADAPTIVE = "adaptive"
    BATCH = "batch"


class RetryStrategy(Enum):
    """Retry strategies for failed steps."""
    NONE = "none"
    IMMEDIATE = "immediate"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"


@dataclass
class ExecutionConfig:
    """Configuration for execution engine."""
    mode: ExecutionMode = ExecutionMode.ADAPTIVE
    max_workers: int = 4
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout_seconds: Optional[int] = None
    enable_caching: bool = True
    enable_monitoring: bool = True
    batch_size: int = 10
    adaptive_threshold: float = 0.5


@dataclass
class ExecutionResult:
    """Result of step execution."""
    step_name: str
    success: bool
    duration_seconds: float
    rows_processed: int
    error: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionStats:
    """Statistics for execution performance."""
    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration: float = 0.0
    parallel_efficiency: float = 0.0
    average_step_duration: float = 0.0
    retry_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0


class ExecutionEngine:
    """
    Advanced execution engine for pipeline steps with comprehensive features.
    
    Features:
    - Multiple execution modes (sequential, parallel, adaptive, batch)
    - Retry mechanisms with different strategies
    - Performance monitoring and caching
    - Error handling and recovery
    - Resource management and optimization
    """
    
    def __init__(
        self, 
        spark: SparkSession, 
        logger: PipelineLogger, 
        thresholds: Dict[str, float], 
        schema: str = "",
        config: Optional[ExecutionConfig] = None
    ):
        self.spark = spark
        self.logger = logger
        self.thresholds = thresholds
        self.schema = schema
        self.config = config or ExecutionConfig()
        
        # Execution state
        self._execution_cache: Dict[str, Any] = {}
        self._execution_stats = ExecutionStats()
        self._active_futures: Set[Future] = set()
        self._lock = threading.Lock()
        
        # Performance tracking
        self._step_timings: Dict[str, List[float]] = {}
        self._resource_usage: Dict[str, Any] = {}
    
    def execute_silver_step(
        self, 
        sname: str, 
        sstep: SilverStep, 
        bronze_in: DataFrame, 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Execute a single Silver step with enhanced error handling and monitoring.
        
        Args:
            sname: Step name
            sstep: Silver step configuration
            bronze_in: Input bronze DataFrame
            prior_silvers: Prior silver DataFrames
            mode: Execution mode
            context: Execution context
            
        Returns:
            Tuple of (step_name, result_dict)
        """
        fqn_name = fqn(self.schema, sstep.table_name)
        start_time = time.time()
        
        try:
            # Check cache first (but not for incremental runs to ensure fresh data)
            if self.config.enable_caching and mode != "incremental" and sname in self._execution_cache:
                self._execution_stats.cache_hits += 1
                self.logger.debug(f"Using cached result for step {sname}")
                return sname, self._execution_cache[sname]
            
            self._execution_stats.cache_misses += 1
            
            # Execute step based on type
            if sstep.existing:
                result = self._execute_existing_silver(sname, sstep, fqn_name, context)
            else:
                result = self._execute_transform_silver(
                    sname, sstep, bronze_in, prior_silvers, mode, fqn_name, bronze_steps, context
                )
            
            # Cache successful results
            if self.config.enable_caching and not result.get("skipped", False):
                self._execution_cache[sname] = result
            
            # Update statistics
            duration = time.time() - start_time
            self._update_step_timing(sname, duration)
            self._execution_stats.successful_steps += 1
            
            return sname, result
            
        except Exception as e:
            duration = time.time() - start_time
            self._update_step_timing(sname, duration)
            self._execution_stats.failed_steps += 1
            
            error_entry = {
                "table_fqn": fqn_name,
                "error": str(e),
                "skipped": False,
                "duration_seconds": duration,
                "retry_count": 0
            }
            
            self.logger.error(f"Silver step {sname} failed: {e}")
            import traceback
            self.logger.error(f"Silver step {sname} traceback: {traceback.format_exc()}")
            return sname, error_entry
    
    def execute_silver_steps(
        self, 
        silver_steps_to_execute: List[str], 
        silver_steps: Dict[str, SilverStep],
        bronze_valid: Dict[str, DataFrame], 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """
        Execute Silver steps using the configured execution mode.
        
        Args:
            silver_steps_to_execute: List of step names to execute
            silver_steps: Dictionary of step configurations
            bronze_valid: Valid bronze DataFrames
            prior_silvers: Prior silver DataFrames
            mode: Execution mode
            bronze_steps: Dictionary of Bronze step configurations (optional)
            context: Execution context
            
        Returns:
            Dictionary of execution results
        """
        if not silver_steps_to_execute:
            return {}
        
        self.logger.info(f"Executing {len(silver_steps_to_execute)} silver steps in {self.config.mode.value} mode")
        
        start_time = time.time()
        
        try:
            if self.config.mode == ExecutionMode.SEQUENTIAL:
                results = self._execute_sequential(
                    silver_steps_to_execute, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
                )
            elif self.config.mode == ExecutionMode.PARALLEL:
                results = self._execute_parallel(
                    silver_steps_to_execute, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
                )
            elif self.config.mode == ExecutionMode.ADAPTIVE:
                results = self._execute_adaptive(
                    silver_steps_to_execute, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
                )
            elif self.config.mode == ExecutionMode.BATCH:
                results = self._execute_batch(
                    silver_steps_to_execute, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
                )
            else:
                raise ValueError(f"Unknown execution mode: {self.config.mode}")
            
            # Update execution statistics
            self._execution_stats.total_duration = time.time() - start_time
            self._execution_stats.total_steps = len(silver_steps_to_execute)
            self._calculate_parallel_efficiency()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Execution failed: {e}")
            raise
    
    def _execute_sequential(
        self, 
        silver_steps_to_execute: List[str], 
        silver_steps: Dict[str, SilverStep],
        bronze_valid: Dict[str, DataFrame], 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute steps sequentially."""
        results = {}
        
        for sname in silver_steps_to_execute:
            sstep = silver_steps[sname]
            bronze_in = bronze_valid[sstep.source_bronze]
            
            step_name, entry = self.execute_silver_step(sname, sstep, bronze_in, prior_silvers, mode, bronze_steps, context)
            results[step_name] = entry
            
            # Update prior_silvers for next iteration
            if not entry.get("skipped", False) and "error" not in entry:
                fqn_name = fqn(self.schema, sstep.table_name)
                try:
                    prior_silvers[sname] = self.spark.table(fqn_name)
                except Exception as e:
                    self.logger.warning(f"Could not update prior_silvers for {sname}: {e}")
        
        return results
    
    def _execute_parallel(
        self, 
        silver_steps_to_execute: List[str], 
        silver_steps: Dict[str, SilverStep],
        bronze_valid: Dict[str, DataFrame], 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute steps in parallel."""
        if len(silver_steps_to_execute) <= 1:
            return self._execute_sequential(
                silver_steps_to_execute, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
            )
        
        self.logger.parallel_start(silver_steps_to_execute, 0)
        
        results = {}
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_step = {}
            for sname in silver_steps_to_execute:
                sstep = silver_steps[sname]
                bronze_in = bronze_valid[sstep.source_bronze]
                
                future = executor.submit(
                    self.execute_silver_step,
                    sname, sstep, bronze_in, prior_silvers, mode, bronze_steps, context
                )
                future_to_step[future] = sname
                self._active_futures.add(future)
            
            # Collect results as they complete
            for future in as_completed(future_to_step):
                sname = future_to_step[future]
                try:
                    step_name, entry = future.result(timeout=self.config.timeout_seconds)
                    results[step_name] = entry
                    self.logger.parallel_complete(step_name)
                except Exception as e:
                    self.logger.error(f"Silver step {sname} failed in parallel execution: {e}")
                    import traceback
                    self.logger.error(f"Silver step {sname} parallel execution traceback: {traceback.format_exc()}")
                    results[sname] = {"error": str(e), "skipped": False}
                finally:
                    self._active_futures.discard(future)
        
        return results
    
    def _execute_adaptive(
        self, 
        silver_steps_to_execute: List[str], 
        silver_steps: Dict[str, SilverStep],
        bronze_valid: Dict[str, DataFrame], 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute steps using adaptive strategy based on step characteristics."""
        # Analyze step dependencies and characteristics
        independent_steps = []
        dependent_steps = []
        
        for sname in silver_steps_to_execute:
            sstep = silver_steps[sname]
            # Simple heuristic: steps with no prior_silvers dependencies can run in parallel
            if not hasattr(sstep, 'depends_on_silvers') or not sstep.depends_on_silvers:
                independent_steps.append(sname)
            else:
                dependent_steps.append(sname)
        
        results = {}
        
        # Execute independent steps in parallel
        if independent_steps:
            self.logger.info(f"Executing {len(independent_steps)} independent steps in parallel")
            parallel_results = self._execute_parallel(
                independent_steps, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
            )
            results.update(parallel_results)
        
        # Execute dependent steps sequentially
        if dependent_steps:
            self.logger.info(f"Executing {len(dependent_steps)} dependent steps sequentially")
            sequential_results = self._execute_sequential(
                dependent_steps, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
            )
            results.update(sequential_results)
        
        return results
    
    def _execute_batch(
        self, 
        silver_steps_to_execute: List[str], 
        silver_steps: Dict[str, SilverStep],
        bronze_valid: Dict[str, DataFrame], 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute steps in batches."""
        results = {}
        batch_size = self.config.batch_size
        
        for i in range(0, len(silver_steps_to_execute), batch_size):
            batch = silver_steps_to_execute[i:i + batch_size]
            self.logger.info(f"Executing batch {i//batch_size + 1}: {len(batch)} steps")
            
            batch_results = self._execute_parallel(
                batch, silver_steps, bronze_valid, prior_silvers, mode, bronze_steps, context
            )
            results.update(batch_results)
        
        return results
    
    def _execute_existing_silver(
        self, 
        sname: str, 
        sstep: SilverStep, 
        fqn_name: str,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute validation of existing Silver table."""
        self.logger.step_start("silver", sname)
        
        df = self.spark.table(fqn_name)
        v_start = now_dt()
        valid, _inv, stats = apply_column_rules(df, sstep.rules, "silver", sname)
        v_end = now_dt()
        self._enforce_threshold("silver", stats)
        
        rows_written, w_secs, w_start, w_end = time_write_operation("overwrite", valid, fqn_name)
        
        entry = {
            "table_fqn": fqn_name,
            "validation": create_validation_dict(stats, start_at=v_start, end_at=v_end),
            "write": create_write_dict("overwrite", rows_written, w_secs, fqn_name, skipped=False, start_at=w_start, end_at=w_end),
            "skipped": False,
        }
        
        self.logger.step_complete("silver", sname, stats.duration_secs)
        return entry
    
    def _execute_transform_silver(
        self, 
        sname: str, 
        sstep: SilverStep, 
        bronze_in: DataFrame, 
        prior_silvers: Dict[str, DataFrame], 
        mode: str, 
        fqn_name: str,
        bronze_steps: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """Execute Silver transform step with enhanced error handling."""
        in_rows = bronze_in.count()
        
        # If incremental and there are no new rows for this Silver, SKIP this step
        if mode == "incremental" and in_rows == 0:
            entry = {
                "table_fqn": fqn_name,
                "transform": create_transform_dict(0, 0, 0.0, skipped=True, start_at=None, end_at=None),
                "validation": create_validation_dict(None, start_at=None, end_at=None),
                "write": create_write_dict("append", 0, 0.0, fqn_name, skipped=True, start_at=None, end_at=None),
                "skipped": True,
            }
            self.logger.step_skipped("silver", sname)
            self._execution_stats.skipped_steps += 1
            return entry
        
        t_start = now_dt()
        sout = self._call_silver_transform(sstep.transform, self.spark, bronze_in, prior_silvers)
        t_end = now_dt()
        out_rows = sout.count()
        
        svalid, sstats, v_start, v_end = self._validate_silver(sout, sstep.rules, sname)
        
        # Determine write mode based on Bronze step incremental capability
        if mode == "initial":
            write_mode = "overwrite"
        else:
            # Check if source Bronze step has incremental capability
            source_bronze_has_incremental = True  # Default to True for backward compatibility
            if bronze_steps and sstep.source_bronze in bronze_steps:
                bronze_step = bronze_steps[sstep.source_bronze]
                if hasattr(bronze_step, 'has_incremental_capability'):
                    source_bronze_has_incremental = bronze_step.has_incremental_capability
                elif hasattr(bronze_step, 'incremental_col'):
                    source_bronze_has_incremental = bronze_step.incremental_col is not None
            
            if source_bronze_has_incremental:
                write_mode = "append"
            else:
                write_mode = "overwrite"  # Force full refresh when Bronze has no incremental column
                self.logger.info(f"Silver step {sname} using overwrite mode (Bronze step {sstep.source_bronze} has no incremental column)")
        
        rows_written, w_secs, w_start, w_end = time_write_operation(write_mode, svalid, fqn_name)
        
        entry = {
            "table_fqn": fqn_name,
            "transform": create_transform_dict(in_rows, out_rows, (t_end - t_start).total_seconds(), skipped=False, start_at=t_start, end_at=t_end),
            "validation": create_validation_dict(sstats, start_at=v_start, end_at=v_end),
            "write": create_write_dict(write_mode, rows_written, w_secs, fqn_name, skipped=False, start_at=w_start, end_at=w_end),
            "skipped": False,
        }
        
        self.logger.step_complete("silver", sname, sstats.duration_secs)
        return entry
    
    def _call_silver_transform(
        self,
        fn: Callable[..., DataFrame],
        spark: SparkSession,
        bronze_df: DataFrame,
        prior_silvers: Dict[str, DataFrame],
    ) -> DataFrame:
        """Robustly support both (spark, bronze_df, prior_silvers) and (spark, bronze_df)."""
        try:
            return fn(spark, bronze_df, prior_silvers)
        except TypeError:
            # Fall back to the 2-arg signature
            return fn(spark, bronze_df)
    
    def _validate_silver(
        self, 
        df: DataFrame, 
        rules: Dict[str, List[Any]], 
        step_name: str
    ) -> Tuple[DataFrame, StageStats, datetime, datetime]:
        """Validate Silver DataFrame and enforce thresholds."""
        v_start = now_dt()
        valid, _invalid, stats = apply_column_rules(df, rules, "silver", step_name)
        v_end = now_dt()
        self._enforce_threshold("silver", stats)
        return valid, stats, v_start, v_end
    
    def _enforce_threshold(self, stage: str, stats: StageStats) -> None:
        """Enforce validation thresholds."""
        if stats and stats.validation_rate < self.thresholds[stage]:
            self.logger.validation_failed(stage, stats.step, stats.validation_rate, self.thresholds[stage])
            raise ValueError(
                f"[{stage}:{stats.step}] validation {stats.validation_rate:.2f}% "
                f"below required {self.thresholds[stage]:.2f}%"
            )
    
    def _update_step_timing(self, step_name: str, duration: float) -> None:
        """Update step timing statistics."""
        with self._lock:
            if step_name not in self._step_timings:
                self._step_timings[step_name] = []
            self._step_timings[step_name].append(duration)
    
    def _calculate_parallel_efficiency(self) -> None:
        """Calculate parallel execution efficiency."""
        if self._execution_stats.total_steps > 1:
            sequential_time = sum(
                sum(timings) for timings in self._step_timings.values()
            )
            if sequential_time > 0:
                self._execution_stats.parallel_efficiency = (
                    sequential_time / self._execution_stats.total_duration
                )
                self._execution_stats.average_step_duration = (
                    sequential_time / self._execution_stats.total_steps
                )
    
    def retry_step(
        self, 
        sname: str, 
        sstep: SilverStep, 
        bronze_in: DataFrame, 
        prior_silvers: Dict[str, DataFrame], 
        mode: str,
        context: Optional[ExecutionContext] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Retry a failed step with configured retry strategy."""
        if self.config.retry_strategy == RetryStrategy.NONE:
            return sname, {"error": "Retry disabled", "skipped": False}
        
        for attempt in range(self.config.max_retries):
            try:
                # Calculate delay based on strategy
                delay = self._calculate_retry_delay(attempt)
                if delay > 0:
                    time.sleep(delay)
                
                self.logger.info(f"Retrying step {sname} (attempt {attempt + 1}/{self.config.max_retries})")
                
                result = self.execute_silver_step(sname, sstep, bronze_in, prior_silvers, mode, context)
                if not result[1].get("error"):
                    self._execution_stats.retry_count += 1
                    return result
                
            except Exception as e:
                self.logger.warning(f"Retry attempt {attempt + 1} failed for {sname}: {e}")
        
        return sname, {"error": f"Failed after {self.config.max_retries} retries", "skipped": False}
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay based on strategy."""
        if self.config.retry_strategy == RetryStrategy.IMMEDIATE:
            return 0.0
        elif self.config.retry_strategy == RetryStrategy.LINEAR_BACKOFF:
            return self.config.retry_delay * (attempt + 1)
        elif self.config.retry_strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            return self.config.retry_delay * (2 ** attempt)
        else:
            return 0.0
    
    def get_execution_stats(self) -> ExecutionStats:
        """Get current execution statistics."""
        return self._execution_stats
    
    def get_step_performance(self, step_name: str) -> Dict[str, Any]:
        """Get performance metrics for a specific step."""
        if step_name not in self._step_timings:
            return {}
        
        timings = self._step_timings[step_name]
        return {
            "step_name": step_name,
            "execution_count": len(timings),
            "average_duration": sum(timings) / len(timings),
            "min_duration": min(timings),
            "max_duration": max(timings),
            "total_duration": sum(timings)
        }
    
    def clear_cache(self) -> None:
        """Clear execution cache."""
        self._execution_cache.clear()
        self.logger.info("Execution cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "cache_size": len(self._execution_cache),
            "cache_hits": self._execution_stats.cache_hits,
            "cache_misses": self._execution_stats.cache_misses,
            "hit_ratio": (
                self._execution_stats.cache_hits / 
                (self._execution_stats.cache_hits + self._execution_stats.cache_misses)
                if (self._execution_stats.cache_hits + self._execution_stats.cache_misses) > 0
                else 0.0
            )
        }
    
    def stop_execution(self) -> None:
        """Stop all active executions."""
        with self._lock:
            for future in self._active_futures:
                future.cancel()
            self._active_futures.clear()
        self.logger.info("Execution stopped")
    
    @contextmanager
    def execution_context(self, context: ExecutionContext):
        """Context manager for execution with proper cleanup."""
        try:
            yield self
        finally:
            self.stop_execution()
    
    def optimize_configuration(self) -> ExecutionConfig:
        """Optimize execution configuration based on performance data."""
        if not self._step_timings:
            return self.config
        
        # Analyze performance patterns
        avg_duration = sum(
            sum(timings) / len(timings) for timings in self._step_timings.values()
        ) / len(self._step_timings)
        
        # Adjust configuration based on performance
        if avg_duration < 1.0:  # Fast steps
            optimized_config = ExecutionConfig(
                mode=ExecutionMode.PARALLEL,
                max_workers=min(self.config.max_workers * 2, 8),
                batch_size=max(self.config.batch_size // 2, 1)
            )
        elif avg_duration > 10.0:  # Slow steps
            optimized_config = ExecutionConfig(
                mode=ExecutionMode.SEQUENTIAL,
                max_workers=1,
                batch_size=1
            )
        else:  # Medium steps
            optimized_config = ExecutionConfig(
                mode=ExecutionMode.ADAPTIVE,
                max_workers=self.config.max_workers,
                batch_size=self.config.batch_size
            )
        
        self.logger.info(f"Optimized configuration: {optimized_config.mode.value} mode, {optimized_config.max_workers} workers")
        return optimized_config