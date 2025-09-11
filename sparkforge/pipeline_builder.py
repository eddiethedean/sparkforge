#!/usr/bin/env python3
"""
Main Pipeline Builder and Runner classes.

This module provides comprehensive pipeline building and execution capabilities
with advanced features like dependency analysis, parallel processing, monitoring,
and error handling.

Key Features:
- Fluent API for pipeline construction
- Advanced dependency analysis and optimization
- Multiple execution modes and strategies
- Comprehensive monitoring and reporting
- Error handling and recovery mechanisms
- Performance optimization and caching
- Extensible architecture for custom steps
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple
from datetime import datetime
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

from .models import (
    BronzeStep, SilverStep, GoldStep, PipelineConfig, ExecutionContext,
    StageStats, SilverDependencyInfo, BaseModel
)
from .config import ConfigManager, ConfigTemplate
from .logger import PipelineLogger, ExecutionTimer
from .table_operations import fqn
from .performance import now_dt, time_write_operation
from .reporting import create_validation_dict, create_transform_dict, create_write_dict
from .validation import apply_column_rules
from .dependency_analyzer import DependencyAnalyzer, DependencyAnalysisResult
from .execution_engine import ExecutionEngine, ExecutionConfig, ExecutionMode


class PipelineMode(Enum):
    """Pipeline execution modes."""
    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    VALIDATION_ONLY = "validation_only"


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration: float = 0.0
    bronze_duration: float = 0.0
    silver_duration: float = 0.0
    gold_duration: float = 0.0
    total_rows_processed: int = 0
    total_rows_written: int = 0
    parallel_efficiency: float = 0.0
    cache_hit_rate: float = 0.0
    error_count: int = 0
    retry_count: int = 0


@dataclass
class PipelineReport:
    """Comprehensive pipeline execution report."""
    pipeline_id: str
    execution_id: str
    mode: PipelineMode
    status: PipelineStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)
    bronze_results: Dict[str, Any] = field(default_factory=dict)
    silver_results: Dict[str, Any] = field(default_factory=dict)
    gold_results: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    dependency_analysis: Optional[DependencyAnalysisResult] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "mode": self.mode.value,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "metrics": {
                "total_steps": self.metrics.total_steps,
                "successful_steps": self.metrics.successful_steps,
                "failed_steps": self.metrics.failed_steps,
                "skipped_steps": self.metrics.skipped_steps,
                "total_duration": self.metrics.total_duration,
                "bronze_duration": self.metrics.bronze_duration,
                "silver_duration": self.metrics.silver_duration,
                "gold_duration": self.metrics.gold_duration,
                "total_rows_processed": self.metrics.total_rows_processed,
                "total_rows_written": self.metrics.total_rows_written,
                "parallel_efficiency": self.metrics.parallel_efficiency,
                "cache_hit_rate": self.metrics.cache_hit_rate,
                "error_count": self.metrics.error_count,
                "retry_count": self.metrics.retry_count
            },
            "bronze_results": self.bronze_results,
            "silver_results": self.silver_results,
            "gold_results": self.gold_results,
            "errors": self.errors,
            "warnings": self.warnings,
            "recommendations": self.recommendations
        }


class StepValidator(Protocol):
    """Protocol for custom step validators."""
    
    def validate(self, step: BaseModel, context: ExecutionContext) -> List[str]:
        """Validate a step and return any validation errors."""
        ...


class PipelineBuilder:
    """
    Advanced builder for creating data pipelines with Bronze → Silver → Gold architecture.
    
    Features:
    - Fluent API for easy pipeline construction
    - Advanced validation and error checking
    - Support for custom step types and validators
    - Configuration management and templates
    - Performance monitoring and optimization
    """
    
    def __init__(
        self, 
        *, 
        spark: SparkSession, 
        schema: str, 
        min_bronze_rate: float = 95.0, 
        min_silver_rate: float = 98.0, 
        min_gold_rate: float = 99.0, 
        verbose: bool = True, 
        enable_parallel_silver: bool = True, 
        max_parallel_workers: int = 4,
        execution_mode: ExecutionMode = ExecutionMode.ADAPTIVE,
        enable_caching: bool = True,
        enable_monitoring: bool = True
    ) -> None:
        self.spark = spark
        self.schema = schema
        self.pipeline_id = str(uuid.uuid4())
        
        # Create configuration
        self.config = ConfigManager.create_config(
            schema=schema,
            min_bronze_rate=min_bronze_rate,
            min_silver_rate=min_silver_rate,
            min_gold_rate=min_gold_rate,
            enable_parallel_silver=enable_parallel_silver,
            max_parallel_workers=max_parallel_workers,
            verbose=verbose
        )
        
        # Pipeline steps
        self.bronze_steps: Dict[str, BronzeStep] = {}
        self.silver_steps: Dict[str, SilverStep] = {}
        self.gold_steps: Dict[str, GoldStep] = {}
        
        # Custom validators
        self.validators: List[StepValidator] = []
        
        # Initialize components
        self.logger = PipelineLogger(verbose=self.config.verbose)
        self.dependency_analyzer = DependencyAnalyzer(self.logger)
        
        # Create execution engine with advanced configuration
        execution_config = ExecutionConfig(
            mode=execution_mode,
            max_workers=max_parallel_workers,
            enable_caching=enable_caching,
            enable_monitoring=enable_monitoring
        )
        
        self.execution_engine = ExecutionEngine(
            spark=self.spark, 
            logger=self.logger, 
            thresholds={
                'bronze': self.config.thresholds.bronze,
                'silver': self.config.thresholds.silver,
                'gold': self.config.thresholds.gold
            },
            schema=self.config.schema,
            config=execution_config
        )
        
        self.logger.info(f"🔧 PipelineBuilder initialized (ID: {self.pipeline_id})")
    
    def with_bronze_rules(
        self, 
        *, 
        name: str, 
        rules: Dict[str, List[Any]], 
        incremental_col: Optional[str] = None,
        description: Optional[str] = None
    ) -> 'PipelineBuilder':
        """
        Add Bronze step validation rules.
        
        Args:
            name: Step name
            rules: Validation rules for the step
            incremental_col: Column used for incremental processing (optional)
                           If None, forces full refresh of downstream Silver tables
            description: Optional description of the step
        """
        step = BronzeStep(name, rules, incremental_col)
        if description:
            step.description = description
        
        self.bronze_steps[name] = step
        
        if incremental_col:
            self.logger.info(f"➕ Bronze step registered: {name} (incremental: {incremental_col})")
        else:
            self.logger.info(f"➕ Bronze step registered: {name} (full refresh mode)")
        
        return self
    
    def with_silver_rules(
        self, 
        *, 
        name: str, 
        table_name: str, 
        rules: Dict[str, List[Any]], 
        watermark_col: str,
        description: Optional[str] = None
    ) -> 'PipelineBuilder':
        """Add existing Silver table validation rules."""
        step = SilverStep(
            name=name, 
            source_bronze=None, 
            transform=None, 
            rules=rules, 
            table_name=table_name, 
            watermark_col=watermark_col,
            existing=True
        )
        if description:
            step.description = description
        
        self.silver_steps[name] = step
        self.logger.info(f"➕ Existing Silver registered: {name}")
        return self
    
    def add_silver_transform(
        self, 
        *, 
        name: str, 
        source_bronze: str, 
        transform: Callable[..., DataFrame], 
        rules: Dict[str, List[Any]], 
        table_name: str, 
        watermark_col: Optional[str] = None,
        description: Optional[str] = None,
        depends_on: Optional[List[str]] = None
    ) -> 'PipelineBuilder':
        """
        Add Silver transform step.
        
        Args:
            name: Step name
            source_bronze: Source bronze step name
            transform: Transform function
            rules: Validation rules for the step
            table_name: Target table name
            watermark_col: Watermark column for incremental processing (optional)
                         If None, the step will use overwrite mode for all runs
            description: Optional description of the step
            depends_on: Optional list of other Silver steps this depends on
        """
        step = SilverStep(
            name=name, 
            source_bronze=source_bronze, 
            transform=transform, 
            rules=rules, 
            table_name=table_name, 
            watermark_col=watermark_col
        )
        if description:
            step.description = description
        if depends_on:
            step.depends_on_silvers = set(depends_on)
        
        self.silver_steps[name] = step
        self.logger.info(f"➕ Silver step registered: {name}")
        return self
    
    def add_gold_transform(
        self, 
        *, 
        name: str, 
        transform: Callable[[SparkSession, Dict[str, DataFrame]], DataFrame], 
        rules: Dict[str, List[Any]], 
        table_name: str, 
        source_silvers: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> 'PipelineBuilder':
        """Add Gold transform step."""
        step = GoldStep(
            name=name, 
            transform=transform, 
            rules=rules, 
            table_name=table_name, 
            source_silvers=source_silvers
        )
        if description:
            step.description = description
        
        self.gold_steps[name] = step
        self.logger.info(f"➕ Gold step registered: {name}")
        return self
    
    def add_validator(self, validator: StepValidator) -> 'PipelineBuilder':
        """Add a custom step validator."""
        self.validators.append(validator)
        self.logger.info(f"➕ Custom validator registered: {validator.__class__.__name__}")
        return self
    
    def validate_pipeline(self) -> List[str]:
        """Validate the entire pipeline configuration."""
        errors = []
        
        # Validate bronze steps
        for name, step in self.bronze_steps.items():
            try:
                step.validate()
            except Exception as e:
                errors.append(f"Bronze step '{name}': {str(e)}")
        
        # Validate silver steps
        for name, step in self.silver_steps.items():
            try:
                step.validate()
                # Check source bronze exists
                if not step.existing and step.source_bronze not in self.bronze_steps:
                    errors.append(f"Silver step '{name}': source bronze '{step.source_bronze}' not found")
            except Exception as e:
                errors.append(f"Silver step '{name}': {str(e)}")
        
        # Validate gold steps
        for name, step in self.gold_steps.items():
            try:
                step.validate()
                # Check source silvers exist
                if step.source_silvers:
                    for silver_name in step.source_silvers:
                        if silver_name not in self.silver_steps:
                            errors.append(f"Gold step '{name}': source silver '{silver_name}' not found")
            except Exception as e:
                errors.append(f"Gold step '{name}': {str(e)}")
        
        # Run custom validators
        for validator in self.validators:
            for step in list(self.bronze_steps.values()) + list(self.silver_steps.values()) + list(self.gold_steps.values()):
                try:
                    context = ExecutionContext(mode="initial", start_time=now_dt())
                    validator_errors = validator.validate(step, context)
                    errors.extend(validator_errors)
                except Exception as e:
                    errors.append(f"Validator error for step '{step.name}': {str(e)}")
        
        return errors
    
    def to_pipeline(self) -> 'PipelineRunner':
        """Create a PipelineRunner from the configured steps."""
        # Validate pipeline before creating runner
        validation_errors = self.validate_pipeline()
        if validation_errors:
            error_msg = "Pipeline validation failed:\n" + "\n".join(f"  - {error}" for error in validation_errors)
            raise ValueError(error_msg)
        
        return PipelineRunner(
            pipeline_id=self.pipeline_id,
            spark=self.spark,
            config=self.config,
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            logger=self.logger,
            dependency_analyzer=self.dependency_analyzer,
            execution_engine=self.execution_engine
        )


class PipelineRunner:
    """
    Advanced pipeline execution engine with comprehensive monitoring and error handling.
    
    Features:
    - Multiple execution modes and strategies
    - Advanced dependency analysis and optimization
    - Comprehensive monitoring and reporting
    - Error handling and recovery mechanisms
    - Performance optimization and caching
    - Real-time status updates and progress tracking
    """
    
    def __init__(
        self, 
        *,
        pipeline_id: str,
        spark: SparkSession, 
        config: PipelineConfig,
        bronze_steps: Dict[str, BronzeStep], 
        silver_steps: Dict[str, SilverStep], 
        gold_steps: Dict[str, GoldStep],
        logger: PipelineLogger,
        dependency_analyzer: DependencyAnalyzer,
        execution_engine: ExecutionEngine
    ) -> None:
        self.pipeline_id = pipeline_id
        self.spark = spark
        self.config = config
        self.bronze_steps = bronze_steps
        self.silver_steps = silver_steps
        self.gold_steps = gold_steps
        self.logger = logger
        self.dependency_analyzer = dependency_analyzer
        self.execution_engine = execution_engine
        
        # Execution state
        self._current_report: Optional[PipelineReport] = None
        self._is_running = False
        self._cancelled = False
        
        self.logger.info(f"🔩 PipelineRunner ready (ID: {self.pipeline_id})")
    
    def initial_load(self, *, bronze_sources: Dict[str, DataFrame]) -> PipelineReport:
        """Execute initial pipeline load."""
        return self._run(PipelineMode.INITIAL, bronze_sources=bronze_sources)
    
    def run_incremental(self, *, bronze_sources: Dict[str, DataFrame]) -> PipelineReport:
        """Execute incremental pipeline run."""
        return self._run(PipelineMode.INCREMENTAL, bronze_sources=bronze_sources)
    
    def run_full_refresh(self, *, bronze_sources: Dict[str, DataFrame]) -> PipelineReport:
        """Execute full refresh pipeline run."""
        return self._run(PipelineMode.FULL_REFRESH, bronze_sources=bronze_sources)
    
    def run_validation_only(self, *, bronze_sources: Dict[str, DataFrame]) -> PipelineReport:
        """Execute validation-only pipeline run."""
        return self._run(PipelineMode.VALIDATION_ONLY, bronze_sources=bronze_sources)
    
    def _run(self, mode: PipelineMode, *, bronze_sources: Dict[str, DataFrame]) -> PipelineReport:
        """Main execution engine for all pipeline modes."""
        if self._is_running:
            raise RuntimeError("Pipeline is already running")
        
        execution_id = str(uuid.uuid4())
        start_time = now_dt()
        
        # Create execution report
        self._current_report = PipelineReport(
            pipeline_id=self.pipeline_id,
            execution_id=execution_id,
            mode=mode,
            status=PipelineStatus.RUNNING,
            start_time=start_time
        )
        
        self._is_running = True
        self._cancelled = False
        
        try:
            with ExecutionTimer(self.logger, f"{mode.value.upper()} pipeline execution"):
                context = ExecutionContext(
                    mode=mode.value, 
                    start_time=start_time,
                    run_id=execution_id
                )
                
                self.logger.info(f"🚀 Pipeline execution started (Mode: {mode.value}, ID: {execution_id})")
                
                # Execute Bronze validation
                bronze_start = time.time()
                bronze_valid = self._execute_bronze_steps(mode, bronze_sources, context)
                bronze_duration = time.time() - bronze_start
                self._current_report.metrics.bronze_duration = bronze_duration
                
                # Execute Silver steps with advanced processing
                silver_start = time.time()
                silver_results = self._execute_silver_steps(mode, bronze_valid, context)
                silver_duration = time.time() - silver_start
                self._current_report.metrics.silver_duration = silver_duration
                
                # Execute Gold steps (can run concurrently with Bronze in future iterations)
                gold_start = time.time()
                self._execute_gold_steps(silver_results, context)
                gold_duration = time.time() - gold_start
                self._current_report.metrics.gold_duration = gold_duration
                
                # Finalize execution
                end_time = now_dt()
                total_duration = time.time() - bronze_start
                
                context.end_time = end_time
                context.duration_secs = total_duration
                
                self._current_report.end_time = end_time
                self._current_report.duration_seconds = total_duration
                self._current_report.status = PipelineStatus.COMPLETED
                
                # Update metrics
                self._update_metrics()
                
                # Generate recommendations
                self._generate_recommendations()
                
                self.logger.execution_summary(mode.value, total_duration, self._current_report.metrics.total_rows_written)
                
                return self._current_report
                
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self._current_report.status = PipelineStatus.FAILED
            self._current_report.errors.append(str(e))
            self._current_report.end_time = now_dt()
            self._current_report.duration_seconds = time.time() - start_time.timestamp()
            raise
        finally:
            self._is_running = False
    
    def _execute_bronze_steps(
        self, 
        mode: PipelineMode, 
        bronze_sources: Dict[str, DataFrame], 
        context: ExecutionContext
    ) -> Dict[str, DataFrame]:
        """Execute Bronze validation steps with enhanced error handling and concurrent processing."""
        if not self.bronze_steps:
            return {}
        
        # Check if parallel execution is enabled and we have multiple bronze steps
        if (self.config.parallel.enabled and 
            len(self.bronze_steps) > 1 and 
            self.config.parallel.max_workers > 1):
            return self._execute_bronze_steps_parallel(mode, bronze_sources, context)
        else:
            return self._execute_bronze_steps_sequential(mode, bronze_sources, context)
    
    def _execute_bronze_steps_sequential(
        self, 
        mode: PipelineMode, 
        bronze_sources: Dict[str, DataFrame], 
        context: ExecutionContext
    ) -> Dict[str, DataFrame]:
        """Execute Bronze validation steps sequentially."""
        bronze_valid = {}
        
        for bname, bstep in self.bronze_steps.items():
            if self._cancelled:
                raise RuntimeError("Pipeline execution cancelled")
            
            if bname not in bronze_sources:
                error_msg = f"Missing bronze source: {bname}"
                self._current_report.errors.append(error_msg)
                raise ValueError(error_msg)
            
            self.logger.step_start("bronze", bname)
            
            try:
                df = bronze_sources[bname]
                v_start = now_dt()
                valid, _inv, stats = apply_column_rules(df, bstep.rules, "bronze", bname)
                v_end = now_dt()
                
                # Enforce validation threshold
                if stats.validation_rate < self.config.thresholds.bronze:
                    error_msg = f"[bronze:{bname}] validation {stats.validation_rate:.2f}% below required {self.config.thresholds.bronze:.2f}%"
                    self.logger.validation_failed("bronze", bname, stats.validation_rate, self.config.thresholds.bronze)
                    self._current_report.errors.append(error_msg)
                    raise ValueError(error_msg)
                
                bronze_valid[bname] = valid
                
                entry = {
                    "validation": create_validation_dict(stats, start_at=v_start, end_at=v_end),
                    "skipped": False,
                }
                self._current_report.bronze_results[bname] = entry
                
                self.logger.step_complete("bronze", bname, stats.duration_secs)
                self._current_report.metrics.successful_steps += 1
                
            except Exception as e:
                self.logger.error(f"Bronze step {bname} failed: {e}")
                self._current_report.errors.append(f"Bronze step {bname}: {str(e)}")
                self._current_report.metrics.failed_steps += 1
                raise
        
        return bronze_valid
    
    def _execute_bronze_steps_parallel(
        self, 
        mode: PipelineMode, 
        bronze_sources: Dict[str, DataFrame], 
        context: ExecutionContext
    ) -> Dict[str, DataFrame]:
        """Execute Bronze validation steps in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        bronze_valid = {}
        bronze_steps_list = list(self.bronze_steps.keys())
        
        self.logger.parallel_start(bronze_steps_list, 0)
        
        with ThreadPoolExecutor(max_workers=self.config.parallel.max_workers) as executor:
            # Submit all bronze validation tasks
            future_to_step = {}
            for bname, bstep in self.bronze_steps.items():
                if self._cancelled:
                    raise RuntimeError("Pipeline execution cancelled")
                
                if bname not in bronze_sources:
                    error_msg = f"Missing bronze source: {bname}"
                    self._current_report.errors.append(error_msg)
                    raise ValueError(error_msg)
                
                future = executor.submit(
                    self._execute_single_bronze_step,
                    bname, bstep, bronze_sources[bname], mode, context
                )
                future_to_step[future] = bname
            
            # Collect results as they complete
            for future in as_completed(future_to_step):
                bname = future_to_step[future]
                try:
                    if self._cancelled:
                        raise RuntimeError("Pipeline execution cancelled")
                    
                    valid_df, entry = future.result(timeout=self.config.parallel.timeout_secs)
                    bronze_valid[bname] = valid_df
                    self._current_report.bronze_results[bname] = entry
                    self._current_report.metrics.successful_steps += 1
                    
                    self.logger.parallel_complete(bname)
                    
                except Exception as e:
                    self.logger.error(f"Bronze step {bname} failed: {e}")
                    self._current_report.errors.append(f"Bronze step {bname}: {str(e)}")
                    self._current_report.metrics.failed_steps += 1
                    raise
        
        return bronze_valid
    
    def _execute_single_bronze_step(
        self, 
        bname: str, 
        bstep: BronzeStep, 
        df: DataFrame, 
        mode: PipelineMode, 
        context: ExecutionContext
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Execute a single bronze step (for parallel execution)."""
        self.logger.step_start("bronze", bname)
        
        try:
            v_start = now_dt()
            valid, _inv, stats = apply_column_rules(df, bstep.rules, "bronze", bname)
            v_end = now_dt()
            
            # Enforce validation threshold
            if stats.validation_rate < self.config.thresholds.bronze:
                error_msg = f"[bronze:{bname}] validation {stats.validation_rate:.2f}% below required {self.config.thresholds.bronze:.2f}%"
                self.logger.validation_failed("bronze", bname, stats.validation_rate, self.config.thresholds.bronze)
                raise ValueError(error_msg)
            
            entry = {
                "validation": create_validation_dict(stats, start_at=v_start, end_at=v_end),
                "skipped": False,
            }
            
            self.logger.step_complete("bronze", bname, stats.duration_secs)
            
            return valid, entry
            
        except Exception as e:
            self.logger.error(f"Bronze step {bname} failed: {e}")
            raise
    
    def _execute_silver_steps(
        self, 
        mode: PipelineMode, 
        bronze_valid: Dict[str, DataFrame], 
        context: ExecutionContext
    ) -> Dict[str, Dict[str, Any]]:
        """Execute Silver steps with advanced dependency analysis and parallel processing."""
        if not self.silver_steps:
            return {}
        
        # Analyze dependencies for optimal execution
        dependency_result = self.dependency_analyzer.analyze_dependencies(self.silver_steps)
        self._current_report.dependency_analysis = dependency_result
        
        # Execute steps using execution engine
        silver_results = self.execution_engine.execute_silver_steps(
            list(self.silver_steps.keys()),
            self.silver_steps,
            bronze_valid,
            {},
            mode.value,
            self.bronze_steps,
            context
        )
        
        # Process results
        for sname, entry in silver_results.items():
            self._current_report.silver_results[sname] = entry
            
            if entry.get("skipped", False):
                self._current_report.metrics.skipped_steps += 1
            elif "error" in entry:
                self._current_report.metrics.failed_steps += 1
                self._current_report.errors.append(f"Silver step {sname}: {entry['error']}")
            else:
                self._current_report.metrics.successful_steps += 1
                
                # Update row counts
                rows_written = entry.get("write", {}).get("rows_written", 0)
                self._current_report.metrics.total_rows_written += rows_written
                
                if mode == PipelineMode.INCREMENTAL:
                    self._current_report.metrics.total_rows_processed += rows_written
        
        return silver_results
    
    def _execute_gold_steps(self, silver_results: Dict[str, Dict[str, Any]], context: ExecutionContext) -> None:
        """Execute Gold transformation steps with enhanced error handling and concurrent processing."""
        if not self.gold_steps:
            return
        
        # Check if parallel execution is enabled and we have multiple gold steps
        if (self.config.parallel.enabled and 
            len(self.gold_steps) > 1 and 
            self.config.parallel.max_workers > 1):
            self._execute_gold_steps_parallel(silver_results, context)
        else:
            self._execute_gold_steps_sequential(silver_results, context)
    
    def _execute_gold_steps_sequential(self, silver_results: Dict[str, Dict[str, Any]], context: ExecutionContext) -> None:
        """Execute Gold transformation steps sequentially."""
        for gname, gstep in self.gold_steps.items():
            if self._cancelled:
                raise RuntimeError("Pipeline execution cancelled")
            
            self.logger.step_start("gold", gname)
            
            try:
                # Get required Silver tables - read actual DataFrames from tables
                required_silvers = gstep.source_silvers or list(silver_results.keys())
                silvers_dict = {}
                
                for name in required_silvers:
                    if name in silver_results:
                        silver_entry = silver_results[name]
                        if not silver_entry.get("skipped", False) and "table_fqn" in silver_entry:
                            # Read the actual DataFrame from the table
                            table_fqn = silver_entry["table_fqn"]
                            silvers_dict[name] = self.spark.table(table_fqn)
                
                if not silvers_dict:
                    self.logger.step_skipped("gold", gname, "No Silver data available")
                    self._current_report.metrics.skipped_steps += 1
                    continue
                
                # Execute Gold transform
                t_start = now_dt()
                gold_df = gstep.transform(self.spark, silvers_dict)
                t_end = now_dt()
                
                # Validate Gold data
                v_start = now_dt()
                valid, _inv, stats = apply_column_rules(gold_df, gstep.rules, "gold", gname)
                v_end = now_dt()
                
                # Enforce validation threshold
                if stats.validation_rate < self.config.thresholds.gold:
                    error_msg = f"[gold:{gname}] validation {stats.validation_rate:.2f}% below required {self.config.thresholds.gold:.2f}%"
                    self.logger.validation_failed("gold", gname, stats.validation_rate, self.config.thresholds.gold)
                    self._current_report.errors.append(error_msg)
                    raise ValueError(error_msg)
                
                # Write Gold table
                fqn_name = fqn(self.config.schema, gstep.table_name)
                rows_written, w_secs, w_start, w_end = time_write_operation("overwrite", valid, fqn_name)
                
                entry = {
                    "transform": create_transform_dict(
                        gold_df.count(), valid.count(), (t_end - t_start).total_seconds(),
                        skipped=False, start_at=t_start, end_at=t_end
                    ),
                    "validation": create_validation_dict(stats, start_at=v_start, end_at=v_end),
                    "write": create_write_dict("overwrite", rows_written, w_secs, fqn_name, skipped=False, start_at=w_start, end_at=w_end),
                    "skipped": False,
                }
                self._current_report.gold_results[gname] = entry
                self._current_report.metrics.total_rows_written += rows_written
                self._current_report.metrics.successful_steps += 1
                
                self.logger.step_complete("gold", gname, stats.duration_secs)
                
            except Exception as e:
                self.logger.error(f"Gold step {gname} failed: {e}")
                self._current_report.errors.append(f"Gold step {gname}: {str(e)}")
                self._current_report.metrics.failed_steps += 1
                raise
    
    def _execute_gold_steps_parallel(self, silver_results: Dict[str, Dict[str, Any]], context: ExecutionContext) -> None:
        """Execute Gold transformation steps in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        gold_steps_list = list(self.gold_steps.keys())
        
        self.logger.parallel_start(gold_steps_list, 0)
        
        with ThreadPoolExecutor(max_workers=self.config.parallel.max_workers) as executor:
            # Submit all gold transformation tasks
            future_to_step = {}
            for gname, gstep in self.gold_steps.items():
                if self._cancelled:
                    raise RuntimeError("Pipeline execution cancelled")
                
                future = executor.submit(
                    self._execute_single_gold_step,
                    gname, gstep, silver_results, context
                )
                future_to_step[future] = gname
            
            # Collect results as they complete
            for future in as_completed(future_to_step):
                gname = future_to_step[future]
                try:
                    if self._cancelled:
                        raise RuntimeError("Pipeline execution cancelled")
                    
                    entry = future.result(timeout=self.config.parallel.timeout_secs)
                    if entry:  # Only add if not skipped
                        self._current_report.gold_results[gname] = entry
                        self._current_report.metrics.successful_steps += 1
                        self._current_report.metrics.total_rows_written += entry.get("write", {}).get("rows_written", 0)
                    else:
                        self._current_report.metrics.skipped_steps += 1
                    
                    self.logger.parallel_complete(gname)
                    
                except Exception as e:
                    self.logger.error(f"Gold step {gname} failed: {e}")
                    self._current_report.errors.append(f"Gold step {gname}: {str(e)}")
                    self._current_report.metrics.failed_steps += 1
                    raise
    
    def _execute_single_gold_step(
        self, 
        gname: str, 
        gstep: GoldStep, 
        silver_results: Dict[str, Dict[str, Any]], 
        context: ExecutionContext
    ) -> Optional[Dict[str, Any]]:
        """Execute a single gold step (for parallel execution)."""
        self.logger.step_start("gold", gname)
        
        try:
            # Get required Silver tables - read actual DataFrames from tables
            required_silvers = gstep.source_silvers or list(silver_results.keys())
            silvers_dict = {}
            
            for name in required_silvers:
                if name in silver_results:
                    silver_entry = silver_results[name]
                    if not silver_entry.get("skipped", False) and "table_fqn" in silver_entry:
                        # Read the actual DataFrame from the table
                        table_fqn = silver_entry["table_fqn"]
                        silvers_dict[name] = self.spark.table(table_fqn)
            
            if not silvers_dict:
                self.logger.step_skipped("gold", gname, "No Silver data available")
                return None
            
            # Execute Gold transform
            t_start = now_dt()
            gold_df = gstep.transform(self.spark, silvers_dict)
            t_end = now_dt()
            
            # Validate Gold data
            v_start = now_dt()
            valid, _inv, stats = apply_column_rules(gold_df, gstep.rules, "gold", gname)
            v_end = now_dt()
            
            # Enforce validation threshold
            if stats.validation_rate < self.config.thresholds.gold:
                error_msg = f"[gold:{gname}] validation {stats.validation_rate:.2f}% below required {self.config.thresholds.gold:.2f}%"
                self.logger.validation_failed("gold", gname, stats.validation_rate, self.config.thresholds.gold)
                raise ValueError(error_msg)
            
            # Write Gold table
            fqn_name = fqn(self.config.schema, gstep.table_name)
            rows_written, w_secs, w_start, w_end = time_write_operation("overwrite", valid, fqn_name)
            
            entry = {
                "transform": create_transform_dict(
                    gold_df.count(), valid.count(), (t_end - t_start).total_seconds(),
                    skipped=False, start_at=t_start, end_at=t_end
                ),
                "validation": create_validation_dict(stats, start_at=v_start, end_at=v_end),
                "write": create_write_dict("overwrite", rows_written, w_secs, fqn_name, skipped=False, start_at=w_start, end_at=w_end),
                "skipped": False,
            }
            
            self.logger.step_complete("gold", gname, stats.duration_secs)
            
            return entry
            
        except Exception as e:
            self.logger.error(f"Gold step {gname} failed: {e}")
            raise
    
    def _update_metrics(self) -> None:
        """Update pipeline metrics."""
        if not self._current_report:
            return
        
        metrics = self._current_report.metrics
        metrics.total_steps = len(self.bronze_steps) + len(self.silver_steps) + len(self.gold_steps)
        metrics.total_duration = self._current_report.duration_seconds
        metrics.error_count = len(self._current_report.errors)
        
        # Calculate parallel efficiency
        if self._current_report.dependency_analysis:
            metrics.parallel_efficiency = self._current_report.dependency_analysis.get_parallelization_ratio()
        
        # Calculate cache hit rate
        cache_stats = self.execution_engine.get_cache_stats()
        metrics.cache_hit_rate = cache_stats.get("hit_ratio", 0.0)
    
    def _generate_recommendations(self) -> None:
        """Generate performance and optimization recommendations."""
        if not self._current_report:
            return
        
        recommendations = []
        
        # Performance recommendations
        if self._current_report.metrics.parallel_efficiency < 0.5:
            recommendations.append("Consider optimizing dependencies to increase parallelization")
        
        if self._current_report.metrics.cache_hit_rate < 0.3:
            recommendations.append("Consider enabling caching for better performance")
        
        # Error recommendations
        if self._current_report.metrics.error_count > 0:
            recommendations.append("Review and fix pipeline errors to improve reliability")
        
        # Duration recommendations
        if self._current_report.metrics.silver_duration > self._current_report.metrics.bronze_duration * 2:
            recommendations.append("Silver processing is taking longer than expected - consider optimization")
        
        self._current_report.recommendations = recommendations
    
    def cancel(self) -> None:
        """Cancel the current pipeline execution."""
        if self._is_running:
            self._cancelled = True
            self.execution_engine.stop_execution()
            self.logger.warning("Pipeline execution cancelled by user")
    
    def get_status(self) -> PipelineStatus:
        """Get current pipeline status."""
        if self._is_running:
            return PipelineStatus.RUNNING
        elif self._cancelled:
            return PipelineStatus.CANCELLED
        elif self._current_report:
            return self._current_report.status
        else:
            return PipelineStatus.PENDING
    
    def get_current_report(self) -> Optional[PipelineReport]:
        """Get the current execution report."""
        return self._current_report
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get detailed performance statistics."""
        if not self._current_report:
            return {}
        
        return {
            "execution_engine_stats": self.execution_engine.get_execution_stats(),
            "cache_stats": self.execution_engine.get_cache_stats(),
            "pipeline_metrics": self._current_report.metrics,
            "dependency_analysis": self._current_report.dependency_analysis
        }
    
    @contextmanager
    def execution_context(self, mode: PipelineMode, bronze_sources: Dict[str, DataFrame]):
        """Context manager for pipeline execution with proper cleanup."""
        try:
            yield self._run(mode, bronze_sources=bronze_sources)
        finally:
            if self._is_running:
                self.cancel()