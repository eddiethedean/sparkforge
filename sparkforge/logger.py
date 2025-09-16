#


# logger.py
"""
Enhanced centralized logging system for the Pipeline Builder.

This module contains an advanced logging system with rich formatting, specialized
logging methods for different pipeline events, and comprehensive monitoring capabilities.

Key Features:
- Rich formatting with colors and emojis
- Specialized logging methods for pipeline events
- Performance monitoring and timing
- Structured logging with context
- Log level management
- File and console output support
- Log rotation and archival
"""

import json
import logging
import logging.handlers
import sys
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from typing import Callable, Dict, Generator, List, Optional, Union
from dataclasses import dataclass

# ============================================================================
# Type Definitions
# ============================================================================

# Specific types for log values instead of Any
LogValue = Union[str, int, float, bool, List[str], Dict[str, str]]
ContextData = Dict[str, LogValue]

@dataclass
class LogContext:
    """Explicit context for log messages instead of **kwargs."""
    pipeline_id: Optional[str] = None
    step_id: Optional[str] = None
    execution_id: Optional[str] = None
    user_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Optional[Dict[str, LogValue]] = None

@dataclass
class PerformanceContext:
    """Explicit context for performance logging."""
    operation_name: Optional[str] = None
    duration: Optional[float] = None
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None
    rows_processed: Optional[int] = None
    metadata: Optional[Dict[str, LogValue]] = None

@dataclass
class ValidationContext:
    """Explicit context for validation logging."""
    stage: Optional[str] = None
    step: Optional[str] = None
    validation_rate: Optional[float] = None
    threshold: Optional[float] = None
    passed: Optional[bool] = None
    metadata: Optional[Dict[str, LogValue]] = None

# ============================================================================
# Custom Log Levels
# ============================================================================


class PipelineLogLevel:
    """Custom log levels for pipeline operations."""

    # Add custom levels between existing ones
    PIPELINE_START = 25
    PIPELINE_END = 24
    STEP_START = 23
    STEP_END = 22
    VALIDATION = 21
    PERFORMANCE = 20

    @classmethod
    def add_custom_levels(cls) -> None:
        """Add custom log levels to logging module."""
        logging.addLevelName(cls.PIPELINE_START, "PIPELINE_START")
        logging.addLevelName(cls.PIPELINE_END, "PIPELINE_END")
        logging.addLevelName(cls.STEP_START, "STEP_START")
        logging.addLevelName(cls.STEP_END, "STEP_END")
        logging.addLevelName(cls.VALIDATION, "VALIDATION")
        logging.addLevelName(cls.PERFORMANCE, "PERFORMANCE")


# ============================================================================
# Colored Formatter
# ============================================================================


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""

    # Color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
        "PIPELINE_START": "\033[94m",  # Light Blue
        "PIPELINE_END": "\033[92m",  # Light Green
        "STEP_START": "\033[96m",  # Light Cyan
        "STEP_END": "\033[92m",  # Light Green
        "VALIDATION": "\033[93m",  # Light Yellow
        "PERFORMANCE": "\033[95m",  # Light Magenta
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        if hasattr(record, "levelname") and record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            reset = self.COLORS["RESET"]
            record.levelname = f"{color}{record.levelname}{reset}"

        return super().format(record)


# ============================================================================
# Structured Formatter
# ============================================================================


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for log files."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


# ============================================================================
# Enhanced Pipeline Logger
# ============================================================================


class PipelineLogger:
    """
    Enhanced centralized logging for pipeline operations.

    Features:
    - Multiple output handlers (console, file, structured)
    - Colored console output
    - Performance monitoring
    - Context management
    - Log level filtering
    - Structured logging for analysis
    """

    def __init__(
        self,
        verbose: bool = True,
        name: str = "PipelineBuilder",
        log_level: int = logging.INFO,
        log_file: Optional[str] = None,
        structured_log: bool = False,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
    ):
        self.verbose = verbose
        self.name = name
        self.log_level = log_level
        self.log_file = log_file
        self.structured_log = structured_log
        self.max_file_size = max_file_size
        self.backup_count = backup_count

        # Initialize logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        # Clear existing handlers
        self.logger.handlers.clear()

        # Add custom levels
        PipelineLogLevel.add_custom_levels()

        # Setup handlers
        self._setup_handlers()

        # Performance tracking
        self._performance_data: Dict[str, List[float]] = {}
        self._context_stack: List[ContextData] = []

    def _setup_handlers(self) -> None:
        """Setup logging handlers."""
        # Console handler with colors
        if self.verbose:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = ColoredFormatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(self.log_level)
            self.logger.addHandler(console_handler)

        # File handler
        if self.log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file,
                maxBytes=self.max_file_size,
                backupCount=self.backup_count,
            )

            if self.structured_log:
                file_formatter: logging.Formatter = StructuredFormatter()
            else:
                file_formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )

            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(self.log_level)
            self.logger.addHandler(file_handler)

    def _merge_metadata(self, base_metadata: Optional[Dict[str, LogValue]], context_metadata: Optional[Dict[str, LogValue]]) -> Dict[str, LogValue]:
        """Safely merge metadata dictionaries."""
        if base_metadata is None:
            base_metadata = {}
        if context_metadata is None:
            return base_metadata
        return {**base_metadata, **context_metadata}

    def _log_with_context(self, level: int, message: str, context: Optional[LogContext] = None) -> None:
        """Log message with context information."""
        extra_fields: ContextData = {}
        if self._context_stack:
            extra_fields.update(self._context_stack[-1])

        if context:
            context_dict: ContextData = {}
            if context.pipeline_id:
                context_dict["pipeline_id"] = context.pipeline_id
            if context.step_id:
                context_dict["step_id"] = context.step_id
            if context.execution_id:
                context_dict["execution_id"] = context.execution_id
            if context.user_id:
                context_dict["user_id"] = context.user_id
            if context.timestamp:
                context_dict["timestamp"] = context.timestamp.isoformat()
            if context.metadata:
                context_dict.update(context.metadata)
            extra_fields.update(context_dict)

        self.logger.log(level, message, extra={"extra_fields": extra_fields})

    def _add_performance_data(self, operation: str, duration: float) -> None:
        """Add performance data for analysis."""
        if operation not in self._performance_data:
            self._performance_data[operation] = []
        self._performance_data[operation].append(duration)

    # ========================================================================
    # Basic Logging Methods
    # ========================================================================

    def debug(self, message: str, context: Optional[LogContext] = None) -> None:
        """Log debug message."""
        self._log_with_context(logging.DEBUG, message, context)

    def info(self, message: str, context: Optional[LogContext] = None) -> None:
        """Log info message."""
        self._log_with_context(logging.INFO, message, context)

    def warning(self, message: str, context: Optional[LogContext] = None) -> None:
        """Log warning message."""
        self._log_with_context(logging.WARNING, message, context)

    def error(self, message: str, context: Optional[LogContext] = None) -> None:
        """Log error message."""
        self._log_with_context(logging.ERROR, message, context)

    def critical(self, message: str, context: Optional[LogContext] = None) -> None:
        """Log critical message."""
        self._log_with_context(logging.CRITICAL, message, context)

    # ========================================================================
    # Pipeline-Specific Logging Methods
    # ========================================================================

    def pipeline_start(self, pipeline_name: str, mode: str, context: Optional[LogContext] = None) -> None:
        """Log pipeline start."""
        # Create context with pipeline-specific data
        pipeline_context = LogContext(
            pipeline_id=pipeline_name,
            metadata={"mode": mode}
        )
        if context:
            # Merge with provided context
            pipeline_context.pipeline_id = context.pipeline_id or pipeline_name
            pipeline_context.step_id = context.step_id
            pipeline_context.execution_id = context.execution_id
            pipeline_context.user_id = context.user_id
            pipeline_context.timestamp = context.timestamp
            pipeline_context.metadata = self._merge_metadata(pipeline_context.metadata, context.metadata)
        
        self._log_with_context(
            PipelineLogLevel.PIPELINE_START,
            f"🚀 Starting pipeline: {pipeline_name} (mode: {mode})",
            pipeline_context,
        )

    def pipeline_end(
        self, pipeline_name: str, duration: float, success: bool, context: Optional[LogContext] = None
    ) -> None:
        """Log pipeline end."""
        status = "✅ Success" if success else "❌ Failed"
        # Create context with pipeline-specific data
        pipeline_context = LogContext(
            pipeline_id=pipeline_name,
            metadata={"duration": duration, "success": success}
        )
        if context:
            # Merge with provided context
            pipeline_context.pipeline_id = context.pipeline_id or pipeline_name
            pipeline_context.step_id = context.step_id
            pipeline_context.execution_id = context.execution_id
            pipeline_context.user_id = context.user_id
            pipeline_context.timestamp = context.timestamp
            pipeline_context.metadata = self._merge_metadata(pipeline_context.metadata, context.metadata)
        
        self._log_with_context(
            PipelineLogLevel.PIPELINE_END,
            f"{status} pipeline: {pipeline_name} ({duration:.2f}s)",
            pipeline_context,
        )

    def step_start(self, stage: str, step: str, context: Optional[LogContext] = None) -> None:
        """Log start of a pipeline step."""
        # Create context with step-specific data
        step_context = LogContext(
            step_id=step,
            metadata={"stage": stage}
        )
        if context:
            # Merge with provided context
            step_context.pipeline_id = context.pipeline_id
            step_context.step_id = context.step_id or step
            step_context.execution_id = context.execution_id
            step_context.user_id = context.user_id
            step_context.timestamp = context.timestamp
            step_context.metadata = self._merge_metadata(step_context.metadata, context.metadata)
        
        self._log_with_context(
            PipelineLogLevel.STEP_START,
            f"🚀 Starting {stage.upper()} step: {step}",
            step_context,
        )

    def step_complete(
        self, stage: str, step: str, duration: float, rows_processed: int = 0, context: Optional[LogContext] = None
    ) -> None:
        """Log completion of a pipeline step."""
        self._add_performance_data(f"{stage}_{step}", duration)
        # Create context with step-specific data
        step_context = LogContext(
            step_id=step,
            metadata={"stage": stage, "duration": duration, "rows_processed": rows_processed}
        )
        if context:
            # Merge with provided context
            step_context.pipeline_id = context.pipeline_id
            step_context.step_id = context.step_id or step
            step_context.execution_id = context.execution_id
            step_context.user_id = context.user_id
            step_context.timestamp = context.timestamp
            step_context.metadata = self._merge_metadata(step_context.metadata, context.metadata)
        
        self._log_with_context(
            PipelineLogLevel.STEP_END,
            f"✅ Completed {stage.upper()} step: {step} ({duration:.2f}s, {rows_processed:,} rows)",
            step_context,
        )

    def step_skipped(
        self, stage: str, step: str, reason: str = "No data", context: Optional[LogContext] = None
    ) -> None:
        """Log skipped pipeline step."""
        # Create context with step-specific data
        step_context = LogContext(
            step_id=step,
            metadata={"stage": stage, "reason": reason}
        )
        if context:
            # Merge with provided context
            step_context.pipeline_id = context.pipeline_id
            step_context.step_id = context.step_id or step
            step_context.execution_id = context.execution_id
            step_context.user_id = context.user_id
            step_context.timestamp = context.timestamp
            step_context.metadata = self._merge_metadata(step_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.INFO,
            f"⏭️ Skipped {stage.upper()} step: {step} ({reason})",
            step_context,
        )

    def step_failed(
        self, stage: str, step: str, error: str, duration: float = 0, context: Optional[LogContext] = None
    ) -> None:
        """Log failed pipeline step."""
        # Create context with step-specific data
        step_context = LogContext(
            step_id=step,
            metadata={"stage": stage, "error": error, "duration": duration}
        )
        if context:
            # Merge with provided context
            step_context.pipeline_id = context.pipeline_id
            step_context.step_id = context.step_id or step
            step_context.execution_id = context.execution_id
            step_context.user_id = context.user_id
            step_context.timestamp = context.timestamp
            step_context.metadata = self._merge_metadata(step_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.ERROR,
            f"❌ Failed {stage.upper()} step: {step} ({duration:.2f}s) - {error}",
            step_context,
        )

    def parallel_start(self, steps: List[str], group: int, context: Optional[LogContext] = None) -> None:
        """Log start of parallel execution."""
        # Create context with parallel-specific data
        parallel_context = LogContext(
            metadata={"steps": steps, "group": group, "parallel": True}
        )
        if context:
            # Merge with provided context
            parallel_context.pipeline_id = context.pipeline_id
            parallel_context.step_id = context.step_id
            parallel_context.execution_id = context.execution_id
            parallel_context.user_id = context.user_id
            parallel_context.timestamp = context.timestamp
            parallel_context.metadata = self._merge_metadata(parallel_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.INFO,
            f"🚀 Executing Silver group {group}: {steps} (parallel)",
            parallel_context,
        )

    def parallel_complete(self, completed_step: str, context: Optional[LogContext] = None) -> None:
        """Log completion of parallel step."""
        # Create context with parallel-specific data
        parallel_context = LogContext(
            step_id=completed_step,
            metadata={"parallel": True}
        )
        if context:
            # Merge with provided context
            parallel_context.pipeline_id = context.pipeline_id
            parallel_context.step_id = context.step_id or completed_step
            parallel_context.execution_id = context.execution_id
            parallel_context.user_id = context.user_id
            parallel_context.timestamp = context.timestamp
            parallel_context.metadata = self._merge_metadata(parallel_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.INFO,
            f"✅ Completed Silver step: {completed_step}",
            parallel_context,
        )

    def validation_passed(
        self, stage: str, step: str, rate: float, threshold: float, context: Optional[ValidationContext] = None
    ) -> None:
        """Log validation success."""
        # Create context with validation-specific data
        validation_context = ValidationContext(
            stage=stage,
            step=step,
            validation_rate=rate,
            threshold=threshold,
            passed=True
        )
        if context:
            # Merge with provided context
            validation_context.stage = context.stage or stage
            validation_context.step = context.step or step
            validation_context.validation_rate = context.validation_rate or rate
            validation_context.threshold = context.threshold or threshold
            validation_context.passed = context.passed if context.passed is not None else True
            if context.metadata:
                validation_context.metadata = context.metadata
        
        # Convert to LogContext for logging
        log_context = LogContext(
            step_id=validation_context.step,
            metadata={
                "stage": validation_context.stage or "",
                "validation_rate": validation_context.validation_rate or 0.0,
                "threshold": validation_context.threshold or 0.0,
                "passed": validation_context.passed or False
            }
        )
        
        self._log_with_context(
            PipelineLogLevel.VALIDATION,
            f"✅ Validation passed for {stage}:{step} - {rate:.2f}% >= {threshold:.2f}%",
            log_context,
        )

    def validation_failed(
        self, stage: str, step: str, rate: float, threshold: float, context: Optional[ValidationContext] = None
    ) -> None:
        """Log validation failure."""
        # Create context with validation-specific data
        validation_context = ValidationContext(
            stage=stage,
            step=step,
            validation_rate=rate,
            threshold=threshold,
            passed=False
        )
        if context:
            # Merge with provided context
            validation_context.stage = context.stage or stage
            validation_context.step = context.step or step
            validation_context.validation_rate = context.validation_rate or rate
            validation_context.threshold = context.threshold or threshold
            validation_context.passed = context.passed if context.passed is not None else False
            if context.metadata:
                validation_context.metadata = context.metadata
        
        # Convert to LogContext for logging
        log_context = LogContext(
            step_id=validation_context.step,
            metadata={
                "stage": validation_context.stage or "",
                "validation_rate": validation_context.validation_rate or 0.0,
                "threshold": validation_context.threshold or 0.0,
                "passed": validation_context.passed or False
            }
        )
        
        self._log_with_context(
            PipelineLogLevel.VALIDATION,
            f"❌ Validation failed for {stage}:{step} - {rate:.2f}% < {threshold:.2f}%",
            log_context,
        )

    def performance_metric(
        self, metric_name: str, value: float, unit: str = "s", context: Optional[PerformanceContext] = None
    ) -> None:
        """Log performance metric."""
        # Create context with performance-specific data
        performance_context = PerformanceContext(
            operation_name=metric_name,
            duration=value if unit == "s" else None,
            metadata={"metric_name": metric_name, "value": value, "unit": unit}
        )
        if context:
            # Merge with provided context
            performance_context.operation_name = context.operation_name or metric_name
            performance_context.duration = context.duration
            performance_context.memory_usage = context.memory_usage
            performance_context.cpu_usage = context.cpu_usage
            performance_context.rows_processed = context.rows_processed
            performance_context.metadata = self._merge_metadata(performance_context.metadata, context.metadata)
        
        # Convert to LogContext for logging
        log_context = LogContext(
            metadata=performance_context.metadata
        )
        
        self._log_with_context(
            PipelineLogLevel.PERFORMANCE,
            f"📊 {metric_name}: {value:.2f}{unit}",
            log_context,
        )

    def execution_summary(
        self,
        mode: str,
        duration: float,
        total_rows: int,
        success_rate: float = 100.0,
        context: Optional[LogContext] = None,
    ) -> None:
        """Log execution summary."""
        # Create context with execution-specific data
        execution_context = LogContext(
            metadata={
                "mode": mode,
                "duration": duration,
                "total_rows": total_rows,
                "success_rate": success_rate
            }
        )
        if context:
            # Merge with provided context
            execution_context.pipeline_id = context.pipeline_id
            execution_context.step_id = context.step_id
            execution_context.execution_id = context.execution_id
            execution_context.user_id = context.user_id
            execution_context.timestamp = context.timestamp
            execution_context.metadata = self._merge_metadata(execution_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.INFO,
            f"📊 {mode.upper()} execution completed in {duration:.2f}s - {total_rows:,} rows processed (success: {success_rate:.1f}%)",
            execution_context,
        )

    def dependency_analysis(self, groups: Dict[int, List[str]], context: Optional[LogContext] = None) -> None:
        """Log dependency analysis results."""
        self.info("🔍 Silver execution plan:")
        for group_num in sorted(groups.keys()):
            steps = groups[group_num]
            if len(steps) > 1:
                self.info(f"  Group {group_num}: {steps} (parallel)")
            else:
                self.info(f"  Group {group_num}: {steps} (sequential)")

        # Create context with dependency-specific data
        dependency_context = LogContext(
            metadata={"execution_groups": str(groups)}
        )
        if context:
            # Merge with provided context
            dependency_context.pipeline_id = context.pipeline_id
            dependency_context.step_id = context.step_id
            dependency_context.execution_id = context.execution_id
            dependency_context.user_id = context.user_id
            dependency_context.timestamp = context.timestamp
            dependency_context.metadata = self._merge_metadata(dependency_context.metadata, context.metadata)

        self._log_with_context(
            logging.INFO,
            "Dependency analysis completed",
            dependency_context,
        )

    def data_quality_report(
        self, stage: str, step: str, quality_score: float, issues: List[str], context: Optional[LogContext] = None
    ) -> None:
        """Log data quality report."""
        status = (
            "✅ Good"
            if quality_score >= 90
            else "⚠️ Needs attention"
            if quality_score >= 70
            else "❌ Poor"
        )
        
        # Create context with quality-specific data
        quality_context = LogContext(
            step_id=step,
            metadata={
                "stage": stage,
                "quality_score": quality_score,
                "issues": issues,
                "status": status
            }
        )
        if context:
            # Merge with provided context
            quality_context.pipeline_id = context.pipeline_id
            quality_context.step_id = context.step_id or step
            quality_context.execution_id = context.execution_id
            quality_context.user_id = context.user_id
            quality_context.timestamp = context.timestamp
            quality_context.metadata = self._merge_metadata(quality_context.metadata, context.metadata)
        
        self._log_with_context(
            logging.INFO,
            f"{status} Data quality for {stage}:{step} - Score: {quality_score:.1f}%",
            quality_context,
        )

    # ========================================================================
    # Context Management
    # ========================================================================

    @contextmanager
    def context(self, **context_data: LogValue) -> Generator[None, None, None]:
        """Add context to all log messages within this block."""
        self._context_stack.append(context_data)
        try:
            yield
        finally:
            self._context_stack.pop()

    def set_context(self, context: LogContext) -> None:
        """Set persistent context for all subsequent log messages."""
        context_dict: ContextData = {}
        if context.pipeline_id:
            context_dict["pipeline_id"] = context.pipeline_id
        if context.step_id:
            context_dict["step_id"] = context.step_id
        if context.execution_id:
            context_dict["execution_id"] = context.execution_id
        if context.user_id:
            context_dict["user_id"] = context.user_id
        if context.timestamp:
            context_dict["timestamp"] = context.timestamp.isoformat()
        if context.metadata:
            context_dict.update(context.metadata)
        
        if self._context_stack:
            self._context_stack[-1].update(context_dict)
        else:
            self._context_stack.append(context_dict)

    def clear_context(self) -> None:
        """Clear all context data."""
        self._context_stack.clear()

    # ========================================================================
    # Performance Analysis
    # ========================================================================

    def get_performance_summary(self) -> Dict[str, Dict[str, float]]:
        """Get performance summary for all operations."""
        summary = {}
        for operation, durations in self._performance_data.items():
            if durations:
                summary[operation] = {
                    "count": len(durations),
                    "total": sum(durations),
                    "average": sum(durations) / len(durations),
                    "min": min(durations),
                    "max": max(durations),
                }
        return summary

    def log_performance_summary(self) -> None:
        """Log performance summary."""
        summary = self.get_performance_summary()
        if summary:
            self.info("📊 Performance Summary:")
            for operation, stats in summary.items():
                self.info(
                    f"  {operation}: {stats['count']} runs, "
                    f"avg: {stats['average']:.2f}s, "
                    f"min: {stats['min']:.2f}s, "
                    f"max: {stats['max']:.2f}s"
                )

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def set_level(self, level: int) -> None:
        """Set logging level."""
        self.log_level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)

    def add_file_handler(self, filename: str, level: int = logging.INFO) -> None:
        """Add additional file handler."""
        file_handler = logging.handlers.RotatingFileHandler(
            filename, maxBytes=self.max_file_size, backupCount=self.backup_count
        )

        if self.structured_log:
            file_formatter: logging.Formatter = StructuredFormatter()
        else:
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)
        self.logger.addHandler(file_handler)

    def close(self) -> None:
        """Close all handlers."""
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()


# ============================================================================
# Enhanced Execution Timer
# ============================================================================


class ExecutionTimer:
    """Enhanced context manager for timing operations with detailed logging."""

    def __init__(self, logger: PipelineLogger, operation: str, **context_data: LogValue) -> None:
        self.logger = logger
        self.operation = operation
        self.context_data = context_data
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: Optional[float] = None

    def __enter__(self) -> "ExecutionTimer":
        self.start_time = datetime.utcnow()
        # Create LogContext from context_data
        context = LogContext(metadata=self.context_data)
        self.logger.info(f"⏱️ Starting {self.operation}", context)
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[object]) -> None:
        self.end_time = datetime.utcnow()
        if self.start_time is not None:
            self.duration = (self.end_time - self.start_time).total_seconds()
        else:
            self.duration = 0.0

        if exc_type is None:
            context = LogContext(metadata=self.context_data)
            self.logger.info(
                f"✅ {self.operation} completed in {self.duration:.2f}s",
                context,
            )
        else:
            context = LogContext(metadata=self.context_data)
            self.logger.error(
                f"❌ {self.operation} failed after {self.duration:.2f}s: {exc_val}",
                context,
            )

    def get_duration(self) -> Optional[float]:
        """Get operation duration."""
        return self.duration


# ============================================================================
# Performance Monitoring Decorator
# ============================================================================


def log_performance(operation_name: Optional[str] = None, log_args: bool = False) -> Callable:
    """Decorator to automatically log function performance."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            name = operation_name or f"{func.__module__}.{func.__name__}"

            # Get logger from args if available
            logger = None
            for arg in args:
                if isinstance(arg, PipelineLogger):
                    logger = arg
                    break

            if not logger:
                logger = PipelineLogger()

            with ExecutionTimer(logger, name):
                if log_args:
                    logger.debug(f"Calling {name} with args: {args}, kwargs: {kwargs}")

                return func(*args, **kwargs)

        return wrapper

    return decorator


# ============================================================================
# Factory Functions
# ============================================================================


def create_logger(
    verbose: bool = True,
    name: str = "PipelineBuilder",
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    structured_log: bool = False,
) -> PipelineLogger:
    """Create a new enhanced pipeline logger."""
    return PipelineLogger(
        verbose=verbose,
        name=name,
        log_level=log_level,
        log_file=log_file,
        structured_log=structured_log,
    )


def create_file_logger(
    log_file: str,
    name: str = "PipelineBuilder",
    log_level: int = logging.INFO,
    structured_log: bool = True,
) -> PipelineLogger:
    """Create a logger that only writes to file."""
    return PipelineLogger(
        verbose=False,
        name=name,
        log_level=log_level,
        log_file=log_file,
        structured_log=structured_log,
    )


def create_console_logger(
    name: str = "PipelineBuilder", log_level: int = logging.INFO
) -> PipelineLogger:
    """Create a logger that only outputs to console."""
    return PipelineLogger(verbose=True, name=name, log_level=log_level, log_file=None)


# ============================================================================
# Global Logger Instance
# ============================================================================

# Create a default global logger instance
_global_logger: Optional[PipelineLogger] = None


def get_global_logger() -> PipelineLogger:
    """Get the global logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = create_logger()
    return _global_logger


def set_global_logger(logger: PipelineLogger) -> None:
    """Set the global logger instance."""
    global _global_logger
    _global_logger = logger


def reset_global_logger() -> None:
    """Reset the global logger instance."""
    global _global_logger
    if _global_logger:
        _global_logger.close()
    _global_logger = None
