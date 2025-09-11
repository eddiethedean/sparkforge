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

import logging
import logging.handlers
import sys
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
from functools import wraps
import json
import time


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
    def add_custom_levels(cls):
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
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'PIPELINE_START': '\033[94m',  # Light Blue
        'PIPELINE_END': '\033[92m',    # Light Green
        'STEP_START': '\033[96m',      # Light Cyan
        'STEP_END': '\033[92m',        # Light Green
        'VALIDATION': '\033[93m',      # Light Yellow
        'PERFORMANCE': '\033[95m',     # Light Magenta
        'RESET': '\033[0m'             # Reset
    }
    
    def format(self, record):
        """Format log record with colors."""
        if hasattr(record, 'levelname') and record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            reset = self.COLORS['RESET']
            record.levelname = f"{color}{record.levelname}{reset}"
        
        return super().format(record)


# ============================================================================
# Structured Formatter
# ============================================================================

class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for log files."""
    
    def format(self, record):
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
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
        backup_count: int = 5
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
        self._context_stack: List[Dict[str, Any]] = []
    
    def _setup_handlers(self):
        """Setup logging handlers."""
        # Console handler with colors
        if self.verbose:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = ColoredFormatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(self.log_level)
            self.logger.addHandler(console_handler)
        
        # File handler
        if self.log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file,
                maxBytes=self.max_file_size,
                backupCount=self.backup_count
            )
            
            if self.structured_log:
                file_formatter = StructuredFormatter()
            else:
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
            
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(self.log_level)
            self.logger.addHandler(file_handler)
    
    def _log_with_context(self, level: int, message: str, **kwargs):
        """Log message with context information."""
        extra_fields = {}
        if self._context_stack:
            extra_fields['context'] = self._context_stack[-1]
        
        extra_fields.update(kwargs)
        
        self.logger.log(level, message, extra={'extra_fields': extra_fields})
    
    def _add_performance_data(self, operation: str, duration: float):
        """Add performance data for analysis."""
        if operation not in self._performance_data:
            self._performance_data[operation] = []
        self._performance_data[operation].append(duration)
    
    # ========================================================================
    # Basic Logging Methods
    # ========================================================================
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log_with_context(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self._log_with_context(logging.CRITICAL, message, **kwargs)
    
    # ========================================================================
    # Pipeline-Specific Logging Methods
    # ========================================================================
    
    def pipeline_start(self, pipeline_name: str, mode: str, **kwargs) -> None:
        """Log pipeline start."""
        self._log_with_context(
            PipelineLogLevel.PIPELINE_START,
            f"🚀 Starting pipeline: {pipeline_name} (mode: {mode})",
            pipeline_name=pipeline_name,
            mode=mode,
            **kwargs
        )
    
    def pipeline_end(self, pipeline_name: str, duration: float, success: bool, **kwargs) -> None:
        """Log pipeline end."""
        status = "✅ Success" if success else "❌ Failed"
        self._log_with_context(
            PipelineLogLevel.PIPELINE_END,
            f"{status} pipeline: {pipeline_name} ({duration:.2f}s)",
            pipeline_name=pipeline_name,
            duration=duration,
            success=success,
            **kwargs
        )
    
    def step_start(self, stage: str, step: str, **kwargs) -> None:
        """Log start of a pipeline step."""
        self._log_with_context(
            PipelineLogLevel.STEP_START,
            f"🚀 Starting {stage.upper()} step: {step}",
            stage=stage,
            step=step,
            **kwargs
        )
    
    def step_complete(self, stage: str, step: str, duration: float, rows_processed: int = 0, **kwargs) -> None:
        """Log completion of a pipeline step."""
        self._add_performance_data(f"{stage}_{step}", duration)
        self._log_with_context(
            PipelineLogLevel.STEP_END,
            f"✅ Completed {stage.upper()} step: {step} ({duration:.2f}s, {rows_processed:,} rows)",
            stage=stage,
            step=step,
            duration=duration,
            rows_processed=rows_processed,
            **kwargs
        )
    
    def step_skipped(self, stage: str, step: str, reason: str = "No data", **kwargs) -> None:
        """Log skipped pipeline step."""
        self._log_with_context(
            logging.INFO,
            f"⏭️ Skipped {stage.upper()} step: {step} ({reason})",
            stage=stage,
            step=step,
            reason=reason,
            **kwargs
        )
    
    def step_failed(self, stage: str, step: str, error: str, duration: float = 0, **kwargs) -> None:
        """Log failed pipeline step."""
        self._log_with_context(
            logging.ERROR,
            f"❌ Failed {stage.upper()} step: {step} ({duration:.2f}s) - {error}",
            stage=stage,
            step=step,
            error=error,
            duration=duration,
            **kwargs
        )
    
    def parallel_start(self, steps: List[str], group: int, **kwargs) -> None:
        """Log start of parallel execution."""
        self._log_with_context(
            logging.INFO,
            f"🚀 Executing Silver group {group}: {steps} (parallel)",
            steps=steps,
            group=group,
            parallel=True,
            **kwargs
        )
    
    def parallel_complete(self, completed_step: str, **kwargs) -> None:
        """Log completion of parallel step."""
        self._log_with_context(
            logging.INFO,
            f"✅ Completed Silver step: {completed_step}",
            step=completed_step,
            parallel=True,
            **kwargs
        )
    
    def validation_passed(self, stage: str, step: str, rate: float, threshold: float, **kwargs) -> None:
        """Log validation success."""
        self._log_with_context(
            PipelineLogLevel.VALIDATION,
            f"✅ Validation passed for {stage}:{step} - {rate:.2f}% >= {threshold:.2f}%",
            stage=stage,
            step=step,
            validation_rate=rate,
            threshold=threshold,
            passed=True,
            **kwargs
        )
    
    def validation_failed(self, stage: str, step: str, rate: float, threshold: float, **kwargs) -> None:
        """Log validation failure."""
        self._log_with_context(
            PipelineLogLevel.VALIDATION,
            f"❌ Validation failed for {stage}:{step} - {rate:.2f}% < {threshold:.2f}%",
            stage=stage,
            step=step,
            validation_rate=rate,
            threshold=threshold,
            passed=False,
            **kwargs
        )
    
    def performance_metric(self, metric_name: str, value: float, unit: str = "s", **kwargs) -> None:
        """Log performance metric."""
        self._log_with_context(
            PipelineLogLevel.PERFORMANCE,
            f"📊 {metric_name}: {value:.2f}{unit}",
            metric_name=metric_name,
            value=value,
            unit=unit,
            **kwargs
        )
    
    def execution_summary(self, mode: str, duration: float, total_rows: int, success_rate: float = 100.0, **kwargs) -> None:
        """Log execution summary."""
        self._log_with_context(
            logging.INFO,
            f"📊 {mode.upper()} execution completed in {duration:.2f}s - {total_rows:,} rows processed (success: {success_rate:.1f}%)",
            mode=mode,
            duration=duration,
            total_rows=total_rows,
            success_rate=success_rate,
            **kwargs
        )
    
    def dependency_analysis(self, groups: Dict[int, List[str]], **kwargs) -> None:
        """Log dependency analysis results."""
        self.info("🔍 Silver execution plan:")
        for group_num in sorted(groups.keys()):
            steps = groups[group_num]
            if len(steps) > 1:
                self.info(f"  Group {group_num}: {steps} (parallel)")
            else:
                self.info(f"  Group {group_num}: {steps} (sequential)")
        
        self._log_with_context(
            logging.INFO,
            "Dependency analysis completed",
            execution_groups=groups,
            **kwargs
        )
    
    def data_quality_report(self, stage: str, step: str, quality_score: float, issues: List[str], **kwargs) -> None:
        """Log data quality report."""
        status = "✅ Good" if quality_score >= 90 else "⚠️ Needs attention" if quality_score >= 70 else "❌ Poor"
        self._log_with_context(
            logging.INFO,
            f"{status} Data quality for {stage}:{step} - Score: {quality_score:.1f}%",
            stage=stage,
            step=step,
            quality_score=quality_score,
            issues=issues,
            **kwargs
        )
    
    # ========================================================================
    # Context Management
    # ========================================================================
    
    @contextmanager
    def context(self, **context_data):
        """Add context to all log messages within this block."""
        self._context_stack.append(context_data)
        try:
            yield
        finally:
            self._context_stack.pop()
    
    def set_context(self, **context_data):
        """Set persistent context for all subsequent log messages."""
        if self._context_stack:
            self._context_stack[-1].update(context_data)
        else:
            self._context_stack.append(context_data)
    
    def clear_context(self):
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
                    'count': len(durations),
                    'total': sum(durations),
                    'average': sum(durations) / len(durations),
                    'min': min(durations),
                    'max': max(durations)
                }
        return summary
    
    def log_performance_summary(self):
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
    
    def set_level(self, level: int):
        """Set logging level."""
        self.log_level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
    
    def add_file_handler(self, filename: str, level: int = logging.INFO):
        """Add additional file handler."""
        file_handler = logging.handlers.RotatingFileHandler(
            filename,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count
        )
        
        if self.structured_log:
            file_formatter = StructuredFormatter()
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)
        self.logger.addHandler(file_handler)
    
    def close(self):
        """Close all handlers."""
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()


# ============================================================================
# Enhanced Execution Timer
# ============================================================================

class ExecutionTimer:
    """Enhanced context manager for timing operations with detailed logging."""
    
    def __init__(self, logger: PipelineLogger, operation: str, **context_data):
        self.logger = logger
        self.operation = operation
        self.context_data = context_data
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: Optional[float] = None
    
    def __enter__(self) -> 'ExecutionTimer':
        self.start_time = datetime.utcnow()
        self.logger.info(f"⏱️ Starting {self.operation}", **self.context_data)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.end_time = datetime.utcnow()
        self.duration = (self.end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"✅ {self.operation} completed in {self.duration:.2f}s", **self.context_data)
        else:
            self.logger.error(f"❌ {self.operation} failed after {self.duration:.2f}s: {exc_val}", **self.context_data)
    
    def get_duration(self) -> Optional[float]:
        """Get operation duration."""
        return self.duration


# ============================================================================
# Performance Monitoring Decorator
# ============================================================================

def log_performance(operation_name: str = None, log_args: bool = False):
    """Decorator to automatically log function performance."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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
    structured_log: bool = False
) -> PipelineLogger:
    """Create a new enhanced pipeline logger."""
    return PipelineLogger(
        verbose=verbose,
        name=name,
        log_level=log_level,
        log_file=log_file,
        structured_log=structured_log
    )


def create_file_logger(
    log_file: str,
    name: str = "PipelineBuilder",
    log_level: int = logging.INFO,
    structured_log: bool = True
) -> PipelineLogger:
    """Create a logger that only writes to file."""
    return PipelineLogger(
        verbose=False,
        name=name,
        log_level=log_level,
        log_file=log_file,
        structured_log=structured_log
    )


def create_console_logger(
    name: str = "PipelineBuilder",
    log_level: int = logging.INFO
) -> PipelineLogger:
    """Create a logger that only outputs to console."""
    return PipelineLogger(
        verbose=True,
        name=name,
        log_level=log_level,
        log_file=None
    )


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

def set_global_logger(logger: PipelineLogger):
    """Set the global logger instance."""
    global _global_logger
    _global_logger = logger

def reset_global_logger():
    """Reset the global logger instance."""
    global _global_logger
    if _global_logger:
        _global_logger.close()
    _global_logger = None