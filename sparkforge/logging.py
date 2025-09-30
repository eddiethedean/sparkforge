"""
Simplified logging system for SparkForge.

This module provides a clean, focused logging system for pipeline operations
without the complexity of the previous over-engineered system.
"""

import logging
import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Union


class PipelineLogger:
    """
    Simple, focused logging for pipeline operations.

    Features:
    - Basic logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - Console and file output
    - Simple context management
    - Performance timing
    """

    def __init__(
        self,
        name: str = "SparkForge",
        level: int = logging.INFO,
        log_file: Optional[str] = None,
        verbose: bool = True,
    ):
        self.name = name
        self.level = level
        self.log_file = log_file
        self.verbose = verbose

        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Clear existing handlers
        self.logger.handlers.clear()

        # Setup handlers
        self._setup_handlers()

        # Performance tracking
        self._timers: Dict[str, datetime] = {}

    def _setup_handlers(self) -> None:
        """Setup logging handlers."""
        # Console handler
        if self.verbose:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(self.level)
            self.logger.addHandler(console_handler)

        # File handler
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(self.level)
            self.logger.addHandler(file_handler)

    # Basic logging methods
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(self._format_message(message, kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(self._format_message(message, kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(self._format_message(message, kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(self._format_message(message, kwargs))

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self.logger.critical(self._format_message(message, kwargs))

    def _format_message(self, message: str, kwargs: Dict[str, Any]) -> str:
        """Format message with context."""
        if not kwargs:
            return message

        context = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{message} | {context}"

    # Pipeline-specific logging methods
    def pipeline_start(self, pipeline_name: str, mode: str = "initial") -> None:
        """Log pipeline start."""
        self.info(f"🚀 Starting pipeline: {pipeline_name} (mode: {mode})")

    def pipeline_end(
        self, pipeline_name: str, duration: float, success: bool = True
    ) -> None:
        """Log pipeline end."""
        status = "✅ Success" if success else "❌ Failed"
        self.info(f"{status} pipeline: {pipeline_name} ({duration:.2f}s)")

    def step_start(self, stage: str, step: str) -> None:
        """Log step start."""
        self.info(f"🚀 Starting {stage.upper()} step: {step}")

    def step_complete(
        self, stage: str, step: str, duration: float, rows: int = 0
    ) -> None:
        """Log step completion."""
        self.info(
            f"✅ Completed {stage.upper()} step: {step} ({duration:.2f}s, {rows:,} rows)"
        )

    def step_failed(
        self, stage: str, step: str, error: str, duration: float = 0
    ) -> None:
        """Log step failure."""
        self.error(
            f"❌ Failed {stage.upper()} step: {step} ({duration:.2f}s) - {error}"
        )

    def validation_passed(
        self, stage: str, step: str, rate: float, threshold: float
    ) -> None:
        """Log validation success."""
        self.info(
            f"✅ Validation passed for {stage}:{step} - {rate:.2f}% >= {threshold:.2f}%"
        )

    def validation_failed(
        self, stage: str, step: str, rate: float, threshold: float
    ) -> None:
        """Log validation failure."""
        self.warning(
            f"❌ Validation failed for {stage}:{step} - {rate:.2f}% < {threshold:.2f}%"
        )

    def performance_metric(
        self, metric_name: str, value: float, unit: str = "s"
    ) -> None:
        """Log performance metric."""
        self.info(f"📊 {metric_name}: {value:.2f}{unit}")

    # Context management
    @contextmanager
    def context(
        self,
        **context_data: Union[str, int, float, bool, List[str], Dict[str, str], None],
    ) -> Generator[None, None, None]:
        """Add context to all log messages within this block."""
        # Store context in logger's extra data
        old_extra = getattr(self.logger, "extra", {})
        self.logger.extra = {**old_extra, **context_data}
        try:
            yield
        finally:
            self.logger.extra = old_extra

    # Performance timing
    def start_timer(self, operation: str) -> None:
        """Start timing an operation."""
        self._timers[operation] = datetime.utcnow()

    def end_timer(self, operation: str) -> float:
        """End timing an operation and return duration."""
        if operation not in self._timers:
            return 0.0

        start_time = self._timers.pop(operation)
        duration = (datetime.utcnow() - start_time).total_seconds()
        self.performance_metric(operation, duration)
        return duration

    @contextmanager
    def timer(self, operation: str) -> Generator[None, None, None]:
        """Context manager for timing operations."""
        self.start_timer(operation)
        try:
            yield
        finally:
            self.end_timer(operation)

    # Utility methods
    def set_level(self, level: int) -> None:
        """Set logging level."""
        self.level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)

    def close(self) -> None:
        """Close all handlers."""
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()


# Global logger instance
_global_logger: Optional[PipelineLogger] = None


def get_logger() -> PipelineLogger:
    """Get the global logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = PipelineLogger()
    return _global_logger


def set_logger(logger: PipelineLogger) -> None:
    """Set the global logger instance."""
    global _global_logger
    _global_logger = logger


def create_logger(
    name: str = "SparkForge",
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    verbose: bool = True,
) -> PipelineLogger:
    """Create a new logger instance."""
    return PipelineLogger(name=name, level=level, log_file=log_file, verbose=verbose)


def get_global_logger() -> PipelineLogger:
    """Get the global logger instance (alias for get_logger)."""
    return get_logger()


def set_global_logger(logger: PipelineLogger) -> None:
    """Set the global logger instance (alias for set_logger)."""
    set_logger(logger)


def reset_global_logger() -> None:
    """Reset the global logger instance."""
    global _global_logger
    if _global_logger is not None:
        _global_logger.close()
    _global_logger = None
