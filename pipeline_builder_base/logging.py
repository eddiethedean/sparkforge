"""
Simplified logging system for the framework.

This module provides a clean, focused logging system for pipeline operations
without the complexity of the previous over-engineered system.
"""

import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional, Union


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
        name: str = "PipelineRunner",
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
    def debug(self, message: str, **kwargs: Union[str, int, float, bool, None]) -> None:
        """Log debug message."""
        self.logger.debug(self._format_message(message, kwargs))

    def info(self, message: str, **kwargs: Union[str, int, float, bool, None]) -> None:
        """Log info message."""
        self.logger.info(self._format_message(message, kwargs))

    def warning(
        self, message: str, **kwargs: Union[str, int, float, bool, None]
    ) -> None:
        """Log warning message."""
        self.logger.warning(self._format_message(message, kwargs))

    def error(self, message: str, **kwargs: Union[str, int, float, bool, None]) -> None:
        """Log error message."""
        self.logger.error(self._format_message(message, kwargs))

    def critical(
        self, message: str, **kwargs: Union[str, int, float, bool, None]
    ) -> None:
        """Log critical message."""
        self.logger.critical(self._format_message(message, kwargs))

    def _format_message(
        self, message: str, kwargs: Dict[str, Union[str, int, float, bool, None]]
    ) -> str:
        """Format message with keyword arguments."""
        if not kwargs:
            return message
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{message} ({kwargs_str})"

    # Performance timing
    @contextmanager
    def time_operation(self, operation_name: str) -> Generator[None, None, None]:
        """Context manager for timing operations."""
        start_time = datetime.now(timezone.utc)
        self._timers[operation_name] = start_time
        try:
            yield
        finally:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            self.info(f"Operation '{operation_name}' took {duration:.2f}s")
            # Clean up timer after operation completes
            if operation_name in self._timers:
                del self._timers[operation_name]

    def start_timer(self, timer_name: str) -> None:
        """Start a named timer."""
        self._timers[timer_name] = datetime.now(timezone.utc)

    def stop_timer(self, timer_name: str) -> float:
        """Stop a named timer and return duration in seconds."""
        if timer_name not in self._timers:
            self.warning(f"Timer '{timer_name}' was not started")
            return 0.0
        start_time = self._timers[timer_name]
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        del self._timers[timer_name]
        return duration

    def get_timer_duration(self, timer_name: str) -> float:
        """Get current duration of a running timer without stopping it."""
        if timer_name not in self._timers:
            return 0.0
        start_time = self._timers[timer_name]
        end_time = datetime.now(timezone.utc)
        return (end_time - start_time).total_seconds()

    # Context management
    @contextmanager
    def log_context(self, context_name: str) -> Generator[None, None, None]:
        """Context manager for logging context."""
        self.info(f"Starting: {context_name}")
        try:
            yield
            self.info(f"Completed: {context_name}")
        except Exception as e:
            self.error(f"Failed: {context_name}", error=str(e))
            raise

    # Step execution logging
    def step_start(self, step_type: str, step_name: str) -> None:
        """Log step start."""
        self.info(f"▶️ Starting {step_type.upper()} step: {step_name}")

    def step_complete(
        self,
        step_type: str,
        step_name: str,
        duration: float,
        rows_processed: int = 0,
        rows_written: int = 0,
        invalid_rows: int = 0,
        validation_rate: float = 100.0,
    ) -> None:
        """Log step completion."""
        self.info(
            f"✅ Completed {step_type.upper()} step: {step_name} ({duration:.2f}s) - "
            f"{rows_processed} rows processed, {rows_written} rows written, "
            f"{invalid_rows} invalid, {validation_rate:.1f}% valid"
        )

    # Utility methods
    def set_level(self, level: int) -> None:
        """Set logging level."""
        self.level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)

    def add_handler(self, handler: logging.Handler) -> None:
        """Add a custom logging handler."""
        self.logger.addHandler(handler)

    def remove_handler(self, handler: logging.Handler) -> None:
        """Remove a logging handler."""
        self.logger.removeHandler(handler)

    def clear_handlers(self) -> None:
        """Clear all logging handlers."""
        self.logger.handlers.clear()

    def close(self) -> None:
        """Close all logging handlers, especially file handlers."""
        for handler in self.logger.handlers[:]:  # Copy list to avoid modification during iteration
            handler.close()
            self.logger.removeHandler(handler)

