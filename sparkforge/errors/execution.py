"""
Execution-specific exceptions for SparkForge.

This module defines exceptions specific to execution operations,
providing detailed error context for execution-related issues.
"""

from __future__ import annotations
from typing import Optional

from .base import SparkForgeError, ErrorCategory, ErrorSeverity


class ExecutionError(SparkForgeError):
    """Raised when execution operations fail."""
    
    def __init__(
        self,
        message: str,
        *,
        execution_step: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.execution_step = execution_step


class ExecutionEngineError(SparkForgeError):
    """Raised when execution engine fails."""
    
    def __init__(
        self,
        message: str,
        *,
        execution_mode: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.execution_mode = execution_mode
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.execution_mode:
            return f"[Mode: {self.execution_mode}] {base_msg}"
        return base_msg


class StepExecutionError(SparkForgeError):
    """Raised when a step fails during execution."""
    
    def __init__(
        self,
        message: str,
        *,
        step_name: str,
        step_type: Optional[str] = None,
        retry_count: int = 0,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.step_name = step_name
        self.step_type = step_type
        self.retry_count = retry_count
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.step_name:
            return f"[{self.step_name}] {base_msg}"
        return base_msg


class StrategyError(SparkForgeError):
    """Raised when execution strategy fails."""
    
    def __init__(
        self,
        message: str,
        *,
        strategy_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.strategy_name = strategy_name
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.strategy_name:
            return f"[Strategy: {self.strategy_name}] {base_msg}"
        return base_msg


class RetryError(SparkForgeError):
    """Raised when retry operations fail."""
    
    def __init__(
        self,
        message: str,
        *,
        step_name: Optional[str] = None,
        max_retries: int = 0,
        retry_count: int = 0,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.step_name = step_name
        self.max_retries = max_retries
        self.retry_count = retry_count
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.step_name:
            return f"[{self.step_name}] {base_msg} (Retry {self.retry_count}/{self.max_retries})"
        return base_msg


class TimeoutError(SparkForgeError):
    """Raised when operations timeout."""
    
    def __init__(
        self,
        message: str,
        *,
        timeout_seconds: float = 0.0,
        step_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.timeout_seconds = timeout_seconds
        self.step_name = step_name
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.timeout_seconds > 0:
            return f"{base_msg} (Timeout: {self.timeout_seconds}s)"
        return base_msg


__all__ = [
    "ExecutionError",
    "ExecutionEngineError", 
    "ExecutionTimeoutError",
    "RetryExhaustedError",
    "TimeoutError"
]
