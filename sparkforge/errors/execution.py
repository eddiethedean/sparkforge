# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

"""
Execution-specific exceptions for SparkForge.

This module defines exceptions specific to execution operations,
providing detailed error context for execution-related issues.
"""

from __future__ import annotations

from .base import ErrorCategory, ErrorSeverity, SparkForgeError


class ExecutionError(SparkForgeError):
    """Raised when execution operations fail."""

    def __init__(self, message: str, *, execution_step: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.execution_step = execution_step


class ExecutionEngineError(SparkForgeError):
    """Raised when execution engine fails."""

    def __init__(self, message: str, *, execution_mode: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
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
        step_type: str | None = None,
        retry_count: int = 0,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
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

    def __init__(self, message: str, *, strategy_name: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
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
        step_name: str | None = None,
        max_retries: int = 0,
        retry_count: int = 0,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
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
        step_name: str | None = None,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXECUTION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
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
    "TimeoutError",
]
