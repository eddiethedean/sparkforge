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
Execution-related exceptions for SparkForge.

This module defines a standardized exception hierarchy for execution errors,
providing better error handling and debugging capabilities.
"""

from typing import Any, Dict, Optional


class ExecutionError(Exception):
    """Base exception for all execution-related errors."""

    def __init__(
        self,
        message: str,
        step_name: Optional[str] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.step_name = step_name
        self.error_code = error_code or "EXECUTION_ERROR"
        self.context = context or {}

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.step_name:
            return f"[{self.step_name}] {base_msg}"
        return base_msg


class StepExecutionError(ExecutionError):
    """Raised when a specific step fails during execution."""

    def __init__(
        self,
        message: str,
        step_name: str,
        step_type: Optional[str] = None,
        retry_count: int = 0,
        **kwargs,
    ):
        super().__init__(message, step_name, "STEP_EXECUTION_ERROR", **kwargs)
        self.step_type = step_type
        self.retry_count = retry_count


class DependencyError(ExecutionError):
    """Raised when dependency-related errors occur."""

    def __init__(
        self,
        message: str,
        step_name: Optional[str] = None,
        missing_dependencies: Optional[list] = None,
        **kwargs,
    ):
        super().__init__(message, step_name, "DEPENDENCY_ERROR", **kwargs)
        self.missing_dependencies = missing_dependencies or []


class ValidationError(ExecutionError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        step_name: str,
        validation_rate: float = 0.0,
        threshold: float = 0.0,
        **kwargs,
    ):
        super().__init__(message, step_name, "VALIDATION_ERROR", **kwargs)
        self.validation_rate = validation_rate
        self.threshold = threshold


class ResourceError(ExecutionError):
    """Raised when resource-related errors occur (memory, timeout, etc.)."""

    def __init__(
        self,
        message: str,
        step_name: Optional[str] = None,
        resource_type: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, step_name, "RESOURCE_ERROR", **kwargs)
        self.resource_type = resource_type


class ConfigurationError(ExecutionError):
    """Raised when configuration-related errors occur."""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        super().__init__(message, error_code="CONFIGURATION_ERROR", **kwargs)
        self.config_key = config_key
