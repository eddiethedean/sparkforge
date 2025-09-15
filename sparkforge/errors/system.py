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
System-specific exceptions for SparkForge.

This module defines exceptions specific to system operations,
providing detailed error context for system-related issues.
"""

from __future__ import annotations

from .base import ErrorCategory, ErrorSeverity, SparkForgeError


class SystemError(SparkForgeError):
    """Base exception for all system-related errors."""

    def __init__(self, message: str, *, component: str | None = None, **kwargs):
        super().__init__(message, **kwargs)
        self.component = component

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.component:
            return f"[{self.component}] {base_msg}"
        return base_msg


class ResourceError(SystemError):
    """Raised when resource-related errors occur."""

    def __init__(
        self,
        message: str,
        *,
        resource_type: str | None = None,
        resource_name: str | None = None,
        **kwargs,
    ):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.resource_type = resource_type
        self.resource_name = resource_name

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.resource_type and self.resource_name:
            return f"[{self.resource_type}: {self.resource_name}] {base_msg}"
        elif self.resource_type:
            return f"[{self.resource_type}] {base_msg}"
        return base_msg


class ConfigurationError(SystemError):
    """Raised when system configuration is invalid."""

    def __init__(self, message: str, *, config_key: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.config_key = config_key

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.config_key:
            return f"[Config: {self.config_key}] {base_msg}"
        return base_msg


class NetworkError(SystemError):
    """Raised when network-related errors occur."""

    def __init__(self, message: str, *, endpoint: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.endpoint = endpoint

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.endpoint:
            return f"[Endpoint: {self.endpoint}] {base_msg}"
        return base_msg


class StorageError(SystemError):
    """Raised when storage-related errors occur."""

    def __init__(self, message: str, *, storage_path: str | None = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.STORAGE,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.storage_path = storage_path

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.storage_path:
            return f"[Path: {self.storage_path}] {base_msg}"
        return base_msg
