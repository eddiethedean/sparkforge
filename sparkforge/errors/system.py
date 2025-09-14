"""
System-specific exceptions for SparkForge.

This module defines exceptions specific to system operations,
providing detailed error context for system-related issues.
"""

from __future__ import annotations
from typing import Optional

from .base import SparkForgeError, ErrorCategory, ErrorSeverity


class SystemError(SparkForgeError):
    """Base exception for all system-related errors."""
    
    def __init__(
        self,
        message: str,
        *,
        component: Optional[str] = None,
        **kwargs
    ):
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
        resource_type: Optional[str] = None,
        resource_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.HIGH,
            **kwargs
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
    
    def __init__(
        self,
        message: str,
        *,
        config_key: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.config_key = config_key
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.config_key:
            return f"[Config: {self.config_key}] {base_msg}"
        return base_msg


class NetworkError(SystemError):
    """Raised when network-related errors occur."""
    
    def __init__(
        self,
        message: str,
        *,
        endpoint: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.endpoint = endpoint
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.endpoint:
            return f"[Endpoint: {self.endpoint}] {base_msg}"
        return base_msg


class StorageError(SystemError):
    """Raised when storage-related errors occur."""
    
    def __init__(
        self,
        message: str,
        *,
        storage_path: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.STORAGE,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.storage_path = storage_path
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.storage_path:
            return f"[Path: {self.storage_path}] {base_msg}"
        return base_msg
