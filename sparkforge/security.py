#!/usr/bin/env python3
"""
Security utilities for SparkForge pipeline framework.

This module provides comprehensive security features including input validation,
sanitization, access control, and audit logging to ensure safe pipeline execution.

Key Features:
- Input validation and sanitization
- SQL injection protection
- Access control and permissions
- Audit logging and data lineage
- Security configuration management
- Threat detection and monitoring
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Union, Pattern
from dataclasses import dataclass, field
from enum import Enum
import re
import logging
import hashlib
from datetime import datetime
from pathlib import Path
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class SecurityLevel(Enum):
    """Security levels for different operations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AccessLevel(Enum):
    """Access levels for different resources."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    EXECUTE = "execute"


@dataclass
class SecurityConfig:
    """Configuration for security features."""
    enable_input_validation: bool = True
    enable_sql_injection_protection: bool = True
    enable_audit_logging: bool = True
    enable_access_control: bool = True
    max_table_name_length: int = 128
    max_schema_name_length: int = 64
    allowed_sql_functions: Set[str] = field(default_factory=lambda: {
        'col', 'lit', 'when', 'otherwise', 'isNull', 'isNotNull',
        'gt', 'lt', 'ge', 'le', 'eq', 'ne', 'isin', 'contains',
        'startswith', 'endswith', 'regexp', 'like', 'rlike',
        'cast', 'coalesce', 'concat', 'substring', 'trim',
        'upper', 'lower', 'length', 'round', 'abs', 'sum',
        'count', 'avg', 'min', 'max', 'first', 'last', 'between'
    })
    forbidden_patterns: List[Pattern] = field(default_factory=lambda: [
        re.compile(r'(?i)(drop|delete|truncate|alter|create|insert|update)'),
        re.compile(r'(?i)(union|select\s+\*|from\s+\w+)'),
        re.compile(r'(?i)(exec|execute|sp_|xp_)'),
        re.compile(r'(?i)(script|javascript|vbscript)'),
        re.compile(r'[;\\"`]'),  # Removed single quote from this pattern
        re.compile(r'(?i)(or\s+1\s*=\s*1|and\s+1\s*=\s*1)'),
        re.compile(r"';.*--"),  # More specific pattern for SQL injection
        re.compile(r"'.*;.*--"),  # Another specific pattern
        re.compile(r"'.*or.*'.*="),  # Pattern for '1' OR '1'='1
        re.compile(r"'.*drop.*table", re.IGNORECASE)  # Pattern for DROP TABLE
    ])
    audit_retention_days: int = 90
    enable_threat_detection: bool = True


class SecurityError(Exception):
    """Base exception for security-related errors."""
    pass


class InputValidationError(SecurityError):
    """Raised when input validation fails."""
    pass


class AccessDeniedError(SecurityError):
    """Raised when access is denied."""
    pass


class SQLInjectionError(SecurityError):
    """Raised when SQL injection is detected."""
    pass


class SecurityManager:
    """Manages security features for SparkForge pipelines."""
    
    def __init__(self, config: Optional[SecurityConfig] = None):
        """Initialize security manager."""
        self.config = config or SecurityConfig()
        self.logger = logging.getLogger(__name__)
        self.audit_log: List[Dict[str, Any]] = []
        self.access_permissions: Dict[str, Set[AccessLevel]] = {}
        
    def validate_table_name(self, table_name: str) -> str:
        """Validate and sanitize table name."""
        if not table_name:
            raise InputValidationError("Table name cannot be empty")
        
        if len(table_name) > self.config.max_table_name_length:
            raise InputValidationError(f"Table name too long: {len(table_name)} > {self.config.max_table_name_length}")
        
        # Check for forbidden characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise InputValidationError(f"Invalid table name format: {table_name}")
        
        # Check for SQL injection patterns
        self._check_sql_injection(table_name)
        
        return table_name.lower()
    
    def validate_schema_name(self, schema_name: str) -> str:
        """Validate and sanitize schema name."""
        if not schema_name:
            raise InputValidationError("Schema name cannot be empty")
        
        if len(schema_name) > self.config.max_schema_name_length:
            raise InputValidationError(f"Schema name too long: {len(schema_name)} > {self.config.max_schema_name_length}")
        
        # Check for forbidden characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
            raise InputValidationError(f"Invalid schema name format: {schema_name}")
        
        return schema_name.lower()
    
    def validate_column_name(self, column_name: str) -> str:
        """Validate and sanitize column name."""
        if not column_name:
            raise InputValidationError("Column name cannot be empty")
        
        # Check for forbidden characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise InputValidationError(f"Invalid column name format: {column_name}")
        
        return column_name.lower()
    
    def validate_sql_expression(self, expression: str) -> str:
        """Validate SQL expression for security threats."""
        if not expression:
            return expression
        
        # Check for SQL injection patterns
        self._check_sql_injection(expression)
        
        # Check for forbidden functions
        self._check_forbidden_functions(expression)
        
        return expression
    
    def validate_validation_rules(self, rules: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
        """Validate validation rules for security."""
        if not isinstance(rules, dict):
            raise InputValidationError("Validation rules must be a dictionary")
        
        validated_rules = {}
        for column_name, rule_list in rules.items():
            # Validate column name
            validated_column = self.validate_column_name(column_name)
            
            # Validate rules
            if not isinstance(rule_list, list):
                raise InputValidationError(f"Rules for column '{column_name}' must be a list")
            
            validated_rules[validated_column] = []
            for rule in rule_list:
                if isinstance(rule, str):
                    # Validate string rules
                    validated_rule = self.validate_sql_expression(rule)
                    validated_rules[validated_column].append(validated_rule)
                else:
                    # For PySpark expressions, we can't easily validate them
                    # but we can log them for audit purposes
                    self._audit_log("rule_validation", {
                        "column": validated_column,
                        "rule_type": type(rule).__name__,
                        "rule": str(rule)
                    })
                    validated_rules[validated_column].append(rule)
        
        return validated_rules
    
    def _check_sql_injection(self, input_string: str) -> None:
        """Check for SQL injection patterns."""
        if not self.config.enable_sql_injection_protection:
            return
        
        for pattern in self.config.forbidden_patterns:
            if pattern.search(input_string):
                self._audit_log("sql_injection_attempt", {
                    "input": input_string,
                    "pattern": pattern.pattern
                })
                raise SQLInjectionError(f"Potential SQL injection detected: {input_string}")
    
    def _check_forbidden_functions(self, expression: str) -> None:
        """Check for forbidden SQL functions."""
        # Extract function names from expression
        function_pattern = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(')
        functions = function_pattern.findall(expression)
        
        for func in functions:
            if func.lower() not in [f.lower() for f in self.config.allowed_sql_functions]:
                self._audit_log("forbidden_function_attempt", {
                    "function": func,
                    "expression": expression
                })
                raise InputValidationError(f"Forbidden function detected: {func}")
    
    def check_access_permission(self, user: str, resource: str, access_level: AccessLevel) -> bool:
        """Check if user has permission to access resource."""
        if not self.config.enable_access_control:
            return True
        
        user_permissions = self.access_permissions.get(user, set())
        return access_level in user_permissions
    
    def grant_permission(self, user: str, access_level: AccessLevel) -> None:
        """Grant permission to user."""
        if user not in self.access_permissions:
            self.access_permissions[user] = set()
        self.access_permissions[user].add(access_level)
        
        self._audit_log("permission_granted", {
            "user": user,
            "access_level": access_level.value
        })
    
    def revoke_permission(self, user: str, access_level: AccessLevel) -> None:
        """Revoke permission from user."""
        if user in self.access_permissions:
            self.access_permissions[user].discard(access_level)
        
        self._audit_log("permission_revoked", {
            "user": user,
            "access_level": access_level.value
        })
    
    def _audit_log(self, event_type: str, details: Dict[str, Any]) -> None:
        """Log security event for audit purposes."""
        if not self.config.enable_audit_logging:
            return
        
        audit_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "details": details,
            "session_id": self._get_session_id()
        }
        
        self.audit_log.append(audit_entry)
        self.logger.info(f"Security audit: {event_type} - {details}")
    
    def _get_session_id(self) -> str:
        """Get current session ID."""
        return hashlib.md5(str(datetime.utcnow()).encode()).hexdigest()[:8]
    
    def get_audit_log(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        if limit:
            return self.audit_log[-limit:]
        return self.audit_log.copy()
    
    def export_audit_log(self, file_path: str) -> None:
        """Export audit log to file."""
        with open(file_path, 'w') as f:
            json.dump(self.audit_log, f, indent=2)
    
    def detect_threats(self) -> List[Dict[str, Any]]:
        """Detect potential security threats."""
        if not self.config.enable_threat_detection:
            return []
        
        threats = []
        
        # Check for repeated failed attempts
        failed_attempts = [entry for entry in self.audit_log 
                          if entry.get("event_type") in ["sql_injection_attempt", "forbidden_function_attempt"]]
        
        if len(failed_attempts) > 10:
            threats.append({
                "type": "repeated_failed_attempts",
                "severity": "high",
                "count": len(failed_attempts),
                "description": "Multiple security violations detected"
            })
        
        # Check for suspicious patterns
        suspicious_entries = [entry for entry in self.audit_log 
                             if "suspicious" in entry.get("event_type", "")]
        
        if suspicious_entries:
            threats.append({
                "type": "suspicious_activity",
                "severity": "medium",
                "count": len(suspicious_entries),
                "description": "Suspicious activity patterns detected"
            })
        
        return threats


# Global security manager instance
_security_manager: Optional[SecurityManager] = None


def get_security_manager() -> SecurityManager:
    """Get global security manager instance."""
    global _security_manager
    if _security_manager is None:
        _security_manager = SecurityManager()
    return _security_manager


def validate_pipeline_inputs(
    schema_name: str,
    table_name: str,
    rules: Optional[Dict[str, List[Any]]] = None
) -> Dict[str, Any]:
    """Validate pipeline inputs for security."""
    security_manager = get_security_manager()
    
    try:
        validated_schema = security_manager.validate_schema_name(schema_name)
        validated_table = security_manager.validate_table_name(table_name)
        validated_rules = security_manager.validate_validation_rules(rules or {})
        
        return {
            "schema_name": validated_schema,
            "table_name": validated_table,
            "rules": validated_rules,
            "valid": True
        }
    except SecurityError as e:
        return {
            "valid": False,
            "error": str(e),
            "error_type": type(e).__name__
        }
