#!/usr/bin/env python3
"""
Basic tests for the security module.

This module provides essential test coverage for the security features
without the complexity that caused issues before.

Key Features Tested:
- SecurityConfig creation
- SecurityManager basic functionality
- Input validation
- Access control
"""

import unittest

from sparkforge.security import (
    AccessDeniedError,
    AccessLevel,
    InputValidationError,
    SecurityConfig,
    SecurityError,
    SecurityManager,
    SQLInjectionError,
    get_security_manager,
    validate_pipeline_inputs,
)


class TestSecurityConfig(unittest.TestCase):
    """Test SecurityConfig dataclass."""

    def test_security_config_defaults(self):
        """Test default security configuration."""
        config = SecurityConfig()

        self.assertTrue(config.enable_input_validation)
        self.assertTrue(config.enable_sql_injection_protection)
        self.assertTrue(config.enable_audit_logging)
        self.assertTrue(config.enable_access_control)
        self.assertEqual(config.max_table_name_length, 128)
        self.assertEqual(config.max_schema_name_length, 64)
        self.assertIsInstance(config.allowed_sql_functions, set)
        self.assertIsInstance(config.forbidden_patterns, list)
        self.assertEqual(config.audit_retention_days, 90)
        self.assertTrue(config.enable_threat_detection)

    def test_security_config_custom(self):
        """Test custom security configuration."""
        config = SecurityConfig(
            enable_input_validation=False,
            max_table_name_length=64,
            audit_retention_days=30,
        )

        self.assertFalse(config.enable_input_validation)
        self.assertEqual(config.max_table_name_length, 64)
        self.assertEqual(config.audit_retention_days, 30)


class TestSecurityManager(unittest.TestCase):
    """Test SecurityManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = SecurityConfig()
        self.security_manager = SecurityManager(self.config)

    def test_security_manager_initialization(self):
        """Test security manager initialization."""
        self.assertEqual(self.security_manager.config, self.config)
        self.assertEqual(len(self.security_manager.audit_log), 0)
        self.assertEqual(len(self.security_manager.access_permissions), 0)

    def test_validate_table_name_valid(self):
        """Test validating valid table names."""
        valid_names = ["users", "user_events", "table123", "my_table_name"]

        for name in valid_names:
            result = self.security_manager.validate_table_name(name)
            self.assertEqual(result, name.lower())

    def test_validate_table_name_invalid(self):
        """Test validating invalid table names."""
        invalid_cases = [
            ("", "empty name"),
            ("a" * 200, "too long"),
            ("123invalid", "starts with number"),
            ("table-name", "contains hyphen"),
            ("table name", "contains space"),
            ("table;name", "contains semicolon"),
        ]

        for name, description in invalid_cases:
            with self.subTest(name=name, description=description):
                with self.assertRaises(InputValidationError):
                    self.security_manager.validate_table_name(name)

    def test_validate_schema_name_valid(self):
        """Test validating valid schema names."""
        valid_names = ["public", "analytics", "schema123", "my_schema"]

        for name in valid_names:
            result = self.security_manager.validate_schema_name(name)
            self.assertEqual(result, name.lower())

    def test_validate_schema_name_invalid(self):
        """Test validating invalid schema names."""
        invalid_cases = [
            ("", "empty name"),
            ("a" * 100, "too long"),
            ("123schema", "starts with number"),
            ("schema-name", "contains hyphen"),
            ("schema name", "contains space"),
        ]

        for name, description in invalid_cases:
            with self.subTest(name=name, description=description):
                with self.assertRaises(InputValidationError):
                    self.security_manager.validate_schema_name(name)

    def test_validate_sql_expression_valid(self):
        """Test validating valid SQL expressions."""
        valid_expressions = [
            "col('id').isNotNull()",
            "col('name').like('John%')",
            "col('age') > 18",
            "col('salary').between(30000, 100000)",
        ]

        for expr in valid_expressions:
            result = self.security_manager.validate_sql_expression(expr)
            self.assertEqual(result, expr)

    def test_validate_sql_expression_sql_injection(self):
        """Test detecting SQL injection attempts."""
        injection_attempts = [
            "DROP TABLE users",
            "DELETE FROM users",
            "UNION SELECT * FROM users",
            "EXEC xp_cmdshell('dir')",
        ]

        for attempt in injection_attempts:
            with self.subTest(attempt=attempt):
                with self.assertRaises(SQLInjectionError):
                    self.security_manager.validate_sql_expression(attempt)

    def test_access_control_permissions(self):
        """Test access control and permissions."""
        # Test initial state - no permissions
        self.assertFalse(
            self.security_manager.check_access_permission(
                "user1", "resource1", AccessLevel.READ
            )
        )

        # Grant permission
        self.security_manager.grant_permission("user1", AccessLevel.READ)
        self.assertTrue(
            self.security_manager.check_access_permission(
                "user1", "resource1", AccessLevel.READ
            )
        )

        # Grant additional permission
        self.security_manager.grant_permission("user1", AccessLevel.WRITE)
        self.assertTrue(
            self.security_manager.check_access_permission(
                "user1", "resource1", AccessLevel.WRITE
            )
        )

        # Revoke permission
        self.security_manager.revoke_permission("user1", AccessLevel.READ)
        self.assertFalse(
            self.security_manager.check_access_permission(
                "user1", "resource1", AccessLevel.READ
            )
        )
        self.assertTrue(
            self.security_manager.check_access_permission(
                "user1", "resource1", AccessLevel.WRITE
            )
        )

    def test_audit_logging(self):
        """Test audit logging functionality."""
        # Test audit log creation
        self.security_manager._audit_log("test_event", {"key": "value"})

        self.assertEqual(len(self.security_manager.audit_log), 1)
        log_entry = self.security_manager.audit_log[0]

        self.assertEqual(log_entry["event_type"], "test_event")
        self.assertEqual(log_entry["details"], {"key": "value"})
        self.assertIn("timestamp", log_entry)
        self.assertIn("session_id", log_entry)


class TestSecurityErrors(unittest.TestCase):
    """Test security error classes."""

    def test_security_error_hierarchy(self):
        """Test security error class hierarchy."""
        # Test base SecurityError
        with self.assertRaises(SecurityError):
            raise SecurityError("Test security error")

        # Test InputValidationError
        with self.assertRaises(InputValidationError):
            raise InputValidationError("Test validation error")

        # Test SQLInjectionError
        with self.assertRaises(SQLInjectionError):
            raise SQLInjectionError("Test SQL injection error")

        # Test AccessDeniedError
        with self.assertRaises(AccessDeniedError):
            raise AccessDeniedError("Test access denied error")

    def test_error_inheritance(self):
        """Test that specific errors inherit from SecurityError."""
        self.assertIsInstance(InputValidationError("test"), SecurityError)
        self.assertIsInstance(SQLInjectionError("test"), SecurityError)
        self.assertIsInstance(AccessDeniedError("test"), SecurityError)


class TestConvenienceFunctions(unittest.TestCase):
    """Test convenience functions."""

    def test_get_security_manager(self):
        """Test getting global security manager."""
        manager1 = get_security_manager()
        manager2 = get_security_manager()

        # Should return the same instance
        self.assertIs(manager1, manager2)

    def test_validate_pipeline_inputs_valid(self):
        """Test validating valid pipeline inputs."""
        result = validate_pipeline_inputs(
            schema_name="test_schema",
            table_name="test_table",
            rules={"id": ["not_null"]},
        )

        self.assertTrue(result["valid"])
        self.assertEqual(result["schema_name"], "test_schema")
        self.assertEqual(result["table_name"], "test_table")
        self.assertIn("id", result["rules"])

    def test_validate_pipeline_inputs_invalid(self):
        """Test validating invalid pipeline inputs."""
        result = validate_pipeline_inputs(
            schema_name="",  # Invalid empty schema
            table_name="test_table",
            rules={"id": ["not_null"]},
        )

        self.assertFalse(result["valid"])
        self.assertIn("error", result)
        self.assertIn("error_type", result)
        self.assertEqual(result["error_type"], "InputValidationError")


if __name__ == "__main__":
    unittest.main()
