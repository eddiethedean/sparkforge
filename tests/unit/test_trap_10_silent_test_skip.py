"""
Test cases for Trap 10: Silent Skip in Test Parsing.

This module tests that test parsing no longer silently skips files that can't be parsed,
and instead logs warnings and tracks parsing failures.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, Mock
from tests.unit.test_python38_compatibility import Python38CompatibilityTest


class TestTrap10SilentTestSkip:
    """Test cases for silent test skip fixes."""

    def test_parsing_errors_are_logged_and_tracked(self):
        """Test that parsing errors are logged and tracked instead of silently skipped."""
        test_instance = Python38CompatibilityTest()
        
        # Create a temporary file with invalid Python syntax
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("def invalid_syntax(\n")  # Missing closing parenthesis
            temp_file = f.name
        
        try:
            # Mock the _get_python_files method to return our temp file
            with patch.object(test_instance, '_get_python_files', return_value=[temp_file]):
                with patch('logging.getLogger') as mock_logger:
                    mock_logger_instance = Mock()
                    mock_logger.return_value = mock_logger_instance
                    
                    # Call the method that should handle parsing errors
                    violations = test_instance._find_dict_type_annotations()
                    
                    # Verify that a warning was logged
                    mock_logger_instance.warning.assert_called_once()
                    warning_call = mock_logger_instance.warning.call_args[0][0]
                    assert "Failed to parse file" in warning_call
                    assert "unexpected EOF while parsing" in warning_call
                    
                    # Verify that the parsing error was tracked in violations
                    assert len(violations) == 1
                    assert violations[0][0] == temp_file
                    assert violations[0][1] == 0  # Line 0 indicates parsing error
                    assert "Parse error:" in violations[0][2]
        
        finally:
            # Clean up temp file
            os.unlink(temp_file)

    def test_parsing_errors_in_dict_syntax_are_logged_and_tracked(self):
        """Test that parsing errors in dict syntax checking are logged and tracked."""
        test_instance = Python38CompatibilityTest()
        
        # Create a temporary file with invalid Python syntax
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("def invalid_syntax(\n")  # Missing closing parenthesis
            temp_file = f.name
        
        try:
            # Mock the _get_python_files method to return our temp file
            with patch.object(test_instance, '_get_python_files', return_value=[temp_file]):
                with patch('logging.getLogger') as mock_logger:
                    mock_logger_instance = Mock()
                    mock_logger.return_value = mock_logger_instance
                    
                    # Call the method that should handle parsing errors
                    violations = test_instance._find_dict_syntax_annotations()
                    
                    # Verify that a warning was logged
                    mock_logger_instance.warning.assert_called_once()
                    warning_call = mock_logger_instance.warning.call_args[0][0]
                    assert "Failed to parse file" in warning_call
                    assert "unexpected EOF while parsing" in warning_call
                    
                    # Verify that the parsing error was tracked in violations
                    assert len(violations) == 1
                    assert violations[0][0] == temp_file
                    assert violations[0][1] == 0  # Line 0 indicates parsing error
                    assert "Parse error:" in violations[0][2]
        
        finally:
            # Clean up temp file
            os.unlink(temp_file)

    def test_valid_files_are_processed_normally(self):
        """Test that valid files are processed normally without logging warnings."""
        test_instance = Python38CompatibilityTest()
        
        # Create a temporary file with valid Python syntax
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("def valid_function():\n    return 'hello'\n")
            temp_file = f.name
        
        try:
            # Mock the _get_python_files method to return our temp file
            with patch.object(test_instance, '_get_python_files', return_value=[temp_file]):
                with patch('logging.getLogger') as mock_logger:
                    mock_logger_instance = Mock()
                    mock_logger.return_value = mock_logger_instance
                    
                    # Call the method that should process valid files
                    violations = test_instance._find_dict_type_annotations()
                    
                    # Verify that no warning was logged for valid files
                    mock_logger_instance.warning.assert_not_called()
                    
                    # Verify that no parsing errors were tracked
                    parse_errors = [v for v in violations if v[1] == 0 and "Parse error:" in v[2]]
                    assert len(parse_errors) == 0
        
        finally:
            # Clean up temp file
            os.unlink(temp_file)

    def test_multiple_parsing_errors_are_all_tracked(self):
        """Test that multiple parsing errors are all tracked."""
        test_instance = Python38CompatibilityTest()
        
        # Create multiple temporary files with invalid Python syntax
        temp_files = []
        try:
            for i in range(3):
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    f.write(f"def invalid_syntax_{i}(\n")  # Missing closing parenthesis
                    temp_files.append(f.name)
            
            # Mock the _get_python_files method to return our temp files
            with patch.object(test_instance, '_get_python_files', return_value=temp_files):
                with patch('logging.getLogger') as mock_logger:
                    mock_logger_instance = Mock()
                    mock_logger.return_value = mock_logger_instance
                    
                    # Call the method that should handle parsing errors
                    violations = test_instance._find_dict_type_annotations()
                    
                    # Verify that warnings were logged for each file
                    assert mock_logger_instance.warning.call_count == 3
                    
                    # Verify that all parsing errors were tracked
                    parse_errors = [v for v in violations if v[1] == 0 and "Parse error:" in v[2]]
                    assert len(parse_errors) == 3
                    
                    # Verify that all temp files are represented in violations
                    violation_files = {v[0] for v in parse_errors}
                    assert violation_files == set(temp_files)
        
        finally:
            # Clean up temp files
            for temp_file in temp_files:
                os.unlink(temp_file)

    def test_logging_uses_correct_module_name(self):
        """Test that logging uses the correct module name."""
        test_instance = Python38CompatibilityTest()
        
        # Create a temporary file with invalid Python syntax
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("def invalid_syntax(\n")  # Missing closing parenthesis
            temp_file = f.name
        
        try:
            # Mock the _get_python_files method to return our temp file
            with patch.object(test_instance, '_get_python_files', return_value=[temp_file]):
                with patch('logging.getLogger') as mock_logger:
                    mock_logger_instance = Mock()
                    mock_logger.return_value = mock_logger_instance
                    
                    # Call the method that should handle parsing errors
                    test_instance._find_dict_type_annotations()
                    
                    # Verify that logging.getLogger was called with the correct module name
                    mock_logger.assert_called_once_with('tests.unit.test_python38_compatibility')
        
        finally:
            # Clean up temp file
            os.unlink(temp_file)
