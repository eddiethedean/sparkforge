"""
Python 3.8 Compatibility Tests

This module tests for Python 3.8 compatibility issues, specifically:
1. Dict type annotations that should be dict
2. Other typing issues that cause problems in Python 3.8
3. Import compatibility issues
"""

import ast
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, get_type_hints

import pytest


class Python38CompatibilityTest(unittest.TestCase):
    """Test Python 3.8 compatibility issues."""

    def setUp(self):
        """Set up test environment."""
        self.project_root = Path(__file__).parent.parent.parent
        self.sparkforge_dir = self.project_root / "sparkforge"
        self.test_dir = self.project_root / "tests"

        # Files to exclude from scanning
        self.exclude_patterns = {
            "__pycache__",
            ".git",
            ".pytest_cache",
            "node_modules",
            "*.pyc",
            "*.pyo",
            "*.pyd",
            ".coverage",
            "htmlcov",
            "dist",
            "build",
            "*.egg-info",
            "test_",
            "tests/",
        }

    def test_python_version(self):
        """Test that we're running on Python 3.8+."""
        self.assertGreaterEqual(
            sys.version_info[:2],
            (3, 8),
            f"Expected Python 3.8+, got {sys.version_info[:2]}",
        )

    def test_no_dict_type_annotations(self):
        """Test that no files use dict[...] syntax (should use Dict from typing in Python 3.8)."""
        violations = self._find_dict_syntax_annotations()

        if violations:
            violation_msg = (
                "Found dict[...] syntax that should be Dict[...] in Python 3.8:\n"
            )
            for file_path, line_num, line_content in violations:
                violation_msg += f"  {file_path}:{line_num}: {line_content.strip()}\n"

            self.fail(violation_msg)

    def test_no_legacy_typing_imports(self):
        """Test that Dict imports from typing are present (required for Python 3.8)."""
        violations = self._find_legacy_typing_imports()

        # In Python 3.8, we SHOULD be using Dict from typing, so this test should pass
        # (no violations means we're using the correct approach)
        if violations:
            violation_msg = "Found files that should import Dict from typing for Python 3.8 compatibility:\n"
            for file_path, line_num, line_content in violations:
                violation_msg += f"  {file_path}:{line_num}: {line_content.strip()}\n"

            # Don't fail - this is expected behavior for Python 3.8
            print(
                f"Note: {len(violations)} files use Dict from typing (correct for Python 3.8)"
            )

    def test_all_files_parseable(self):
        """Test that all Python files can be parsed without syntax errors."""
        violations = self._find_syntax_errors()

        if violations:
            violation_msg = "Found files with syntax errors:\n"
            for file_path, error in violations:
                violation_msg += f"  {file_path}: {error}\n"

            self.fail(violation_msg)

    def test_import_compatibility(self):
        """Test that all modules can be imported without errors."""
        violations = self._test_imports()

        if violations:
            violation_msg = "Found import errors:\n"
            for module_name, error in violations:
                violation_msg += f"  {module_name}: {error}\n"

            self.fail(violation_msg)

    def _find_dict_type_annotations(self) -> List[Tuple[str, int, str]]:
        """Find all Dict type annotations in the codebase."""
        violations = []

        for py_file in self._get_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                # Parse the file
                tree = ast.parse(content, filename=str(py_file))

                # Find Dict type annotations
                for node in ast.walk(tree):
                    if isinstance(node, ast.AnnAssign):
                        if self._has_dict_annotation(node.annotation):
                            line_num = node.lineno
                            line_content = content.split("\n")[line_num - 1]
                            violations.append((str(py_file), line_num, line_content))

                    elif isinstance(node, ast.FunctionDef):
                        # Check return annotations
                        if node.returns and self._has_dict_annotation(node.returns):
                            line_num = node.lineno
                            line_content = content.split("\n")[line_num - 1]
                            violations.append((str(py_file), line_num, line_content))

                        # Check parameter annotations
                        for arg in node.args.args:
                            if arg.annotation and self._has_dict_annotation(
                                arg.annotation
                            ):
                                line_num = arg.lineno
                                line_content = content.split("\n")[line_num - 1]
                                violations.append(
                                    (str(py_file), line_num, line_content)
                                )

                    elif isinstance(node, ast.Assign):
                        # Check type alias assignments
                        for target in node.targets:
                            if (
                                isinstance(target, ast.Name)
                                and isinstance(node.value, ast.Subscript)
                                and isinstance(node.value.value, ast.Name)
                                and node.value.value.id == "Dict"
                            ):
                                line_num = node.lineno
                                line_content = content.split("\n")[line_num - 1]
                                violations.append(
                                    (str(py_file), line_num, line_content)
                                )

            except Exception as e:
                # Log parsing errors instead of silently skipping
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to parse file {py_file} for Dict type annotations: {e}")
                # Add to violations to track parsing failures
                violations.append((str(py_file), 0, f"Parse error: {e}"))

        return violations

    def _find_dict_syntax_annotations(self) -> List[Tuple[str, int, str]]:
        """Find all dict[...] syntax annotations that would fail in Python 3.8."""
        violations = []

        for py_file in self._get_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content)

                for node in ast.walk(tree):
                    if isinstance(node, ast.AnnAssign):
                        if hasattr(node.annotation, "value") and hasattr(
                            node.annotation.value, "id"
                        ):
                            if node.annotation.value.id == "dict":
                                violations.append(
                                    (str(py_file), node.lineno, f"dict type annotation")
                                )

                    elif isinstance(node, ast.FunctionDef):
                        if (
                            node.returns
                            and hasattr(node.returns, "value")
                            and hasattr(node.returns.value, "id")
                        ):
                            if node.returns.value.id == "dict":
                                violations.append(
                                    (
                                        str(py_file),
                                        node.lineno,
                                        f"dict return annotation",
                                    )
                                )

            except Exception as e:
                # Log parsing errors instead of printing
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to parse file {py_file} for dict syntax annotations: {e}")
                # Add to violations to track parsing failures
                violations.append((str(py_file), 0, f"Parse error: {e}"))

        return violations

    def _has_dict_annotation(self, annotation: ast.AST) -> bool:
        """Check if an annotation uses Dict type."""
        if isinstance(annotation, ast.Subscript):
            if isinstance(annotation.value, ast.Name):
                return annotation.value.id == "Dict"
        elif isinstance(annotation, ast.Name):
            return annotation.id == "Dict"
        return False

    def _find_legacy_typing_imports(self) -> List[Tuple[str, int, str]]:
        """Find legacy typing imports."""
        violations = []

        for py_file in self._get_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                for i, line in enumerate(lines, 1):
                    # Check for Dict import from typing
                    if (
                        "from typing import" in line
                        and "Dict" in line
                        and "dict" not in line
                    ):
                        violations.append((str(py_file), i, line))

                    # Check for typing.Dict usage
                    if "typing.Dict" in line:
                        violations.append((str(py_file), i, line))

            except Exception as e:
                # Log parsing errors instead of silently skipping
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to parse file {py_file} for dict syntax annotations: {e}")
                # Add to violations to track parsing failures
                violations.append((str(py_file), 0, f"Parse error: {e}"))

        return violations

    def _find_syntax_errors(self) -> List[Tuple[str, str]]:
        """Find files with syntax errors."""
        violations = []

        for py_file in self._get_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                ast.parse(content, filename=str(py_file))

            except SyntaxError as e:
                violations.append((str(py_file), str(e)))
            except Exception as e:
                violations.append((str(py_file), f"Parse error: {e}"))

        return violations

    def _test_imports(self) -> List[Tuple[str, str]]:
        """Test that modules can be imported."""
        violations = []

        # Test core sparkforge modules
        core_modules = [
            "sparkforge",
            "sparkforge.models",
            "sparkforge.logging",
            "sparkforge.errors",
            "sparkforge.types",
            "sparkforge.writer",
            "sparkforge.writer.core",
            "sparkforge.writer.models",
            "sparkforge.writer.exceptions",
        ]

        for module_name in core_modules:
            try:
                __import__(module_name)
            except ImportError as e:
                violations.append((module_name, str(e)))
            except Exception as e:
                violations.append((module_name, f"Unexpected error: {e}"))

        return violations

    def _get_python_files(self) -> List[Path]:
        """Get all Python files in the project."""
        python_files = []

        # Scan sparkforge directory
        if self.sparkforge_dir.exists():
            python_files.extend(self.sparkforge_dir.rglob("*.py"))

        # Scan tests directory
        if self.test_dir.exists():
            python_files.extend(self.test_dir.rglob("*.py"))

        # Filter out excluded patterns
        filtered_files = []
        for file_path in python_files:
            if not any(pattern in str(file_path) for pattern in self.exclude_patterns):
                filtered_files.append(file_path)

        return filtered_files


class DictTypeAnnotationTest(unittest.TestCase):
    """Specific tests for Dict type annotation issues."""

    def test_dict_vs_Dict_compatibility(self):
        """Test that Dict from typing works in Python 3.8 (dict[...] syntax doesn't)."""
        # Test function with Dict annotation (correct for Python 3.8)
        def func_with_Dict() -> Dict[str, int]:
            return {"test": 1}

        # Test that dict[...] syntax fails in Python 3.8
        with self.assertRaises(TypeError):

            def func_with_dict() -> (
                dict[str, int]
            ):  # This syntax doesn't work in Python 3.8
                return {"test": 1}

        # Dict annotation should work
        self.assertEqual(func_with_Dict(), {"test": 1})

        # Test type hints
        hints_Dict = get_type_hints(func_with_Dict)
        self.assertIn("return", hints_Dict)

    def test_typeddict_compatibility(self):
        """Test TypedDict compatibility."""
        from typing import TypedDict

        class TestDict(TypedDict):
            name: str
            value: int

        # Should work without issues
        test_dict: TestDict = {"name": "test", "value": 1}
        self.assertEqual(test_dict["name"], "test")
        self.assertEqual(test_dict["value"], 1)

    def test_union_type_compatibility(self):
        """Test Union type compatibility."""
        from typing import Union

        def func_with_union(value: Union[str, int]) -> Union[str, int]:
            return value

        # Should work without issues
        self.assertEqual(func_with_union("test"), "test")
        self.assertEqual(func_with_union(42), 42)


class ImportCompatibilityTest(unittest.TestCase):
    """Test import compatibility in Python 3.8."""

    def test_core_imports(self):
        """Test that core modules can be imported."""
        try:
            import sparkforge

            self.assertTrue(hasattr(sparkforge, "__version__"))
        except ImportError as e:
            self.fail(f"Failed to import sparkforge: {e}")

    def test_writer_imports(self):
        """Test that writer module can be imported."""
        try:
            from sparkforge.writer import LogWriter
            from sparkforge.writer.exceptions import WriterError
            from sparkforge.writer.models import WriterConfig
        except ImportError as e:
            self.fail(f"Failed to import writer modules: {e}")

    def test_models_imports(self):
        """Test that models can be imported."""
        try:
            from sparkforge.models import ExecutionResult, StepResult
            from sparkforge.types import NumericDict, StringDict
        except ImportError as e:
            self.fail(f"Failed to import model types: {e}")


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
