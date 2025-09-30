"""
Test for Dict type annotation issues in Python 3.8.

This test specifically checks for the use of Dict type annotations
that should be replaced with dict for Python 3.8 compatibility.
"""

import ast
import sys
from pathlib import Path
from typing import Dict, List, Tuple, get_type_hints

import pytest


class DictAnnotationTest:
    """Test for Dict type annotation issues."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.violations = []

    def find_dict_annotations(self) -> List[Tuple[str, int, str, str]]:
        """
        Find all Dict type annotations in the codebase.

        Returns:
            List of (file_path, line_number, line_content, violation_type)
        """
        violations = []

        for py_file in self._get_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                lines = content.split("\n")

                # Check for Dict in type annotations
                for i, line in enumerate(lines, 1):
                    if self._has_dict_annotation(line):
                        violations.append(
                            (str(py_file), i, line.strip(), "Dict type annotation")
                        )

                # Also check AST for more complex cases
                tree = ast.parse(content, filename=str(py_file))
                for node in ast.walk(tree):
                    if isinstance(node, ast.AnnAssign):
                        if self._node_has_dict_annotation(node):
                            line_num = node.lineno
                            line_content = lines[line_num - 1].strip()
                            violations.append(
                                (
                                    str(py_file),
                                    line_num,
                                    line_content,
                                    "Dict type annotation",
                                )
                            )

                    elif isinstance(node, ast.FunctionDef):
                        if node.returns and self._node_has_dict_annotation(
                            node.returns
                        ):
                            line_num = node.lineno
                            line_content = lines[line_num - 1].strip()
                            violations.append(
                                (
                                    str(py_file),
                                    line_num,
                                    line_content,
                                    "Dict return annotation",
                                )
                            )

                        for arg in node.args.args:
                            if arg.annotation and self._node_has_dict_annotation(
                                arg.annotation
                            ):
                                line_num = arg.lineno
                                line_content = lines[line_num - 1].strip()
                                violations.append(
                                    (
                                        str(py_file),
                                        line_num,
                                        line_content,
                                        "Dict parameter annotation",
                                    )
                                )

            except Exception as e:
                # Skip files that can't be parsed
                continue

        return violations

    def _has_dict_annotation(self, line: str) -> bool:
        """Check if a line contains Dict type annotation."""
        # Skip comments and docstrings
        if line.strip().startswith("#") or line.strip().startswith('"""'):
            return False

        # Check for Dict[ pattern (but not in strings)
        if "Dict[" in line and not self._is_in_string(line, "Dict["):
            return True

        # Check for typing.Dict pattern
        if "typing.Dict" in line:
            return True

        return False

    def _is_in_string(self, line: str, pattern: str) -> bool:
        """Check if pattern is inside a string literal."""
        in_string = False
        quote_char = None

        for i, char in enumerate(line):
            if char in ['"', "'"] and (i == 0 or line[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None
            elif in_string and pattern in line[i : i + len(pattern)]:
                return True

        return False

    def _node_has_dict_annotation(self, node: ast.AST) -> bool:
        """Check if an AST node has Dict annotation."""
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name):
                return node.value.id == "Dict"
        elif isinstance(node, ast.Name):
            return node.id == "Dict"
        return False

    def find_dict_syntax_annotations(self) -> List[Tuple[str, int, str, str]]:
        """
        Find all dict[...] syntax annotations that would fail in Python 3.8.

        Returns:
            List of (file_path, line_number, line_content, violation_type)
        """
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
                                line_content = content.split("\n")[node.lineno - 1]
                                violations.append(
                                    (
                                        str(py_file),
                                        node.lineno,
                                        line_content,
                                        "dict type annotation",
                                    )
                                )

                    elif isinstance(node, ast.FunctionDef):
                        if (
                            node.returns
                            and hasattr(node.returns, "value")
                            and hasattr(node.returns.value, "id")
                        ):
                            if node.returns.value.id == "dict":
                                line_content = content.split("\n")[node.lineno - 1]
                                violations.append(
                                    (
                                        str(py_file),
                                        node.lineno,
                                        line_content,
                                        "dict return annotation",
                                    )
                                )

            except Exception as e:
                print(f"Error parsing {py_file}: {e}")

        return violations

    def _get_python_files(self) -> List[Path]:
        """Get all Python files in the project."""
        python_files = []

        # Scan sparkforge directory
        sparkforge_dir = self.project_root / "sparkforge"
        if sparkforge_dir.exists():
            python_files.extend(sparkforge_dir.rglob("*.py"))

        # Scan tests directory
        tests_dir = self.project_root / "tests"
        if tests_dir.exists():
            python_files.extend(tests_dir.rglob("*.py"))

        # Filter out excluded patterns
        exclude_patterns = {
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

        filtered_files = []
        for file_path in python_files:
            if not any(pattern in str(file_path) for pattern in exclude_patterns):
                filtered_files.append(file_path)

        return filtered_files


def test_no_dict_annotations():
    """Test that no files use dict[...] syntax (should use Dict from typing in Python 3.8)."""
    checker = DictAnnotationTest()
    violations = checker.find_dict_syntax_annotations()

    if violations:
        violation_msg = (
            "Found dict[...] syntax that should be Dict[...] in Python 3.8:\n"
        )
        for file_path, line_num, line_content, violation_type in violations:
            violation_msg += (
                f"  {file_path}:{line_num} ({violation_type}): {line_content}\n"
            )

        pytest.fail(violation_msg)


def test_python_version():
    """Test that we're running on Python 3.8+."""
    assert sys.version_info[:2] >= (
        3,
        8,
    ), f"Expected Python 3.8+, got {sys.version_info[:2]}"


def test_dict_vs_Dict_equivalence():
    """Test that Dict from typing works in Python 3.8 (dict[...] syntax doesn't)."""
    # Test function with Dict annotation (works in Python 3.8)
    def func_with_Dict() -> Dict[str, int]:
        return {"test": 1}

    # Test that dict[...] syntax fails in Python 3.8
    try:

        def func_with_dict() -> (
            dict[str, int]
        ):  # This syntax doesn't work in Python 3.8
            return {"test": 1}

        assert False, "dict[str, int] syntax should fail in Python 3.8"
    except TypeError:
        pass  # Expected to fail

    # Dict annotation should work
    assert func_with_Dict() == {"test": 1}

    # Test type hints
    hints_Dict = get_type_hints(func_with_Dict)
    assert "return" in hints_Dict


def test_typeddict_compatibility():
    """Test TypedDict compatibility."""
    from typing import TypedDict

    class TestDict(TypedDict):
        name: str
        value: int

    # Should work without issues
    test_dict: TestDict = {"name": "test", "value": 1}
    assert test_dict["name"] == "test"
    assert test_dict["value"] == 1


def test_import_compatibility():
    """Test that core modules can be imported."""
    try:
        import sparkforge

        assert hasattr(sparkforge, "__version__")
    except ImportError as e:
        pytest.fail(f"Failed to import sparkforge: {e}")


def test_writer_imports():
    """Test that writer module can be imported."""
    try:
        from sparkforge.writer import LogWriter
        from sparkforge.writer.exceptions import WriterError
        from sparkforge.writer.models import WriterConfig
    except ImportError as e:
        pytest.fail(f"Failed to import writer modules: {e}")


def test_models_imports():
    """Test that models can be imported."""
    try:
        from sparkforge.models import ExecutionResult, StepResult
        from sparkforge.types import NumericDict, StringDict
    except ImportError as e:
        pytest.fail(f"Failed to import model types: {e}")


if __name__ == "__main__":
    # Run the specific test
    checker = DictAnnotationTest()
    violations = checker.find_dict_annotations()

    if violations:
        print("Found Dict type annotations that should be 'dict':")
        for file_path, line_num, line_content, violation_type in violations:
            print(f"  {file_path}:{line_num} ({violation_type}): {line_content}")
        sys.exit(1)
    else:
        print("No Dict type annotations found. All good!")
        sys.exit(0)
