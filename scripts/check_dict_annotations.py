#!/usr/bin/env python3
"""
Script to check for Dict type annotations that should be dict in Python 3.8.

This script scans the codebase for Dict type annotations and reports
any violations that would cause issues in Python 3.8.
"""

import ast
import sys
from pathlib import Path
from typing import List, Tuple


def find_dict_annotations(project_root: Path) -> List[Tuple[str, int, str, str]]:
    """
    Find all Dict type annotations in the codebase.
    
    Args:
        project_root: Root directory of the project
        
    Returns:
        List of (file_path, line_number, line_content, violation_type)
    """
    violations = []
    
    # Get all Python files
    python_files = []
    
    # Scan sparkforge directory
    sparkforge_dir = project_root / "sparkforge"
    if sparkforge_dir.exists():
        python_files.extend(sparkforge_dir.rglob("*.py"))
    
    # Scan tests directory
    tests_dir = project_root / "tests"
    if tests_dir.exists():
        python_files.extend(tests_dir.rglob("*.py"))
    
    # Filter out excluded patterns
    exclude_patterns = {
        "__pycache__", ".git", ".pytest_cache", "node_modules",
        "*.pyc", "*.pyo", "*.pyd", ".coverage", "htmlcov",
        "dist", "build", "*.egg-info"
    }
    
    filtered_files = []
    for file_path in python_files:
        if not any(pattern in str(file_path) for pattern in exclude_patterns):
            filtered_files.append(file_path)
    
    # Check each file
    for py_file in filtered_files:
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # Check for Dict in type annotations
            for i, line in enumerate(lines, 1):
                if has_dict_annotation(line):
                    violations.append((
                        str(py_file), 
                        i, 
                        line.strip(), 
                        "Dict type annotation"
                    ))
            
            # Also check AST for more complex cases
            try:
                tree = ast.parse(content, filename=str(py_file))
                for node in ast.walk(tree):
                    if isinstance(node, ast.AnnAssign):
                        if node_has_dict_annotation(node):
                            line_num = node.lineno
                            line_content = lines[line_num - 1].strip()
                            violations.append((
                                str(py_file), 
                                line_num, 
                                line_content, 
                                "Dict type annotation"
                            ))
                    
                    elif isinstance(node, ast.FunctionDef):
                        if (node.returns and 
                            node_has_dict_annotation(node.returns)):
                            line_num = node.lineno
                            line_content = lines[line_num - 1].strip()
                            violations.append((
                                str(py_file), 
                                line_num, 
                                line_content, 
                                "Dict return annotation"
                            ))
                        
                        for arg in node.args.args:
                            if (arg.annotation and 
                                node_has_dict_annotation(arg.annotation)):
                                line_num = arg.lineno
                                line_content = lines[line_num - 1].strip()
                                violations.append((
                                    str(py_file), 
                                    line_num, 
                                    line_content, 
                                    "Dict parameter annotation"
                                ))
                    
                    elif isinstance(node, ast.Assign):
                        # Check type alias assignments
                        for target in node.targets:
                            if (isinstance(target, ast.Name) and 
                                isinstance(node.value, ast.Subscript) and
                                isinstance(node.value.value, ast.Name) and
                                node.value.value.id == 'Dict'):
                                line_num = node.lineno
                                line_content = lines[line_num - 1].strip()
                                violations.append((
                                    str(py_file), 
                                    line_num, 
                                    line_content, 
                                    "Dict type alias"
                                ))
                                
            except SyntaxError:
                # Skip files with syntax errors
                continue
                
        except Exception as e:
            # Skip files that can't be read
            continue
    
    return violations


def has_dict_annotation(line: str) -> bool:
    """Check if a line contains Dict type annotation."""
    # Skip comments and docstrings
    if line.strip().startswith('#') or line.strip().startswith('"""'):
        return False
    
    # Check for Dict[ pattern (but not in strings)
    if 'Dict[' in line and not is_in_string(line, 'Dict['):
        return True
    
    # Check for typing.Dict pattern
    if 'typing.Dict' in line:
        return True
    
    return False


def is_in_string(line: str, pattern: str) -> bool:
    """Check if pattern is inside a string literal."""
    in_string = False
    quote_char = None
    
    for i, char in enumerate(line):
        if char in ['"', "'"] and (i == 0 or line[i-1] != '\\'):
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                in_string = False
                quote_char = None
        elif in_string and pattern in line[i:i+len(pattern)]:
            return True
    
    return False


def node_has_dict_annotation(node: ast.AST) -> bool:
    """Check if an AST node has Dict annotation."""
    if isinstance(node, ast.Subscript):
        if isinstance(node.value, ast.Name):
            return node.value.id == 'Dict'
    elif isinstance(node, ast.Name):
        return node.id == 'Dict'
    return False


def main():
    """Main function to check for Dict annotations."""
    project_root = Path(__file__).parent.parent
    violations = find_dict_annotations(project_root)
    
    if violations:
        print("❌ Found Dict type annotations that should be 'dict':")
        print()
        
        # Group by file
        files = {}
        for file_path, line_num, line_content, violation_type in violations:
            if file_path not in files:
                files[file_path] = []
            files[file_path].append((line_num, line_content, violation_type))
        
        for file_path, file_violations in files.items():
            print(f"📁 {file_path}:")
            for line_num, line_content, violation_type in file_violations:
                print(f"  Line {line_num} ({violation_type}): {line_content}")
            print()
        
        print(f"Total violations: {len(violations)}")
        print()
        print("💡 Fix: Replace 'Dict' with 'dict' in type annotations")
        print("   Example: Dict[str, int] -> dict[str, int]")
        print("   Example: from typing import Dict -> from typing import dict (or remove import)")
        
        return 1
    else:
        print("✅ No Dict type annotations found. All good!")
        return 0


if __name__ == '__main__':
    sys.exit(main())
