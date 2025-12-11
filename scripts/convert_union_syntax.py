#!/usr/bin/env python3
"""
Script to convert Python 3.10+ union syntax to Python 3.8 compatible syntax.

Converts:
- `str | None` → `Optional[str]`
- `list[str] | None` → `Optional[list[str]]`
- `A | B` → `Union[A, B]`
- Adds necessary imports (Optional, Union)

Usage:
    python scripts/convert_union_syntax.py [--dry-run] [--path PATH]
"""

import ast
import re
import sys
from pathlib import Path
from typing import Tuple


class UnionSyntaxConverter(ast.NodeTransformer):
    """AST transformer to convert union syntax."""

    def __init__(self):
        self.needs_optional = False
        self.needs_union = False
        self.file_path = ""

    def visit_BinOp(self, node):
        """Convert `A | B` to `Union[A, B]` or `Optional[A]`."""
        if isinstance(node.op, ast.BitOr):
            # Check if one side is None
            left_str = ast.unparse(node.left) if hasattr(ast, "unparse") else None
            right_str = ast.unparse(node.right) if hasattr(ast, "unparse") else None

            # Check for None
            if isinstance(node.right, ast.Constant) and node.right.value is None:
                # `A | None` → `Optional[A]`
                self.needs_optional = True
                return ast.Name(id="Optional", ctx=ast.Load())
            elif isinstance(node.left, ast.Constant) and node.left.value is None:
                # `None | A` → `Optional[A]`
                self.needs_optional = True
                return ast.Name(id="Optional", ctx=ast.Load())
            else:
                # `A | B` → `Union[A, B]`
                self.needs_union = True
                return ast.Name(id="Union", ctx=ast.Load())

        return self.generic_visit(node)


def convert_union_syntax_in_code(content: str) -> Tuple[str, bool, bool]:
    """
    Convert union syntax in Python code string.

    Returns:
        (converted_content, needs_optional, needs_union)
    """
    needs_optional = False
    needs_union = False

    # Pattern 1: `str | None` or `None | str` → `Optional[str]`
    # Match type annotations like: `name: str | None = None`
    pattern_optional = r"\b(\w+(?:\[[^\]]+\])?)\s*\|\s*None\b"
    pattern_optional_reverse = r"\bNone\s*\|\s*(\w+(?:\[[^\]]+\])?)\b"

    def replace_optional(match):
        nonlocal needs_optional
        needs_optional = True
        type_name = match.group(1)
        return f"Optional[{type_name}]"

    # First pass: `A | None`
    content = re.sub(pattern_optional, replace_optional, content)
    # Second pass: `None | A`
    content = re.sub(pattern_optional_reverse, replace_optional, content)

    # Pattern 2: `A | B` (where neither is None) → `Union[A, B]`
    # This is more complex - we need to avoid already converted Optional patterns
    # Match patterns like: `name: A | B` but not `Optional[...]`
    pattern_union = r"\b(\w+(?:\[[^\]]+\])?)\s*\|\s*(\w+(?:\[[^\]]+\])?)\b"

    def replace_union(match):
        nonlocal needs_union
        # Skip if this looks like it's already in Optional[...]
        before = content[: match.start()]
        after = content[match.end() :]

        # Check if we're inside Optional[...]
        optional_count = before.count("Optional[") - before.count("]")
        if optional_count > 0:
            return match.group(0)  # Don't replace inside Optional

        # Check if either side is None (shouldn't happen after first pass, but safety check)
        if "None" in match.group(0):
            return match.group(0)

        needs_union = True
        left = match.group(1)
        right = match.group(2)
        return f"Union[{left}, {right}]"

    # Apply union replacement (but be careful not to break Optional)
    # We'll do a simpler approach: find standalone `A | B` patterns
    lines = content.split("\n")
    new_lines = []
    in_optional = False

    for line in lines:
        # Track Optional brackets
        optional_depth = line.count("Optional[") - line.count("]")
        if optional_depth > 0:
            in_optional = True
        if in_optional and line.count("]") > line.count("Optional["):
            in_optional = False

        if not in_optional:
            # Replace `A | B` patterns (but not `A | None` which we already handled)
            # Match: word | word (not None)
            union_pattern = (
                r"(\w+(?:\[[^\]]+\])?)\s*\|\s*(\w+(?:\[[^\]]+\])?)(?!\s*\|\s*None)"
            )

            def make_union(m):
                nonlocal needs_union
                # Skip if this is part of an Optional we already converted
                if "Optional[" in line[: m.start()]:
                    return m.group(0)
                needs_union = True
                return f"Union[{m.group(1)}, {m.group(2)}]"

            line = re.sub(union_pattern, make_union, line)

        new_lines.append(line)

    content = "\n".join(new_lines)

    return content, needs_optional, needs_union


def update_imports(content: str, needs_optional: bool, needs_union: bool) -> str:
    """Add Optional and/or Union to typing imports if needed."""
    if not needs_optional and not needs_union:
        return content

    lines = content.split("\n")
    new_lines = []
    imports_added = False

    for i, line in enumerate(lines):
        # Check for existing typing imports
        if re.match(r"^from typing import", line):
            # Parse existing imports
            imports = []
            if "Optional" in line:
                imports.append("Optional")
            if "Union" in line:
                imports.append("Union")

            # Add what's needed
            if needs_optional and "Optional" not in line:
                imports.append("Optional")
            if needs_union and "Union" not in line:
                imports.append("Union")

            if imports:
                # Reconstruct import line
                # Extract other imports from the line
                match = re.match(r"^from typing import (.+)$", line)
                if match:
                    existing = match.group(1)
                    # Parse existing imports (handle commas, parentheses for multiline)
                    existing_imports = [
                        imp.strip()
                        for imp in re.split(r"[,()]", existing)
                        if imp.strip()
                    ]
                    # Add our imports
                    all_imports = sorted(set(existing_imports + imports))
                    new_line = f"from typing import {', '.join(all_imports)}"
                    new_lines.append(new_line)
                    imports_added = True
                    continue

        new_lines.append(line)

    # If no typing import found, add one
    if not imports_added:
        # Find the right place to insert (after other imports)
        insert_idx = 0
        for i, line in enumerate(new_lines):
            if line.startswith("import ") or line.startswith("from "):
                insert_idx = i + 1
            elif line.strip() and not line.startswith("#"):
                break

        imports = []
        if needs_optional:
            imports.append("Optional")
        if needs_union:
            imports.append("Union")

        if imports:
            new_lines.insert(insert_idx, f"from typing import {', '.join(imports)}")

    return "\n".join(new_lines)


def process_file(file_path: Path, dry_run: bool = False) -> bool:
    """Process a single Python file."""
    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return False

    # Check if file has union syntax
    if "|" not in content or "| None" not in content:
        # Check for other union patterns
        if not re.search(r"\w+\s*\|\s*\w+", content):
            return False  # No union syntax found

    # Convert union syntax
    converted, needs_optional, needs_union = convert_union_syntax_in_code(content)

    if converted == content and not needs_optional and not needs_union:
        return False  # No changes needed

    # Update imports
    final_content = update_imports(converted, needs_optional, needs_union)

    if final_content == content:
        return False  # No actual changes

    if dry_run:
        print(f"Would convert: {file_path}")
        return True

    # Write back
    try:
        file_path.write_text(final_content, encoding="utf-8")
        print(f"Converted: {file_path}")
        return True
    except Exception as e:
        print(f"Error writing {file_path}: {e}", file=sys.stderr)
        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert Python 3.10+ union syntax to Python 3.8 compatible syntax"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without making changes",
    )
    parser.add_argument(
        "--path", type=str, default="src/", help="Path to process (default: src/)"
    )

    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"Error: Path {path} does not exist", file=sys.stderr)
        return 1

    # Find all Python files
    if path.is_file():
        files = [path]
    else:
        files = list(path.rglob("*.py"))

    if not files:
        print(f"No Python files found in {path}")
        return 0

    print(f"Found {len(files)} Python files")

    converted_count = 0
    for file_path in files:
        if process_file(file_path, dry_run=args.dry_run):
            converted_count += 1

    if args.dry_run:
        print(f"\nWould convert {converted_count} files")
    else:
        print(f"\nConverted {converted_count} files")

    return 0


if __name__ == "__main__":
    sys.exit(main())
