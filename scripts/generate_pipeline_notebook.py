#!/usr/bin/env python3
"""
Generate a Jupyter notebook version of SparkForge PipelineBuilder.

This script creates a standalone notebook where all dependencies are included
as cells in topological order, allowing the entire notebook to be run top-to-bottom
to make PipelineBuilder available without requiring sparkforge installation.

Usage:
    # Generate notebook with default name (uses current version)
    python scripts/generate_pipeline_notebook.py

    # Generate with custom output path
    python scripts/generate_pipeline_notebook.py --output my_notebook.ipynb

    # Show detailed module ordering
    python scripts/generate_pipeline_notebook.py --verbose

How it works:
    1. Extracts version from pipeline_builder/__init__.py
    2. Reads modules from both pipeline_builder_base and pipeline_builder packages
    3. Reads dependency information from module docstrings
    4. Performs topological sort to order modules by dependencies (handles cross-package deps)
    5. Creates notebook cells with module code
    6. Removes all relative imports from pipeline_builder and pipeline_builder_base (comments them out)
    7. Generates a runnable Jupyter notebook

Output:
    A Jupyter notebook file containing:
    - Title and introduction (markdown)
    - All external imports (code)
    - All modules in dependency order (markdown header + code for each)
    - Usage example (markdown + code)

The generated notebook can be run top-to-bottom without requiring sparkforge
to be installed, making it perfect for sharing, teaching, or quick prototyping.
"""

import argparse
import json
import re
import sys
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional


def extract_version() -> str:
    """Extract version from pipeline_builder/__init__.py"""
    candidate_paths = [
        Path("sparkforge/__init__.py"),
        Path("src/pipeline_builder/__init__.py"),
        Path("pipeline_builder/__init__.py"),
    ]

    for init_file in candidate_paths:
        if init_file.exists():
            with open(init_file) as f:
                content = f.read()
            match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)

    return "0.9.0"


def extract_dependencies(file_path: Path) -> List[str]:
    """Extract dependencies from a module's docstring."""
    try:
        with open(file_path) as f:
            content = f.read()

        match = re.search(r"# Depends on:\n((?:#   .+\n)*)", content)
        if match:
            deps = []
            for line in match.group(1).strip().split("\n"):
                line = line.strip()
                if line.startswith("#   "):
                    deps.append(line.replace("#   ", ""))
            return deps
        return []
    except Exception:
        return []


def parse_import_dependencies(code: str, current_package: str) -> List[str]:
    """Parse import statements to find dependencies, including relative imports."""
    deps = []
    lines = code.split("\n")
    in_type_checking = False

    for line in lines:
        # Skip TYPE_CHECKING blocks
        if "if TYPE_CHECKING:" in line:
            in_type_checking = True
            continue
        if in_type_checking and (
            line.strip().startswith("else:")
            or (
                line.strip()
                and not line.strip().startswith("#")
                and len(line) - len(line.lstrip()) <= 4
            )
        ):
            in_type_checking = False
        if in_type_checking:
            continue

        # Match: from package.module import ...
        match = re.match(r"^\s*from\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+import", line)
        if match:
            module_path = match.group(1)
            # Skip standard library and external packages
            if module_path.startswith(("pipeline_builder", "abstracts", "interfaces")):
                # Extract the module path
                deps.append(module_path)

        # Match: from .module import ... or from ..module import ... (relative imports)
        match = re.match(r"^\s*from\s+(\.+)([a-zA-Z_][a-zA-Z0-9_.]*)\s+import", line)
        if match:
            dots = match.group(1)
            rel_module = match.group(2)
            # Convert relative import to absolute based on current package
            # Parse current_package to get the module hierarchy
            parts = current_package.split(".")
            levels_up = len(dots)

            if levels_up == 1:
                # .module -> same package level
                # For "pipeline_builder_base.steps.manager", .validation -> pipeline_builder_base.steps.validation
                abs_module = (
                    f"{current_package.rsplit('.', 1)[0]}.{rel_module}"
                    if "." in current_package
                    else f"{current_package}.{rel_module}"
                )
            elif levels_up == 2:
                # ..module -> go up one level, then access module
                # For "pipeline_builder_base.steps.manager", ..validation -> pipeline_builder_base.validation
                if len(parts) >= 2:
                    # Remove last part (manager), then remove steps, add validation
                    parent_package = ".".join(parts[:-1])  # pipeline_builder_base.steps
                    # Actually, we need to go up from the parent, so remove one more level
                    if len(parts) >= 2:
                        grandparent_package = (
                            ".".join(parts[:-2]) if len(parts) > 2 else parts[0]
                        )
                        abs_module = f"{grandparent_package}.{rel_module}"
                    else:
                        abs_module = f"{parent_package}.{rel_module}"
                else:
                    continue
            else:
                # More dots - go up more levels
                # For each dot after the first, remove one more level
                if levels_up - 1 < len(parts):
                    parent_package = ".".join(parts[: -(levels_up - 1)])
                    abs_module = f"{parent_package}.{rel_module}"
                else:
                    continue

            # Only add if it's one of our packages
            if abs_module.startswith(("pipeline_builder", "abstracts", "interfaces")):
                deps.append(abs_module)

        # Match: import package.module
        match = re.match(r"^\s*import\s+([a-zA-Z_][a-zA-Z0-9_.]*)", line)
        if match:
            module_path = match.group(1)
            if module_path.startswith(("pipeline_builder", "abstracts", "interfaces")):
                deps.append(module_path)

    return deps


def get_module_info(
    base_root: Path, spark_root: Path, abstracts_root: Optional[Path] = None
) -> Dict[str, Dict]:
    """
    Get information about all modules including their dependencies from both
    pipeline_builder_base, pipeline_builder, and abstracts packages.

    Args:
        base_root: Path to pipeline_builder_base directory
        spark_root: Path to pipeline_builder directory
        abstracts_root: Path to abstracts directory (optional)

    Returns:
        Dict mapping module_path to {file_path, dependencies, code, package}
    """
    modules = {}

    # Process pipeline_builder_base first (base package)
    if base_root.exists():
        for py_file in base_root.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            if py_file.name == "__init__.py":
                continue

            # Get module path with package prefix
            rel_path = py_file.relative_to(base_root.parent)
            module_path = str(rel_path).replace(".py", "").replace("/", ".")

            # Store the package-relative path for dependency resolution
            package_rel_path = py_file.relative_to(base_root)
            package_module_path = (
                str(package_rel_path).replace(".py", "").replace("/", ".")
            )

            # Read file content
            with open(py_file) as f:
                code = f.read()

            # Extract dependencies from docstring and also parse imports
            deps = extract_dependencies(py_file)
            # Also parse import statements to find dependencies
            # Determine current module path for relative import resolution
            current_module = str(package_rel_path).replace(".py", "").replace("/", ".")
            import_deps = parse_import_dependencies(
                code, f"pipeline_builder_base.{current_module}"
            )
            deps.extend(import_deps)
            deps = list(set(deps))  # Remove duplicates

            # Normalize dependencies to use module paths
            normalized_deps = []
            for dep in deps:
                # If dependency is a module name, check if it exists in base or spark
                if dep in modules:
                    normalized_deps.append(dep)
                else:
                    # Try to resolve relative dependencies within the same package
                    # e.g., "models.base" -> "pipeline_builder_base.models.base"
                    if "." in dep and not dep.startswith("pipeline_builder"):
                        # Relative path like "models.base" or "errors"
                        # Try resolving within the same package first
                        base_full_path = f"pipeline_builder_base.{dep}"
                        if base_full_path in modules:
                            normalized_deps.append(base_full_path)
                            continue
                    elif "." in dep:
                        # Already a full path, check if it exists
                        if dep in modules:
                            normalized_deps.append(dep)
                            continue
                    else:
                        # Simple name, try both packages
                        base_path = f"pipeline_builder_base.{dep}"
                        spark_path = f"pipeline_builder.{dep}"
                        if base_path in modules:
                            normalized_deps.append(base_path)
                            continue
                        elif spark_path in modules:
                            normalized_deps.append(spark_path)
                            continue

                    # If still not found, try relative resolution
                    # For base package, try resolving relative to current module's package
                    if package_module_path:
                        # Try resolving relative to current module's directory
                        current_dir = package_rel_path.parent
                        if current_dir != Path("."):
                            rel_dep_path = current_dir / dep.replace(".", "/")
                            if rel_dep_path.suffix == "":
                                rel_dep_path = rel_dep_path.with_suffix(".py")
                            full_rel_path = base_root / rel_dep_path
                            if full_rel_path.exists():
                                full_module_path = f"pipeline_builder_base.{str(rel_dep_path).replace('.py', '').replace('/', '.')}"
                                if full_module_path in modules:
                                    normalized_deps.append(full_module_path)
                                    continue

                    # Keep original for now, will be resolved during sort
                    normalized_deps.append(dep)

            modules[module_path] = {
                "file_path": py_file,
                "dependencies": normalized_deps,
                "code": code,
                "name": py_file.stem,
                "package": "pipeline_builder_base",
                "package_rel_path": package_module_path,
            }

    # Process pipeline_builder (Spark-specific package)
    if spark_root.exists():
        for py_file in spark_root.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            if py_file.name == "__init__.py":
                continue

            # Get module path with package prefix
            rel_path = py_file.relative_to(spark_root.parent)
            module_path = str(rel_path).replace(".py", "").replace("/", ".")

            # Store the package-relative path for dependency resolution
            package_rel_path = py_file.relative_to(spark_root)
            package_module_path = (
                str(package_rel_path).replace(".py", "").replace("/", ".")
            )

            # Read file content
            with open(py_file) as f:
                code = f.read()

            # Extract dependencies from docstring and also parse imports
            deps = extract_dependencies(py_file)
            # Also parse import statements to find dependencies
            # Determine current module path for relative import resolution
            current_module = str(package_rel_path).replace(".py", "").replace("/", ".")
            import_deps = parse_import_dependencies(
                code, f"pipeline_builder.{current_module}"
            )
            deps.extend(import_deps)
            deps = list(set(deps))  # Remove duplicates

            # Special case: pipeline_builder.pipeline.builder imports PipelineValidator from pipeline_builder_base
            # Add this as a dependency
            if module_path == "pipeline_builder.pipeline.builder":
                if "pipeline_builder_base.validation.pipeline_validator" not in deps:
                    deps.append("pipeline_builder_base.validation.pipeline_validator")
            # Normalize dependencies - convert pipeline_builder_base references
            normalized_deps = []
            for dep in deps:
                # Check if dependency refers to pipeline_builder_base
                if dep.startswith("pipeline_builder_base."):
                    # Keep as is - it's an absolute reference
                    normalized_deps.append(dep)
                elif "." in dep and not dep.startswith("pipeline_builder"):
                    # Relative path like "models.base" - resolve within pipeline_builder
                    spark_full_path = f"pipeline_builder.{dep}"
                    if spark_full_path in modules:
                        normalized_deps.append(spark_full_path)
                    else:
                        # Try resolving relative to current module's directory
                        current_dir = package_rel_path.parent
                        if current_dir != Path("."):
                            rel_dep_path = current_dir / dep.replace(".", "/")
                            if rel_dep_path.suffix == "":
                                rel_dep_path = rel_dep_path.with_suffix(".py")
                            full_rel_path = spark_root / rel_dep_path
                            if full_rel_path.exists():
                                full_module_path = f"pipeline_builder.{str(rel_dep_path).replace('.py', '').replace('/', '.')}"
                                if full_module_path in modules:
                                    normalized_deps.append(full_module_path)
                                    continue
                        normalized_deps.append(dep)
                elif "." in dep:
                    # Already a full path, check if it exists
                    if dep in modules:
                        normalized_deps.append(dep)
                    else:
                        normalized_deps.append(dep)
                else:
                    # Simple name, try to resolve
                    base_path = f"pipeline_builder_base.{dep}"
                    spark_path = f"pipeline_builder.{dep}"
                    if base_path in modules:
                        normalized_deps.append(base_path)
                    elif spark_path in modules:
                        normalized_deps.append(spark_path)
                    else:
                        normalized_deps.append(dep)

            modules[module_path] = {
                "file_path": py_file,
                "dependencies": normalized_deps,
                "code": code,
                "name": py_file.stem,
                "package": "pipeline_builder",
                "package_rel_path": package_module_path,
            }

    # Process abstracts package if provided
    if abstracts_root and abstracts_root.exists():
        for py_file in abstracts_root.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            if py_file.name == "__init__.py":
                continue

            # Get module path with package prefix
            rel_path = py_file.relative_to(abstracts_root.parent)
            module_path = str(rel_path).replace(".py", "").replace("/", ".")

            # Store the package-relative path for dependency resolution
            package_rel_path = py_file.relative_to(abstracts_root)
            package_module_path = (
                str(package_rel_path).replace(".py", "").replace("/", ".")
            )

            # Read file content
            with open(py_file) as f:
                code = f.read()

            # Extract dependencies from docstring and also parse imports
            deps = extract_dependencies(py_file)
            # Also parse import statements to find dependencies
            # Determine current module path for relative import resolution
            current_module = str(package_rel_path).replace(".py", "").replace("/", ".")
            import_deps = parse_import_dependencies(code, f"abstracts.{current_module}")
            deps.extend(import_deps)
            deps = list(set(deps))  # Remove duplicates
            normalized_deps = []
            for dep in deps:
                if dep in modules:
                    normalized_deps.append(dep)
                elif "." in dep and not dep.startswith("abstracts"):
                    # Relative path like "reports.transform" - resolve within abstracts
                    abstracts_full_path = f"abstracts.{dep}"
                    if abstracts_full_path in modules:
                        normalized_deps.append(abstracts_full_path)
                    else:
                        # Try resolving relative to current module's directory
                        current_dir = package_rel_path.parent
                        if current_dir != Path("."):
                            rel_dep_path = current_dir / dep.replace(".", "/")
                            if rel_dep_path.suffix == "":
                                rel_dep_path = rel_dep_path.with_suffix(".py")
                            full_rel_path = abstracts_root / rel_dep_path
                            if full_rel_path.exists():
                                full_module_path = f"abstracts.{str(rel_dep_path).replace('.py', '').replace('/', '.')}"
                                if full_module_path in modules:
                                    normalized_deps.append(full_module_path)
                                    continue
                        normalized_deps.append(dep)
                elif "." in dep:
                    if dep in modules:
                        normalized_deps.append(dep)
                    else:
                        normalized_deps.append(dep)
                else:
                    # Simple name, try to resolve
                    abstracts_path = f"abstracts.{dep}"
                    if abstracts_path in modules:
                        normalized_deps.append(abstracts_path)
                    else:
                        normalized_deps.append(dep)

            modules[module_path] = {
                "file_path": py_file,
                "dependencies": normalized_deps,
                "code": code,
                "name": py_file.stem,
                "package": "abstracts",
                "package_rel_path": package_module_path,
            }

    return modules


def topological_sort(modules: Dict[str, Dict]) -> List[str]:
    """
    Perform topological sort on modules based on dependencies.
    Handles circular dependencies by breaking cycles strategically.

    Args:
        modules: Dict of module info

    Returns:
        List of module paths in dependency order
    """
    # Build adjacency list and in-degree count
    in_degree = dict.fromkeys(modules, 0)
    adjacency = defaultdict(list)
    edges = []  # Track all edges for cycle detection

    for module, info in modules.items():
        for dep in info["dependencies"]:
            # Check if dependency exists in modules (exact match)
            if dep in modules:
                adjacency[dep].append(module)
                in_degree[module] += 1
                edges.append((dep, module))
            else:
                # Try to find partial matches (e.g., "errors" might refer to "pipeline_builder_base.errors")
                # or "models.base" might refer to "pipeline_builder_base.models.base"
                for mod_key in modules:
                    # Check if dependency is a suffix of module path
                    if mod_key.endswith(f".{dep}") or mod_key == dep:
                        adjacency[mod_key].append(module)
                        in_degree[module] += 1
                        edges.append((mod_key, module))
                        break
                    # Check if dependency matches a module name
                    if modules[mod_key]["name"] == dep:
                        adjacency[mod_key].append(module)
                        in_degree[module] += 1
                        edges.append((mod_key, module))
                        break

    # Find nodes with no incoming edges
    queue = deque([module for module, degree in in_degree.items() if degree == 0])
    result = []

    while queue:
        module = queue.popleft()
        result.append(module)

        # Reduce in-degree for dependent modules
        for dependent in adjacency[module]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Handle circular dependencies
    if len(result) != len(modules):
        remaining = [m for m in modules if m not in result]

        # For circular dependencies, sort by:
        # 1. Modules with fewer dependencies first
        # 2. Alphabetically for consistency
        remaining_sorted = sorted(
            remaining, key=lambda m: (len(modules[m]["dependencies"]), m)
        )

        if remaining_sorted:
            print(
                f"⚠️  Note: {len(remaining_sorted)} module(s) with circular dependencies added at end",
                file=sys.stderr,
            )
            print(f"    Modules: {', '.join(remaining_sorted)}", file=sys.stderr)

        result.extend(remaining_sorted)

    return result


def _collapse_blank_lines(code: str, max_consecutive: int = 1) -> str:
    """
    Collapse runs of blank lines so that there are at most `max_consecutive`.

    This helps avoid large vertical gaps in the generated notebook, especially
    when it is imported or run from another notebook (e.g. in Databricks).
    """
    lines = code.split("\n")
    result_lines: List[str] = []
    empty_count = 0

    for line in lines:
        if line.strip() == "":
            empty_count += 1
            if empty_count <= max_consecutive:
                # Preserve at most `max_consecutive` empty lines in a row
                result_lines.append("")
        else:
            empty_count = 0
            # Strip trailing whitespace but keep indentation
            result_lines.append(line.rstrip())

    # Trim leading and trailing empty lines
    while result_lines and result_lines[0].strip() == "":
        result_lines.pop(0)
    while result_lines and result_lines[-1].strip() == "":
        result_lines.pop()

    return "\n".join(result_lines)


def remove_sparkforge_imports(code: str) -> str:
    """
    Remove sparkforge imports and references, adjust code to work standalone.

    Args:
        code: Original module code

    Returns:
        Modified code with sparkforge imports removed and references cleaned
    """
    # Normalize absolute imports so we can treat them uniformly in the line pass
    code = re.sub(r"(?m)^\s*from\s+pipeline_builder\.", "from .", code)
    code = re.sub(r"(?m)^\s*from\s+pipeline_builder_base\.", "from .", code)
    code = re.sub(r"(?m)^\s*from\s+sparkforge\.", "from .", code)
    code = re.sub(r"(?m)^\s*from\s+abstracts\.", "from .", code)
    code = re.sub(r"(?m)^\s*import\s+abstracts\b", "import .", code)

    lines = code.split("\n")
    new_lines = []
    in_multiline_import = False
    in_mock_spark_import = False
    in_type_checking_block = False
    type_checking_indent = 0

    for line in lines:
        # Preserve TYPE_CHECKING blocks completely (they're compile-time only)
        if re.match(r"^\s*if\s+TYPE_CHECKING\s*:", line):
            in_type_checking_block = True
            type_checking_indent = len(line) - len(line.lstrip())
            new_lines.append(line)
            continue
        elif in_type_checking_block:
            current_indent = (
                len(line) - len(line.lstrip())
                if line.strip()
                else type_checking_indent + 1
            )
            # Check if we've exited the TYPE_CHECKING block (else: or dedented code)
            if (
                line.strip().startswith("else:")
                and current_indent == type_checking_indent
            ):
                in_type_checking_block = False
                new_lines.append(line)
                continue
            elif (
                line.strip()
                and current_indent <= type_checking_indent
                and not line.strip().startswith("#")
            ):
                # We've dedented past the if block
                in_type_checking_block = False
            else:
                # Still in TYPE_CHECKING block, preserve it
                new_lines.append(line)
                continue

        # Check for mock_spark imports (which we want to completely remove)
        if re.match(r"^\s*from\s+mock_spark", line) or re.match(
            r"^\s*import\s+mock_spark", line
        ):
            # Skip mock_spark imports entirely
            if "(" in line and ")" not in line:
                in_mock_spark_import = True
            continue
        elif in_mock_spark_import:
            # Skip continuation lines of mock_spark imports
            if ")" in line:
                in_mock_spark_import = False
            continue
        # Check for relative imports from performance module (time_operation)
        elif re.match(r"^\s*from\s+\.performance\s+import", line):
            # Extract what's being imported
            match = re.search(r"import\s+(.+)", line)
            imported_items = match.group(1).strip() if match else ""

            # Comment out the import
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)

            # Add fallback: define a no-op decorator that will work until performance module is loaded
            # Since modules execute in order, if performance comes after, we need a temporary decorator
            if "time_operation" in imported_items:
                new_lines.append(
                    " " * indent
                    + "# Fallback: time_operation decorator (will be replaced when performance module loads)\n"
                    + " " * indent
                    + "def time_operation(name):\n"
                    + " " * (indent + 4)
                    + "def decorator(func):\n"
                    + " " * (indent + 8)
                    + "return func\n"
                    + " " * (indent + 4)
                    + "return decorator\n"
                )
            continue
        # Check for any relative imports (starting with one or more dots)
        elif re.match(r"^\s*from\s+\.", line):
            # Check if this import includes F or types from compat - we need to handle it specially
            imports_f_from_compat = "F" in line and (
                "compat" in line or "..compat" in line
            )
            imports_types_from_compat = "types" in line and (
                "compat" in line or "..compat" in line
            )

            # Comment out the import
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)

            # If this import includes F from compat, add a fallback
            # F is imported in the imports cell as: from pyspark.sql import functions as F
            # In standalone notebooks, we use the global F from pyspark
            if imports_f_from_compat:
                # Add import to ensure F is available
                new_lines.append(
                    " " * indent
                    + "from pyspark.sql import functions as F  # F from pyspark (not from compat)"
                )
            if imports_types_from_compat:
                # Add import to ensure types is available
                new_lines.append(
                    " " * indent
                    + "from pyspark.sql import types  # types from pyspark (not from compat)"
                )

            # Check if this starts a multi-line import
            if "(" in line and ")" not in line:
                in_multiline_import = True
        elif re.match(r"^\s*import\s+pipeline_builder\b", line):
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)
        elif re.match(r"^\s*from\s+pipeline_builder_base\b", line):
            # Comment out pipeline_builder_base imports
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)
            # Check if this starts a multi-line import
            if "(" in line and ")" not in line:
                in_multiline_import = True
        elif re.match(r"^\s*import\s+pipeline_builder_base\b", line):
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)
        # Keep abstracts imports - they're part of the source code
        # (abstracts modules are included in the notebook)
        elif in_multiline_import:
            # Comment out continuation lines of multi-line imports
            indent = len(line) - len(line.lstrip())
            comment = "# " + line.strip()
            new_lines.append(" " * indent + comment)

            # Check if this line ends the multi-line import
            if ")" in line:
                in_multiline_import = False
        else:
            new_lines.append(line)

    code = "\n".join(new_lines)

    # Remove mock_spark conditional imports - replace with direct pyspark imports
    # Pattern: if os.environ.get("SPARK_MODE"...) block with from mock_spark import
    code = re.sub(
        r"# Use mock functions when in mock mode\n"
        + r'if os\.environ\.get\("SPARK_MODE"[^)]+\)[^:]+:\n'
        + r"    from mock_spark import functions as F\n"
        + r"else:\n"
        + r"    from pyspark\.sql import functions as F",
        "from pyspark.sql import functions as F",
        code,
        flags=re.MULTILINE,
    )

    # Fix _try_import_mockspark to simply return None in standalone notebooks
    # After removing imports, the function body is broken - replace it
    code = re.sub(
        r'def _try_import_mockspark\(\).*?"""Try to import mock-spark modules\.""".*?try:.*?return _DataFrame.*?except Exception:.*?return None',
        'def _try_import_mockspark() -> (\n    tuple[type[Any], type[Any], type[Any], Any, Any, type[Exception]] | None\n):\n    """Try to import mock-spark modules."""\n    # Mock-spark not available in standalone PySpark notebook\n    return None',
        code,
        flags=re.DOTALL,
    )

    # Remove references to "sparkforge" in various contexts
    # Replace in import statements
    code = re.sub(r"\bPipelineValidator\b", "UnifiedValidator", code)

    # Replace in docstrings and comments (but preserve technical meaning)
    code = re.sub(r"SparkForge\s+PipelineBuilder", "PipelineBuilder", code)
    code = re.sub(r"for\s+SparkForge", "for the framework", code)
    code = re.sub(r"SparkForge\s+pipelines", "pipelines", code)
    code = re.sub(r"SparkForge\s+models", "framework models", code)
    code = re.sub(r"SparkForge\s+errors", "framework errors", code)
    code = re.sub(r"SparkForge\s+error", "framework error", code)
    code = re.sub(r"all\s+SparkForge\s+errors", "all framework errors", code)
    code = re.sub(
        r"all\s+other\s+SparkForge\s+exceptions", "all other framework exceptions", code
    )
    code = re.sub(r"SparkForge\s+exceptions", "framework exceptions", code)
    code = re.sub(r"from\s+sparkforge", "from the framework", code)
    code = re.sub(r"SparkForge\s+integration", "framework integration", code)
    code = re.sub(r"with\s+SparkForge", "with the framework", code)

    # Replace standalone "SparkForge" references
    code = re.sub(r'name\s*=\s*["\']SparkForge["\']', 'name="PipelineFramework"', code)
    code = re.sub(r'"SparkForge"', '"PipelineFramework"', code)
    code = re.sub(r"'SparkForge'", "'PipelineFramework'", code)

    # Fix: When compat imports are commented out, ensure F is available
    # For cells that use F after compat import is commented out, add explicit fallback
    # Pattern: "# from ..compat import ... F ..." followed by "col = F.col"
    if "# from ..compat import" in code and "F" in code and "col = F.col" in code:
        # Ensure F is available - add import if needed
        # In standalone notebooks, F should be from pyspark.sql.functions
        if "from pyspark.sql import functions as F" not in code:
            # Add import before col = F.col
            code = re.sub(
                r"(\s*col = F\.col)",
                r"\n    # Ensure F is available from pyspark (imported in imports cell)\n    from pyspark.sql import functions as F\1",
                code,
                flags=re.MULTILINE,
            )

    # Finally, normalize excessive blank lines to avoid large gaps in the
    # generated notebook when it is executed via `%run` in environments like
    # Databricks.
    code = _collapse_blank_lines(code, max_consecutive=1)

    return code


def extract_code_without_docstring(code: str) -> str:
    """Extract code, optionally removing only the module-level docstring."""
    lines = code.split("\n")

    # Find module-level docstring (first docstring at top level)
    docstring_start = -1
    docstring_end = -1
    in_docstring = False
    quote_type = None
    found_first_statement = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip shebang, encoding, blank lines, comments at start
        if stripped.startswith("#!") or stripped.startswith("# -*-"):
            continue
        if not stripped or stripped.startswith("#"):
            continue

        # Track when we find the first actual statement (not docstring)
        if not found_first_statement:
            # Check if this is a docstring
            if ('"""' in stripped or "'''" in stripped) and not in_docstring:
                quote_type = '"""' if '"""' in stripped else "'''"
                docstring_start = i

                if stripped.count(quote_type) >= 2:
                    # Single-line docstring
                    docstring_end = i
                    # Continue to find first actual code statement
                else:
                    in_docstring = True
            elif in_docstring:
                if quote_type in line:
                    docstring_end = i
                    in_docstring = False
            else:
                # This is actual code, not a docstring
                found_first_statement = True
                if docstring_start != -1 and docstring_end == -1:
                    # We were in a docstring but it wasn't closed - this shouldn't happen
                    # but if it does, don't remove anything
                    docstring_start = -1
                break

    # Return code without module docstring (but keep all other code intact)
    if docstring_start != -1 and docstring_end != -1:
        # Remove the docstring lines
        result_lines = lines[:docstring_start] + lines[docstring_end + 1 :]
    else:
        result_lines = lines

    # Remove leading blank lines only (keep trailing structure)
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)

    return "\n".join(result_lines)


def create_notebook_cell(
    code: str,
    cell_type: str = "code",
    metadata: Optional[Dict[Any, Any]] = None,
    collapsed: bool = True,
) -> Dict:
    """Create a Jupyter notebook cell."""
    if metadata is None:
        metadata = {}

    # Add collapsed metadata for code cells (unless explicitly set to False)
    if cell_type == "code" and collapsed:
        metadata["collapsed"] = True
        metadata["jupyter"] = {"source_hidden": True}

    # Format source with proper newlines for Jupyter
    # Each line should end with \n except the last line
    if code:
        lines = code.split("\n")
        source = [line + "\n" for line in lines[:-1]]
        if lines[-1]:  # Add last line without \n if it's not empty
            source.append(lines[-1])
    else:
        source = []

    if cell_type == "code":
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": metadata,
            "outputs": [],
            "source": source,
        }
    else:  # markdown
        return {"cell_type": "markdown", "metadata": metadata, "source": source}


def generate_notebook(
    modules: Dict[str, Dict], sorted_modules: List[str], version: str
) -> Dict:
    """
    Generate the Jupyter notebook structure.

    Args:
        modules: Dict of module information
        sorted_modules: List of modules in dependency order
        version: Package version

    Returns:
        Notebook dict structure
    """
    cells = []

    # Title cell (as Python comments for Databricks compatibility)
    title = f"""# PipelineBuilder & LogWriter v{version} - Standalone Notebook
#
# This notebook contains the complete PipelineBuilder and LogWriter implementation
# as a standalone, executable notebook. All dependencies are included as cells
# in the correct order.
#
# Usage:
# 1. Run all cells from top to bottom
# 2. The PipelineBuilder and LogWriter classes will be available after all cells execute
# 3. Use PipelineBuilder to build and execute data pipelines
# 4. Use LogWriter to log and analyze pipeline execution results
#
# Note: This is generated from version {version}. Module dependencies are
# resolved automatically from source code analysis."""

    cells.append(create_notebook_cell(title, "code"))

    # Imports cell
    imports_code = """# External imports (PySpark, standard library)
from __future__ import annotations

import logging
import sys
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Protocol, Set, Tuple, TypedDict, TypeVar, Union, cast

# PySpark imports
from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# Delta Lake imports
try:
    from delta.tables import DeltaTable
except ImportError:
    print("⚠️  Delta Lake not available. Some features may not work.")
    DeltaTable = None

# Optional imports
try:
    import psutil
except ImportError:
    print("⚠️  psutil not available. Memory monitoring disabled.")
    psutil = None"""

    cells.append(create_notebook_cell(imports_code, "code"))

    # Add module cells in dependency order
    for module_path in sorted_modules:
        info = modules[module_path]

        # Create comment header for the module
        package_name = info.get("package", "unknown")
        module_header = f"# Module: {module_path} ({package_name})\n#\n"
        if info["dependencies"]:
            module_header += (
                f"# Dependencies: {', '.join(sorted(info['dependencies']))}\n\n"
            )
        else:
            module_header += "# Dependencies: None (base module)\n\n"

        # Remove sparkforge imports (keep docstrings for documentation)
        code = info["code"]
        # Remove only the "Depends on:" section from docstring
        code = re.sub(r"# Depends on:\n((?:#   .+\n)*)", "", code)
        code = remove_sparkforge_imports(code)

        # Fix Protocol StepValidator shadowing issue
        # If this is the Protocol StepValidator from pipeline_validation, rename it to avoid shadowing
        if module_path == "pipeline_builder.validation.pipeline_validation":
            # Rename the Protocol StepValidator class to StepValidatorProtocol
            # Match: class StepValidator: (not Protocol, just a regular class acting as protocol)
            code = re.sub(
                r"^class StepValidator:\s*$",
                "class StepValidatorProtocol:",
                code,
                flags=re.MULTILINE,
            )
            # Update references to this Protocol (but not the real StepValidator from base)
            # Only replace in contexts where it's clear it's the Protocol version
            # This is tricky - we'll be conservative and only rename the class definition
            # The usage should be minimal since it's a Protocol

        # Fix abstracts.builder.PipelineBuilder name collision
        # Store it with a unique name to avoid collision with pipeline_builder.pipeline.builder.PipelineBuilder
        if module_path == "abstracts.builder":
            # After the class definition ends, add a global alias
            # Find the end of the class (last method/line before next class or end of module)
            # Actually, simpler: just add it at the very end of the code
            code = (
                code.rstrip()
                + "\n\n# Store as global alias to avoid name collision with pipeline_builder.pipeline.builder.PipelineBuilder\n_AbstractsPipelineBuilderClass = PipelineBuilder\n"
            )

        # Fix table_operations to handle time_operation import
        if module_path == "pipeline_builder.table_operations":
            # Add fallback for time_operation if import is commented out
            if "# from .performance import" in code and "@time_operation" in code:
                # Add import at the top of the module after commented imports
                code = re.sub(
                    r"(# from \.performance import[^\n]*\n[^\n]*# time_operation[^\n]*\n)",
                    r"\1# Import time_operation from performance module (available from notebook cells above)\n"
                    r"# Note: time_operation decorator will work if performance module was executed earlier\n",
                    code,
                    flags=re.MULTILINE,
                )
                # If time_operation is used but not imported, add a try/except wrapper
                # Actually, since performance module is in the notebook, time_operation should be available
                # We just need to make sure the import path works
                # The performance module defines time_operation, so it should be in namespace

        # Fix compat module to use global F from pyspark instead of engine
        if module_path == "pipeline_builder.compat":
            # In standalone notebooks, F should come from pyspark.sql.functions (imported in imports cell)
            # Replace engine-based F assignment with global F
            code = re.sub(
                r"F = _eng\.functions",
                "# F = _eng.functions  # In standalone notebook, use global F from pyspark\n    # F is available from imports cell (pyspark.sql.functions)",
                code,
            )
            # Also update __getattr__ to return global F
            code = re.sub(
                r'if name == "F":\s+return eng\.functions',
                'if name == "F":\n            # In standalone notebook, return global F from pyspark\n            from pyspark.sql import functions as F\n            return F',
                code,
                flags=re.MULTILINE | re.DOTALL,
            )
            # Also ensure types is available
            code = re.sub(
                r'if name == "types":\s+return eng\.types',
                'if name == "types":\n            # In standalone notebook, return types from pyspark.sql.types\n            from pyspark.sql import types\n            return types',
                code,
                flags=re.MULTILINE | re.DOTALL,
            )

        # Fix BasePipelineValidator alias issue
        # PipelineValidator is actually UnifiedValidator in pipeline_builder_base
        # But PipelineValidator exists in pipeline_builder_base.validation.__init__ as an alias
        if module_path == "pipeline_builder.pipeline.builder":
            # Replace BasePipelineValidator with UnifiedValidator (the actual class name)
            # Or we can use PipelineValidator if it's available from __init__
            # Actually, let's just remove the cast since it's not needed for runtime
            code = re.sub(r"\bBasePipelineValidator\b", "UnifiedValidator", code)
            code = re.sub(r"cast\(UnifiedValidator,", "cast(UnifiedValidator,", code)
            # Remove the commented import block
            code = re.sub(
                r"# from pipeline_builder_base\.validation import[^\n]*\n[^\n]*PipelineValidator[^\n]*\n[^\n]*\)[^\n]*\n",
                "",
                code,
                flags=re.MULTILINE,
            )
            # Fix AbstractsPipelineBuilder - it's from abstracts.builder, but the import is commented
            # We created a global alias _AbstractsPipelineBuilderClass when processing abstracts.builder
            # Use that alias instead
            if "AbstractsPipelineBuilder" in code:
                # Replace with the global alias we created
                code = re.sub(
                    r"\bAbstractsPipelineBuilder\b",
                    "_AbstractsPipelineBuilderClass",
                    code,
                )

        # Clean up the code
        code = code.strip()

        if code:
            # Combine header and code in one cell
            combined_code = module_header + code
            cells.append(create_notebook_cell(combined_code, "code"))

    # Add engine configuration helper cell (PySpark-only)
    engine_config_code = """# Engine Configuration Helper (PySpark-only)
# This helper automatically configures the engine with PySpark components
# In standalone notebooks, we use PySpark directly (no mock Spark support)

# Store reference to original configure_engine before we wrap it
# The configure_engine function is already defined in the engine_config module cell above
_original_configure_engine_for_pyspark = configure_engine

def configure_engine_pyspark(spark):
    \"\"\"Configure engine with PySpark components for standalone notebooks.

    This is a convenience function for notebooks that automatically configures
    the engine with PySpark components. In standalone notebooks, we only
    support PySpark (not mock Spark/sparkless).

    Args:
        spark: SparkSession instance
    \"\"\"
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType, FloatType, IntegerType, StringType,
        StructField, StructType, TimestampType
    )
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.window import Window

    # Configure engine with PySpark components
    # Use the original configure_engine function (stored before wrapping)
    # Note: engine_name, dataframe_cls, spark_session_cls, column_cls are optional
    try:
        _original_configure_engine_for_pyspark(
            functions=F,
            types=StructType,
            analysis_exception=AnalysisException,
            window=Window,
            engine_name="pyspark",
            dataframe_cls=type(spark.createDataFrame([], "id int")),
            spark_session_cls=type(spark),
            column_cls=type(F.col("dummy")),
        )
    except TypeError:
        # Fallback if some parameters aren't accepted
        _original_configure_engine_for_pyspark(
            functions=F,
            types=StructType,
            analysis_exception=AnalysisException,
            window=Window,
        )
    print("✅ Engine configured with PySpark components")

# Make configure_engine accept spark parameter for convenience
# This allows using configure_engine(spark=spark) like in examples
# We already stored the original function above as _original_configure_engine_for_pyspark

def configure_engine_wrapper(*, spark=None, **kwargs):
    \"\"\"Configure engine - accepts spark parameter for convenience.

    In standalone notebooks, you can call configure_engine(spark=spark)
    and it will automatically configure with PySpark components.
    \"\"\"
    if spark is not None:
        # Auto-configure with PySpark
        configure_engine_pyspark(spark)
    else:
        # Use original function
        _original_configure_engine_for_pyspark(**kwargs)

# Replace configure_engine in the current namespace
configure_engine = configure_engine_wrapper"""

    cells.append(create_notebook_cell(engine_config_code, "code", collapsed=False))

    # Add usage example cell (as Python comments for Databricks compatibility)
    example_code = """# Usage Example
#
# Here's how to initialize PipelineBuilder and LogWriter:

# Example: Initialize PipelineBuilder and LogWriter
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \\
    .appName("PipelineBuilder Example") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

# Configure engine (required! - uses PySpark automatically)
configure_engine(spark=spark)

# Initialize PipelineBuilder
builder = PipelineBuilder(spark=spark, schema="analytics")
print("✅ PipelineBuilder initialized")

# Initialize LogWriter (simplified API)
log_writer = LogWriter(spark, schema="analytics", table_name="pipeline_logs")
print("✅ LogWriter initialized")"""

    cells.append(create_notebook_cell(example_code, "code", collapsed=False))

    # Create notebook structure
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0",
                "mimetype": "text/x-python",
                "codemirror_mode": {"name": "ipython", "version": 3},
                "pygments_lexer": "ipython3",
                "nbconvert_exporter": "python",
                "file_extension": ".py",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    return notebook


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate Jupyter notebook version of SparkForge PipelineBuilder"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output path for the notebook (default: notebooks/pipeline_builder_v{VERSION}.ipynb)",
    )
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")

    args = parser.parse_args()

    # Get version
    version = extract_version()
    print(f"📦 SparkForge version: {version}")

    # Set output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(f"notebooks/pipeline_builder_v{version}.ipynb")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"📝 Output: {output_path}")
    print("🔍 Analyzing modules...\n")

    # Get all module information from both packages
    base_root = Path("src/pipeline_builder_base")
    spark_root = Path("src/pipeline_builder")
    abstracts_root = Path("src/abstracts")

    # Fallback to old structure if src/ doesn't exist
    if not base_root.exists():
        base_root = Path("pipeline_builder_base")
    if not spark_root.exists():
        spark_root = Path("pipeline_builder")
    if not abstracts_root.exists():
        abstracts_root = Path("abstracts")

    # Fallback to old structure if new structure doesn't exist
    if not base_root.exists() and not spark_root.exists():
        sparkforge_root = Path("sparkforge")
        if not sparkforge_root.exists():
            sparkforge_root = Path("pipeline_builder")
        if not sparkforge_root.exists():
            raise FileNotFoundError(
                "Could not locate source package directory. "
                "Expected 'pipeline_builder_base/' and 'pipeline_builder/' or 'sparkforge/' or 'pipeline_builder/'."
            )
        # Use old single-package structure
        modules = get_module_info(Path("."), sparkforge_root)
    else:
        # Use new multi-package structure (include abstracts if it exists)
        modules = get_module_info(
            base_root, spark_root, abstracts_root if abstracts_root.exists() else None
        )

    print(f"Found {len(modules)} modules")

    # Perform topological sort
    if args.verbose:
        print("\n🔄 Performing topological sort...")
    sorted_modules = topological_sort(modules)

    # Ensure critical executor base class ordering inside circular dependency group.
    # When cycles exist between step executors and related modules, the generic
    # topological sort may place `pipeline_builder.step_executors.base` *after*
    # the concrete executors. In the generated notebook this leads to a runtime
    # error because `BaseStepExecutor` is not yet defined when the bronze/silver/
    # gold executor classes are declared.
    #
    # To make the notebook reliably runnable top‑to‑bottom, we explicitly
    # reorder these four modules relative to each other while preserving the
    # relative order of all other modules.
    reordering_group = [
        "pipeline_builder.step_executors.base",
        "pipeline_builder.step_executors.bronze",
        "pipeline_builder.step_executors.silver",
        "pipeline_builder.step_executors.gold",
    ]
    # Keep only the modules that actually exist in this run
    present_group = [m for m in reordering_group if m in sorted_modules]
    if len(present_group) > 1:
        # Remove them from the list while preserving the order of everything else
        remaining = [m for m in sorted_modules if m not in present_group]
        # Insert the group back starting at the earliest original index, in the
        # desired order (base first, then bronze, silver, gold).
        first_index = min(sorted_modules.index(m) for m in present_group)
        # Preserve the specified ordering, but only for modules that are present
        ordered_present_group = [m for m in reordering_group if m in present_group]
        sorted_modules = (
            remaining[:first_index] + ordered_present_group + remaining[first_index:]
        )

    if args.verbose:
        print(f"\nModule order ({len(sorted_modules)} modules):")
        for i, module in enumerate(sorted_modules, 1):
            deps = modules[module]["dependencies"]
            deps_str = (
                f" (depends on: {', '.join(deps)})" if deps else " (no dependencies)"
            )
            print(f"  {i:2d}. {module}{deps_str}")

    # Generate notebook
    print("\n📓 Generating notebook...")
    notebook = generate_notebook(modules, sorted_modules, version)

    # Write notebook
    with open(output_path, "w") as f:
        json.dump(notebook, f, indent=2)

    print(f"\n✅ Successfully generated notebook: {output_path}")
    print(f"   - Total cells: {len(notebook['cells'])}")
    print(f"   - Modules included: {len(sorted_modules)}")
    print("   - Ready to run from top to bottom!")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(2)
