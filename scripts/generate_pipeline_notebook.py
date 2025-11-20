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


def get_module_info(base_root: Path, spark_root: Path) -> Dict[str, Dict]:
    """
    Get information about all modules including their dependencies from both
    pipeline_builder_base and pipeline_builder packages.

    Args:
        base_root: Path to pipeline_builder_base directory
        spark_root: Path to pipeline_builder directory

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

            # Read file content
            with open(py_file) as f:
                code = f.read()

            # Extract dependencies and normalize them
            deps = extract_dependencies(py_file)
            # Normalize dependencies to use module paths
            normalized_deps = []
            for dep in deps:
                # If dependency is a module name, check if it exists in base or spark
                if dep in modules:
                    normalized_deps.append(dep)
                else:
                    # Try to find the module
                    # Dependencies might be relative like "errors" or "models.base"
                    # Convert to full module path
                    if "." in dep:
                        # Already a full path, check if it exists
                        if dep in modules:
                            normalized_deps.append(dep)
                    else:
                        # Simple name, try both packages
                        base_path = f"pipeline_builder_base.{dep}"
                        spark_path = f"pipeline_builder.{dep}"
                        if base_path in modules:
                            normalized_deps.append(base_path)
                        elif spark_path in modules:
                            normalized_deps.append(spark_path)
                        else:
                            # Keep original for now, will be resolved during sort
                            normalized_deps.append(dep)

            modules[module_path] = {
                "file_path": py_file,
                "dependencies": normalized_deps,
                "code": code,
                "name": py_file.stem,
                "package": "pipeline_builder_base",
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

            # Read file content
            with open(py_file) as f:
                code = f.read()

            # Extract dependencies and normalize them
            deps = extract_dependencies(py_file)
            # Normalize dependencies - convert pipeline_builder_base references
            normalized_deps = []
            for dep in deps:
                # Check if dependency refers to pipeline_builder_base
                if dep.startswith("pipeline_builder_base."):
                    # Keep as is - it's an absolute reference
                    normalized_deps.append(dep)
                elif "." in dep:
                    # Relative path within pipeline_builder, convert to full path
                    full_path = f"pipeline_builder.{dep}"
                    if full_path in modules:
                        normalized_deps.append(full_path)
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
                found = False
                for mod_key in modules:
                    # Check if dependency is a suffix of module path
                    if mod_key.endswith(f".{dep}") or mod_key == dep:
                        adjacency[mod_key].append(module)
                        in_degree[module] += 1
                        edges.append((mod_key, module))
                        found = True
                        break
                    # Check if dependency matches a module name
                    if modules[mod_key]["name"] == dep:
                        adjacency[mod_key].append(module)
                        in_degree[module] += 1
                        edges.append((mod_key, module))
                        found = True
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

    lines = code.split("\n")
    new_lines = []
    in_multiline_import = False
    in_mock_spark_import = False

    for line in lines:
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
        # Check for any relative imports (starting with one or more dots)
        elif re.match(r"^\s*from\s+\.", line):
            # Comment out the import
            indent = len(line) - len(line.lstrip())
            comment = (
                "# " + line.strip() + "  # Removed: defined in notebook cells above"
            )
            new_lines.append(" " * indent + comment)

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

    return code


def extract_code_without_docstring(code: str) -> str:
    """Extract code without the module docstring."""
    lines = code.split("\n")

    # Find and skip module docstring
    docstring_start = -1
    docstring_end = -1
    in_docstring = False
    quote_type = None

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip shebang, encoding, comments at start
        if stripped.startswith("#"):
            continue
        if not stripped:
            continue

        # Skip imports before docstring
        if stripped.startswith("from ") or stripped.startswith("import "):
            if docstring_start == -1:
                continue
            else:
                break

        # Found docstring
        if not in_docstring:
            if '"""' in stripped or "'''" in stripped:
                quote_type = '"""' if '"""' in stripped else "'''"
                docstring_start = i

                if stripped.count(quote_type) >= 2:
                    docstring_end = i
                    break
                else:
                    in_docstring = True
        elif in_docstring and quote_type in line:
            docstring_end = i
            break

    # Return code without docstring
    if docstring_start != -1:
        result_lines = lines[:docstring_start] + lines[docstring_end + 1 :]
    else:
        result_lines = lines

    # Remove leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()

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

        # Extract code without docstring and remove sparkforge imports
        code = extract_code_without_docstring(info["code"])
        code = remove_sparkforge_imports(code)

        # Clean up the code
        code = code.strip()

        if code:
            # Combine header and code in one cell
            combined_code = module_header + code
            cells.append(create_notebook_cell(combined_code, "code"))

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
    base_root = Path("pipeline_builder_base")
    spark_root = Path("pipeline_builder")
    
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
        # Use new two-package structure
        modules = get_module_info(base_root, spark_root)

    print(f"Found {len(modules)} modules")

    # Perform topological sort
    if args.verbose:
        print("\n🔄 Performing topological sort...")
    sorted_modules = topological_sort(modules)

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
