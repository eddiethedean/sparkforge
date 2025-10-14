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
    1. Extracts version from sparkforge/__init__.py
    2. Reads dependency information from module docstrings
    3. Performs topological sort to order modules by dependencies
    4. Creates notebook cells with module code
    5. Removes all relative sparkforge imports (comments them out)
    6. Generates a runnable Jupyter notebook

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
from typing import Dict, List


def extract_version() -> str:
    """Extract version from sparkforge/__init__.py"""
    init_file = Path('sparkforge/__init__.py')
    with open(init_file) as f:
        content = f.read()
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if match:
        return match.group(1)
    return '0.9.0'


def extract_dependencies(file_path: Path) -> List[str]:
    """Extract dependencies from a module's docstring."""
    try:
        with open(file_path) as f:
            content = f.read()

        match = re.search(r'# Depends on:\n((?:#   .+\n)*)', content)
        if match:
            deps = []
            for line in match.group(1).strip().split('\n'):
                line = line.strip()
                if line.startswith('#   '):
                    deps.append(line.replace('#   ', ''))
            return deps
        return []
    except Exception:
        return []


def get_module_info(sparkforge_root: Path) -> Dict[str, Dict]:
    """
    Get information about all modules including their dependencies.

    Returns:
        Dict mapping module_path to {file_path, dependencies, code}
    """
    modules = {}

    for py_file in sparkforge_root.rglob('*.py'):
        if '__pycache__' in str(py_file):
            continue
        if py_file.name == '__init__.py':
            continue

        # Get module path
        rel_path = py_file.relative_to(sparkforge_root)
        module_path = str(rel_path).replace('.py', '').replace('/', '.')

        # Read file content
        with open(py_file) as f:
            code = f.read()

        # Extract dependencies
        deps = extract_dependencies(py_file)

        modules[module_path] = {
            'file_path': py_file,
            'dependencies': deps,
            'code': code,
            'name': py_file.stem
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
        for dep in info['dependencies']:
            if dep in modules:
                adjacency[dep].append(module)
                in_degree[module] += 1
                edges.append((dep, module))

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
            remaining,
            key=lambda m: (len(modules[m]['dependencies']), m)
        )

        if remaining_sorted:
            print(f"⚠️  Note: {len(remaining_sorted)} module(s) with circular dependencies added at end", file=sys.stderr)
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
    lines = code.split('\n')
    new_lines = []
    in_multiline_import = False

    for line in lines:
        # Check for any relative imports (starting with one or more dots)
        if re.match(r'^\s*from\s+\.', line):
            # Comment out the import
            indent = len(line) - len(line.lstrip())
            comment = '# ' + line.strip() + '  # Removed: defined in notebook cells above'
            new_lines.append(' ' * indent + comment)

            # Check if this starts a multi-line import
            if '(' in line and ')' not in line:
                in_multiline_import = True
        elif in_multiline_import:
            # Comment out continuation lines of multi-line imports
            indent = len(line) - len(line.lstrip())
            comment = '# ' + line.strip()
            new_lines.append(' ' * indent + comment)

            # Check if this line ends the multi-line import
            if ')' in line:
                in_multiline_import = False
        else:
            new_lines.append(line)

    code = '\n'.join(new_lines)

    # Remove mock_spark conditional imports - replace with direct pyspark imports
    # Pattern: if os.environ.get("SPARK_MODE"...) block with from mock_spark import
    code = re.sub(
        r'# Use mock functions when in mock mode\n' +
        r'if os\.environ\.get\("SPARK_MODE"[^)]+\)[^:]+:\n' +
        r'    from mock_spark import functions as F\n' +
        r'else:\n' +
        r'    from pyspark\.sql import functions as F',
        'from pyspark.sql import functions as F',
        code,
        flags=re.MULTILINE
    )

    # Remove any remaining mock_spark references
    code = re.sub(r'from mock_spark import .*\n', '', code)
    code = re.sub(r'import mock_spark.*\n', '', code)

    # Remove references to "sparkforge" in various contexts
    # Replace in import statements
    code = re.sub(r'from sparkforge\.', 'from .', code)
    code = re.sub(r'import sparkforge', '# import sparkforge  # Not needed in notebook', code)

    # Replace in docstrings and comments (but preserve technical meaning)
    code = re.sub(r'SparkForge\s+PipelineBuilder', 'PipelineBuilder', code)
    code = re.sub(r'for\s+SparkForge', 'for the framework', code)
    code = re.sub(r'SparkForge\s+pipelines', 'pipelines', code)
    code = re.sub(r'SparkForge\s+models', 'framework models', code)
    code = re.sub(r'SparkForge\s+errors', 'framework errors', code)
    code = re.sub(r'SparkForge\s+error', 'framework error', code)
    code = re.sub(r'all\s+SparkForge\s+errors', 'all framework errors', code)
    code = re.sub(r'all\s+other\s+SparkForge\s+exceptions', 'all other framework exceptions', code)
    code = re.sub(r'SparkForge\s+exceptions', 'framework exceptions', code)
    code = re.sub(r'from\s+sparkforge', 'from the framework', code)
    code = re.sub(r'SparkForge\s+integration', 'framework integration', code)
    code = re.sub(r'with\s+SparkForge', 'with the framework', code)

    # Replace standalone "SparkForge" references
    code = re.sub(r'name\s*=\s*["\']SparkForge["\']', 'name="PipelineFramework"', code)
    code = re.sub(r'"SparkForge"', '"PipelineFramework"', code)
    code = re.sub(r"'SparkForge'", "'PipelineFramework'", code)

    return code


def extract_code_without_docstring(code: str) -> str:
    """Extract code without the module docstring."""
    lines = code.split('\n')

    # Find and skip module docstring
    docstring_start = -1
    docstring_end = -1
    in_docstring = False
    quote_type = None

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip shebang, encoding, comments at start
        if stripped.startswith('#'):
            continue
        if not stripped:
            continue

        # Skip imports before docstring
        if stripped.startswith('from ') or stripped.startswith('import '):
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
        result_lines = lines[:docstring_start] + lines[docstring_end + 1:]
    else:
        result_lines = lines

    # Remove leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()

    return '\n'.join(result_lines)


def create_notebook_cell(code: str, cell_type: str = 'code', metadata: Dict = None) -> Dict:
    """Create a Jupyter notebook cell."""
    if metadata is None:
        metadata = {}

    # Format source with proper newlines for Jupyter
    # Each line should end with \n except the last line
    if code:
        lines = code.split('\n')
        source = [line + '\n' for line in lines[:-1]]
        if lines[-1]:  # Add last line without \n if it's not empty
            source.append(lines[-1])
    else:
        source = []

    if cell_type == 'code':
        return {
            'cell_type': 'code',
            'execution_count': None,
            'metadata': metadata,
            'outputs': [],
            'source': source
        }
    else:  # markdown
        return {
            'cell_type': 'markdown',
            'metadata': metadata,
            'source': source
        }


def generate_notebook(modules: Dict[str, Dict], sorted_modules: List[str], version: str) -> Dict:
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

    # Title cell
    title = f"""# PipelineBuilder v{version} - Standalone Notebook

This notebook contains the complete PipelineBuilder implementation
as a standalone, executable notebook. All dependencies are included as cells
in the correct order.

**Usage:**
1. Run all cells from top to bottom
2. The `PipelineBuilder` class will be available after all cells execute
3. Use it to build and execute data pipelines

**Note:** This is generated from version {version}. Module dependencies are
resolved automatically from source code analysis."""

    cells.append(create_notebook_cell(title, 'markdown'))

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
from typing import Any, Callable, Dict, Generator, List, Optional, Protocol, Set, Tuple, TypeVar, Union, cast

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

    cells.append(create_notebook_cell(imports_code, 'code'))

    # Add module cells in dependency order
    for module_path in sorted_modules:
        info = modules[module_path]

        # Create markdown header for the module
        module_header = f"## Module: {module_path}\n\n"
        if info['dependencies']:
            module_header += f"**Dependencies:** {', '.join(sorted(info['dependencies']))}"
        else:
            module_header += "**Dependencies:** None (base module)"

        cells.append(create_notebook_cell(module_header, 'markdown'))

        # Extract code without docstring and remove sparkforge imports
        code = extract_code_without_docstring(info['code'])
        code = remove_sparkforge_imports(code)

        # Clean up the code
        code = code.strip()

        if code:
            cells.append(create_notebook_cell(code, 'code'))

    # Add usage example cell
    usage_example = """## Usage Example

Here's how to use the PipelineBuilder:"""
    cells.append(create_notebook_cell(usage_example, 'markdown'))

    example_code = """# Example: Create a simple pipeline
from pyspark.sql import SparkSession, functions as F

# Initialize Spark (if not already available)
spark = SparkSession.builder \\
    .appName("PipelineBuilder Example") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

# Create sample data
data = [
    ("user1", "click", 100, "2024-01-01"),
    ("user2", "purchase", 200, "2024-01-02"),
    ("user3", "view", 50, "2024-01-03")
]
df = spark.createDataFrame(data, ["user_id", "action", "value", "date"])

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze layer
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    incremental_col="date"
)

# Silver layer
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("value") > 0),
    rules={"value": [F.col("value") > 0]},
    table_name="clean_events"
)

# Gold layer
builder.add_gold_transform(
    name="daily_metrics",
    transform=lambda spark, silvers: silvers["clean_events"].groupBy("action").agg(F.count("*").alias("count")),
    rules={"count": [F.col("count") > 0]},
    table_name="daily_metrics",
    source_silvers=["clean_events"]
)

# Execute pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})
print(f"✅ Pipeline completed: {result.status}")"""

    cells.append(create_notebook_cell(example_code, 'code'))

    # Create notebook structure
    notebook = {
        'cells': cells,
        'metadata': {
            'kernelspec': {
                'display_name': 'Python 3',
                'language': 'python',
                'name': 'python3'
            },
            'language_info': {
                'name': 'python',
                'version': '3.8.0',
                'mimetype': 'text/x-python',
                'codemirror_mode': {
                    'name': 'ipython',
                    'version': 3
                },
                'pygments_lexer': 'ipython3',
                'nbconvert_exporter': 'python',
                'file_extension': '.py'
            }
        },
        'nbformat': 4,
        'nbformat_minor': 4
    }

    return notebook


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Generate Jupyter notebook version of SparkForge PipelineBuilder'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output path for the notebook (default: notebooks/pipeline_builder_v{VERSION}.ipynb)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output'
    )

    args = parser.parse_args()

    # Get version
    version = extract_version()
    print(f"📦 SparkForge version: {version}")

    # Set output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(f'notebooks/pipeline_builder_v{version}.ipynb')

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"📝 Output: {output_path}")
    print("🔍 Analyzing modules...\n")

    # Get all module information
    sparkforge_root = Path('sparkforge')
    modules = get_module_info(sparkforge_root)

    print(f"Found {len(modules)} modules")

    # Perform topological sort
    if args.verbose:
        print("\n🔄 Performing topological sort...")
    sorted_modules = topological_sort(modules)

    if args.verbose:
        print(f"\nModule order ({len(sorted_modules)} modules):")
        for i, module in enumerate(sorted_modules, 1):
            deps = modules[module]['dependencies']
            deps_str = f" (depends on: {', '.join(deps)})" if deps else " (no dependencies)"
            print(f"  {i:2d}. {module}{deps_str}")

    # Generate notebook
    print("\n📓 Generating notebook...")
    notebook = generate_notebook(modules, sorted_modules, version)

    # Write notebook
    with open(output_path, 'w') as f:
        json.dump(notebook, f, indent=2)

    print(f"\n✅ Successfully generated notebook: {output_path}")
    print(f"   - Total cells: {len(notebook['cells'])}")
    print(f"   - Modules included: {len(sorted_modules)}")
    print("   - Ready to run from top to bottom!")

    return 0


if __name__ == '__main__':
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

