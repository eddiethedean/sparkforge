#!/usr/bin/env python3
"""
Update module dependency documentation in SparkForge source files.

This script analyzes imports in all Python files and ensures the module docstring
contains an accurate "# Depends on:" section listing internal sparkforge dependencies.

Usage:
    python scripts/update_module_dependencies.py [--check-only] [--verbose]

Options:
    --check-only    Only check if dependencies are up to date, don't modify files
    --verbose       Show detailed output for each file
    
Exit codes:
    0 - All dependencies are up to date (or successfully updated)
    1 - Dependencies are out of date (in --check-only mode)
    2 - Error occurred during processing
"""

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import List, Set, Tuple


class ImportAnalyzer(ast.NodeVisitor):
    """Analyzes Python AST to find import statements."""
    
    def __init__(self):
        self.imports: List[Tuple[int, str, str]] = []  # (level, module, symbol)
        
    def visit_ImportFrom(self, node):
        module = node.module or ''
        level = node.level
        for alias in node.names:
            self.imports.append((level, module, alias.name))
        self.generic_visit(node)


def find_symbol_source(symbol: str, module_parts: List[str], sparkforge_root: Path) -> str:
    """
    Find where a symbol is actually defined by checking __init__.py for re-exports.
    
    Args:
        symbol: The symbol being imported
        module_parts: The module path parts
        sparkforge_root: Root path of sparkforge package
        
    Returns:
        Full module path where the symbol is actually defined
    """
    if not module_parts:
        return ''
    
    # First, check if the module itself is a direct file
    target_as_file = (
        sparkforge_root / '/'.join(module_parts[:-1]) / f'{module_parts[-1]}.py' 
        if len(module_parts) > 1 
        else sparkforge_root / f'{module_parts[0]}.py'
    )
    
    if target_as_file.exists():
        # Check if symbol is defined in this file
        try:
            with open(target_as_file, 'r') as f:
                content = f.read()
            if re.search(rf'^\s*(class|def)\s+{symbol}\b', content, re.MULTILINE):
                return '.'.join(module_parts)
            if re.search(rf'^{symbol}\s*=', content, re.MULTILINE):
                return '.'.join(module_parts)
        except:
            pass
    
    # Check if this is a package with __init__.py that re-exports
    module_dir = sparkforge_root / '/'.join(module_parts)
    init_file = module_dir / '__init__.py' if module_dir.is_dir() else None
    
    if init_file and init_file.exists():
        try:
            with open(init_file, 'r') as f:
                init_content = f.read()
            init_tree = ast.parse(init_content)
            
            # Look for re-exports of this symbol
            for init_node in init_tree.body:
                if isinstance(init_node, ast.ImportFrom):
                    for init_alias in init_node.names:
                        if init_alias.name == symbol:
                            # Found the re-export!
                            if init_node.module:
                                if init_node.level > 0:
                                    # Relative import in __init__.py (e.g., from .analyzer import ...)
                                    init_level = init_node.level
                                    init_module = init_node.module or ''
                                    # For __init__.py, level 1 means stay in current package
                                    init_base = module_parts[:-(init_level-1)] if init_level > 1 else module_parts
                                    if init_module:
                                        actual_parts = init_base + init_module.split('.')
                                    else:
                                        actual_parts = init_base
                                    return '.'.join(actual_parts)
                                else:
                                    # Absolute import from sparkforge
                                    if init_node.module.startswith('sparkforge.'):
                                        return init_node.module.replace('sparkforge.', '')
                                    # External import, skip
                                    return ''
        except:
            pass
    
    # Default: return the module path as-is
    return '.'.join(module_parts)


def analyze_file_dependencies(file_path: Path, sparkforge_root: Path) -> Set[str]:
    """
    Analyze a file and return its internal sparkforge dependencies.
    
    Args:
        file_path: Path to the Python file to analyze
        sparkforge_root: Root path of sparkforge package
        
    Returns:
        Set of module paths that this file depends on
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        tree = ast.parse(content)
        analyzer = ImportAnalyzer()
        analyzer.visit(tree)
        
        dependencies = set()
        
        # Get current module location
        rel_path = file_path.relative_to(sparkforge_root)
        current_parts = str(rel_path).replace('.py', '').replace('/__init__', '').split('/')
        
        for level, module, symbol in analyzer.imports:
            # Only process sparkforge internal imports (relative imports with level > 0)
            if level > 0:
                # Calculate base path by going up 'level' directories
                base_parts = current_parts[:-level] if level <= len(current_parts) else []
                
                if module:
                    # Relative import with module name
                    full_parts = base_parts + module.split('.')
                else:
                    # Just dots, import from parent package
                    full_parts = base_parts
                
                # Find where the symbol is actually defined
                actual_source = find_symbol_source(symbol, full_parts, sparkforge_root)
                if actual_source:
                    dependencies.add(actual_source)
        
        return dependencies
        
    except Exception as e:
        print(f"  ⚠️  Error analyzing {file_path}: {e}", file=sys.stderr)
        return set()


def extract_current_dependencies(file_path: Path) -> List[str]:
    """Extract current dependencies from a file's docstring."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Find dependencies section
        match = re.search(r'# Depends on:\n((?:#   .+\n)*)', content)
        if match:
            deps = []
            for line in match.group(1).strip().split('\n'):
                line = line.strip()
                if line.startswith('#   '):
                    deps.append(line.replace('#   ', ''))
            return deps
        return []
    except:
        return []


def update_module_docstring(file_path: Path, dependencies: List[str], check_only: bool = False) -> Tuple[bool, str]:
    """
    Update a file's module docstring with dependency information.
    
    Args:
        file_path: Path to the file to update
        dependencies: List of dependencies to add
        check_only: If True, only check if update is needed, don't modify
        
    Returns:
        Tuple of (needs_update, status_message)
    """
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Find module-level docstring
        docstring_start = -1
        docstring_end = -1
        in_docstring = False
        quote_type = None
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Skip shebang, encoding, copyright comments
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
            
            # Found docstring start
            if not in_docstring:
                if '"""' in stripped or "'''" in stripped:
                    quote_type = '"""' if '"""' in stripped else "'''"
                    docstring_start = i
                    
                    # Check if single-line docstring
                    if stripped.count(quote_type) >= 2:
                        docstring_end = i
                        break
                    else:
                        in_docstring = True
            elif in_docstring and quote_type in line:
                docstring_end = i
                break
        
        if docstring_start == -1:
            return (False, "No module docstring found")
        
        # Check if dependencies already exist
        dep_start = -1
        dep_end = -1
        for i in range(docstring_start, docstring_end + 1):
            if '# Depends on:' in lines[i]:
                dep_start = i
                # Find end of dependency block
                for j in range(i + 1, docstring_end + 1):
                    if lines[j].strip().startswith('#   '):
                        dep_end = j
                    else:
                        break
                if dep_end == -1:
                    dep_end = dep_start
                break
        
        # Build new dependency lines
        if dependencies:
            indent = len(lines[docstring_end]) - len(lines[docstring_end].lstrip())
            indent_str = ' ' * indent
            
            new_dep_lines = [f'{indent_str}# Depends on:\n']
            for dep in sorted(dependencies):
                new_dep_lines.append(f'{indent_str}#   {dep}\n')
        else:
            new_dep_lines = []
        
        # Check if update is needed
        current_deps = extract_current_dependencies(file_path)
        needs_update = sorted(current_deps) != sorted(dependencies)
        
        if not needs_update:
            return (False, "Dependencies already up to date")
        
        if check_only:
            return (True, f"Needs update: {sorted(dependencies)} vs {sorted(current_deps)}")
        
        # Update file
        if dep_start != -1:
            # Replace existing dependencies
            new_lines = lines[:dep_start] + new_dep_lines + lines[dep_end + 1:]
        elif new_dep_lines:
            # Add new dependency section
            if docstring_start == docstring_end:
                # Single-line docstring - convert to multi-line
                doc_line = lines[docstring_start]
                indent = len(doc_line) - len(doc_line.lstrip())
                indent_str = ' ' * indent
                
                # Extract content
                doc_content = doc_line.strip()
                if doc_content.startswith('"""'):
                    content = doc_content[3:-3] if doc_content.endswith('"""') else doc_content[3:]
                    quote = '"""'
                else:
                    content = doc_content[3:-3] if doc_content.endswith("'''") else doc_content[3:]
                    quote = "'''"
                
                multi_line_doc = [
                    f'{indent_str}{quote}\n',
                    f'{indent_str}{content}\n',
                    '\n'
                ] + new_dep_lines + [f'{indent_str}{quote}\n']
                
                new_lines = lines[:docstring_start] + multi_line_doc + lines[docstring_start + 1:]
            else:
                # Multi-line docstring - insert before closing quote
                insert_pos = docstring_end
                if lines[docstring_end - 1].strip() and '"""' not in lines[docstring_end - 1]:
                    new_dep_lines = ['\n'] + new_dep_lines
                
                new_lines = lines[:insert_pos] + new_dep_lines + lines[insert_pos:]
        else:
            new_lines = lines
        
        # Write back
        with open(file_path, 'w') as f:
            f.writelines(new_lines)
        
        return (True, "Updated")
        
    except Exception as e:
        return (False, f"Error: {e}")


def main():
    """Main function to process all files."""
    parser = argparse.ArgumentParser(
        description='Update module dependency documentation in SparkForge files'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Only check if dependencies are up to date, don\'t modify files'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output for each file'
    )
    
    args = parser.parse_args()
    
    sparkforge_root = Path('sparkforge')
    
    if not sparkforge_root.exists():
        print("Error: sparkforge/ directory not found. Run this script from the project root.", file=sys.stderr)
        return 2
    
    # Find all Python files (excluding __init__.py files)
    all_files = []
    for py_file in sparkforge_root.rglob('*.py'):
        if '__pycache__' in str(py_file):
            continue
        if py_file.name == '__init__.py':
            continue  # Skip __init__.py files as per user request
        all_files.append(py_file)
    
    print(f"🔍 Analyzing {len(all_files)} Python files (excluding __init__.py)...\n")
    
    # Process each file
    files_updated = 0
    files_skipped = 0
    files_with_deps = 0
    files_without_deps = 0
    files_needing_update = []
    
    for file_path in sorted(all_files):
        deps = analyze_file_dependencies(file_path, sparkforge_root)
        
        if deps:
            files_with_deps += 1
        else:
            files_without_deps += 1
        
        needs_update, status = update_module_docstring(file_path, sorted(deps), check_only=args.check_only)
        
        if needs_update:
            files_needing_update.append(file_path)
            if not args.check_only:
                files_updated += 1
            
            if args.verbose or args.check_only:
                try:
                    rel_path = file_path.relative_to(Path.cwd())
                except ValueError:
                    rel_path = file_path
                print(f"{'⚠️ ' if args.check_only else '✓'} {rel_path}")
                if deps and args.verbose:
                    for dep in sorted(deps):
                        print(f"    {dep}")
                if args.check_only:
                    print(f"    Status: {status}")
        else:
            files_skipped += 1
            if args.verbose:
                try:
                    rel_path = file_path.relative_to(Path.cwd())
                except ValueError:
                    rel_path = file_path
                print(f"  {rel_path} - {status}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Total files analyzed: {len(all_files)}")
    print(f"  Files with internal dependencies: {files_with_deps}")
    print(f"  Files without internal dependencies: {files_without_deps}")
    
    if args.check_only:
        if files_needing_update:
            print(f"\n⚠️  {len(files_needing_update)} file(s) need dependency updates:")
            for f in files_needing_update:
                try:
                    rel_path = f.relative_to(Path.cwd())
                except ValueError:
                    rel_path = f
                print(f"    - {rel_path}")
            print(f"\nRun without --check-only to update dependencies.")
            return 1
        else:
            print(f"\n✓ All dependencies are up to date!")
            return 0
    else:
        print(f"  Files updated: {files_updated}")
        print(f"  Files already up to date: {files_skipped}")
        
        if files_updated > 0:
            print(f"\n✓ Successfully updated {files_updated} file(s)")
        else:
            print(f"\n✓ All dependencies already up to date!")
        return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)

