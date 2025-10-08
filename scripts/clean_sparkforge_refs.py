#!/usr/bin/env python3
"""
Clean sparkforge references from docstrings in source code.

This script removes or replaces references to "sparkforge" in all docstrings
while preserving the technical meaning and clarity of the documentation.
"""

import re
from pathlib import Path
from typing import Tuple


def clean_sparkforge_from_text(text: str) -> Tuple[str, int]:
    """
    Remove sparkforge references from text.
    
    Returns:
        Tuple of (cleaned_text, number_of_replacements)
    """
    original = text
    replacements = 0
    
    # Replace common patterns
    patterns = [
        (r'SparkForge\s+PipelineBuilder', 'PipelineBuilder'),
        (r'for\s+SparkForge', 'for the framework'),
        (r'SparkForge\s+pipelines', 'pipelines'),
        (r'SparkForge\s+pipeline', 'pipeline'),
        (r'the\s+SparkForge\s+framework', 'the framework'),
        (r'SparkForge\s+framework', 'the framework'),
        (r'SparkForge\s+models', 'framework models'),
        (r'SparkForge\s+model', 'framework model'),
        (r'SparkForge\s+errors', 'framework errors'),
        (r'SparkForge\s+error', 'framework error'),
        (r'all\s+SparkForge\s+errors', 'all framework errors'),
        (r'all\s+other\s+SparkForge\s+exceptions', 'all other framework exceptions'),
        (r'SparkForge\s+exceptions', 'framework exceptions'),
        (r'SparkForge\s+exception', 'framework exception'),
        (r'SparkForge\s+integration', 'framework integration'),
        (r'with\s+SparkForge', 'with the framework'),
        (r'from\s+sparkforge', 'from the framework'),
        (r'in\s+SparkForge', 'in the framework'),
        (r'SparkForge\s+codebase', 'the codebase'),
        (r'SparkForge\s+ecosystem', 'the ecosystem'),
        (r'SparkForge\s+components', 'framework components'),
        (r'SparkForge\s+validation', 'validation'),
        (r'existing\s+SparkForge', 'existing framework'),
        
        # Module and titles
        (r'SparkForge\s+Writer\s+Module', 'Writer Module'),
        
        # Standalone references
        (r'"SparkForge\s+Team"', '"Framework Team"'),
        (r"'SparkForge\s+Team'", "'Framework Team'"),
        (r'"SparkForge"', '"PipelineFramework"'),
        (r"'SparkForge'", "'PipelineFramework'"),
        (r'name\s*=\s*"SparkForge"', 'name="PipelineFramework"'),
        (r"name\s*=\s*'SparkForge'", "name='PipelineFramework'"),
        (r'__author__\s*=\s*"SparkForge\s+Team"', '__author__ = "Framework Team"'),
        (r"__author__\s*=\s*'SparkForge\s+Team'", "__author__ = 'Framework Team'"),
        
        # Common phrases
        (r'SparkForge\s+-', 'The framework -'),
        (r'SparkForge\s+provides', 'This framework provides'),
        (r'SparkForge\s+transforms', 'This framework transforms'),
        (r'SparkForge\.', 'The framework.'),
    ]
    
    for pattern, replacement in patterns:
        new_text = re.sub(pattern, replacement, text)
        if new_text != text:
            replacements += len(re.findall(pattern, text))
            text = new_text
    
    return text, replacements


def process_file(file_path: Path, check_only: bool = False) -> Tuple[bool, int]:
    """
    Process a single file to clean sparkforge references.
    
    Returns:
        Tuple of (was_modified, num_replacements)
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Clean the content
        cleaned_content, num_replacements = clean_sparkforge_from_text(content)
        
        if num_replacements > 0:
            if not check_only:
                with open(file_path, 'w') as f:
                    f.write(cleaned_content)
            return True, num_replacements
        
        return False, 0
        
    except Exception as e:
        print(f"  ⚠️  Error processing {file_path}: {e}")
        return False, 0


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean sparkforge references from docstrings')
    parser.add_argument('--check-only', action='store_true', help='Only check, don\'t modify')
    parser.add_argument('--verbose', action='store_true', help='Show details for each file')
    args = parser.parse_args()
    
    sparkforge_root = Path('sparkforge')
    
    if not sparkforge_root.exists():
        print("❌ Error: sparkforge/ directory not found")
        return 2
    
    # Find all Python files
    all_files = []
    for py_file in sparkforge_root.rglob('*.py'):
        if '__pycache__' not in str(py_file):
            all_files.append(py_file)
    
    print(f"🔍 Analyzing {len(all_files)} Python files...\n")
    
    # Process each file
    files_modified = 0
    total_replacements = 0
    modified_files = []
    
    for file_path in sorted(all_files):
        was_modified, num_replacements = process_file(file_path, args.check_only)
        
        if was_modified:
            files_modified += 1
            total_replacements += num_replacements
            modified_files.append(file_path)
            
            if args.verbose or args.check_only:
                rel_path = file_path.relative_to(Path.cwd()) if Path.cwd() in file_path.parents else file_path
                status = "⚠️  Would modify" if args.check_only else "✓ Modified"
                print(f"{status} {rel_path} ({num_replacements} replacements)")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Total files analyzed: {len(all_files)}")
    print(f"  Files with sparkforge references: {files_modified}")
    print(f"  Total replacements: {total_replacements}")
    
    if args.check_only:
        if files_modified > 0:
            print(f"\n⚠️  {files_modified} file(s) would be modified")
            print(f"Run without --check-only to apply changes")
            return 1
        else:
            print(f"\n✅ No sparkforge references found!")
            return 0
    else:
        if files_modified > 0:
            print(f"\n✅ Successfully cleaned {files_modified} file(s)!")
            print(f"   Removed {total_replacements} sparkforge references")
        else:
            print(f"\n✅ All files already clean!")
        return 0


if __name__ == '__main__':
    import sys
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(2)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)

