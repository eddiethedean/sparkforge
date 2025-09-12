#!/usr/bin/env python3
"""
Fix hardcoded 'test_schema' references in test files to use worker-specific schema.
"""

import os
import re
import glob

def fix_schema_references(file_path):
    """Fix hardcoded test_schema references in a file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Replace hardcoded test_schema references
    # Pattern 1: test_schema. (with dot) - most common case
    content = re.sub(r'test_schema\.', 'get_test_schema() + ".', content)
    
    # Pattern 2: test_schema (standalone in Python code, not in strings)
    # Only replace if it's not part of a function call, variable name, or in quotes
    content = re.sub(r'(?<!["\'])\btest_schema\b(?!\s*[\(\.]["\'])', 'get_test_schema()', content)
    
    # Add import if needed
    if 'get_test_schema()' in content and 'from tests.conftest import get_test_schema' not in content:
        # Find the first import line
        lines = content.split('\n')
        import_line = 'from tests.conftest import get_test_schema'
        
        # Find where to insert the import
        insert_idx = 0
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                insert_idx = i + 1
            elif line.strip() == '' and insert_idx > 0:
                break
        
        lines.insert(insert_idx, import_line)
        content = '\n'.join(lines)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    """Fix all test files."""
    test_files = glob.glob('tests/test_*.py')
    fixed_count = 0
    
    for file_path in test_files:
        if fix_schema_references(file_path):
            print(f"Fixed: {file_path}")
            fixed_count += 1
    
    print(f"\nFixed {fixed_count} files")

if __name__ == "__main__":
    main()
