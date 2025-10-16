#!/usr/bin/env python3
"""
Fix Python 3.8 compatibility by replacing dict[...] with Dict[...] from typing.
"""
import re
from pathlib import Path
from typing import List, Tuple

def needs_dict_import(content: str) -> bool:
    """Check if file needs Dict import from typing."""
    # Check if Dict is already imported
    if "from typing import" in content and "Dict" in content:
        return False
    # Check if dict[...] is used
    return "dict[" in content

def add_dict_import(content: str) -> str:
    """Add Dict to typing imports."""
    lines = content.split('\n')
    new_lines = []
    import_added = False
    
    for i, line in enumerate(lines):
        # Check if this is a typing import line
        if line.startswith("from typing import"):
            # Add Dict if not already there
            if "Dict" not in line:
                # Replace the line to add Dict
                if line.endswith(","):
                    new_lines.append(line + " Dict")
                else:
                    new_lines.append(line.replace("from typing import", "from typing import Dict,"))
                import_added = True
            else:
                new_lines.append(line)
        elif line.startswith("import typing") and not import_added:
            # If using import typing, add Dict import after it
            new_lines.append(line)
            new_lines.append("from typing import Dict")
            import_added = True
        else:
            new_lines.append(line)
    
    return '\n'.join(new_lines)

def fix_dict_annotations(content: str) -> Tuple[str, int]:
    """
    Replace dict[...] with Dict[...] in type annotations.
    Returns (fixed_content, number_of_replacements)
    """
    replacements = 0
    
    # Pattern to match dict[type, type] in various contexts
    # This is a simplified version - we'll handle most common cases
    patterns = [
        # Simple dict[K, V]
        (r'\bdict\[([^\]]+)\]', r'Dict[\1]'),
    ]
    
    for pattern, replacement in patterns:
        matches = re.findall(pattern, content)
        if matches:
            content = re.sub(pattern, replacement, content)
            replacements += len(matches)
    
    return content, replacements

def fix_file(file_path: Path) -> Tuple[bool, int]:
    """Fix a single Python file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Check if file needs fixing
        if "dict[" not in content:
            return False, 0
        
        # Add Dict import if needed
        if needs_dict_import(content):
            content = add_dict_import(content)
        
        # Fix dict[...] annotations
        content, replacements = fix_dict_annotations(content)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, replacements
        
        return False, 0
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False, 0

def main():
    """Main function to fix all Python files."""
    sparkforge_dir = Path("sparkforge")
    
    if not sparkforge_dir.exists():
        print("Error: sparkforge directory not found")
        return
    
    # Find all Python files
    python_files = list(sparkforge_dir.rglob("*.py"))
    
    print(f"Found {len(python_files)} Python files in sparkforge/")
    
    total_fixed = 0
    total_replacements = 0
    
    for file_path in python_files:
        fixed, replacements = fix_file(file_path)
        if fixed:
            total_fixed += 1
            total_replacements += replacements
            print(f"✓ Fixed {file_path}: {replacements} replacements")
    
    print(f"\nSummary:")
    print(f"  Files fixed: {total_fixed}")
    print(f"  Total replacements: {total_replacements}")

if __name__ == "__main__":
    main()

