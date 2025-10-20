#!/usr/bin/env python3
"""
Add Dict imports to files that use Dict but don't have it imported.
"""
from pathlib import Path


def add_dict_import(content: str) -> str:
    """Add Dict to typing imports if not already present."""
    lines = content.split('\n')
    new_lines = []
    import_added = False
    typing_import_found = False

    for i, line in enumerate(lines):
        # Check if this is a typing import line
        if line.startswith("from typing import"):
            typing_import_found = True
            # Add Dict if not already there
            if "Dict" not in line:
                # Check if it ends with comma or not
                if line.endswith(","):
                    new_lines.append(line + " Dict")
                elif "," in line:
                    new_lines.append(line + ", Dict")
                else:
                    # Single import, add Dict
                    new_lines.append(line.replace("from typing import", "from typing import Dict,"))
                import_added = True
            else:
                new_lines.append(line)
        elif line.startswith("import typing") and not typing_import_found:
            # If using import typing, add Dict import after it
            new_lines.append(line)
            if not import_added:
                new_lines.append("from typing import Dict")
                import_added = True
        else:
            new_lines.append(line)

    return '\n'.join(new_lines)

def fix_file(file_path: Path) -> bool:
    """Fix a single Python file."""
    try:
        with open(file_path, encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Check if file uses Dict but doesn't import it
        if "Dict[" in content and "from typing import" in content and "Dict" not in content.split("from typing import")[1].split("\n")[0]:
            content = add_dict_import(content)

        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

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

    for file_path in python_files:
        fixed = fix_file(file_path)
        if fixed:
            total_fixed += 1
            print(f"✓ Fixed imports in {file_path}")

    print("\nSummary:")
    print(f"  Files fixed: {total_fixed}")

if __name__ == "__main__":
    main()

