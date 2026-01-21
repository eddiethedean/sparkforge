#!/usr/bin/env python3
"""
Quick validation script to ensure guide examples are syntactically correct.

This doesn't run the examples (that's done in test_stepwise_guide_examples.py),
but it verifies the Python code blocks can be parsed.
"""

import ast
import re
import sys

def extract_python_blocks(content):
    """Extract Python code blocks from markdown."""
    pattern = r'```python\n(.*?)```'
    matches = re.findall(pattern, content, re.DOTALL)
    return matches

def validate_syntax(code_block):
    """Validate Python syntax."""
    try:
        ast.parse(code_block)
        return True, None
    except SyntaxError as e:
        return False, str(e)

def main():
    guide_path = "docs/USER_GUIDE.md"
    
    with open(guide_path, "r") as f:
        content = f.read()
    
    # Extract stepwise execution section
    start_marker = "## Stepwise Execution and Debugging"
    end_marker = "## Validation Rules"
    
    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        print("Could not find stepwise execution section")
        return 1
    
    section = content[start_idx:end_idx]
    code_blocks = extract_python_blocks(section)
    
    print(f"Found {len(code_blocks)} Python code blocks in stepwise execution section")
    
    errors = []
    for i, block in enumerate(code_blocks, 1):
        # Skip blocks that are just comments or incomplete
        if len(block.strip()) < 10:
            continue
        
        # Remove output comments (lines starting with # Output:)
        cleaned = "\n".join(
            line for line in block.split("\n")
            if not line.strip().startswith("# Output:")
        )
        
        valid, error = validate_syntax(cleaned)
        if not valid:
            errors.append((i, error))
            print(f"  Block {i}: Syntax error - {error}")
    
    if errors:
        print(f"\nFound {len(errors)} syntax errors")
        return 1
    else:
        print("All code blocks are syntactically valid!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
