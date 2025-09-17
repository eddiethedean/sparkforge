#!/usr/bin/env python3
"""
Script to fix StepError constructor calls in pipeline/builder.py
"""

import re

def fix_step_error_calls(content):
    """Fix StepError constructor calls to use the simplified error system."""
    
    # Pattern to match StepError calls with step_name and step_type parameters
    pattern = r'StepError\(\s*([^,]+),\s*step_name=([^,]+),\s*step_type=([^,]+),'
    
    def replace_func(match):
        message = match.group(1)
        step_name = match.group(2).strip()
        step_type = match.group(3).strip()
        
        return f'StepError(\n                    {message},\n                    context={{"step_name": {step_name}, "step_type": {step_type}}},'
    
    # Apply the replacement
    content = re.sub(pattern, replace_func, content, flags=re.MULTILINE | re.DOTALL)
    
    return content

def main():
    # Read the file
    with open('sparkforge/pipeline/builder.py', 'r') as f:
        content = f.read()
    
    # Fix the StepError calls
    fixed_content = fix_step_error_calls(content)
    
    # Write back to file
    with open('sparkforge/pipeline/builder.py', 'w') as f:
        f.write(fixed_content)
    
    print("✅ Fixed StepError constructor calls in pipeline/builder.py")

if __name__ == "__main__":
    main()
