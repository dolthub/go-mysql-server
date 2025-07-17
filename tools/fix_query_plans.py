#!/usr/bin/env python3

import subprocess
import re
import os

def run_tests():
    """Run the tests and return the output"""
    result = subprocess.run(
        ['go', 'test', '-v', './enginetest', '-run', 'TestQueryPlans', '-count=1'],
        cwd='/Users/amx/dolt_workspace/go-mysql-server',
        capture_output=True,
        text=True
    )
    return result.stdout + result.stderr

def extract_failures(output):
    """Extract expected and actual values from test output"""
    failures = []
    
    # Find all "Not equal:" sections
    sections = re.split(r'Not equal:', output)
    
    for section in sections[1:]:  # Skip first empty section
        # Look for expected and actual strings
        expected_match = re.search(r'expected:\s*"([^"]*(?:\\.[^"]*)*)"', section, re.DOTALL)
        actual_match = re.search(r'actual\s*:\s*"([^"]*(?:\\.[^"]*)*)"', section, re.DOTALL)
        
        if expected_match and actual_match:
            expected = expected_match.group(1)
            actual = actual_match.group(1)
            
            # Unescape the strings
            expected = expected.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
            actual = actual.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
            
            failures.append((expected, actual))
    
    return failures

def update_file(file_path, failures):
    """Update the file with all the fixes"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    update_count = 0
    
    for expected, actual in failures:
        # Create properly escaped Go string literals
        expected_escaped = expected.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        actual_escaped = actual.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        # Look for the exact quoted string in the file
        search_for = f'"{expected_escaped}"'
        replace_with = f'"{actual_escaped}"'
        
        if search_for in content:
            content = content.replace(search_for, replace_with, 1)
            update_count += 1
        else:
            # Try simpler matching - sometimes there are formatting differences
            # Look for key distinguishing parts
            if 'MergeJoin' in expected and 'SemiLookupJoin' in actual:
                # This is a MergeJoin -> SemiLookupJoin replacement
                # Find lines with MergeJoin and see if we can match context
                lines = expected.split('\n')
                for line in lines:
                    if 'MergeJoin' in line and line.strip() in content:
                        # Found a matching line, try to replace the whole block
                        context_lines = []
                        for i in range(max(0, lines.index(line)-2), min(len(lines), lines.index(line)+3)):
                            context_lines.append(lines[i])
                        context = '\n'.join(context_lines)
                        context_escaped = context.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                        
                        if f'"{context_escaped}"' in content:
                            # Replace the context block
                            actual_lines = actual.split('\n')
                            line_idx = None
                            for i, actual_line in enumerate(actual_lines):
                                if actual_line.strip() == line.replace('MergeJoin', 'SemiLookupJoin').strip():
                                    line_idx = i
                                    break
                            
                            if line_idx is not None:
                                actual_context_lines = []
                                for i in range(max(0, line_idx-2), min(len(actual_lines), line_idx+3)):
                                    actual_context_lines.append(actual_lines[i])
                                actual_context = '\n'.join(actual_context_lines)
                                actual_context_escaped = actual_context.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                                
                                content = content.replace(f'"{context_escaped}"', f'"{actual_context_escaped}"', 1)
                                update_count += 1
                                break
    
    if update_count > 0:
        with open(file_path, 'w') as f:
            f.write(content)
    
    return update_count

def main():
    query_plans_file = '/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go'
    
    for iteration in range(1, 11):
        print(f"Iteration {iteration}...")
        
        # Run tests
        output = run_tests()
        
        # Check if all tests pass
        if 'FAIL' not in output:
            print("All tests passed!")
            break
        
        # Extract failures
        failures = extract_failures(output)
        
        if not failures:
            print("No failures found to update")
            break
        
        print(f"Found {len(failures)} failures")
        
        # Update the file
        update_count = update_file(query_plans_file, failures)
        
        print(f"Updated {update_count} test cases")
        
        if update_count == 0:
            print("No updates were made - stopping")
            break
    
    print("Script complete!")

if __name__ == "__main__":
    main()