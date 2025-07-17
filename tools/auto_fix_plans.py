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
    """Update the file by replacing expected with actual"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    update_count = 0
    
    for i, (expected, actual) in enumerate(failures):
        print(f"Processing failure {i+1}/{len(failures)}...")
        
        # Create properly escaped Go string literals  
        expected_escaped = expected.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        actual_escaped = actual.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        # Look for the exact quoted string in the file
        search_for = f'"{expected_escaped}"'
        replace_with = f'"{actual_escaped}"'
        
        # Count how many times this pattern appears
        count = content.count(search_for)
        if count > 0:
            content = content.replace(search_for, replace_with)
            update_count += count
            print(f"  ✓ Replaced {count} occurrence(s)")
        else:
            # Try to find a unique substring to help debug
            lines = expected.split('\n')
            for line in lines[:3]:  # Check first few lines
                line = line.strip()
                if line and len(line) > 15:  # Look for substantial lines
                    line_escaped = line.replace('\\', '\\\\').replace('"', '\\"')
                    if line_escaped in content:
                        print(f"  Found line '{line[:50]}...' in file but couldn't match full string")
                        break
            else:
                print(f"  ✗ Could not find any part of expected string in file")
                # Print first few lines for debugging
                exp_lines = expected.split('\n')[:3]
                print(f"    Expected starts with: {exp_lines}")
    
    if update_count > 0:
        with open(file_path, 'w') as f:
            f.write(content)
    
    return update_count

def main():
    query_plans_file = '/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go'
    
    print("Running tests to get failures...")
    output = run_tests()
    
    print("Extracting failures...")
    failures = extract_failures(output)
    
    if not failures:
        print("No failures found to update")
        return
    
    print(f"Found {len(failures)} test failures")
    
    # Update the file
    print(f"Updating {query_plans_file}...")
    update_count = update_file(query_plans_file, failures)
    
    print(f"\nMade {update_count} total replacements")
    
    # Run tests again to check improvement
    print("\nRunning tests again to check progress...")
    output2 = run_tests()
    
    if 'FAIL' not in output2:
        print("✓ All tests now pass!")
    else:
        failures2 = extract_failures(output2)
        improvement = len(failures) - len(failures2)
        print(f"Failures: {len(failures)} → {len(failures2)} (improved by {improvement})")

if __name__ == "__main__":
    main()