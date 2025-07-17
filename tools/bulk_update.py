#!/usr/bin/env python3

import subprocess
import re
import os
import sys

def run_tests():
    """Run the tests and return the output"""
    result = subprocess.run(
        ['go', 'test', '-v', './enginetest', '-run', 'TestQueryPlans', '-count=1'],
        cwd='/Users/amx/dolt_workspace/go-mysql-server',
        capture_output=True,
        text=True
    )
    return result.stdout + result.stderr

def extract_test_failures(output):
    """Extract all test failures from the output"""
    failures = []
    
    # Split by test failure markers
    sections = output.split('Not equal:')
    
    for section in sections[1:]:  # Skip first section (before any failures)
        # Find expected and actual values
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
    """Update the file with new expected values"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    updated_content = content
    update_count = 0
    
    for expected, actual in failures:
        # Create the Go string literals
        expected_go = expected.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        actual_go = actual.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        # Look for the expected string in quotes
        search_pattern = f'"{expected_go}"'
        replacement = f'"{actual_go}"'
        
        if search_pattern in updated_content:
            updated_content = updated_content.replace(search_pattern, replacement, 1)
            update_count += 1
    
    if update_count > 0:
        with open(file_path, 'w') as f:
            f.write(updated_content)
    
    return update_count

def main():
    query_plans_file = '/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go'
    
    for iteration in range(1, 21):
        print(f"Iteration {iteration}...")
        
        # Run tests
        output = run_tests()
        
        # Check if all tests passed
        if 'FAIL' not in output:
            print("All tests passed!")
            break
        
        # Extract failures
        failures = extract_test_failures(output)
        
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
    
    print("Bulk update complete!")

if __name__ == "__main__":
    main()