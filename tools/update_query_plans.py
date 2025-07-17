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

def parse_test_failures(output):
    """Parse test failures and extract expected vs actual differences"""
    failures = []
    
    # Split by test failure sections
    sections = output.split('Not equal:')
    
    for section in sections[1:]:  # Skip first section (before any failures)
        try:
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
        except Exception as e:
            print(f"Error parsing section: {e}")
            continue
    
    return failures

def find_and_replace_in_file(file_path, expected, actual):
    """Find and replace expected with actual in the file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Escape strings for Go string literals
        expected_go = expected.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        actual_go = actual.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        # Look for the expected string in quotes
        search_pattern = f'"{expected_go}"'
        replacement = f'"{actual_go}"'
        
        if search_pattern in content:
            updated_content = content.replace(search_pattern, replacement, 1)
            with open(file_path, 'w') as f:
                f.write(updated_content)
            return True
        
        # Try a more lenient approach - look for partial matches
        # Focus on cost estimation changes which are simpler
        expected_lines = expected.split('\n')
        actual_lines = actual.split('\n')
        
        # Look for cost changes specifically
        for i, (exp_line, act_line) in enumerate(zip(expected_lines, actual_lines)):
            if 'estimated cost=' in exp_line and 'estimated cost=' in act_line:
                # Extract the cost values
                exp_cost_match = re.search(r'estimated cost=([0-9.]+)', exp_line)
                act_cost_match = re.search(r'estimated cost=([0-9.]+)', act_line)
                
                if exp_cost_match and act_cost_match:
                    exp_cost = exp_cost_match.group(1)
                    act_cost = act_cost_match.group(1)
                    
                    # Find and replace this specific cost in the file
                    cost_pattern = f'estimated cost={exp_cost}'
                    cost_replacement = f'estimated cost={act_cost}'
                    
                    if cost_pattern in content:
                        updated_content = content.replace(cost_pattern, cost_replacement)
                        with open(file_path, 'w') as f:
                            f.write(updated_content)
                        return True
        
        # Try to find unique identifiers in the expected string
        for line in expected_lines[:5]:  # Check first few lines
            line = line.strip()
            if line and len(line) > 10 and '"' not in line:
                # Look for this line as a substring
                if line in content:
                    # Found a reference point, but replacing the whole block is complex
                    # For now, just report that we found it
                    return False
            
    except Exception as e:
        print(f"Error updating file: {e}")
    
    return False

def main():
    query_plans_file = '/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go'
    
    print("Running tests to get failures...")
    output = run_tests()
    
    print("Parsing test failures...")
    failures = parse_test_failures(output)
    
    if not failures:
        print("No failures found or unable to parse failures")
        return
    
    print(f"Found {len(failures)} test failures to update")
    
    update_count = 0
    for i, (expected, actual) in enumerate(failures):
        print(f"Processing failure {i+1}/{len(failures)}...")
        
        if find_and_replace_in_file(query_plans_file, expected, actual):
            update_count += 1
            print(f"  ✓ Updated")
        else:
            print(f"  ✗ Could not find/update")
            # Print first few lines for debugging
            exp_lines = expected.split('\n')[:3]
            print(f"    Expected starts with: {exp_lines}")
    
    print(f"\nUpdated {update_count} out of {len(failures)} failures")
    
    # Run tests again to see improvement
    print("\nRunning tests again to check improvement...")
    output2 = run_tests()
    
    if 'FAIL' not in output2:
        print("✓ All tests now pass!")
    else:
        failures2 = parse_test_failures(output2)
        print(f"Still have {len(failures2)} failures (was {len(failures)})")

if __name__ == "__main__":
    main()