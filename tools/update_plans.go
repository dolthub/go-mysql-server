package main

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
)

func main() {
	queryPlanFile := "/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go"
	
	for iteration := 1; iteration <= 10; iteration++ {
		fmt.Printf("Iteration %d...\n", iteration)
		
		// Run the test and capture output
		cmd := exec.Command("go", "test", "-v", "./enginetest", "-run", "TestQueryPlans")
		cmd.Dir = "/Users/amx/dolt_workspace/go-mysql-server"
		output, err := cmd.CombinedOutput()
		
		if err == nil {
			fmt.Println("All tests passed!")
			break
		}
		
		outputStr := string(output)
		
		// Find all test failures with expected vs actual - more robust approach
		lines := strings.Split(outputStr, "\n")
		updateCount := 0
		
		// Read the file
		content, err := ioutil.ReadFile(queryPlanFile)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			return
		}
		
		contentStr := string(content)
		updatedContent := contentStr
		
		// Look for expected/actual pairs
		for i := 0; i < len(lines); i++ {
			line := strings.TrimSpace(lines[i])
			if strings.HasPrefix(line, "expected:") && strings.Contains(line, `"`) {
				// Extract expected value
				start := strings.Index(line, `"`)
				if start == -1 {
					continue
				}
				
				// Find the end of the expected string, handling multi-line
				expected := ""
				currentLine := line[start+1:]
				lineIdx := i
				
				// Keep reading until we find the closing quote
				for lineIdx < len(lines) {
					if lineIdx == i {
						// First line
						if strings.HasSuffix(currentLine, `"`) && !strings.HasSuffix(currentLine, `\"`) {
							expected = currentLine[:len(currentLine)-1]
							break
						}
						expected = currentLine
					} else {
						// Continuation lines
						currentLine = lines[lineIdx]
						if strings.HasSuffix(currentLine, `"`) && !strings.HasSuffix(currentLine, `\"`) {
							expected += "\n" + currentLine[:len(currentLine)-1]
							break
						}
						expected += "\n" + currentLine
					}
					lineIdx++
				}
				
				// Look for the actual value
				actualStart := -1
				for j := lineIdx + 1; j < len(lines) && j < lineIdx + 10; j++ {
					if strings.Contains(lines[j], "actual") && strings.Contains(lines[j], `"`) {
						actualStart = j
						break
					}
				}
				
				if actualStart == -1 {
					continue
				}
				
				// Extract actual value
				actualLine := lines[actualStart]
				start = strings.Index(actualLine, `"`)
				if start == -1 {
					continue
				}
				
				actual := ""
				currentLine = actualLine[start+1:]
				lineIdx = actualStart
				
				// Keep reading until we find the closing quote
				for lineIdx < len(lines) {
					if lineIdx == actualStart {
						// First line
						if strings.HasSuffix(currentLine, `"`) && !strings.HasSuffix(currentLine, `\"`) {
							actual = currentLine[:len(currentLine)-1]
							break
						}
						actual = currentLine
					} else {
						// Continuation lines
						currentLine = lines[lineIdx]
						if strings.HasSuffix(currentLine, `"`) && !strings.HasSuffix(currentLine, `\"`) {
							actual += "\n" + currentLine[:len(currentLine)-1]
							break
						}
						actual += "\n" + currentLine
					}
					lineIdx++
				}
				
				// Try to find and replace in the file
				if expected != "" && actual != "" && strings.Contains(updatedContent, `"`+expected+`"`) {
					updatedContent = strings.ReplaceAll(updatedContent, `"`+expected+`"`, `"`+actual+`"`)
					updateCount++
				}
			}
		}
		
		fmt.Printf("Found %d failures to update\n", updateCount)
		
		if updateCount > 0 {
			// Write back to file
			err = ioutil.WriteFile(queryPlanFile, []byte(updatedContent), 0644)
			if err != nil {
				fmt.Printf("Error writing file: %v\n", err)
				return
			}
			fmt.Printf("Made %d updates\n", updateCount)
		} else {
			fmt.Printf("No updates made in iteration %d\n", iteration)
			break
		}
	}
	
	fmt.Println("Update complete!")
}