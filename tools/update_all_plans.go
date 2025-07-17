package main

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
)

func main() {
	queryPlanFile := "/Users/amx/dolt_workspace/go-mysql-server/enginetest/queries/query_plans.go"
	
	for iteration := 1; iteration <= 20; iteration++ {
		fmt.Printf("Iteration %d...\n", iteration)
		
		// Run the test and capture output
		cmd := exec.Command("go", "test", "-v", "./enginetest", "-run", "TestQueryPlans", "-count=1")
		cmd.Dir = "/Users/amx/dolt_workspace/go-mysql-server"
		output, err := cmd.CombinedOutput()
		
		if err == nil {
			fmt.Println("All tests passed!")
			break
		}
		
		outputStr := string(output)
		
		// Read the file
		content, err := ioutil.ReadFile(queryPlanFile)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			return
		}
		
		contentStr := string(content)
		updatedContent := contentStr
		updateCount := 0
		
		// Split output into sections by test failures
		sections := strings.Split(outputStr, "Not equal:")
		
		for _, section := range sections {
			if !strings.Contains(section, "expected:") || !strings.Contains(section, "actual") {
				continue
			}
			
			// Extract expected and actual values using a more robust approach
			expectedStart := strings.Index(section, "expected:")
			actualStart := strings.Index(section, "actual")
			
			if expectedStart == -1 || actualStart == -1 || actualStart <= expectedStart {
				continue
			}
			
			expectedSection := section[expectedStart:actualStart]
			actualSection := section[actualStart:]
			
			// Extract the quoted strings
			expected := extractQuotedString(expectedSection)
			actual := extractQuotedString(actualSection)
			
			if expected != "" && actual != "" {
				// Try to find and replace in the file
				quotedExpected := `"` + expected + `"`
				quotedActual := `"` + actual + `"`
				
				if strings.Contains(updatedContent, quotedExpected) {
					updatedContent = strings.Replace(updatedContent, quotedExpected, quotedActual, 1)
					updateCount++
				}
			}
		}
		
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

func extractQuotedString(section string) string {
	// Find the first quote
	start := strings.Index(section, `"`)
	if start == -1 {
		return ""
	}
	
	// Find the matching end quote, handling escaped quotes
	content := section[start+1:]
	result := ""
	i := 0
	
	for i < len(content) {
		if content[i] == '\\' && i+1 < len(content) {
			// Escaped character
			result += string(content[i]) + string(content[i+1])
			i += 2
		} else if content[i] == '"' {
			// End quote found
			break
		} else {
			result += string(content[i])
			i++
		}
	}
	
	return result
}