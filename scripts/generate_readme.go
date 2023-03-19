package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/expression/function"
)

type Entry struct {
	Name    string
	Desc    string
	NumArgs int
	IsFuncN bool
}

func main() {
	// Hold in a list of Entry
	var entries []Entry

	// Combine and iterate through all functions
	numSupported := 0
	var funcs []sql.Function
	funcs = append(function.BuiltIns, function.GetLockingFuncs(nil)...)
	for _, f := range funcs {
		var numArgs int
		isFunctionN := false
		switch f.(type) {
		case sql.Function0:
			numArgs = 0
		case sql.Function1:
			numArgs = 1
		case sql.Function2:
			numArgs = 2
		case sql.Function3:
			numArgs = 3
		// TODO: there are no sql.Function 4,5,6,7 yet
		case sql.FunctionN:
			// Mark as FunctionN
			isFunctionN = true
			// try with no args to get error
			_, err := f.NewInstance([]sql.Expression{})
			// use error to get correct arg number
			if err != nil {
				numArgs = int(strings.Split(err.Error(), " ")[3][0]) - '0'
			} else {
				numArgs = 0
			}
		default:
			panic("Encountered unknown function type; probably b/c missing Function 4,5,6,7")
		}

		// Fill with appropriate number of arguments
		args := make([]sql.Expression, numArgs)
		for i := 0; i < numArgs; i++ {
			args[i] = expression.NewStar()
		}

		// special case for date_add and date_sub
		if f.FunctionName() == "date_add" || f.FunctionName() == "date_sub" {
			args = []sql.Expression{expression.NewStar(), expression.NewInterval(expression.NewStar(), "hi")}
		}

		// Create new instance
		_f, err := f.NewInstance(args)
		if err != nil {
			// Ignore unsupported functions that throw error at New
			if strings.Contains(err.Error(), "unsupported") {
				continue
			}
			panic(err)
		}

		// Ignore unsupported functions that throw error at Eval
		_, ok := _f.(sql.UnsupportedFunctionStub)
		if ok {
			continue
		}

		fn := _f.(sql.FunctionExpression)
		entries = append(entries, Entry{f.FunctionName(), fn.Description(), numArgs, isFunctionN})
		numSupported++
	}

	// Sort entries
	sort.SliceStable(entries, func(i, j int) bool {
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})

	// Read existing contents to a buffer
	file, err := os.Open("../README.md")
	if err != nil {
		panic(err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(file)
	contents := buf.String()
	file.Close()

	// Create README.md
	file, err = os.Create("../README.md")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Define useful constants
	const beginFuncsTag = "<!-- BEGIN FUNCTIONS -->"
	const tableHeader = "|     Name     |                                               Description                                                                      |\n|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|\n"
	const endFuncsTag = "<!-- END FUNCTIONS -->"

	// Extract portions of README
	preTableString := contents[:strings.Index(contents, beginFuncsTag)]
	postTableString := contents[strings.Index(contents, endFuncsTag):]

	// Write to README
	file.WriteString(preTableString)
	file.WriteString(beginFuncsTag)
	file.WriteString("\n")
	file.WriteString(tableHeader)
	for _, e := range entries {
		// TODO: need to include argument types somehow
		var args string
		if e.IsFuncN {
			args = "(...)"
		} else if e.NumArgs == 0 {
			args = "()"
		} else if e.NumArgs == 1 {
			args = "(expr)"
		} else {
			args = "(expr1"
			for i := 1; i < e.NumArgs; i++ {
				args += ", "
				args += "expr" + fmt.Sprintf("%d", i+1)
			}
			args += ")"
		}
		file.WriteString("|`" + strings.ToUpper(e.Name) + args + "`| " + strings.ToUpper(string(e.Desc[0])) + e.Desc[1:] + "|\n")
	}
	file.WriteString(postTableString)

	// Might be useful for dolt docs
	fmt.Println("num defined:", len(funcs))
	fmt.Println("num supported: ", numSupported)
}
