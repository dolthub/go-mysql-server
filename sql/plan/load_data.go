// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type LoadData struct {
	Local                   bool
	File                    string
	Destination             sql.Node
	ColumnNames             []string
	ResponsePacketSent      bool
	Fields                  *sqlparser.Fields
	Lines                   *sqlparser.Lines
	IgnoreNum               int64
	fieldsTerminatedByDelim string
	fieldsEnclosedByDelim   string
	fieldsOptionallyDelim   bool
	fieldsEscapedByDelim    string
	linesTerminatedByDelim  string
	linesStartingByDelim    string
}

// Default values as defined here: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
const (
	defaultFieldsTerminatedByDelim = "\t"
	defaultFieldsEnclosedByDelim   = ""
	defaultFieldsOptionallyDelim   = false
	defaultFieldsEscapedByDelim    = "\\"
	defaultLinesTerminatedByDelim  = "\n"
	defaultLinesStartingByDelim    = ""
)

func (l *LoadData) Resolved() bool {
	return l.Destination.Resolved()
}

func (l *LoadData) String() string {
	pr := sql.NewTreePrinter()

	_ = pr.WriteNode("LOAD DATA %s", l.File)
	return pr.String()
}

func (l *LoadData) Schema() sql.Schema {
	return l.Destination.Schema()
}

func (l *LoadData) Children() []sql.Node {
	return []sql.Node{l.Destination}
}

func (l *LoadData) splitLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Return nothing if at end of file and no data passed.
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Find the index of the LINES TERMINATED BY delim.
	if i := strings.Index(string(data), l.linesTerminatedByDelim); i >= 0 {
		return i + len(l.linesTerminatedByDelim), data[0:i], nil
	}

	// If at end of file with data return the data.
	if atEOF {
		return len(data), data, nil
	}

	return
}

// setParsingValues parses the LoadData object to get the delimiter into FIELDS and LINES terms.
func (l *LoadData) setParsingValues() error {
	if l.Lines != nil {
		ll := l.Lines
		if ll.StartingBy != nil {
			l.linesStartingByDelim = string(ll.StartingBy.Val)
		}
		if ll.TerminatedBy != nil {
			l.linesTerminatedByDelim = string(ll.TerminatedBy.Val)
		}
	}

	if l.Fields != nil {
		lf := l.Fields

		if lf.TerminatedBy != nil {
			l.fieldsTerminatedByDelim = string(lf.TerminatedBy.Val)
		}

		if lf.EscapedBy != nil {
			if len(string(lf.EscapedBy.Val)) > 1 {
				return sql.ErrLoadDataCharacterLength.New(fmt.Sprintf("LOAD DATA ESCAPED BY %s", lf.EscapedBy))
			}

			l.fieldsEscapedByDelim = string(lf.EscapedBy.Val)
		}

		if lf.EnclosedBy != nil {
			lfe := lf.EnclosedBy

			if lfe.Optionally {
				l.fieldsOptionallyDelim = true
			}

			if lfe.Delim != nil {
				if len(string(lfe.Delim.Val)) > 1 {
					return sql.ErrLoadDataCharacterLength.New("LOAD DATA ENCLOSED BY")
				}

				l.fieldsEnclosedByDelim = string(lfe.Delim.Val)
			}
		}
	}

	return nil
}

func (l *LoadData) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Start the parsing by grabbing all the config variables.
	err := l.setParsingValues()
	if err != nil {
		return nil, err
	}

	var reader io.ReadCloser

	if l.Local {
		_, localInfile, ok := sql.SystemVariables.GetGlobal("local_infile")
		if !ok {
			return nil, fmt.Errorf("error: local_infile variable was not found")
		}

		if localInfile.(int8) == 0 {
			return nil, fmt.Errorf("local_infile needs to be set to 1 to use LOCAL")
		}

		reader, err = ctx.LoadInfile(l.File)
		if err != nil {
			return nil, err
		}
	} else {
		_, dir, ok := sql.SystemVariables.GetGlobal("secure_file_priv")
		if !ok {
			return nil, fmt.Errorf("error: secure_file_priv variable was not found")
		}
		if dir == nil {
			dir = ""
		}

		fileName := filepath.Join(dir.(string), l.File)
		file, err := os.Open(fileName)
		if err != nil {
			return nil, sql.ErrLoadDataCannotOpen.New(err.Error())
		}
		reader = file
	}

	scanner := bufio.NewScanner(reader)

	// Set the split function for lines.
	scanner.Split(l.splitLines)

	// Skip through the lines that need to be ignored.
	for l.IgnoreNum > 0 && scanner.Scan() {
		scanner.Text()
		l.IgnoreNum--
	}

	if scanner.Err() != nil {
		reader.Close()
		return nil, scanner.Err()
	}

	sch := l.Schema()
	source := sch[0].Source // Schema will always have at least one column
	columnNames := l.ColumnNames
	if len(columnNames) == 0 {
		columnNames = make([]string, len(sch))
		for i, col := range sch {
			columnNames[i] = col.Name
		}
	}
	fieldToColumnMap := make([]int, len(columnNames))
	for fieldIndex, columnName := range columnNames {
		fieldToColumnMap[fieldIndex] = sch.IndexOf(columnName, source)
	}

	return &loadDataIter{
		destination:             l.Destination,
		reader:                  reader,
		scanner:                 scanner,
		columnCount:             len(l.ColumnNames), // Needs to be the original column count
		fieldToColumnMap:        fieldToColumnMap,
		fieldsTerminatedByDelim: l.fieldsTerminatedByDelim,
		fieldsEnclosedByDelim:   l.fieldsEnclosedByDelim,
		fieldsOptionallyDelim:   l.fieldsOptionallyDelim,
		fieldsEscapedByDelim:    l.fieldsEscapedByDelim,
		linesTerminatedByDelim:  l.linesTerminatedByDelim,
		linesStartingByDelim:    l.linesStartingByDelim,
	}, nil
}

type loadDataIter struct {
	scanner                 *bufio.Scanner
	destination             sql.Node
	reader                  io.ReadCloser
	columnCount             int
	fieldToColumnMap        []int
	fieldsTerminatedByDelim string
	fieldsEnclosedByDelim   string
	fieldsOptionallyDelim   bool
	fieldsEscapedByDelim    string
	linesTerminatedByDelim  string
	linesStartingByDelim    string
}

func (l loadDataIter) Next(ctx *sql.Context) (returnRow sql.Row, returnErr error) {
	var exprs []sql.Expression
	var err error
	// If exprs is nil then this is a skipped line (see test cases). Keep skipping
	// until exprs != nil
	for exprs == nil {
		keepGoing := l.scanner.Scan()
		if !keepGoing {
			if l.scanner.Err() != nil {
				return nil, l.scanner.Err()
			}
			return nil, io.EOF
		}

		line := l.scanner.Text()
		exprs, err = l.parseFields(line)

		if err != nil {
			return nil, err
		}
	}

	row := make(sql.Row, len(exprs))
	var secondPass []int
	for i, expr := range exprs {
		if expr != nil {
			if defaultVal, ok := expr.(*sql.ColumnDefaultValue); ok && !defaultVal.IsLiteral() {
				secondPass = append(secondPass, i)
				continue
			}
			row[i], err = expr.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, index := range secondPass {
		row[index], err = exprs[index].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	return sql.NewRow(row...), nil
}

func (l loadDataIter) Close(ctx *sql.Context) error {
	return l.reader.Close()
}

// parseLinePrefix searches for the delim defined by linesStartingByDelim.
func (l loadDataIter) parseLinePrefix(line string) string {
	if l.linesStartingByDelim == "" {
		return line
	}

	prefixIndex := strings.Index(line, l.linesStartingByDelim)

	// The prefix wasn't found so we need to skip this line.
	if prefixIndex < 0 {
		return ""
	} else {
		return line[prefixIndex+len(l.linesStartingByDelim):]
	}
}

func (l loadDataIter) parseFields(line string) ([]sql.Expression, error) {
	// Step 1. Start by Searching for prefix if there is one
	line = l.parseLinePrefix(line)
	if line == "" {
		return nil, nil
	}

	// Step 2: Split the lines into fields given the delim
	fields := strings.Split(line, l.fieldsTerminatedByDelim)

	// Step 3: Go through each field and see if it was enclosed by something
	// TODO: Support the OPTIONALLY parameter.
	if l.fieldsEnclosedByDelim != "" {
		for i, field := range fields {
			if string(field[0]) == l.fieldsEnclosedByDelim && string(field[len(field)-1]) == l.fieldsEnclosedByDelim {
				fields[i] = field[1 : len(field)-1]
			} else {
				return nil, fmt.Errorf("error: field not properly enclosed")
			}
		}
	}

	//Step 4: Handle the ESCAPED BY parameter.
	if l.fieldsEscapedByDelim != "" {
		for i, field := range fields {
			if field == "\\N" {
				fields[i] = "NULL"
			} else if field == "\\Z" {
				fields[i] = fmt.Sprintf("%c", 26) // ASCII 26
			} else if field == "\\0" {
				fields[i] = fmt.Sprintf("%c", 0) // ASCII 0
			} else {
				fields[i] = strings.ReplaceAll(field, l.fieldsEscapedByDelim, "")
			}
		}
	}

	exprs := make([]sql.Expression, len(l.destination.Schema()))

	limit := len(exprs)
	if len(fields) < limit {
		limit = len(fields)
	}

	destSch := l.destination.Schema()
	for i := 0; i < limit; i++ {
		field := fields[i]
		destCol := destSch[l.fieldToColumnMap[i]]
		// Replace the empty string with defaults
		if field == "" {
			_, ok := destCol.Type.(sql.StringType)
			if !ok {
				if destCol.Default != nil {
					exprs[i] = destCol.Default
				} else {
					exprs[i] = expression.NewLiteral(nil, sql.Null)
				}
			} else {
				exprs[i] = expression.NewLiteral(field, sql.LongText)
			}
		} else if field == "NULL" {
			exprs[i] = expression.NewLiteral(nil, sql.Null)
		} else {
			exprs[i] = expression.NewLiteral(field, sql.LongText)
		}
	}

	// Due to how projections work, if no columns are provided (each row may have a variable number of values), the
	// projection will not insert default values, so we must do it here.
	if l.columnCount == 0 {
		for i, expr := range exprs {
			if expr == nil && destSch[i].Default != nil {
				exprs[i] = destSch[i].Default
			}
		}
	}

	return exprs, nil
}

func (l *LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	l.Destination = children[0]
	return l, nil
}

// CheckPrivileges implements the interface sql.Node.
func (l *LoadData) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_File))
}

func NewLoadData(local bool, file string, destination sql.Node, cols []string, fields *sqlparser.Fields, lines *sqlparser.Lines, ignoreNum int64) *LoadData {
	return &LoadData{
		Local:                   local,
		File:                    file,
		Destination:             destination,
		ColumnNames:             cols,
		Fields:                  fields,
		Lines:                   lines,
		IgnoreNum:               ignoreNum,
		linesStartingByDelim:    defaultLinesStartingByDelim,
		linesTerminatedByDelim:  defaultLinesTerminatedByDelim,
		fieldsEnclosedByDelim:   defaultFieldsEnclosedByDelim,
		fieldsTerminatedByDelim: defaultFieldsTerminatedByDelim,
		fieldsOptionallyDelim:   defaultFieldsOptionallyDelim,
		fieldsEscapedByDelim:    defaultFieldsEscapedByDelim,
	}
}
