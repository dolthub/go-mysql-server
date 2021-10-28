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

const (
	TmpfileName = ".LOADFILE"
)

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
		return i + 1, data[0:i], nil
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

	var fileName string
	if l.Local {
		localInfile, err := ctx.GetSessionVariable(ctx, "local_infile")
		if err != nil {
			return nil, err
		}
		if localInfile.(int8) == 0 {
			return nil, fmt.Errorf("local_infile needs to be set to 1 to use LOCAL")
		}

		tmpdir, err := ctx.GetSessionVariable(ctx, "tmpdir")
		fileName = tmpdir.(string) + TmpfileName
	} else {
		dir, err := ctx.GetSessionVariable(ctx, "secure_file_priv")
		if err != nil {
			return nil, err
		}
		if dir == nil {
			dir = ""
		}

		fileName = filepath.Join(dir.(string), l.File)
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, sql.ErrLoadDataCannotOpen.New(err.Error())
	}

	scanner := bufio.NewScanner(file)

	// Set the split function for lines.
	scanner.Split(l.splitLines)

	// Skip through the lines that need to be ignored.
	for l.IgnoreNum > 0 && scanner.Scan() {
		scanner.Text()
		l.IgnoreNum--
	}

	return &loadDataIter{
		scanner:                 scanner,
		destination:             l.Destination,
		ctx:                     ctx,
		file:                    file,
		local:                   l.Local,
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
	ctx                     *sql.Context
	file                    *os.File
	local                   bool
	fieldsTerminatedByDelim string
	fieldsEnclosedByDelim   string
	fieldsOptionallyDelim   bool
	fieldsEscapedByDelim    string
	linesTerminatedByDelim  string
	linesStartingByDelim    string
}

func (l loadDataIter) Next() (returnRow sql.Row, returnErr error) {
	keepGoing := l.scanner.Scan()
	if !keepGoing {
		return nil, io.EOF
	}

	line := l.scanner.Text()
	exprs, err := l.parseFields(line)

	if err != nil {
		return nil, err
	}

	// If exprs is nil then this is a skipped line (see test cases). Keep skipping
	// until exprs != nil
	for exprs == nil {
		keepGoing = l.scanner.Scan()
		if !keepGoing {
			return nil, io.EOF
		}

		line = l.scanner.Text()
		exprs, err = l.parseFields(line)

		if err != nil {
			return nil, err
		}
	}

	// TODO: Match schema with column order
	// return the exprs as a row
	vals := make([]interface{}, len(exprs))
	for i, expr := range exprs {
		if expr != nil {
			vals[i], err = expr.Eval(l.ctx, returnRow)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.NewRow(vals...), nil
}

func (l loadDataIter) Close(ctx *sql.Context) error {
	if l.local {
		err := os.Remove(l.file.Name())
		if err != nil {
			return err
		}
	} else {
		err := l.file.Close()
		if err != nil {
			return err
		}
	}

	if err := l.scanner.Err(); err != nil {
		return err
	}

	return nil
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

	for i := 0; i < limit; i++ {
		field := fields[i]
		dSchema := l.destination.Schema()[i]
		// Replace the empty string with defaults
		if field == "" {
			_, ok := dSchema.Type.(sql.StringType)
			if !ok {
				exprs[i] = expression.NewLiteral(dSchema.Default, dSchema.Type)
			} else {
				exprs[i] = expression.NewLiteral(field, sql.LongText)
			}
		} else if field == "NULL" {
			exprs[i] = nil
		} else {
			exprs[i] = expression.NewLiteral(field, sql.LongText)
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
