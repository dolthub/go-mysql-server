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
	"os"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type LoadData struct {
	Local              bool
	File               string
	Destination        sql.Node
	ColumnNames        []string
	ResponsePacketSent bool
	Fields             *sqlparser.Fields
	Lines              *sqlparser.Lines
	IgnoreNum          int8
}

const (
	Tmpfiledir  = "/tmp/"
	TmpfileName = ".LOADFILE"
)

var (
	fieldsTerminatedByDelim = "\t"
	fieldsEnclosedByDelim   = ""
	fieldsOptionallyDelim   = false
	fieldsEscapedByDelim    = "\\"
	linesTerminatedByDelim  = "\n"
	linesStartingByDelim    = ""
)

func (l *LoadData) Resolved() bool {
	return l.Destination.Resolved()
}

func (l *LoadData) String() string {
	pr := sql.NewTreePrinter()

	_ = pr.WriteNode("LOAD DATA")
	_ = pr.WriteChildren(l.Destination.String())
	return pr.String()
}

func (l *LoadData) Schema() sql.Schema {
	return l.Destination.Schema()
}

func (l *LoadData) Children() []sql.Node {
	return []sql.Node{l.Destination}
}

func (l *LoadData) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// TODO: Add the security variables for mysql

	// Start the parsing by grabbing all the config variables.
	err := l.updateParsingConsts()
	if err != nil {
		return nil, err
	}

	// TODO: Add tmpdir setting for mysql
	var fileName = l.File
	if l.Local {
		fileName = Tmpfiledir + TmpfileName
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer os.Remove(fileName)

	scanner := bufio.NewScanner(file)
	parseLines(scanner)

	var values [][]sql.Expression
	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if l.IgnoreNum <= 0 {
			exprs, err := parseFields(line)

			if err != nil {
				return nil, err
			}

			// line was skipped
			if exprs != nil {
				values = append(values, exprs)
			}
		} else {
			l.IgnoreNum--
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	newValue := NewValues(values)

	return newInsertIter(ctx, l.Destination, newValue, false, nil, row)
}

func (l *LoadData) updateParsingConsts() error {
	if l.Lines != nil {
		ll := l.Lines
		if ll.StartingBy != "" {
			sb := ll.StartingBy[1 : len(ll.StartingBy)-1]
			linesStartingByDelim = sb
		}
		if ll.TerminatedBy != "" {
			tb, err := strconv.Unquote(ll.TerminatedBy)
			if err != nil {
				return err
			}
			linesTerminatedByDelim = tb
		}
	}

	if l.Fields != nil {
		lf := l.Fields

		if lf.TerminatedBy != "" {
			tb, err := strconv.Unquote(lf.TerminatedBy)
			if err != nil {
				return err
			}
			fieldsTerminatedByDelim = tb
		}

		if lf.EscapedBy != "" {
			if len(lf.EscapedBy) > 1 {
				return fmt.Errorf("error: LOAD DATA ESCAPED BY must be 1 character long")
			}

			eb, err := strconv.Unquote(lf.TerminatedBy)
			if err != nil {
				return err
			}
			fieldsEscapedByDelim = eb
		}

		if lf.EnclosedBy != nil {
			lfe := lf.EnclosedBy

			if lfe.Optionally {
				fieldsOptionallyDelim = true
			}

			if lfe.Delim != "" {
				if len(lfe.Delim) > 1 {
					return fmt.Errorf("error: LOAD DATA ENCLOSED BY must be 1 character long")
				}

				eb, err := strconv.Unquote(lfe.Delim)
				if err != nil {
					return err
				}
				fieldsEnclosedByDelim = eb
			}
		}

	}

	return nil
}

func parseLines(scanner *bufio.Scanner) {
	splitFunc := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Return nothing if at end of file and no data passed
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		// Find the index of the input of a newline followed by a
		// pound sign.
		if i := strings.Index(string(data), linesTerminatedByDelim); i >= 0 {
			return i + 1, data[0:i], nil
		}

		// If at end of file with data return the data
		if atEOF {
			return len(data), data, nil
		}

		return
	}
	scanner.Split(splitFunc)
}

func parseLinePrefix(line string) string {
	if linesStartingByDelim == "" {
		return line
	}

	prefixIndex := strings.Index(line, linesStartingByDelim)

	// The prefix wasn't found so we need to skip this line.
	if prefixIndex < 0 {
		return ""
	} else {
		return line[prefixIndex+len(linesStartingByDelim):]
	}
}

func parseFields(line string) ([]sql.Expression, error) {
	// Step 1. Start by Searching for prefix if there is one
	line = parseLinePrefix(line)
	if line == "" {
		return nil, nil
	}

	// Step 2: Split the lines into fields given the delim
	fields := strings.Split(line, fieldsTerminatedByDelim)

	// Step 3: Go through each field and see if it was enclosed by something
	// TODO: Support the OPTIONALLY parameter.
	if fieldsEnclosedByDelim != "" {
		for i, field := range fields {
			if string(field[0]) == fieldsEnclosedByDelim && string(field[len(field)-1]) == fieldsEnclosedByDelim {
				fields[i] = field[1 : len(field)-1]
			} else {
				return nil, fmt.Errorf("error: dield not properly enclosed")
			}
		}
	}

	// TODO: Step 4: Check for the ESCAPED BY parameter.
	exprs := make([]sql.Expression, len(fields))

	for i, field := range fields {
		exprs[i] = expression.NewLiteral(field, sql.LongText)
	}

	return exprs, nil
}

// TODO: Do robust path finding for load data.
func getLoadPath(fileName string, local bool) string {
	return ""
}

func (l *LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	l.Destination = children[0]
	return l, nil
}

func NewLoadData(local bool, file string, destination sql.Node, cols []string, fields *sqlparser.Fields, lines *sqlparser.Lines, ignoreNum int8) *LoadData {
	return &LoadData{
		Local:       local,
		File:        file,
		Destination: destination,
		ColumnNames: cols,
		Fields:      fields,
		Lines:       lines,
		IgnoreNum:   ignoreNum,
	}
}
