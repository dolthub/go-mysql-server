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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"os"
	"strconv"
	"strings"
)

type LoadData struct {
	Local bool
	File string
	Destination sql.Node
	ColumnNames []string
	ResponsePacketSent bool
	Fields *sqlparser.Fields
	Lines *sqlparser.Lines
}

var (
	fieldsTerminatedByDelim = "\t"
	fieldsEnclosedByDelim = ""
	fieldsOptionallyDelim = false
	fieldsEscapedByDelim = "\\"
	linesTerminatedByDelim = "\n"
	linesStartingByDelim = ""
)

func (l LoadData) Resolved() bool {
	return l.Destination.Resolved()
}

func (l LoadData) String() string {
	return "Load data yooyoyoy"
}

func (l LoadData) Schema() sql.Schema {
	return l.Destination.Schema()
}

func (l LoadData) Children() []sql.Node {
	return []sql.Node{l.Destination}
}

func (l LoadData) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Get Files as an InsertRows
	// 1. How do I attach a column name to an inserter (might need to use update iter instead)
	// 2. How do i go through the non local path for discovering files
	// 3. Biggest Risk is nailing the parsing algorithm and getting the types correct
	// TODO: Add the security variables for mysql

	// Start the parsing by grabbing all the config variables.
	l.updateParsingConsts()

	// TODO: Add tmpdir setting for mysql
	var fileName = l.File
	if l.Local {
		fileName = "/tmp/.LOADFILE"
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	//scanner := bufio.NewScanner(file)
	//parseLines(scanner)

	var values [][]sql.Expression
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		txt := scanner.Text()
		exprs := make([]sql.Expression, 1)

		val, err := strconv.ParseInt(txt, 10, 64)
		if err != nil {
			return nil, err
		}

		exprs[0] = expression.NewLiteral(val, sql.Int8)
		values = append(values, exprs)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	newValue := NewValues(values)

	return newInsertIter(ctx, l.Destination, newValue, false, nil, row)
}

func (l LoadData) updateParsingConsts() {
	if l.Lines != nil {
		ll := l.Lines
		if ll.StartingBy != "" {
			linesStartingByDelim = ll.StartingBy
		}
		if ll.TerminatedBy != "" {
			linesTerminatedByDelim = ll.TerminatedBy
		}
	}

	if l.Fields != nil {
		lf := l.Fields

		if lf.TerminatedBy != "" {
			fieldsTerminatedByDelim = lf.TerminatedBy
		}

		if lf.EscapedBy != "" {
			fieldsEscapedByDelim = lf.EscapedBy
		}

		if lf.EnclosedBy != nil {
			lfe := lf.EnclosedBy

			if lfe.Optionally {
				fieldsOptionallyDelim = true
			}

			if lfe.Delim != "" {
				fieldsEnclosedByDelim = lfe.Delim
			}
		}

	}
}

func parseLines(scanner *bufio.Scanner) string {
	splitFunc := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF {
			return 0, nil, nil
		}

		// Find the prefix
		startingDelimPos := -1
		if linesStartingByDelim != "" {
			startingDelimPos = strings.Index(string(data), linesTerminatedByDelim)
		}

		// Find the ending token
		endingDelimPos := -1
		if linesTerminatedByDelim != "" {
			endingDelimPos = strings.Index(string(data), linesTerminatedByDelim)
		}

		// Throw an error if an ending token was expected but not given. TODO: make sure this correct
		if endingDelimPos < 0 && linesTerminatedByDelim != "" {
			return 0, nil, fmt.Errorf("error: data does not meet parsing criteria")
		}

		if linesStartingByDelim == "" {
			if endingDelimPos >= 0 {
				return endingDelimPos+len(linesTerminatedByDelim), data[0:endingDelimPos], nil
			} else {
				return len(data), data[0:], nil
			}
		}

		// If the starting delimeter isn't found and is non-empty we need to skip this data.
		if startingDelimPos < 0 {
			// if the starting delim wasn't found and the ending delim wasn't found throw an error
			if endingDelimPos < 0 {
				return 0, nil, fmt.Errorf("error: data does not meet parsing criteria")
			} else {
				advancePos := endingDelimPos + len(linesTerminatedByDelim)

				return advancePos, nil, nil
			}
		} else {
			startPos := startingDelimPos + len(linesStartingByDelim)
			if endingDelimPos < 0 {
				return len(data), data[startPos:], nil
			} else {
				advancePos := endingDelimPos + len(linesTerminatedByDelim)
				return advancePos, data[startPos:endingDelimPos], nil
			}
		}
	}

	scanner.Split(splitFunc)

	for scanner.Scan() {
		x := scanner.Text()
		fmt.Println(x)
	}

	return ""
}

func getLoadPath(fileName string, local bool) string {
	return ""
}

func (l LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	l.Destination = children[0]
	return l, nil
}

func NewLoadData(local bool, file string, destination sql.Node, cols []string, fields *sqlparser.Fields, lines *sqlparser.Lines) *LoadData {
	return &LoadData{
		Local: local,
		File: file,
		Destination: destination,
		ColumnNames: cols,
		Fields: fields,
		Lines: lines,
	}
}