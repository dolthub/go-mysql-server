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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

type LoadData struct {
	Local                   bool
	File                    string
	DestSch                 sql.Schema
	ColumnNames             []string
	ResponsePacketSent      bool
	Fields                  *sqlparser.Fields
	Lines                   *sqlparser.Lines
	IgnoreNum               int64
	IsIgnore                bool
	IsReplace               bool
	FieldsTerminatedByDelim string
	FieldsEnclosedByDelim   string
	FieldsOptionallyDelim   bool
	FieldsEscapedByDelim    string
	LinesTerminatedByDelim  string
	LinesStartingByDelim    string
}

var _ sql.Node = (*LoadData)(nil)
var _ sql.CollationCoercible = (*LoadData)(nil)

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
	return l.DestSch.Resolved()
}

func (l *LoadData) String() string {
	pr := sql.NewTreePrinter()

	_ = pr.WriteNode("LOAD DATA %s", l.File)
	return pr.String()
}

func (l *LoadData) Schema() sql.Schema {
	return l.DestSch
}

func (l *LoadData) Children() []sql.Node {
	return nil
}

func (l *LoadData) IsReadOnly() bool {
	return false
}

func (l *LoadData) SplitLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Return Nothing if at end of file and no data passed.
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Find the index of the LINES TERMINATED BY delim.
	if i := strings.Index(string(data), l.LinesTerminatedByDelim); i >= 0 {
		return i + len(l.LinesTerminatedByDelim), data[0:i], nil
	}

	// If at end of file with data return the data.
	if atEOF {
		return len(data), data, nil
	}

	return
}

// SetParsingValues parses the LoadData object to get the delimiter into FIELDS and LINES terms.
func (l *LoadData) SetParsingValues() error {
	if l.Lines != nil {
		ll := l.Lines
		if ll.StartingBy != nil {
			l.LinesStartingByDelim = string(ll.StartingBy.Val)
		}
		if ll.TerminatedBy != nil {
			l.LinesTerminatedByDelim = string(ll.TerminatedBy.Val)
		}
	}

	if l.Fields != nil {
		lf := l.Fields

		if lf.TerminatedBy != nil {
			l.FieldsTerminatedByDelim = string(lf.TerminatedBy.Val)
		}

		if lf.EscapedBy != nil {
			if len(string(lf.EscapedBy.Val)) > 1 {
				return sql.ErrLoadDataCharacterLength.New(fmt.Sprintf("LOAD DATA ESCAPED BY %s", lf.EscapedBy))
			}

			l.FieldsEscapedByDelim = string(lf.EscapedBy.Val)
		}

		if lf.EnclosedBy != nil {
			lfe := lf.EnclosedBy

			if lfe.Optionally {
				l.FieldsOptionallyDelim = true
			}

			if lfe.Delim != nil {
				if len(string(lfe.Delim.Val)) > 1 {
					return sql.ErrLoadDataCharacterLength.New("LOAD DATA ENCLOSED BY")
				}

				l.FieldsEnclosedByDelim = string(lfe.Delim.Val)
			}
		}
	}

	return nil
}

func (l *LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 0)
	}
	return l, nil
}

// CheckPrivileges implements the interface sql.Node.
func (l *LoadData) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_File))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*LoadData) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func NewLoadData(local bool, file string, destSch sql.Schema, cols []string, fields *sqlparser.Fields, lines *sqlparser.Lines, ignoreNum int64, ignoreOrReplace string) *LoadData {
	isReplace := ignoreOrReplace == sqlparser.ReplaceStr
	isIgnore := ignoreOrReplace == sqlparser.IgnoreStr || (local && !isReplace)
	return &LoadData{
		Local:                   local,
		File:                    file,
		DestSch:                 destSch,
		ColumnNames:             cols,
		Fields:                  fields,
		Lines:                   lines,
		IgnoreNum:               ignoreNum,
		IsIgnore:                isIgnore,
		IsReplace:               isReplace,
		LinesStartingByDelim:    defaultLinesStartingByDelim,
		LinesTerminatedByDelim:  defaultLinesTerminatedByDelim,
		FieldsEnclosedByDelim:   defaultFieldsEnclosedByDelim,
		FieldsTerminatedByDelim: defaultFieldsTerminatedByDelim,
		FieldsOptionallyDelim:   defaultFieldsOptionallyDelim,
		FieldsEscapedByDelim:    defaultFieldsEscapedByDelim,
	}
}
