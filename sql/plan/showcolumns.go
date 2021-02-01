// Copyright 2020-2021 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
)

// ShowColumns shows the columns details of a table.
type ShowColumns struct {
	UnaryNode
	Full    bool
	Indexes []sql.Index
}

var (
	showColumnsSchema = sql.Schema{
		{Name: "Field", Type: sql.LongText},
		{Name: "Type", Type: sql.LongText},
		{Name: "Null", Type: sql.LongText},
		{Name: "Key", Type: sql.LongText},
		{Name: "Default", Type: sql.LongText, Nullable: true},
		{Name: "Extra", Type: sql.LongText},
	}

	showColumnsFullSchema = sql.Schema{
		{Name: "Field", Type: sql.LongText},
		{Name: "Type", Type: sql.LongText},
		{Name: "Collation", Type: sql.LongText, Nullable: true},
		{Name: "Null", Type: sql.LongText},
		{Name: "Key", Type: sql.LongText},
		{Name: "Default", Type: sql.LongText, Nullable: true},
		{Name: "Extra", Type: sql.LongText},
		{Name: "Privileges", Type: sql.LongText},
		{Name: "Comment", Type: sql.LongText},
	}
)

// NewShowColumns creates a new ShowColumns node.
func NewShowColumns(full bool, child sql.Node) *ShowColumns {
	return &ShowColumns{UnaryNode: UnaryNode{Child: child}, Full: full}
}

var _ sql.Node = (*ShowColumns)(nil)

// Schema implements the sql.Node interface.
func (s *ShowColumns) Schema() sql.Schema {
	if s.Full {
		return showColumnsFullSchema
	}
	return showColumnsSchema
}

// RowIter creates a new ShowColumns node.
func (s *ShowColumns) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, _ := ctx.Span("plan.ShowColumns")

	schema := s.Child.Schema()
	var rows = make([]sql.Row, len(schema))
	for i, col := range schema {
		var row sql.Row
		var collation interface{}
		if sql.IsTextOnly(col.Type) {
			collation = sql.Collation_Default.String()
		}

		var null = "NO"
		if col.Nullable {
			null = "YES"
		}

		node := s.Child
		if exchange, ok := node.(*Exchange); ok {
			node = exchange.Child
		}
		key := ""
		switch table := node.(type) {
		case *ResolvedTable:
			if col.PrimaryKey {
				key = "PRI"
			} else if s.isFirstColInUniqueKey(col, table) {
				key = "UNI"
			} else if s.isFirstColInNonUniqueKey(col, table) {
				key = "MUL"
			}
		case *SubqueryAlias:
			// no key info for views
		default:
			panic(fmt.Sprintf("unexpected type %T", s.Child))
		}

		var defaultVal string
		if col.Default != nil {
			defaultVal = col.Default.String()
		}

		// TODO: rather than lower-casing here, we should lower-case the String() method of types
		if s.Full {
			row = sql.Row{
				col.Name,
				strings.ToLower(col.Type.String()),
				collation,
				null,
				key,
				defaultVal,
				col.Extra,
				"", // Privileges
				col.Comment,
			}
		} else {
			row = sql.Row{
				col.Name,
				strings.ToLower(col.Type.String()),
				null,
				key,
				defaultVal,
				col.Extra,
			}
		}

		rows[i] = row
	}

	return sql.NewSpanIter(span, sql.RowsToRowIter(rows...)), nil
}

// WithChildren implements the Node interface.
func (s *ShowColumns) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	showColumns := NewShowColumns(s.Full, children[0])
	showColumns.Indexes = s.Indexes
	return showColumns, nil
}

func (s *ShowColumns) String() string {
	tp := sql.NewTreePrinter()
	if s.Full {
		_ = tp.WriteNode("ShowColumns(full)")
	} else {
		_ = tp.WriteNode("ShowColumns")
	}
	_ = tp.WriteChildren(s.Child.String())
	return tp.String()
}

func (s *ShowColumns) isFirstColInUniqueKey(col *sql.Column, table sql.Table) bool {
	for _, idx := range s.Indexes {
		if !idx.IsUnique() {
			continue
		}

		firstIndexCol := GetColumnFromIndexExpr(idx.Expressions()[0], table)
		if firstIndexCol != nil && firstIndexCol.Name == col.Name {
			return true
		}
	}

	return false
}

func (s *ShowColumns) isFirstColInNonUniqueKey(col *sql.Column, table sql.Table) bool {
	for _, idx := range s.Indexes {
		if idx.IsUnique() {
			continue
		}

		firstIndexCol := GetColumnFromIndexExpr(idx.Expressions()[0], table)
		if firstIndexCol != nil && firstIndexCol.Name == col.Name {
			return true
		}
	}

	return false
}
