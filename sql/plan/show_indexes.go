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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// ShowIndexes is a node that shows the indexes on a table.
type ShowIndexes struct {
	UnaryNode
	IndexesToShow []sql.Index
}

// NewShowIndexes creates a new ShowIndexes node. The node must represent a table.
func NewShowIndexes(table sql.Node) sql.Node {
	return &ShowIndexes{
		UnaryNode: UnaryNode{table},
	}
}

var _ sql.Node = (*ShowIndexes)(nil)

// WithChildren implements the Node interface.
func (n *ShowIndexes) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	return &ShowIndexes{
		UnaryNode:     UnaryNode{children[0]},
		IndexesToShow: n.IndexesToShow,
	}, nil
}

// String implements the fmt.Stringer interface.
func (n *ShowIndexes) String() string {
	return fmt.Sprintf("ShowIndexes(%s)", n.Child)
}

// Schema implements the Node interface.
func (n *ShowIndexes) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.LongText},
		&sql.Column{Name: "Non_unique", Type: sql.Int32},
		&sql.Column{Name: "Key_name", Type: sql.LongText},
		&sql.Column{Name: "Seq_in_index", Type: sql.Int32},
		&sql.Column{Name: "Column_name", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Collation", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Cardinality", Type: sql.Int64},
		&sql.Column{Name: "Sub_part", Type: sql.Int64, Nullable: true},
		&sql.Column{Name: "Packed", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Null", Type: sql.LongText},
		&sql.Column{Name: "Index_type", Type: sql.LongText},
		&sql.Column{Name: "Comment", Type: sql.LongText},
		&sql.Column{Name: "Index_comment", Type: sql.LongText},
		&sql.Column{Name: "Visible", Type: sql.LongText},
		&sql.Column{Name: "Expression", Type: sql.LongText, Nullable: true},
	}
}

// RowIter implements the Node interface.
func (n *ShowIndexes) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, ok := n.Child.(*ResolvedTable)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T", n.Child))
	}

	return &showIndexesIter{
		table: table,
		idxs:  newIndexesToShow(n.IndexesToShow),
		ctx:   ctx,
	}, nil
}

func newIndexesToShow(indexes []sql.Index) *indexesToShow {
	return &indexesToShow{
		indexes: indexes,
	}
}

type showIndexesIter struct {
	table *ResolvedTable
	idxs  *indexesToShow
	ctx   *sql.Context
}

func (i *showIndexesIter) Next() (sql.Row, error) {
	show, err := i.idxs.next()
	if err != nil {
		return nil, err
	}

	var expression, columnName interface{}
	columnName, expression = nil, show.expression
	tbl := i.table

	if err != nil {
		return nil, err
	}

	nullable := ""
	if col := GetColumnFromIndexExpr(show.expression, tbl); col != nil {
		columnName, expression = col.Name, nil
		if col.Nullable {
			nullable = "YES"
		}
	}

	visible := "YES"
	if x, ok := show.index.(sql.DriverIndex); ok && len(x.Driver()) > 0 {
		if !i.ctx.GetIndexRegistry().CanUseIndex(x) {
			visible = "NO"
		}
	}

	nonUnique := 0
	if !show.index.IsUnique() {
		nonUnique = 1
	}

	return sql.NewRow(
		show.index.Table(),     // "Table" string
		nonUnique,              // "Non_unique" int32, Values [0, 1]
		show.index.ID(),        // "Key_name" string
		show.exPosition+1,      // "Seq_in_index" int32
		columnName,             // "Column_name" string
		nil,                    // "Collation" string, Values [A, D, NULL]
		int64(0),               // "Cardinality" int64 (not calculated)
		nil,                    // "Sub_part" int64
		nil,                    // "Packed" string
		nullable,               // "Null" string, Values [YES, '']
		show.index.IndexType(), // "Index_type" string
		show.index.Comment(),   // "Comment" string
		"",                     // "Index_comment" string
		visible,                // "Visible" string, Values [YES, NO]
		expression,             // "Expression" string
	), nil
}

// GetColumnFromIndexExpr returns column from the table given using the expression string given, in the form
// "table.column". Returns nil if the expression doesn't represent a column.
func GetColumnFromIndexExpr(expr string, table sql.Table) *sql.Column {
	for _, col := range table.Schema() {
		if col.Source+"."+col.Name == expr {
			return col
		}
	}

	return nil
}

func (i *showIndexesIter) Close(*sql.Context) error {
	return nil
}

type indexesToShow struct {
	indexes []sql.Index
	pos     int
	epos    int
}

type idxToShow struct {
	index      sql.Index
	expression string
	exPosition int
}

func (i *indexesToShow) next() (*idxToShow, error) {
	if i.pos >= len(i.indexes) {
		return nil, io.EOF
	}

	index := i.indexes[i.pos]
	expressions := index.Expressions()
	if i.epos >= len(expressions) {
		i.pos++
		if i.pos >= len(i.indexes) {
			return nil, io.EOF
		}

		index = i.indexes[i.pos]
		i.epos = 0
		expressions = index.Expressions()
	}

	show := &idxToShow{
		index:      index,
		expression: expressions[i.epos],
		exPosition: i.epos,
	}

	i.epos++
	return show, nil
}
