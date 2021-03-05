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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	// ErrIndexActionNotImplemented is returned when the action has not been implemented
	ErrIndexActionNotImplemented = errors.NewKind("alter table index action is not implemented: %v")
	// ErrCreateIndexMissingColumns is returned when a CREATE INDEX statement does not provide any columns
	ErrCreateIndexMissingColumns = errors.NewKind("cannot create an index without columns")
	// ErrCreateIndexNonExistentColumn is returned when a key is provided in the index that isn't in the table
	ErrCreateIndexNonExistentColumn = errors.NewKind("column `%v` does not exist in the table")
	// ErrCreateIndexDuplicateColumn is returned when a CREATE INDEX statement has the same column multiple times
	ErrCreateIndexDuplicateColumn = errors.NewKind("cannot have duplicates of columns in an index: `%v`")
)

type IndexAction byte

const (
	IndexAction_Create IndexAction = iota
	IndexAction_Drop
	IndexAction_Rename
)

type AlterIndex struct {
	// Action states whether it's a CREATE, DROP, or RENAME
	Action IndexAction
	// Table is the table that is being referenced
	Table sql.Node
	// IndexName is the index name, and in the case of a RENAME it represents the new name
	IndexName string
	// PreviousIndexName states the old name when renaming an index
	PreviousIndexName string
	// Using states whether you're using BTREE, HASH, or none
	Using sql.IndexUsing
	// Constraint specifies whether this is UNIQUE, FULLTEXT, SPATIAL, or none
	Constraint sql.IndexConstraint
	// Columns contains the column names (and possibly lengths) when creating an index
	Columns []sql.IndexColumn
	// Comment is the comment that was left at index creation, if any
	Comment string
}

func NewAlterCreateIndex(table sql.Node, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) *AlterIndex {
	return &AlterIndex{
		Action:     IndexAction_Create,
		Table:      table,
		IndexName:  indexName,
		Using:      using,
		Constraint: constraint,
		Columns:    columns,
		Comment:    comment,
	}
}

func NewAlterDropIndex(table sql.Node, indexName string) *AlterIndex {
	return &AlterIndex{
		Action:    IndexAction_Drop,
		Table:     table,
		IndexName: indexName,
	}
}

func NewAlterRenameIndex(table sql.Node, fromIndexName, toIndexName string) *AlterIndex {
	return &AlterIndex{
		Action:            IndexAction_Rename,
		Table:             table,
		IndexName:         toIndexName,
		PreviousIndexName: fromIndexName,
	}
}

// Schema implements the Node interface.
func (p *AlterIndex) Schema() sql.Schema {
	return nil
}

func getIndexAlterable(node sql.Node) (sql.IndexAlterableTable, error) {
	switch node := node.(type) {
	case sql.IndexAlterableTable:
		return node, nil
	case *ResolvedTable:
		return getIndexAlterableTable(node.Table)
	case sql.TableWrapper:
		return getIndexAlterableTable(node.Underlying())
	default:
		return nil, ErrNotIndexable.New()
	}
}

func getIndexAlterableTable(t sql.Table) (sql.IndexAlterableTable, error) {
	switch t := t.(type) {
	case sql.IndexAlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getIndexAlterableTable(t.Underlying())
	default:
		return nil, ErrNotIndexable.New()
	}
}

// Execute inserts the rows in the database.
func (p *AlterIndex) Execute(ctx *sql.Context) error {
	indexable, err := getIndexAlterable(p.Table)
	if err != nil {
		return err
	}

	switch p.Action {
	case IndexAction_Create:
		if len(p.Columns) == 0 {
			return ErrCreateIndexMissingColumns.New()
		}

		// Make sure that all columns are valid, in the table, and there are no duplicates
		seenCols := make(map[string]bool)
		for _, col := range indexable.Schema() {
			seenCols[col.Name] = false
		}
		for _, indexCol := range p.Columns {
			if seen, ok := seenCols[indexCol.Name]; ok {
				if !seen {
					seenCols[indexCol.Name] = true
				} else {
					return ErrCreateIndexDuplicateColumn.New(indexCol.Name)
				}
			} else {
				return ErrCreateIndexNonExistentColumn.New(indexCol.Name)
			}
		}

		return indexable.CreateIndex(ctx, p.IndexName, p.Using, p.Constraint, p.Columns, p.Comment)
	case IndexAction_Drop:
		return indexable.DropIndex(ctx, p.IndexName)
	case IndexAction_Rename:
		return indexable.RenameIndex(ctx, p.PreviousIndexName, p.IndexName)
	default:
		return ErrIndexActionNotImplemented.New(p.Action)
	}
}

// RowIter implements the Node interface.
func (p *AlterIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *AlterIndex) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	switch p.Action {
	case IndexAction_Create:
		return NewAlterCreateIndex(children[0], p.IndexName, p.Using, p.Constraint, p.Columns, p.Comment), nil
	case IndexAction_Drop:
		return NewAlterDropIndex(children[0], p.IndexName), nil
	case IndexAction_Rename:
		return NewAlterRenameIndex(children[0], p.PreviousIndexName, p.IndexName), nil
	default:
		return nil, ErrIndexActionNotImplemented.New(p.Action)
	}
}

func (p AlterIndex) String() string {
	pr := sql.NewTreePrinter()
	switch p.Action {
	case IndexAction_Create:
		_ = pr.WriteNode("CreateIndex(%s)", p.IndexName)
		children := []string{fmt.Sprintf("Table(%s)", p.Table.String())}
		switch p.Constraint {
		case sql.IndexConstraint_Unique:
			children = append(children, "Constraint(UNIQUE)")
		case sql.IndexConstraint_Spatial:
			children = append(children, "Constraint(SPATIAL)")
		case sql.IndexConstraint_Fulltext:
			children = append(children, "Constraint(FULLTEXT)")
		}
		switch p.Using {
		case sql.IndexUsing_BTree, sql.IndexUsing_Default:
			children = append(children, "Using(BTREE)")
		case sql.IndexUsing_Hash:
			children = append(children, "Using(HASH)")
		}
		cols := make([]string, len(p.Columns))
		for i, col := range p.Columns {
			if col.Length == 0 {
				cols[i] = col.Name
			} else {
				cols[i] = fmt.Sprintf("%s(%v)", col.Name, col.Length)
			}
		}
		children = append(children, fmt.Sprintf("Columns(%s)", strings.Join(cols, ", ")))
		children = append(children, fmt.Sprintf("Comment(%s)", p.Comment))
		_ = pr.WriteChildren(children...)
	case IndexAction_Drop:
		_ = pr.WriteNode("DropIndex(%s)", p.IndexName)
		_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.Table.String()))
	case IndexAction_Rename:
		_ = pr.WriteNode("RenameIndex")
		_ = pr.WriteChildren(
			fmt.Sprintf("Table(%s)", p.Table.String()),
			fmt.Sprintf("FromIndex(%s)", p.PreviousIndexName),
			fmt.Sprintf("ToIndex(%s)", p.IndexName),
		)
	default:
		_ = pr.WriteNode("Unknown_Index_Action(%v)", p.Action)
	}
	return pr.String()
}

func (p *AlterIndex) Resolved() bool {
	return p.Table.Resolved()
}

func (p *AlterIndex) Children() []sql.Node {
	return []sql.Node{p.Table}
}
