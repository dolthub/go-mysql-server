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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var ErrDeleteFromNotSupported = errors.NewKind("table doesn't support DELETE FROM")

// DeleteFrom is a node describing a deletion from some table.
type DeleteFrom struct {
	UnaryNode
}

var _ sql.Databaseable = (*DeleteFrom)(nil)

// NewDeleteFrom creates a DeleteFrom node.
func NewDeleteFrom(n sql.Node) *DeleteFrom {
	return &DeleteFrom{UnaryNode{n}}
}

func GetDeletable(node sql.Node) (sql.DeletableTable, error) {
	switch node := node.(type) {
	case sql.DeletableTable:
		return node, nil
	case *IndexedTableAccess:
		return GetDeletable(node.ResolvedTable)
	case *ResolvedTable:
		return getDeletableTable(node.Table)
	case *SubqueryAlias:
		return nil, ErrDeleteFromNotSupported.New()
	case *TriggerExecutor:
		return GetDeletable(node.Left())
	case sql.TableWrapper:
		return getDeletableTable(node.Underlying())
	}
	if len(node.Children()) > 1 {
		return nil, ErrDeleteFromNotSupported.New()
	}
	for _, child := range node.Children() {
		deleter, _ := GetDeletable(child)
		if deleter != nil {
			return deleter, nil
		}
	}
	return nil, ErrDeleteFromNotSupported.New()
}

func getDeletableTable(t sql.Table) (sql.DeletableTable, error) {
	switch t := t.(type) {
	case sql.DeletableTable:
		return t, nil
	case sql.TableWrapper:
		return getDeletableTable(t.Underlying())
	default:
		return nil, ErrDeleteFromNotSupported.New()
	}
}

func deleteDatabaseHelper(node sql.Node) string {
	switch node := node.(type) {
	case sql.DeletableTable:
		return ""
	case *IndexedTableAccess:
		return deleteDatabaseHelper(node.ResolvedTable)
	case *ResolvedTable:
		return node.Database.Name()
	case *UnresolvedTable:
		return node.Database()
	}

	for _, child := range node.Children() {
		return deleteDatabaseHelper(child)
	}

	return ""
}

func (p *DeleteFrom) Database() string {
	return deleteDatabaseHelper(p.Child)
}

// RowIter implements the Node interface.
func (p *DeleteFrom) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// If an empty table is passed in (potentially from a bad filter) return an empty row iter.
	// Note: emptyTable could also implement sql.DetetableTable
	if _, ok := p.Child.(*emptyTable); ok {
		return sql.RowsToRowIter(), nil
	}

	deletable, err := GetDeletable(p.Child)
	if err != nil {
		return nil, err
	}

	iter, err := p.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	deleter := deletable.Deleter(ctx)

	return newDeleteIter(iter, deleter, deletable.Schema()), nil
}

type deleteIter struct {
	deleter   sql.RowDeleter
	schema    sql.Schema
	childIter sql.RowIter
	closed    bool
}

func (d *deleteIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := d.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Reduce the row to the length of the schema. The length can differ when some update values come from an outer
	// scope, which will be the first N values in the row.
	// TODO: handle this in the analyzer instead?
	if len(d.schema) < len(row) {
		row = row[len(row)-len(d.schema):]
	}

	return row, d.deleter.Delete(ctx, row)
}

func (d *deleteIter) Close(ctx *sql.Context) error {
	if !d.closed {
		d.closed = true
		if err := d.deleter.Close(ctx); err != nil {
			return err
		}
		return d.childIter.Close(ctx)
	}
	return nil
}

func newDeleteIter(childIter sql.RowIter, deleter sql.RowDeleter, schema sql.Schema) sql.RowIter {
	return NewTableEditorIter(deleter, &deleteIter{
		deleter:   deleter,
		childIter: childIter,
		schema:    schema,
	})
}

// WithChildren implements the Node interface.
func (p *DeleteFrom) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewDeleteFrom(children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *DeleteFrom) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: If column values are retrieved then the SELECT privilege is required
	// For example: "DELETE FROM table WHERE z > 0"
	// We would need SELECT privileges on the "z" column as it's retrieving values
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(p.Database(), getTableName(p.Child), "", sql.PrivilegeType_Delete))
}

func (p DeleteFrom) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p DeleteFrom) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}
