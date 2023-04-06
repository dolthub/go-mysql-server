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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var ErrDeleteFromNotSupported = errors.NewKind("table doesn't support DELETE FROM")

// DeleteFrom is a node describing a deletion from some table.
type DeleteFrom struct {
	UnaryNode
	// targets are the explicitly specified table nodes from which rows should be deleted. For simple DELETES against a
	// single source table, targets do NOT need to be explicitly specified and will not be set here. For DELETE FROM JOIN
	// statements, targets MUST be explicitly specified by the user and will be populated here.
	explicitTargets []sql.Node
}

var _ sql.Databaseable = (*DeleteFrom)(nil)
var _ sql.Node = (*DeleteFrom)(nil)
var _ sql.CollationCoercible = (*DeleteFrom)(nil)

// NewDeleteFrom creates a DeleteFrom node.
func NewDeleteFrom(n sql.Node, targets []sql.Node) *DeleteFrom {
	return &DeleteFrom{
		UnaryNode:       UnaryNode{n},
		explicitTargets: targets,
	}
}

// HasExplicitTargets returns true if the target delete tables were explicitly specified. This can only happen with
// DELETE FROM JOIN statements – for DELETE FROM statements using a single source table, the target is NOT explicitly
// specified and is assumed to be the single source table.
func (p *DeleteFrom) HasExplicitTargets() bool {
	return len(p.explicitTargets) > 0
}

// WithExplicitTargets returns a new DeleteFrom node instance with the specified |targets| set as the explicitly
// specified targets of the delete operation.
func (p *DeleteFrom) WithExplicitTargets(targets []sql.Node) *DeleteFrom {
	copy := *p
	copy.explicitTargets = targets
	return &copy
}

// GetDeleteTargets returns the sql.Nodes representing the tables from which rows should be deleted. For a DELETE FROM
// JOIN statement, this will return the tables explicitly specified by the caller. For a DELETE FROM statement this will
// return the single table in the DELETE FROM source that is implicitly assumed to be the target of the delete operation.
func (p *DeleteFrom) GetDeleteTargets() []sql.Node {
	if len(p.explicitTargets) == 0 {
		return []sql.Node{p.Child}
	} else {
		return p.explicitTargets
	}
}

func GetDeletable(node sql.Node) (sql.DeletableTable, error) {
	switch node := node.(type) {
	case sql.DeletableTable:
		return node, nil
	case *IndexedTableAccess:
		return GetDeletable(node.ResolvedTable)
	case *ResolvedTable:
		return getDeletableTable(node.Table)
	case *TableAlias:
		return GetDeletable(node.Child)
	case *SubqueryAlias:
		return nil, ErrDeleteFromNotSupported.New()
	case *TriggerExecutor:
		return GetDeletable(node.Left())
	case sql.TableWrapper:
		return getDeletableTable(node.Underlying())
	case *JSONTable:
		return nil, fmt.Errorf("target table %s of the DELETE is not updatable", node.Name())
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

// Resolved implements the sql.Resolvable interface.
func (p *DeleteFrom) Resolved() bool {
	if p.Child.Resolved() == false {
		return false
	}

	for _, target := range p.explicitTargets {
		if target.Resolved() == false {
			return false
		}
	}

	return true
}

func (p *DeleteFrom) Database() string {
	return deleteDatabaseHelper(p.Child)
}

// RowIter implements the Node interface.
func (p *DeleteFrom) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// If an empty table is passed in (potentially from a bad filter) return an empty row iter.
	// Note: emptyTable could also implement sql.DetetableTable
	if _, ok := p.Child.(*EmptyTable); ok {
		return sql.RowsToRowIter(), nil
	}

	iter, err := p.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	targets := p.GetDeleteTargets()
	schemaPositionDeleters := make([]schemaPositionDeleter, len(targets))
	for i, target := range targets {
		deletable, err := GetDeletable(target)
		if err != nil {
			return nil, err
		}
		deleter := deletable.Deleter(ctx)

		// By default the sourceName in the schema is the table name, but if there is a
		// table alias applied, then use that instead.
		sourceName := deletable.Name()
		transform.Inspect(target, func(node sql.Node) bool {
			if tableAlias, ok := node.(*TableAlias); ok {
				sourceName = tableAlias.Name()
				return false
			}
			return true
		})

		start, end, err := findSourcePosition(p.Child.Schema(), sourceName)
		if err != nil {
			return nil, err
		}
		schemaPositionDeleters[i] = schemaPositionDeleter{deleter, int(start), int(end)}
	}
	return newDeleteIter(iter, p.Child.Schema(), schemaPositionDeleters...), nil
}

// schemaPositionDeleter contains a sql.RowDeleter and the start (inclusive) and end (exclusive) position
// within a schema that indicate the portion of the schema that is associated with this specific deleter.
type schemaPositionDeleter struct {
	deleter     sql.RowDeleter
	schemaStart int
	schemaEnd   int
}

// findSourcePosition searches the specified |schema| for the first group of columns whose source is |name|,
// and returns the start position of that source in the schema (inclusive) and the end position (exclusive).
// If any problems were an encountered, such as not finding any columns from the specified source name,
// an error is returned.
func findSourcePosition(schema sql.Schema, name string) (uint, uint, error) {
	foundStart := false
	name = strings.ToLower(name)
	var start uint
	for i, col := range schema {
		if strings.ToLower(col.Source) == name {
			if !foundStart {
				start = uint(i)
				foundStart = true
			}
		} else {
			if foundStart {
				return start, uint(i), nil
			}
		}
	}
	if foundStart {
		return start, uint(len(schema)), nil
	}

	return 0, 0, fmt.Errorf("unable to find any columns in schema from source %q", name)
}

// deleteIter executes the DELETE FROM logic to delete rows from tables as they flow through the iterator. For every
// table the deleteIter needs to delete rows from, it needs a schemaPositionDeleter that provides the RowDeleter
// interface as well as start and end position for that table's full row in the row this iterator consumes from its
// child. For simple DELETE FROM statements deleting from a single table, this will likely be the full row contents,
// but in more complex scenarios when there are columns contributed by outer scopes and for DELETE FROM JOIN statements
// the child iterator will return a row that is composed of rows from multiple table sources.
type deleteIter struct {
	deleters  []schemaPositionDeleter
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

	// For each target table from which we are deleting rows, reduce the row from our child iterator to just
	// the columns that are part of that target table. This means looking at the position in the schema for
	// the target table and also removing any prepended columns contributed by outer scopes.
	fullSchemaLength := len(d.schema)
	rowLength := len(row)
	for _, deleter := range d.deleters {
		schemaLength := deleter.schemaEnd - deleter.schemaStart
		subSlice := row
		if schemaLength < rowLength {
			subSlice = row[(rowLength - fullSchemaLength + deleter.schemaStart):(rowLength - fullSchemaLength + deleter.schemaEnd)]
		}
		err = deleter.deleter.Delete(ctx, subSlice)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (d *deleteIter) Close(ctx *sql.Context) error {
	if !d.closed {
		d.closed = true
		var firstErr error
		// Make sure we close all the deleters and the childIter, and track the first
		// error seen so we can return it after safely closing all resources.
		for _, deleter := range d.deleters {
			err := deleter.deleter.Close(ctx)
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		err := d.childIter.Close(ctx)

		if firstErr != nil {
			return firstErr
		} else {
			return err
		}
	}
	return nil
}

func newDeleteIter(childIter sql.RowIter, schema sql.Schema, deleters ...schemaPositionDeleter) sql.RowIter {
	openerClosers := make([]sql.EditOpenerCloser, len(deleters))
	for i, ds := range deleters {
		openerClosers[i] = ds.deleter
	}
	return NewTableEditorIter(&deleteIter{
		deleters:  deleters,
		childIter: childIter,
		schema:    schema,
	}, openerClosers...)
}

// WithChildren implements the Node interface.
func (p *DeleteFrom) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewDeleteFrom(children[0], p.explicitTargets), nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *DeleteFrom) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// TODO: If column values are retrieved then the SELECT privilege is required
	//       For example: "DELETE FROM table WHERE z > 0"
	//       We would need SELECT privileges on the "z" column as it's retrieving values

	for _, target := range p.GetDeleteTargets() {
		deletable, err := GetDeletable(target)
		if err != nil {
			ctx.GetLogger().Warnf("unable to determine deletable table from delete target: %v", target)
			return false
		}
		op := sql.NewPrivilegedOperation(p.Database(), deletable.Name(), "", sql.PrivilegeType_Delete)
		if opChecker.UserHasPrivileges(ctx, op) == false {
			return false
		}
	}

	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DeleteFrom) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (p *DeleteFrom) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *DeleteFrom) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}
