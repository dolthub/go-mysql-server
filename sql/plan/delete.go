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
	"gopkg.in/src-d/go-errors.v1"
	"strings"

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

// Validate checks for invalid settings, such as deleting from multiple databases, specifying a delete target table
// multiple times, or using a DELETE FROM JOIN without specifying any explicit delete target tables, and returns an
// error if any validation issues were detected.
func (p *DeleteFrom) Validate() error {
	// Check that delete from join only targets tables that exist in the join
	if p.HasExplicitTargets() {
		sourceTables := make(map[string]struct{})
		transform.Inspect(p.Child, func(node sql.Node) bool {
			if rt, ok := node.(*ResolvedTable); ok {
				sourceTables[rt.Name()] = struct{}{}
			}
			return true
		})

		for _, target := range p.GetDeleteTargets() {
			tableName := getTableName(target)
			if _, ok := sourceTables[tableName]; !ok {
				return fmt.Errorf("table %q not found in DELETE FROM sources", tableName)
			}
		}
	}

	// Duplicate explicit target tables or from explicit target tables from multiple databases
	databases := make(map[string]struct{})
	tables := make(map[string]struct{})
	if p.HasExplicitTargets() {
		for _, target := range p.GetDeleteTargets() {
			// Check for multiple databases
			if dber, ok := target.(sql.Databaseable); ok {
				databases[dber.Database()] = struct{}{}
			} else if rt, ok := target.(*ResolvedTable); ok {
				databases[rt.Database.Name()] = struct{}{}
			} else {
				return fmt.Errorf("could not determine database for node of type: %T", target)
			}
			if len(databases) > 1 {
				return fmt.Errorf("multiple databases specified as delete from targets")
			}

			// Check for duplicate targets
			if nameable, ok := target.(sql.Nameable); ok {
				if _, ok := tables[nameable.Name()]; ok {
					return fmt.Errorf("duplicate tables specified as delete from targets")
				}
			} else {
				return fmt.Errorf("target node does not implement sql.Nameable: %T", target)
			}
		}
	}

	// DELETE FROM JOIN with no target tables specified
	deleteFromJoin := false
	transform.Inspect(p.Child, func(node sql.Node) bool {
		if _, ok := node.(*JoinNode); ok {
			deleteFromJoin = true
			return false
		}
		return true
	})
	if deleteFromJoin {
		if len(p.explicitTargets) == 0 {
			return fmt.Errorf("delete from statement with join requires specifying explicit delete target tables")
		}
	}

	return nil
}

// RowIter implements the Node interface.
func (p *DeleteFrom) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Validate()
	if err != nil {
		return nil, err
	}

	// If an empty table is passed in (potentially from a bad filter) return an empty row iter.
	// Note: emptyTable could also implement sql.DetetableTable
	if _, ok := p.Child.(*emptyTable); ok {
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
		start, end, err := findSourcePosition(p.Child.Schema(), deletable.Name())
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
		for _, deleter := range d.deleters {
			if err := deleter.deleter.Close(ctx); err != nil {
				return err
			}
		}
		return d.childIter.Close(ctx)
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
	//TODO: If column values are retrieved then the SELECT privilege is required
	// For example: "DELETE FROM table WHERE z > 0"
	// We would need SELECT privileges on the "z" column as it's retrieving values

	for _, target := range p.GetDeleteTargets() {
		tableName := getTableName(target)
		op := sql.NewPrivilegedOperation(p.Database(), tableName, "", sql.PrivilegeType_Delete)
		if opChecker.UserHasPrivileges(ctx, op) == false {
			return false
		}
	}

	return true
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
