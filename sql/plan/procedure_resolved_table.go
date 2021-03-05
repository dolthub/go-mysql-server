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
	"github.com/dolthub/go-mysql-server/sql"
)

// ProcedureResolvedTable represents a resolved SQL Table inside of a stored procedure. These are initially resolved to
// verify that they exist, and are then reloaded when another statement accesses its data. Some integrators return a
// snapshot of a table during the analysis step as an internal optimization, which is incompatible with stored
// procedures as they require the latest data at each statement.
type ProcedureResolvedTable struct {
	ResolvedTable *ResolvedTable
}

var _ sql.Node = (*ProcedureResolvedTable)(nil)
var _ sql.DebugStringer = (*ProcedureResolvedTable)(nil)
var _ sql.TableWrapper = (*ProcedureResolvedTable)(nil)
var _ sql.Table = (*ProcedureResolvedTable)(nil)

// NewProcedureResolvedTable returns a *ProcedureResolvedTable.
func NewProcedureResolvedTable(rt *ResolvedTable) *ProcedureResolvedTable {
	return &ProcedureResolvedTable{rt}
}

// Resolved implements the sql.Node interface.
func (t *ProcedureResolvedTable) Resolved() bool {
	return t.ResolvedTable.Resolved()
}

// String implements the sql.Node interface.
func (t *ProcedureResolvedTable) String() string {
	return t.ResolvedTable.String()
}

// Schema implements the sql.Node interface.
func (t *ProcedureResolvedTable) Schema() sql.Schema {
	return t.ResolvedTable.Schema()
}

// DebugString implements the sql.DebugStringer interface.
func (t *ProcedureResolvedTable) DebugString() string {
	return sql.DebugString(t.ResolvedTable)
}

// Children implements the sql.Node interface.
func (t *ProcedureResolvedTable) Children() []sql.Node {
	return []sql.Node{t.ResolvedTable}
}

// RowIter implements the sql.Node interface.
func (t *ProcedureResolvedTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rt, err := t.newestTable(ctx)
	if err != nil {
		return nil, err
	}
	return rt.RowIter(ctx, row)
}

// WithChildren implements the sql.Node interface.
func (t *ProcedureResolvedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	// Even though we return the *ResolvedTable in Children, we cannot assume that the given child is still
	// *ResolvedTable. In the analyzer, there are instances where the table is buried under other nodes such as
	// tracking nodes, so we must walk the tree and find the table.
	return TransformUp(children[0], func(n sql.Node) (sql.Node, error) {
		rt, ok := children[0].(*ResolvedTable)
		if !ok {
			return n, nil
		}
		return NewProcedureResolvedTable(rt), nil
	})
}

// Underlying implements the sql.TableWrapper interface.
func (t *ProcedureResolvedTable) Underlying() sql.Table {
	return t.ResolvedTable.Table
}

// Name implements the sql.Table interface.
func (t *ProcedureResolvedTable) Name() string {
	return t.ResolvedTable.Name()
}

// Partitions implements the sql.Table interface.
func (t *ProcedureResolvedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	rt, err := t.newestTable(ctx)
	if err != nil {
		return nil, err
	}
	return rt.Partitions(ctx)
}

// PartitionRows implements the sql.Table interface.
func (t *ProcedureResolvedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rt, err := t.newestTable(ctx)
	if err != nil {
		return nil, err
	}
	return rt.PartitionRows(ctx, partition)
}

// newestTable fetches the newest copy of the contained table from the database.
func (t *ProcedureResolvedTable) newestTable(ctx *sql.Context) (*ResolvedTable, error) {
	// If no database was given, such as with the "dual" table, then we return the given table as-is.
	if t.ResolvedTable.Database == nil {
		return t.ResolvedTable, nil
	}

	if t.ResolvedTable.AsOf == nil {
		tbl, ok, err := t.ResolvedTable.Database.GetTableInsensitive(ctx, t.ResolvedTable.Table.Name())
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, sql.ErrTableNotFound.New(t.ResolvedTable.Table.Name())
		}
		return t.ResolvedTable.WithTable(tbl)
	} else {
		versionedDb, ok := t.ResolvedTable.Database.(sql.VersionedDatabase)
		if !ok {
			return nil, sql.ErrAsOfNotSupported.New(t.ResolvedTable.Database.Name())
		}

		tbl, ok, err := versionedDb.GetTableInsensitiveAsOf(ctx, t.ResolvedTable.Table.Name(), t.ResolvedTable.AsOf)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, sql.ErrTableNotFound.New(t.ResolvedTable.Table.Name())
		}
		return t.ResolvedTable.WithTable(tbl)
	}
}
