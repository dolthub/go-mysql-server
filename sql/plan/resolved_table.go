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

	"github.com/dolthub/go-mysql-server/memory"

	"github.com/dolthub/go-mysql-server/sql"
)

// ResolvedTable represents a resolved SQL Table.
type ResolvedTable struct {
	sql.Table
	Database sql.Database
	AsOf     interface{}
}

var _ sql.Node = (*ResolvedTable)(nil)
var _ sql.Node2 = (*ResolvedTable)(nil)
var _ sql.RenameableNode = (*ResolvedTable)(nil)

// Can't embed Table2 like we do Table1 as it's an extension not everyone implements
var _ sql.Table2 = (*ResolvedTable)(nil)

// NewResolvedTable creates a new instance of ResolvedTable.
func NewResolvedTable(table sql.Table, db sql.Database, asOf interface{}) *ResolvedTable {
	return &ResolvedTable{Table: table, Database: db, AsOf: asOf}
}

// NewResolvedDualTable creates a new instance of ResolvedTable.
func NewResolvedDualTable() *ResolvedTable {
	return &ResolvedTable{Table: NewDualSqlTable(), Database: memory.NewDatabase(""), AsOf: nil}
}

func (t *ResolvedTable) WithName(s string) sql.Node {
	return NewTableAlias(s, t)
}

// Resolved implements the Resolvable interface.
func (*ResolvedTable) Resolved() bool {
	return true
}

func (t *ResolvedTable) String() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("Table")
	table := seethroughTableWrapper(t)
	children := []string{fmt.Sprintf("name: %s", t.Name())}

	if pt, ok := table.(sql.ProjectedTable); ok {
		projections := pt.Projections()
		if projections != nil {
			columns := make([]string, len(projections))
			for i, c := range projections {
				columns[i] = strings.ToLower(c)
			}
			children = append(children, fmt.Sprintf("columns: %v", columns))
		}
	}

	if ft, ok := table.(sql.FilteredTable); ok {
		var filters []string
		for _, f := range ft.Filters() {
			filters = append(filters, f.String())
		}
		if len(filters) > 0 {
			children = append(children, fmt.Sprintf("filters: %v", filters))
		}
	}

	pr.WriteChildren(children...)
	return pr.String()
}

func (t *ResolvedTable) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("Table")
	table := seethroughTableWrapper(t)
	children := []string{fmt.Sprintf("name: %s", t.Name())}

	var columns []string
	if pt, ok := table.(sql.ProjectedTable); ok && pt.Projections() != nil {
		projections := pt.Projections()
		columns = make([]string, len(projections))
		for i, c := range projections {
			columns[i] = strings.ToLower(c)
		}
	} else {
		columns = make([]string, len(table.Schema()))
		for i, c := range table.Schema() {
			columns[i] = strings.ToLower(c.Name)
		}
	}
	children = append(children, fmt.Sprintf("columns: %v", columns))

	if ft, ok := table.(sql.FilteredTable); ok {
		var filters []string
		for _, f := range ft.Filters() {
			filters = append(filters, f.String())
		}
		if len(filters) > 0 {
			children = append(children, fmt.Sprintf("filters: %v", filters))
		}
	}

	pr.WriteChildren(children...)
	return pr.String()
}

// Children implements the Node interface.
func (*ResolvedTable) Children() []sql.Node { return nil }

// RowIter implements the RowIter interface.
func (t *ResolvedTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.ResolvedTable")

	partitions, err := t.Table.Partitions(ctx)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, sql.NewTableRowIter(ctx, t.Table, partitions)), nil
}

func (t *ResolvedTable) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	span, ctx := ctx.Span("plan.ResolvedTable")

	partitions, err := t.Table.Partitions(ctx)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, sql.NewTableRowIter(ctx, t.Table, partitions)).(sql.RowIter2), nil
}

// PartitionRows2 implements sql.Table2. sql.Table methods are embedded in the type.
func (t *ResolvedTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	return t.Table.(sql.Table2).PartitionRows2(ctx, part)
}

// WithChildren implements the Node interface.
func (t *ResolvedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 0)
	}

	return t, nil
}

// CheckPrivileges implements the interface sql.Node.
func (t *ResolvedTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// It is assumed that if we've landed upon this node, then we're doing a SELECT operation. Most other nodes that
	// may contain a ResolvedTable will have their own privilege checks, so we should only end up here if the parent
	// nodes are things such as indexed access, filters, limits, etc.
	if IsDualTable(t) {
		return true
	}

	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(t.Database.Name(), t.Table.Name(), "", sql.PrivilegeType_Select))
}

// WithTable returns this Node with the given table. The new table should have the same name as the previous table.
func (t *ResolvedTable) WithTable(table sql.Table) (*ResolvedTable, error) {
	if t.Name() != table.Name() {
		return nil, fmt.Errorf("attempted to update ResolvedTable `%s` with table `%s`", t.Name(), table.Name())
	}
	nt := *t
	nt.Table = table
	return &nt, nil
}

func seethroughTableWrapper(n *ResolvedTable) sql.Table {
	if tw, ok := n.Table.(sql.TableWrapper); ok {
		return tw.Underlying()
	} else {
		return n.Table
	}
}
