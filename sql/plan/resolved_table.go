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
	SqlDatabase sql.Database
	AsOf        interface{}
	comment     string
}

// UnderlyingTable returns the table wrapped by the ResolvedTable.
func (t *ResolvedTable) UnderlyingTable() sql.Table {
	if w, ok := t.Table.(sql.TableWrapper); ok {
		return w.Underlying()
	}
	return t.Table
}

func (t *ResolvedTable) WrappedTable() sql.Table {
	return t.Table
}

func (t *ResolvedTable) Database() sql.Database {
	return t.SqlDatabase
}

func (t *ResolvedTable) WithDatabase(database sql.Database) (sql.Node, error) {
	newNode := *t
	t.SqlDatabase = database
	return &newNode, nil
}

var _ sql.Node = (*ResolvedTable)(nil)
var _ sql.TableNode = (*ResolvedTable)(nil)
var _ sql.Databaser = (*ResolvedTable)(nil)
var _ sql.CommentedNode = (*ResolvedTable)(nil)
var _ sql.RenameableNode = (*ResolvedTable)(nil)
var _ sql.CollationCoercible = (*ResolvedTable)(nil)
var _ sql.MutableTableNode = (*ResolvedTable)(nil)

// NewResolvedTable creates a new instance of ResolvedTable.
func NewResolvedTable(table sql.Table, db sql.Database, asOf interface{}) *ResolvedTable {
	return &ResolvedTable{Table: table, SqlDatabase: db, AsOf: asOf}
}

// NewResolvedDualTable creates a new instance of ResolvedTable.
func NewResolvedDualTable() *ResolvedTable {
	return &ResolvedTable{Table: NewDualSqlTable(), SqlDatabase: memory.NewDatabase(""), AsOf: nil}
}

func (t *ResolvedTable) WithComment(s string) sql.Node {
	ret := *t
	ret.comment = s
	return &ret
}

func (t *ResolvedTable) Comment() string {
	return t.comment
}

func (t *ResolvedTable) WithName(s string) sql.Node {
	return NewTableAlias(s, t)
}

// Resolved implements the Resolvable interface.
func (*ResolvedTable) Resolved() bool {
	return true
}

func (*ResolvedTable) IsReadOnly() bool {
	return true
}

func (t *ResolvedTable) String() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("Table")
	table := t.UnderlyingTable()
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
	table := t.Table
	// TableWrappers may want to print their own debug info
	if wrapper, ok := table.(sql.TableWrapper); ok {
		if ds, ok := wrapper.(sql.DebugStringer); ok {
			return sql.DebugString(ds)
		}
	}

	var additionalChildren []string
	if t.comment != "" {
		additionalChildren = []string{fmt.Sprintf("comment: %s", t.comment)}
	}

	return TableDebugString(table, additionalChildren...)
}

func TableDebugString(table sql.Table, additionalChildren ...string) string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("Table")
	children := []string{fmt.Sprintf("name: %s", table.Name())}

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

	pr.WriteChildren(append(children, additionalChildren...)...)
	return pr.String()
}

// Children implements the Node interface.
func (*ResolvedTable) Children() []sql.Node { return nil }

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
	// may contain a TableNode will have their own privilege checks, so we should only end up here if the parent
	// nodes are things such as indexed access, filters, limits, etc.
	if IsDualTable(t) {
		return true
	}

	db := t.SqlDatabase
	checkDbName := CheckPrivilegeNameForDatabase(db)

	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(checkDbName, t.Table.Name(), "", sql.PrivilegeType_Select))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ResolvedTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// WithTable returns this Node with the given table, re-wrapping it with any MutableTableWrapper that was
// wrapping it prior to this call.
func (t ResolvedTable) WithTable(table sql.Table) (sql.MutableTableNode, error) {
	if t.Name() != table.Name() {
		return nil, fmt.Errorf("attempted to update TableNode `%s` with table `%s`", t.Name(), table.Name())
	}

	if mtw, ok := t.Table.(sql.MutableTableWrapper); ok {
		t.Table = mtw.WithUnderlying(table)
	} else {
		t.Table = table
	}

	return &t, nil
}

// ReplaceTable returns this Node with the given table without performing any re-wrapping of any MutableTableWrapper
func (t ResolvedTable) ReplaceTable(table sql.Table) (sql.MutableTableNode, error) {
	if t.Name() != table.Name() {
		return nil, fmt.Errorf("attempted to update TableNode `%s` with table `%s`", t.Name(), table.Name())
	}

	t.Table = table
	return &t, nil
}
