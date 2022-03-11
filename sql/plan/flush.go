// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

// FlushPrivileges reads privileges from grant tables and registers any unregistered privileges found.
type FlushPrivileges struct {
	writesToBinlog bool
	grantTables    sql.Database
}

var _ sql.Node = (*FlushPrivileges)(nil)
var _ sql.Databaser = (*FlushPrivileges)(nil)

// NewFlushPrivileges creates a new FlushPrivileges node.
func NewFlushPrivileges(ft bool) *FlushPrivileges {
	return &FlushPrivileges{
		writesToBinlog: ft,
		grantTables:    sql.UnresolvedDatabase("mysql"),
	}
}

// RowIter implements the interface sql.Node.
func (f *FlushPrivileges) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	gts, ok := f.grantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	err := gts.Persist(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// String implements the interface sql.Node.
func (*FlushPrivileges) String() string { return "FLUSH PRIVILEGES" }

// WithChildren implements the interface sql.Node.
func (f *FlushPrivileges) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}

	return f, nil
}

// CheckPrivileges implements the interface sql.Node.
func (f *FlushPrivileges) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("mysql", "", "", sql.PrivilegeType_Reload)) {
		return true
	}
	return false
}

// Resolved implements the interface sql.Node.
func (f *FlushPrivileges) Resolved() bool {
	_, ok := f.grantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*FlushPrivileges) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*FlushPrivileges) Schema() sql.Schema { return sql.OkResultSchema }

// Database implements the sql.Databaser interface.
func (f *FlushPrivileges) Database() sql.Database {
	return f.grantTables
}

// WithDatabase implements the sql.Databaser interface.
func (f *FlushPrivileges) WithDatabase(db sql.Database) (sql.Node, error) {
	fp := *f
	fp.grantTables = db
	return &fp, nil
}
