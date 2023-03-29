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

import "github.com/dolthub/go-mysql-server/sql"

func IsEmptyTable(n sql.Node) bool {
	_, ok := n.(*EmptyTable)
	return ok
}
func NewEmptyTableWithSchema(schema sql.Schema) sql.Node {
	return &EmptyTable{schema: schema}
}

var _ sql.Node = (*EmptyTable)(nil)
var _ sql.CollationCoercible = (*EmptyTable)(nil)

type EmptyTable struct {
	schema sql.Schema
}

func (e *EmptyTable) Name() string       { return "__emptytable" }
func (e *EmptyTable) Schema() sql.Schema { return e.schema }
func (*EmptyTable) Children() []sql.Node { return nil }
func (*EmptyTable) Resolved() bool       { return true }
func (e *EmptyTable) String() string     { return "EmptyTable" }

func (*EmptyTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (e *EmptyTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 0)
	}

	return e, nil
}

// CheckPrivileges implements the interface sql.Node.
func (e *EmptyTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*EmptyTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}
