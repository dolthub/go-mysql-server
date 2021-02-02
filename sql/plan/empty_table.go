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

// EmptyTable is a node representing an empty table.
var EmptyTable = new(emptyTable)

type emptyTable struct{}

func (emptyTable) Schema() sql.Schema   { return nil }
func (emptyTable) Children() []sql.Node { return nil }
func (emptyTable) Resolved() bool       { return true }
func (e *emptyTable) String() string    { return "EmptyTable" }

func (emptyTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (e *emptyTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 0)
	}

	return e, nil
}
