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

// Nothing is a node that will return no rows.
var Nothing nothing

type nothing struct{}

func (nothing) String() string       { return "NOTHING" }
func (nothing) Resolved() bool       { return true }
func (nothing) Schema() sql.Schema   { return nil }
func (nothing) Children() []sql.Node { return nil }
func (nothing) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (n nothing) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}

	return Nothing, nil
}
