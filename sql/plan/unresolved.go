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

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// ErrUnresolvedTable is thrown when a table cannot be resolved
var ErrUnresolvedTable = errors.NewKind("unresolved table")

// UnresolvedTable is a table that has not been resolved yet but whose name is known.
type UnresolvedTable struct {
	name     string
	Database string
	AsOf     sql.Expression
}

// NewUnresolvedTable creates a new Unresolved table.
func NewUnresolvedTable(name, db string) *UnresolvedTable {
	return &UnresolvedTable{name, db, nil}
}

// NewUnresolvedTableAsOf creates a new Unresolved table with an AS OF expression.
func NewUnresolvedTableAsOf(name, db string, asOf sql.Expression) *UnresolvedTable {
	return &UnresolvedTable{name, db, asOf}
}

var _ sql.Expressioner = (*UnresolvedTable)(nil)

// Name implements the Nameable interface.
func (t *UnresolvedTable) Name() string {
	return t.name
}

// Resolved implements the Resolvable interface.
func (*UnresolvedTable) Resolved() bool {
	return false
}

// Children implements the Node interface.
func (*UnresolvedTable) Children() []sql.Node { return nil }

// Schema implements the Node interface.
func (*UnresolvedTable) Schema() sql.Schema { return nil }

// RowIter implements the RowIter interface.
func (*UnresolvedTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, ErrUnresolvedTable.New()
}

// WithChildren implements the Node interface.
func (t *UnresolvedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 0)
	}

	return t, nil
}

// WithAsOf returns a copy of this unresolved table with its AsOf field set to the given value. Analagous to
// WithChildren. This type is the only Node that can take an AS OF expression, so this isn't an interface.
func (t *UnresolvedTable) WithAsOf(asOf sql.Expression) (*UnresolvedTable, error) {
	t2 := *t
	t2.AsOf = asOf
	return &t2, nil
}

// WithDatabase returns a copy of this unresolved table with its Database field set to the given value. Analagous to
// WithChildren.
func (t *UnresolvedTable) WithDatabase(database string) (*UnresolvedTable, error) {
	t2 := *t
	t2.Database = database
	return &t2, nil
}

func (t *UnresolvedTable) Expressions() []sql.Expression {
	if t.AsOf != nil {
		return []sql.Expression{t.AsOf}
	}
	return nil
}

func (t *UnresolvedTable) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if t.AsOf == nil {
		if len(expressions) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 0)
		}
		return t, nil
	}

	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 1)
	}

	return t.WithAsOf(expressions[0])
}

func (t UnresolvedTable) String() string {
	return fmt.Sprintf("UnresolvedTable(%s)", t.name)
}
